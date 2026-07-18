#!/usr/bin/env python3
"""Nightly feed-quality audit over the LIVE published feeds.

Runs the feed_specs checks (the same toolkit the checker GUI uses — imported
from this repo, so the two can never drift) against every non-paused feed's
published output XML and reports what FAVI would see.

Design contract (deliberate — do not "harden" this into a gate):
  * ADVISORY ONLY. Nothing is ever blocked, paused, or mutated. The checker
    is intentionally allowed to be stricter than FAVI itself.
  * Per-item issues are WARN-ONLY counts (missing GTIN, price format, ...).
    They appear in the report but can never fail the run.
  * The run fails (=> GitHub notifies) ONLY on NEW feed-level blockers vs the
    previous run's state: a feed that suddenly stops parsing, empties out,
    halves its item count, or breaks every price. A finding that exists run
    after run is background; a CHANGE is the signal — this holds even if the
    underlying check is stricter than FAVI.
  * First run (no previous state) never fails: it just establishes baseline.

Usage:
  python audit/audit_feeds.py --state state.json --report report.md \
      [--prev prev-state.json] [--limit N]

Needs: AWS credentials with dynamodb:Scan on feed_configs (feed list only —
feed CONTENT is fetched over the public CDN URLs, no cloud creds involved).
"""
from __future__ import annotations

import argparse
import gzip
import json
import os
import sys
import tempfile
from collections import Counter
from datetime import datetime, timezone
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))  # repo root

import requests
from defusedxml import ElementTree as DET

from feed_specs import (
    SPEC,
    analyze_price_text,
    expected_root_locals,
    gather_primary_image,
    is_valid_gtin,
    read_availability,
    read_id,
    read_link,
    read_price_text,
    read_recommended_value,
    strip_ns,
)

TABLE = os.environ.get("TABLE_NAME", "feed_configs")
REGION = os.environ.get("AWS_REGION", "eu-north-1")

# The pipeline publishes google-format XML for FAVI but ALSO other marketplace
# formats (Heureka, Ceneo, ...) for some partners — detect per feed from the
# root element, using SPEC itself so new formats need no audit changes.
# Ambiguous roots (e.g. "rss", "products") resolve google-first, then SPEC
# declaration order.
ROOT_TO_SPEC: dict[str, str] = {}
for _spec in sorted(SPEC, key=lambda s: not s.lower().startswith("google")):
    for _root in expected_root_locals(_spec):
        ROOT_TO_SPEC.setdefault(_root.lower(), _spec)


def _item_locals(spec_name: str) -> set[str]:
    cfg = SPEC[spec_name]
    paths = cfg.get("item_paths") or cfg.get("items") or cfg.get("item") or []
    if isinstance(paths, str):
        paths = [paths]
    locals_ = {strip_ns(p.rsplit("/", 1)[-1]).lower() for p in paths}
    return locals_ or {"item", "entry"}
MAX_BYTES = int(os.environ.get("AUDIT_MAX_FEED_MB", "900")) * 1024 * 1024
FETCH_TIMEOUT = (15, 180)
# Item-count crash detection: only meaningful for feeds with a real baseline.
DROP_MIN_BASELINE = 20
DROP_RATIO = 0.5

WARN_KEYS = (
    "missing_id", "missing_link", "missing_price", "missing_image",
    "missing_availability", "invalid_price_format", "overprecision_price",
    "invalid_gtin", "missing_gtin",
)


def list_feeds() -> list[dict]:
    import boto3
    ddb = boto3.client("dynamodb", region_name=REGION)
    feeds, kwargs = [], {
        "TableName": TABLE,
        "ProjectionExpression": "feedId, outputUrl, isPaused, shopName",
    }
    while True:
        page = ddb.scan(**kwargs)
        for row in page.get("Items", []):
            feeds.append({
                "feedId": row.get("feedId", {}).get("S", ""),
                "outputUrl": row.get("outputUrl", {}).get("S", ""),
                "isPaused": row.get("isPaused", {}).get("BOOL", False),
                "shopName": row.get("shopName", {}).get("S", ""),
            })
        if "LastEvaluatedKey" not in page:
            return feeds
        kwargs["ExclusiveStartKey"] = page["LastEvaluatedKey"]


def fetch_to_tmp(url: str) -> str:
    """Stream the published feed to a temp file (runner disk, not RAM)."""
    fd, path = tempfile.mkstemp(suffix=".xml")
    written = 0
    try:
        with os.fdopen(fd, "wb") as out, requests.get(
            url, stream=True, timeout=FETCH_TIMEOUT,
            headers={"User-Agent": "favi-feed-quality-audit/1.0"},
        ) as resp:
            resp.raise_for_status()
            for chunk in resp.iter_content(chunk_size=1 << 20):
                written += len(chunk)
                if written > MAX_BYTES:
                    raise RuntimeError(f"exceeded {MAX_BYTES // (1024*1024)} MB cap")
                out.write(chunk)
        return path
    except BaseException:
        os.unlink(path)
        raise


def open_maybe_gzip(path: str):
    with open(path, "rb") as probe:
        magic = probe.read(2)
    return gzip.open(path, "rb") if magic == b"\x1f\x8b" else open(path, "rb")


def audit_feed(path: str) -> dict:
    """Stream-parse one published feed; return counts + feed-level blockers."""
    blockers: list[str] = []
    warns: Counter = Counter()
    items = 0
    priced = 0
    invalid_fmt = 0
    root = None
    spec = None
    item_locals: set[str] = set()

    with open_maybe_gzip(path) as fh:
        stream = DET.iterparse(fh, events=("start", "end"))
        for event, elem in stream:
            if event == "start":
                if root is None:
                    root = elem
                    root_local = strip_ns(elem.tag).lower()
                    spec = ROOT_TO_SPEC.get(root_local)
                    if spec is None:
                        blockers.append(f"unexpected-root:{root_local}")
                        break
                    item_locals = _item_locals(spec)
                continue
            if strip_ns(elem.tag).lower() not in item_locals:
                continue

            items += 1
            if not read_id(elem, spec):
                warns["missing_id"] += 1
            if not read_link(elem, spec):
                warns["missing_link"] += 1
            if not gather_primary_image(elem, spec, do_percent_encode=False):
                warns["missing_image"] += 1
            if not read_availability(elem, spec):
                warns["missing_availability"] += 1

            raw_price = (read_price_text(elem, spec) or "").strip()
            if not raw_price:
                warns["missing_price"] += 1
            else:
                priced += 1
                amt, valid_fmt, overprec, _reason = analyze_price_text(raw_price)
                if (not valid_fmt) or amt is None or amt <= 0:
                    warns["invalid_price_format"] += 1
                    invalid_fmt += 1
                elif overprec:
                    warns["overprecision_price"] += 1

            gtin = read_recommended_value(elem, "gtin")
            if not gtin:
                warns["missing_gtin"] += 1
            elif not is_valid_gtin(gtin):
                warns["invalid_gtin"] += 1

            # Clearing the finished item subtree frees ~all of its memory. The
            # emptied elements still accumulate under <channel> (bounded:
            # ~0.5 KB each), which is the safe trade — clearing root/channel
            # mid-iterparse detaches the branch being parsed and silently
            # ACCUMULATES full items instead.
            elem.clear()

    if items == 0:
        blockers.append("zero-items")
    if priced > 0 and invalid_fmt == priced:
        blockers.append("all-prices-invalid")
    return {"itemCount": items, "warnings": dict(warns), "blockers": blockers,
            "spec": spec or "UNKNOWN"}


def main() -> int:
    ap = argparse.ArgumentParser()
    ap.add_argument("--state", required=True)
    ap.add_argument("--report", required=True)
    ap.add_argument("--prev")
    ap.add_argument("--limit", type=int, default=0, help="audit only first N feeds (smoke)")
    args = ap.parse_args()

    prev = {}
    if args.prev and os.path.exists(args.prev):
        with open(args.prev) as fh:
            prev = json.load(fh).get("feeds", {})
        print(f"previous state: {len(prev)} feed(s)")
    else:
        print("no previous state — baseline run, delta alerting disabled")

    feeds = [f for f in list_feeds() if f["feedId"] and f["outputUrl"]]
    active = [f for f in feeds if not f["isPaused"]]
    skipped_paused = len(feeds) - len(active)
    if args.limit:
        active = active[: args.limit]
    print(f"auditing {len(active)} active feed(s) ({skipped_paused} paused skipped)")

    state_feeds: dict[str, dict] = {}
    for i, feed in enumerate(active, 1):
        fid, shop = feed["feedId"], feed["shopName"] or feed["feedId"][:8]
        try:
            path = fetch_to_tmp(feed["outputUrl"])
        except Exception as exc:  # noqa: BLE001
            state_feeds[fid] = {"shop": shop, "itemCount": 0, "warnings": {},
                                "blockers": [f"fetch-error:{type(exc).__name__}"]}
            print(f"[{i}/{len(active)}] {shop}: FETCH ERROR {exc}")
            continue
        try:
            result = audit_feed(path)
        except Exception as exc:  # noqa: BLE001
            result = {"itemCount": 0, "warnings": {},
                      "blockers": [f"xml-parse-error:{type(exc).__name__}"]}
        finally:
            os.unlink(path)

        prev_count = prev.get(fid, {}).get("itemCount", 0)
        if (prev_count >= DROP_MIN_BASELINE
                and result["itemCount"] < prev_count * DROP_RATIO):
            result["blockers"].append(
                f"item-count-drop:{prev_count}->{result['itemCount']}")
        result["shop"] = shop
        state_feeds[fid] = result
        flag = " !! " + ",".join(result["blockers"]) if result["blockers"] else ""
        print(f"[{i}/{len(active)}] {shop}: {result['itemCount']} items{flag}")

    # Delta: a blocker KIND is new for a feed if that kind wasn't present in
    # the previous state (kind = text before ':' so changing detail doesn't
    # re-alert; item-count-drop compares fresh each run by construction).
    def kinds(blockers):
        return {b.split(":", 1)[0] for b in blockers}

    new_blockers: list[str] = []
    if prev:
        for fid, cur in state_feeds.items():
            fresh = kinds(cur["blockers"]) - kinds(prev.get(fid, {}).get("blockers", []))
            for b in cur["blockers"]:
                if b.split(":", 1)[0] in fresh:
                    new_blockers.append(f"{cur['shop']} ({fid[:8]}): {b}")

    ongoing = [f"{v['shop']}: {b}" for v in state_feeds.values() for b in v["blockers"]]
    totals: Counter = Counter()
    for v in state_feeds.values():
        totals.update(v["warnings"])

    now = datetime.now(timezone.utc).isoformat(timespec="seconds")
    with open(args.state, "w") as fh:
        json.dump({"generatedAt": now, "feeds": state_feeds}, fh, indent=1)

    worst = sorted(state_feeds.values(),
                   key=lambda v: sum(v["warnings"].values()), reverse=True)[:15]
    lines = [
        "# Feed quality audit", "",
        f"{now} · {len(state_feeds)} feeds audited · {skipped_paused} paused skipped",
        "",
        f"## New blockers since last run: {len(new_blockers) or 'none'}",
        *[f"- :rotating_light: {b}" for b in new_blockers], "",
        f"## All current blockers: {len(ongoing) or 'none'}",
        *[f"- {b}" for b in ongoing], "",
        "## Fleet warning totals (advisory — warn-only by design)",
        *[f"- {k}: {totals[k]}" for k in WARN_KEYS if totals.get(k)], "",
        "## Feeds with most warnings",
        "| Shop | Items | Warnings |", "|---|---|---|",
        *[
            f"| {v['shop']} | {v['itemCount']} | "
            + ", ".join(f"{k}={n}" for k, n in sorted(v["warnings"].items()) if n)
            + " |"
            for v in worst if sum(v["warnings"].values())
        ],
    ]
    report = "\n".join(lines) + "\n"
    with open(args.report, "w") as fh:
        fh.write(report)
    summary = os.environ.get("GITHUB_STEP_SUMMARY")
    if summary:
        with open(summary, "a") as fh:
            fh.write(report)

    print(f"\nreport -> {args.report}; state -> {args.state}")
    if new_blockers:
        print(f"NEW blockers: {len(new_blockers)} — failing run to notify")
        return 1
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
