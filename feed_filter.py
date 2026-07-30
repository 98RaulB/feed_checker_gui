# feed_filter.py
# Standalone, dependency-free filtering engine for FAVI product feeds.
#
# Why this lives apart from feed_checker_gui.py:
#   * The GUI script is single-shot (it `st.stop()`s unless the form was just
#     submitted), so it cannot host live, re-run-driven filtering. This module
#     has NO Streamlit dependency at all — it is pure Python over the same
#     feed_specs readers the validator uses, so the extracted values always
#     agree with what the checker reports.
#   * It is the piece that will move to Cloud Run: the Streamlit page is a thin
#     shell over `extract()` + `apply_rules()`.
#
# Memory model (the whole reason this is safe on a ~1 GB Streamlit host):
#   * COLUMNAR storage — one list per field, not one dict per product. That is
#     3-5x lighter than list-of-dicts and mirrors what the GUI already does with
#     its parallel `ids`/`links`/`prices_amt` arrays.
#   * Low-cardinality string columns (availability, brand, category) are
#     INTERNED so repeated values share one object.
#   * A hard item cap (`DEFAULT_ITEM_CAP`) bounds the table regardless of feed
#     size. Counts are EXACT within the loaded set; beyond the cap the caller is
#     told the table is a sample (full-feed exact counts are the Cloud Run job).
from __future__ import annotations

from typing import List, Dict, Any, Iterable, Optional, Tuple
import gzip
import io
import os
import re

try:
    from defusedxml import ElementTree as ET  # type: ignore
except Exception:  # pragma: no cover - defusedxml is optional
    import xml.etree.ElementTree as ET  # type: ignore

from feed_specs import (
    SPEC,
    strip_ns,
    detect_spec,
    get_item_nodes,
    read_id,
    read_link,
    read_availability,
    read_price,
    gather_primary_image,
    gather_gallery,
    read_recommended_value,
    is_valid_gtin,
    _named_param_values,   # name->value pairs from <PARAM>/<attrs> containers
)

# Above this (uncompressed, non-gzip) we stream instead of building a full DOM,
# matching the GUI's SMALL_SIZE_LIMIT so behaviour is consistent across tools.
SMALL_SIZE_LIMIT = 30 * 1024 * 1024
# Hard ceiling on rows held in memory for interactive filtering.
DEFAULT_ITEM_CAP = 200_000

# ---- Field catalogue: what an AM can filter on, and each field's data type. ----
# `type` drives which operators the UI offers and how a rule is evaluated.
# The "param" field is special: it filters an arbitrary named product parameter
# and is only usable when the feed was extracted with index_params=True.
FIELDS: List[Dict[str, str]] = [
    {"key": "price",              "label": "Price (amount)",         "type": "number"},
    {"key": "category_depth",     "label": "Category depth (levels)","type": "number"},
    {"key": "title_length",       "label": "Title length (chars)",   "type": "number"},
    {"key": "description_length", "label": "Description length",      "type": "number"},
    {"key": "image_count",        "label": "Image count",            "type": "number"},
    {"key": "availability",       "label": "Availability (raw)",     "type": "text"},
    {"key": "category",           "label": "Category",               "type": "text"},
    {"key": "brand",              "label": "Brand / manufacturer",   "type": "text"},
    {"key": "title",              "label": "Product name",           "type": "text"},
    {"key": "id",                 "label": "Item ID",                "type": "text"},
    {"key": "url",                "label": "Product URL",            "type": "text"},
    {"key": "has_image",          "label": "Has primary image",      "type": "bool"},
    {"key": "has_ean",            "label": "Has valid EAN/GTIN",     "type": "bool"},
    {"key": "price_valid",        "label": "Price is valid (>0)",    "type": "bool"},
    {"key": "has_description",    "label": "Has description",        "type": "bool"},
    {"key": "has_brand",          "label": "Has brand",              "type": "bool"},
    {"key": "has_category",       "label": "Has category",           "type": "bool"},
    {"key": "param",              "label": "Product parameter",      "type": "param"},
]
FIELD_TYPE: Dict[str, str] = {f["key"]: f["type"] for f in FIELDS}
FIELD_LABEL: Dict[str, str] = {f["key"]: f["label"] for f in FIELDS}

TEXT_OPS = [
    "contains", "not contains", "equals", "not equals",
    "starts with", "in list", "not in list", "is empty", "is not empty",
]
NUMBER_OPS = ["<", "<=", ">", ">=", "==", "!=", "between", "is empty", "is not empty"]
BOOL_OPS = ["is true", "is false"]

OPS_BY_TYPE = {"text": TEXT_OPS, "number": NUMBER_OPS, "bool": BOOL_OPS, "param": TEXT_OPS}


# --------------------------------------------------------------------------- #
# Extraction
# --------------------------------------------------------------------------- #
class FeedTable:
    """Columnar snapshot of a feed, capped at `DEFAULT_ITEM_CAP` rows."""

    def __init__(self, spec: str, index_params: bool = False) -> None:
        self.spec = spec
        self.index_params = index_params
        self.columns: Dict[str, list] = {f["key"]: [] for f in FIELDS}
        self.total_seen = 0          # every item element encountered (uncapped)

    @property
    def n(self) -> int:              # rows actually loaded (<= cap)
        return len(self.columns["id"])

    @property
    def truncated(self) -> bool:
        return self.total_seen > self.n


def _is_gzip_path(path: str) -> bool:
    return path.lower().endswith(".gz")


def _open_maybe_gzip(path: str):
    return gzip.open(path, "rb") if _is_gzip_path(path) else open(path, "rb")


def _localnames_from_item_paths(spec_name: str) -> set:
    """Item tag localnames for a spec (mirrors the GUI helper)."""
    names = set()
    for p in SPEC.get(spec_name, {}).get("item_paths", []):
        last = p.split("/")[-1].strip(".")
        if last:
            names.add(strip_ns(last).lower())
    return names


def _detect_spec_from_prefix(path: str, prefix_bytes: int = 262144) -> str:
    """Detect the feed format from a small prefix, for the streaming path."""
    with _open_maybe_gzip(path) as fh:
        raw = fh.read(prefix_bytes)
    try:
        return detect_spec(ET.fromstring(raw)) or "UNKNOWN"
    except ET.ParseError:
        pass
    root_elem = None
    try:
        ctx = ET.iterparse(io.BytesIO(raw), events=("start",))
        seen = 0
        for _, elem in ctx:
            if root_elem is None:
                root_elem = elem
            seen += 1
            if seen >= 200:
                break
    except Exception:
        pass
    if root_elem is not None:
        try:
            spec = detect_spec(root_elem)
            if spec and spec.upper() != "UNKNOWN":
                return spec
        except Exception:
            pass
    return "UNKNOWN"


def _iter_items_stream(file_like, wanted_localnames: Iterable[str]):
    """Yield item elements via iterparse, clearing each after it is consumed so
    the full document never accumulates in RAM (same guard as the GUI)."""
    want = set(wanted_localnames)
    ctx = ET.iterparse(file_like, events=("start", "end"))
    _event, _root = next(ctx)
    open_wanted = 0
    for event, elem in ctx:
        ln = strip_ns(elem.tag).lower() if isinstance(elem.tag, str) else ""
        if event == "start":
            if ln in want:
                open_wanted += 1
            continue
        if ln in want:
            yield elem
            elem.clear()
            open_wanted -= 1
        elif open_wanted == 0:
            elem.clear()


_CAT_SPLIT_RE = re.compile(r"[>|/]")


def _add_row(table: FeedTable, elem, spec: str, interns: Dict[str, dict],
             index_params: bool) -> None:
    """Extract one item's filterable fields into the columnar table."""
    cols = table.columns

    def _intern(field: str, value: str) -> str:
        # Share one string object per distinct value in low-cardinality columns.
        cache = interns[field]
        got = cache.get(value)
        if got is None:
            cache[value] = value
            return value
        return got

    try:
        amt, _raw = read_price(elem, spec)
    except Exception:
        amt = None
    pid = (read_id(elem, spec) or "").strip()
    purl = (read_link(elem, spec) or "").strip()
    title = (read_recommended_value(elem, "title") or "").strip()
    avail = (read_availability(elem, spec) or "").strip()
    brand = (read_recommended_value(elem, "brand") or "").strip()
    cat = (read_recommended_value(elem, "category") or "").strip()
    desc = (read_recommended_value(elem, "description") or "").strip()
    ean = (read_recommended_value(elem, "gtin") or "").strip()
    primary = bool((gather_primary_image(elem, spec) or "").strip())
    try:
        gallery = gather_gallery(elem, spec, do_percent_encode=False)
    except Exception:
        gallery = []
    depth = len([p for p in _CAT_SPLIT_RE.split(cat) if p.strip()]) if cat else 0

    cols["id"].append(pid)
    cols["url"].append(purl)
    cols["title"].append(title)
    cols["availability"].append(_intern("availability", avail))
    cols["brand"].append(_intern("brand", brand))
    cols["category"].append(_intern("category", cat))
    cols["price"].append(amt)
    cols["category_depth"].append(depth)
    cols["title_length"].append(len(title))
    cols["description_length"].append(len(desc))
    cols["image_count"].append((1 if primary else 0) + len(gallery))
    cols["has_image"].append(primary)
    cols["has_ean"].append(bool(ean) and is_valid_gtin(ean))
    cols["price_valid"].append(amt is not None and amt > 0)
    cols["has_description"].append(bool(desc))
    cols["has_brand"].append(bool(brand))
    cols["has_category"].append(bool(cat))
    if index_params:
        params = _named_param_values(elem)  # {name_lower: value}
        cols["param"].append({k: _intern("param", v) for k, v in params.items()})
    else:
        cols["param"].append(None)


def extract(src_path: str, cap: int = DEFAULT_ITEM_CAP,
            index_params: bool = False) -> FeedTable:
    """Parse a feed into a capped columnar `FeedTable`.

    DOM for small plain-XML feeds; streaming (bounded memory) for gzip or
    anything over `SMALL_SIZE_LIMIT`. `total_seen` keeps counting past the cap
    so the caller can tell the user the table is a sample. When `index_params`
    is set, each row also carries its named product parameters (more memory).
    """
    interns: Dict[str, dict] = {"availability": {}, "brand": {}, "category": {}, "param": {}}
    size = os.path.getsize(src_path) if os.path.exists(src_path) else 0
    use_stream = _is_gzip_path(src_path) or size > SMALL_SIZE_LIMIT

    if not use_stream:
        with _open_maybe_gzip(src_path) as fh:
            root = ET.fromstring(fh.read())
        spec = detect_spec(root) or "UNKNOWN"
        table = FeedTable(spec, index_params)
        if spec == "UNKNOWN":
            return table
        for elem in get_item_nodes(root, spec):
            table.total_seen += 1
            if table.n < cap:
                _add_row(table, elem, spec, interns, index_params)
        return table

    spec = _detect_spec_from_prefix(src_path)
    table = FeedTable(spec, index_params)
    if spec == "UNKNOWN":
        # Still count item-like elements so the UI can explain the empty result.
        item_tags = {"item", "entry", "offer", "product"}
        for _s in SPEC:
            item_tags |= _localnames_from_item_paths(_s)
        with _open_maybe_gzip(src_path) as fh:
            for _elem in _iter_items_stream(fh, item_tags):
                table.total_seen += 1
        return table

    item_tags = _localnames_from_item_paths(spec) or {"item", "entry", "offer"}
    with _open_maybe_gzip(src_path) as fh:
        for elem in _iter_items_stream(fh, item_tags):
            table.total_seen += 1
            if table.n < cap:
                _add_row(table, elem, spec, interns, index_params)
    return table


# --------------------------------------------------------------------------- #
# Filtering
# --------------------------------------------------------------------------- #
def _to_float(s: str) -> Optional[float]:
    try:
        return float(str(s).strip().replace(",", "."))
    except (TypeError, ValueError):
        return None


def rule_mask(table: FeedTable, rule: Dict[str, Any]) -> List[bool]:
    """Boolean mask (length n) of rows this single rule matches. An
    unparseable/invalid rule matches nothing (returns all-False)."""
    key = rule.get("field", "")
    op = rule.get("op", "")
    raw_val = rule.get("value", "")
    raw_val2 = rule.get("value2", "")
    n = table.n
    col = table.columns.get(key)
    if col is None:
        return [False] * n
    typ = FIELD_TYPE.get(key, "text")

    if typ == "bool":
        want = op == "is true"
        return [bool(col[i]) is want for i in range(n)]

    if typ == "param":
        # `col[i]` is {name_lower: value} (or None). The parameter name lives in
        # value2; the match text (for the value-taking ops) in value.
        name = str(raw_val2).strip().lower()
        if not name:
            return [False] * n

        def _pv(i: int) -> str:
            d = col[i]
            return (d.get(name) or "").strip().lower() if d else ""

        if op == "is empty":
            return [not _pv(i) for i in range(n)]
        if op == "is not empty":
            return [bool(_pv(i)) for i in range(n)]
        if op in ("in list", "not in list"):
            wanted = {p.strip().lower() for p in str(raw_val).split(",") if p.strip()}
            inside = [_pv(i) in wanted for i in range(n)]
            return inside if op == "in list" else [not x for x in inside]
        needle = str(raw_val).strip().lower()
        pred = {
            "contains":     lambda v: needle in v,
            "not contains": lambda v: needle not in v,
            "equals":       lambda v: v == needle,
            "not equals":   lambda v: v != needle,
            "starts with":  lambda v: v.startswith(needle),
        }.get(op)
        if pred is None:
            return [False] * n
        return [pred(_pv(i)) for i in range(n)]

    if typ == "number":
        if op == "is empty":
            return [col[i] is None for i in range(n)]
        if op == "is not empty":
            return [col[i] is not None for i in range(n)]
        thr = _to_float(raw_val)
        if op == "between":
            hi = _to_float(raw_val2)
            if thr is None or hi is None:
                return [False] * n
            lo, hi = min(thr, hi), max(thr, hi)
            return [col[i] is not None and lo <= col[i] <= hi for i in range(n)]
        if thr is None:
            return [False] * n
        cmp = {
            "<":  lambda v: v <  thr,
            "<=": lambda v: v <= thr,
            ">":  lambda v: v >  thr,
            ">=": lambda v: v >= thr,
            "==": lambda v: v == thr,
            "!=": lambda v: v != thr,
        }.get(op)
        if cmp is None:
            return [False] * n
        return [col[i] is not None and cmp(col[i]) for i in range(n)]

    # text
    needle = str(raw_val).strip().lower()
    if op == "is empty":
        return [not (col[i] or "").strip() for i in range(n)]
    if op == "is not empty":
        return [bool((col[i] or "").strip()) for i in range(n)]
    if op in ("in list", "not in list"):
        wanted = {p.strip().lower() for p in str(raw_val).split(",") if p.strip()}
        inside = [(col[i] or "").strip().lower() in wanted for i in range(n)]
        return inside if op == "in list" else [not x for x in inside]
    pred = {
        "contains":     lambda v: needle in v,
        "not contains": lambda v: needle not in v,
        "equals":       lambda v: v == needle,
        "not equals":   lambda v: v != needle,
        "starts with":  lambda v: v.startswith(needle),
    }.get(op)
    if pred is None:
        return [False] * n
    return [pred((col[i] or "").strip().lower()) for i in range(n)]


def apply_rules(
    table: FeedTable,
    rules: List[Dict[str, Any]],
    combine: str = "AND",
    mode: str = "keep",
) -> Dict[str, Any]:
    """Evaluate `rules` against the table.

    combine: "AND" / "OR" across rules.
    mode:    "keep"   -> resulting feed = rows that MATCH the rules
             "remove" -> resulting feed = rows that do NOT match the rules
    Returns counts plus a `keep_mask` and per-rule match counts.
    """
    n = table.n
    active = [r for r in rules if r.get("field") and r.get("op")]

    if not active:
        match = [True] * n            # no rules => everything "matches"
    else:
        masks = [rule_mask(table, r) for r in active]
        if combine == "OR":
            match = [any(m[i] for m in masks) for i in range(n)]
        else:
            match = [all(m[i] for m in masks) for i in range(n)]

    matched = sum(match)
    keep_mask = match if mode == "keep" else [not x for x in match]
    kept = sum(keep_mask)

    per_rule = [
        {"rule": r, "matched": sum(rule_mask(table, r))}
        for r in active
    ]
    return {
        "n": n,
        "total_seen": table.total_seen,
        "truncated": table.truncated,
        "matched": matched,
        "kept": kept,
        "removed": n - kept,
        "keep_mask": keep_mask,
        "per_rule": per_rule,
    }


# --------------------------------------------------------------------------- #
# Human- and machine-readable rule summaries (the "tell Raul exactly what to
# do" output an AM hands back).
# --------------------------------------------------------------------------- #
def rule_text(rule: Dict[str, Any]) -> str:
    op = rule.get("op", "?")
    if rule.get("field") == "param":
        name = rule.get("value2", "") or "?"
        if op in ("is empty", "is not empty"):
            return f"param '{name}' {op}"
        return f"param '{name}' {op} {rule.get('value','')}".rstrip()
    field = FIELD_LABEL.get(rule.get("field", ""), rule.get("field", "?"))
    if op in ("is empty", "is not empty", "is true", "is false"):
        return f"{field} {op}"
    if op == "between":
        return f"{field} between {rule.get('value','?')} and {rule.get('value2','?')}"
    return f"{field} {op} {rule.get('value','')}".rstrip()


def describe(rules: List[Dict[str, Any]], combine: str, mode: str,
            result: Optional[Dict[str, Any]] = None) -> str:
    active = [r for r in rules if r.get("field") and r.get("op")]
    if not active:
        body = "no filters (all products kept)"
    else:
        joined = f" {combine} ".join(rule_text(r) for r in active)
        verb = "KEEP only" if mode == "keep" else "REMOVE"
        body = f"{verb} products where {joined}"
    if result:
        pct = (result["removed"] / result["n"] * 100) if result["n"] else 0
        scope = " (sample of first %d items)" % result["n"] if result["truncated"] else ""
        body += (
            f"\n→ {result['kept']:,} of {result['n']:,} products remain, "
            f"{result['removed']:,} removed ({pct:.1f}%){scope}"
        )
    return body


def to_spec(rules: List[Dict[str, Any]], combine: str, mode: str) -> Dict[str, Any]:
    """Machine-readable filter spec (JSON-serialisable) to reproduce server-side."""
    return {
        "combine": combine,
        "mode": mode,
        "rules": [
            {k: r.get(k) for k in ("field", "op", "value", "value2") if r.get(k) not in (None, "")}
            for r in rules if r.get("field") and r.get("op")
        ],
    }
