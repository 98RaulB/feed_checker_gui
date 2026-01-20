# feed_checker_gui.py
from __future__ import annotations
from typing import List, Tuple, Dict, Any, Iterable
import re
from collections import defaultdict
import os
import gzip
import tempfile
import streamlit as st

# Shared rules/helpers from your feed_specs.py
from feed_specs import (
    SPEC,
    strip_ns,
    detect_spec,
    get_item_nodes,               # used in DOM path
    read_id,
    read_link,
    read_availability,
    gather_primary_image,
    read_link_raw,                 # RAW (no percent-encoding) to warn on spaces/non-ASCII
    gather_primary_image_raw,      # RAW
    read_price,                    # (amount, raw_text)
)

# Safe XML parsing (defusedxml if present)
try:
    from defusedxml import ElementTree as ET  # type: ignore
except Exception:
    import xml.etree.ElementTree as ET  # type: ignore

st.set_page_config(page_title="FAVI Feed Checker", layout="wide")
st.title("🧪 FAVI Feed Checker")
st.caption("Auto mode for small vs large feeds; optional Sample mode to process only the first N items (streaming & low RAM).")

# --------- Tuning ----------
SMALL_SIZE_LIMIT = 30 * 1024 * 1024   # 30 MB → DOM; above this → streaming
REQUEST_TIMEOUT = 120                 # seconds
STREAM_CHUNK = 1 << 20                # 1 MB

# ---------- UI helpers ----------
def verdict_row(label: str, ok: bool, warn: bool = False, extra: str = "") -> Tuple[str, str]:
    if ok and not warn:
        return (label, f"✅ PASS {extra}".strip())
    if warn and ok:
        return (label, f"⚠️ WARN {extra}".strip())
    return (label, f"❌ FAIL {extra}".strip())

def summarize(pass_fail: Dict[str, Tuple[bool, bool, str]]):
    st.subheader("SUMMARY")
    for k, (ok, warn, extra) in pass_fail.items():
        _, text = verdict_row(k, ok, warn, extra)
        st.write(f"- **{k}**: {text}")

def status_pill(text: str, color: str = "#16a34a"):  # green default
    st.markdown(
        f"""
        <div style="
            display:inline-block;
            padding:6px 12px;
            border-radius:999px;
            background:{color};
            color:white;
            font-weight:600;
            font-size:14px;
        ">{text}</div>
        """,
        unsafe_allow_html=True,
    )

def unique_preserve(xs: List[str]) -> List[str]:
    seen = set()
    out: List[str] = []
    for x in xs:
        if x not in seen:
            seen.add(x)
            out.append(x)
    return out

def show_issue_table(title: str, rows: List[Dict], sample_n: int):
    st.write(f"**{title}:** {len(rows)}")
    if not rows:
        st.write("none 🎉")
        return
    with st.expander(f"Show first {min(sample_n, len(rows))}"):
        st.dataframe(rows[:sample_n], use_container_width=True)

# ---------- Tag helpers ----------
def localname(tag: str) -> str:
    return tag.split('}', 1)[1] if '}' in tag else tag

def localnames_from_item_paths(spec_name: str) -> set[str]:
    """Derive the set of valid item tag localnames from SPEC.item_paths for this spec."""
    paths = SPEC.get(spec_name, {}).get("item_paths", [])
    names = set()
    for p in paths:
        last = p.split("/")[-1].strip(".")  # e.g. "SHOPITEM", "{ns}entry", "product"
        if last:
            names.add(strip_ns(last))
    return names

# ---------- I/O helpers ----------
def download_to_tmp(url: str, chunk=STREAM_CHUNK) -> str:
    """Stream a URL to a temp file (no giant bytes in memory). Returns file path."""
    import requests

    # 1. Define a browser-like header
    custom_headers = {
        "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36",
        "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8"
    }
    
    # 2. Use it in the request
    with requests.get(url, stream=True, timeout=REQUEST_TIMEOUT, headers=custom_headers) as r:
        r.raise_for_status()
        ctype = r.headers.get("Content-Type","").lower()
        size_hdr = int(r.headers.get("Content-Length") or 0)

        suffix = ".xml.gz" if ("gzip" in ctype or url.lower().endswith(".gz")) else ".xml"
        tmp = tempfile.NamedTemporaryFile(delete=False, suffix=suffix)
        written = 0
        for chunk_bytes in r.iter_content(chunk_size=chunk):
            if chunk_bytes:
                tmp.write(chunk_bytes)
                written += len(chunk_bytes)
        tmp.flush(); tmp.close()

        effective_size = written or size_hdr
        if effective_size and effective_size > SMALL_SIZE_LIMIT:
            st.warning(f"Large feed detected: ~{effective_size/1024/1024:.0f} MB. Using streaming parser.")
        return tmp.name

def persist_upload(up) -> str:
    """Save an uploaded file to disk and return the path."""
    head = up.read(2); up.seek(0)
    is_gz = head == b"\x1f\x8b"
    suffix = ".xml.gz" if is_gz or (up.name and up.name.lower().endswith(".gz")) else ".xml"
    tmp = tempfile.NamedTemporaryFile(delete=False, suffix=suffix)
    tmp.write(up.read()); tmp.flush(); tmp.close()
    up.seek(0)
    return tmp.name

def is_gzip_path(path: str) -> bool:
    try:
        with open(path, "rb") as f:
            return f.read(2) == b"\x1f\x8b" or path.lower().endswith(".gz")
    except Exception:
        return path.lower().endswith(".gz")

def open_maybe_gzip(path: str):
    return gzip.open(path, "rb") if is_gzip_path(path) else open(path, "rb")

# ---------- Streaming parser ----------
def iter_items_stream(file_like, wanted_localnames: Iterable[str]):
    """
    Stream items with ET.iterparse. Yields (elem, root) for each end-event of a tag
    whose localname is in wanted_localnames. Caller must elem.clear() after processing.
    """
    want = set(wanted_localnames)
    context = ET.iterparse(file_like, events=("start","end"))
    event, root = next(context)  # root element

    # small debug sampler
    seen_counts: Dict[str, int] = {}
    yielded = 0

    for event, elem in context:
        if event == "end":
            ln = localname(elem.tag)
            if yielded < 1_000:  # sample early for speed
                seen_counts[ln] = seen_counts.get(ln, 0) + 1
            if ln in want:
                yielded += 1
                yield elem, root
                elem.clear()

    # If nothing matched, emit a hint in the UI
    if yielded == 0 and seen_counts:
        top = sorted(seen_counts.items(), key=lambda x: -x[1])[:10]
        st.info("No items matched. Top end-tags seen: " + ", ".join(f"{k}×{v}" for k,v in top))

# ---------- FAVI price-format validation ----------
# Accepted by FAVI:
#  - "8 000"         (space thousands)
#  - "8000"          (plain integer)
#  - "8000,70"       (comma decimals)
#  - "8000.70"       (dot decimals; dot used ONLY as decimal separator)
#
# Not accepted:
#  - "8.000" or "1.234,50" (dot as thousands)
#  - "1,234.50"            (comma as thousands)
#  - More than 2 decimals  → warn: over-precision (will be rounded)
_re_int_plain           = re.compile(r"^\d+$")
_re_int_space_groups    = re.compile(r"^\d{1,3}(?: \d{3})+$")
_re_dec_comma           = re.compile(r"^\d+(?:,\d+)$")
_re_dec_dot             = re.compile(r"^\d+(?:\.\d+)$")
_re_has_dot_thousands   = re.compile(r"\d\.\d{3}(?:[^\d]|$)")
_re_has_comma_thousands = re.compile(r"\d,\d{3}(?:[^\d]|$)")

def _extract_first_numeric_token(s: str) -> str:
    # capture the same token parse_price() would see, but keep punctuation to validate format
    m = re.search(r"\d{1,3}(?:[ .,\u00A0]?\d{3})*(?:[.,]\d+)?|\d+(?:[.,]\d+)?", s)
    return m.group(0) if m else ""

def favi_price_format_flags(raw_text: str) -> Tuple[bool, bool, str]:
    """
    Returns (is_valid_format, has_overprecision, reason)
    - is_valid_format: obeys FAVI allowed grouping/decimal rules
    - has_overprecision: > 2 decimals (FAVI rounds; we warn)
    - reason: explanation when invalid
    """
    if not raw_text:
        return False, False, "missing"
    token = _extract_first_numeric_token(raw_text.replace("\u00A0", " "))
    if not token:
        return False, False, "no numeric token"

    # Disallow dot/comma as thousands separators
    if _re_has_dot_thousands.search(token):
        return False, False, "dot used as thousands separator"
    if _re_has_comma_thousands.search(token):
        return False, False, "comma used as thousands separator"

    # Allowed overall shapes
    allowed = (
        _re_int_plain.match(token)
        or _re_int_space_groups.match(token)
        or _re_dec_comma.match(token)
        or _re_dec_dot.match(token)
    )
    if not allowed:
        return False, False, "format not in {8000 | 8 000 | 8000,dd | 8000.dd}"

    # Over-precision (>2 decimals) → warn
    overprecision = False
    if "," in token:
        dec = token.split(",", 1)[1]
        overprecision = len(dec) > 2
    elif "." in token:
        dec = token.split(".", 1)[1]
        overprecision = len(dec) > 2

    return True, overprecision, ""

# ---------- Form ----------
with st.form("input"):
    url = st.text_input("Feed URL (http/https)", placeholder="https://example.com/feed.xml")
    up = st.file_uploader("…or upload an XML file (.xml or .xml.gz)", type=["xml", "gz"])

    colA, colB = st.columns(2)
    with colA:
        scope = st.selectbox("Processing scope", ["Auto (full)", "Sample first N items"])
    with colB:
        stop_on_first_parse_error = st.checkbox("Stop on XML parse error", value=True)

    # Show the N picker ONLY when Sample mode is chosen
    n_limit = None
    if scope == "Sample first N items":
        n_limit = st.number_input("N (for sample mode)", min_value=100, max_value=200_000, value=5_000, step=500)

    sample_show = st.number_input("Show up to N sample issues per category", 1, 50, 10)

    submitted = st.form_submit_button("Check feed")

if not submitted:
    st.markdown("© 2025 Raul Bertoldini")
    st.stop()

# 1) Get a file on disk
if url.strip():
    if not url.lower().startswith(("http://", "https://")):  # guard
        st.error("URL must start with http:// or https://"); st.stop()
    try:
        src_path = download_to_tmp(url.strip())
        src_label = url.strip()
    except Exception as e:
        st.error(f"Failed to download URL: {e}"); st.stop()
elif up is not None:
    try:
        src_path = persist_upload(up)
        src_label = up.name
    except Exception as e:
        st.error(f"Failed to read uploaded file: {e}"); st.stop()
else:
    st.warning("Provide a URL or upload a file."); st.stop()

st.write(f"**Source:** `{src_label}`")

# Decide parsing strategy for Auto vs Sample
file_size = os.path.getsize(src_path) if os.path.exists(src_path) else 0
auto_force_streaming = is_gzip_path(src_path) or (file_size > SMALL_SIZE_LIMIT)
use_sample_mode = (scope == "Sample first N items")

# Data buckets (shared)
xml_ok = True
spec_name = "UNKNOWN"
total_items = 0                 # total encountered (streamed) or len(items) in DOM
processed_items = 0             # for sample mode display

ids: List[str] = []
links: List[str] = []
images: List[str] = []
avails: List[str] = []

# PRICE buckets
prices_amt: List[float | None] = []
prices_raw: List[str] = []

missing_id_idx: List[int] = []
missing_link_idx: List[int] = []
missing_img_idx: List[int] = []
missing_avail_idx: List[int] = []

# PRICE issue indexes (semantic + format)
missing_price_idx: List[int] = []          # no price node/text
bad_price_idx: List[int] = []              # present but unparseable or <= 0
invalid_price_format_idx: List[int] = []   # violates FAVI format rules
overprecision_price_idx: List[int] = []    # > 2 decimals (FAVI rounds)

raw_links: List[str] = []
raw_imgs: List[str] = []
bad_url_idx: List[int] = []
bad_img_idx: List[int] = []

id_first_seen: Dict[str, int] = {}
link_first_seen: Dict[str, int] = {}
dup_id_pairs: List[Tuple[int, int, str]] = []
dup_link_pairs: List[Tuple[int, int, str]] = []

ascii_only = re.compile(r'^[\x00-\x7F]+$')

def process_item(elem, index: int, spec: str):
    pid = (read_id(elem, spec) or "").strip()
    purl = (read_link(elem, spec) or "").strip()
    pav  = (read_availability(elem, spec) or "").strip()
    pimg = (gather_primary_image(elem, spec) or "").strip()
    purl_raw = (read_link_raw(elem, spec) or "").strip()
    pimg_raw = (gather_primary_image_raw(elem, spec) or "").strip()

    # PRICE read
    try:
        amt, raw_price = read_price(elem, spec)  # from feed_specs.py
    except Exception:
        amt, raw_price = None, ""

    ids.append(pid); links.append(purl); images.append(pimg); avails.append(pav)
    raw_links.append(purl_raw); raw_imgs.append(pimg_raw)

    prices_amt.append(amt)
    prices_raw.append(raw_price or "")

    if not pid: missing_id_idx.append(index)
    if not purl: missing_link_idx.append(index)
    if not pimg: missing_img_idx.append(index)
    if not pav:  missing_avail_idx.append(index)

    # PRICE validations (semantic)
    if (raw_price or "").strip() == "":
        missing_price_idx.append(index)
    else:
        if (amt is None) or (amt is not None and amt <= 0):
            bad_price_idx.append(index)

        # PRICE validations (FAVI format rules)
        valid_fmt, overprec, _reason = favi_price_format_flags(raw_price or "")
        if not valid_fmt:
            invalid_price_format_idx.append(index)
        if overprec:
            overprecision_price_idx.append(index)

    # URL quality warnings from RAW
    if purl_raw and ((" " in purl_raw) or not ascii_only.match(purl_raw)):
        bad_url_idx.append(index)
    if pimg_raw and ((" " in pimg_raw) or not ascii_only.match(pimg_raw)):
        bad_img_idx.append(index)

    # Duplicate tracking
    if pid:
        if pid in id_first_seen:
            dup_id_pairs.append((id_first_seen[pid], index, pid))
        else:
            id_first_seen[pid] = index

    if purl:
        if purl in link_first_seen:
            dup_link_pairs.append((link_first_seen[purl], index, purl))
        else:
            link_first_seen[purl] = index

# ---------- DOM path (Auto + small, non-gz, and only if NOT sample mode) ----------
def run_dom_path() -> bool:
    global xml_ok, spec_name, total_items, processed_items
    try:
        with open(src_path, "rb") as fh:
            xml_bytes = fh.read()
        root = ET.fromstring(xml_bytes)
        st.success("XML syntax: OK")
        spec = detect_spec(root) or "UNKNOWN"
        items = get_item_nodes(root, spec) if spec != "UNKNOWN" else []
        total = len(items)
        for i, it in enumerate(items):
            process_item(it, i, spec)
        spec_name = spec
        total_items = total
        processed_items = total
        return True
    except ET.ParseError as e:
        st.error(f"XML syntax: ERROR — {e}")
        if stop_on_first_parse_error:
            st.stop()
        return False
    except MemoryError:
        st.warning("Memory pressure detected with DOM path; falling back to streaming.")
        return False
    except Exception as e:
        st.warning(f"DOM path failed ({e}). Falling back to streaming.")
        return False

# ---------- Streaming path (Auto-large, any .gz, or Sample mode) ----------
def run_stream_path(limit: int | None):
    global xml_ok, spec_name, total_items, processed_items
    try:
        # Quick root/spec detection
        with open_maybe_gzip(src_path) as fh1:
            context1 = ET.iterparse(fh1, events=("start",))
            _, root_first = next(context1)
            spec_name_local = detect_spec(root_first) or "UNKNOWN"
            spec_name = spec_name_local
            st.success("XML syntax: OK")

        # Build exact set of item tag localnames from SPEC (no broad fallback for known specs)
        if spec_name and spec_name.upper() != "UNKNOWN":
            item_tags = localnames_from_item_paths(spec_name)
            if not item_tags:
                item_tags = {"item", "entry", "offer"}
        else:
            item_tags = {"item", "entry", "offer", "product"}

        st.caption("Looking for item tags: " + ", ".join(sorted(list(item_tags))))

        # Full streaming pass
        processed = 0
        with open_maybe_gzip(src_path) as fh2:
            for elem, root in iter_items_stream(fh2, wanted_localnames=item_tags):
                # Guard: count as item only if at least ID or Link exists for this spec
                pid_peek = (read_id(elem, spec_name) or "").strip()
                purl_peek = (read_link(elem, spec_name) or "").strip()
                if not pid_peek and not purl_peek:
                    continue

                total_items += 1

                if limit is not None and processed >= limit:
                    # keep counting total_items beyond limit without extraction
                    continue

                process_item(elem, processed, spec_name)
                processed += 1
        processed_items = processed
    except ET.ParseError as e:
        xml_ok = False
        st.error(f"XML syntax: ERROR — {e}")
        if stop_on_first_parse_error:
            st.stop()
    except Exception as e:
        st.error(f"Streaming parser error: {e}")
        st.stop()

# Decide and run
used_streaming = False
if use_sample_mode:
    used_streaming = True
    run_stream_path(limit=int(n_limit))  # n_limit is guaranteed not None in this branch
else:
    if auto_force_streaming:
        used_streaming = True
        run_stream_path(limit=None)
    else:
        ok = run_dom_path()
        if not ok:
            used_streaming = True
            run_stream_path(limit=None)

# ---------- TOP ROW ----------
st.markdown("---")
c1, c2, c3, c4, c5 = st.columns([1, 1, 1, 1, 1], gap="large")

with c1:
    status_pill(f"Transformation: {spec_name if spec_name!='UNKNOWN' else 'UNKNOWN'}",
                "#16a34a" if spec_name!="UNKNOWN" else "#6b7280")

with c2:
    if use_sample_mode:
        status_pill(f"Items: {processed_items} / {total_items}", "#16a34a")
    else:
        status_pill(f"Items: {total_items}", "#16a34a")

with c3:
    total_dups = len(dup_id_pairs) + len(dup_link_pairs)
    status_pill(f"Duplicates: {total_dups}", "#dc2626" if total_dups > 0 else "#16a34a")

with c4:
    status_pill(f"Missing IDs: {len(missing_id_idx)}", "#dc2626" if len(missing_id_idx)>0 else "#16a34a")

with c5:
    total_warnings = (
        len(missing_link_idx)
        + len(missing_img_idx)
        + len(missing_avail_idx)
        + len(bad_url_idx)
        + len(bad_img_idx)
        + len(missing_price_idx)
        + len(bad_price_idx)
        + len(invalid_price_format_idx)
        + len(overprecision_price_idx)
    )
    any_warnings = total_warnings > 0
    status_pill(f"Warnings: {total_warnings}", "#f59e0b" if any_warnings else "#16a34a")

# ---------- SUMMARY ----------
pass_fail: Dict[str, Tuple[bool, bool, str]] = {}
pass_fail["XML syntax"] = (xml_ok, False, "")
pass_fail["Transformation detected"] = (spec_name != "UNKNOWN", False, spec_name if spec_name != "UNKNOWN" else "")
pass_fail["IDs present"] = (len(missing_id_idx) == 0, False, f"(missing: {len(missing_id_idx)})")
pass_fail["Duplicate IDs"] = (len(dup_id_pairs) == 0, False, f"(duplicates: {len(dup_id_pairs)})")
pass_fail["Duplicate Product URLs"] = (len(dup_link_pairs) == 0, False, f"(duplicates: {len(dup_link_pairs)})")
pass_fail["Product URL present"] = (True, len(missing_link_idx) > 0, f"(missing: {len(missing_link_idx)})")
pass_fail["Primary image present"] = (True, len(missing_img_idx) > 0, f"(missing: {len(missing_img_idx)})")
pass_fail["Availability present"] = (True, len(missing_avail_idx) > 0, f"(missing: {len(missing_avail_idx)})")
pass_fail["Product URL validity"] = (True, len(bad_url_idx) > 0, f"(bad: {len(bad_url_idx)})")
pass_fail["Image URL validity"] = (True, len(bad_img_idx) > 0, f"(bad: {len(bad_img_idx)})")

# PRICE summary lines (all as WARNs, never FAIL)
pass_fail["Price present"] = (True, len(missing_price_idx) > 0, f"(missing: {len(missing_price_idx)})")
pass_fail["Price numeric > 0"] = (True, len(bad_price_idx) > 0, f"(invalid: {len(bad_price_idx)})")
pass_fail["Price format follows FAVI rules"] = (True, len(invalid_price_format_idx) > 0,
                                               f"(invalid format: {len(invalid_price_format_idx)})")
pass_fail["Price precision (<= 2 decimals)"] = (True, len(overprecision_price_idx) > 0,
                                               f"(over-precision: {len(overprecision_price_idx)})")

st.markdown("---")
summarize(pass_fail)

# ---------- DETAILS ----------
st.markdown("---")
st.subheader("Details")

def safe_get(lst, i, default=""):
    try:
        return lst[i]
    except Exception:
        return default

# Missing fields
missing_id_rows = [
    {"id": "(missing)", "link": safe_get(links, i), "image": "yes" if safe_get(images, i) else "no", "availability": safe_get(avails, i) or "(missing)"}
    for i in missing_id_idx
]
show_issue_table("Missing ID (by example values)", missing_id_rows, sample_show)

missing_link_rows = [
    {"id": safe_get(ids, i), "link": "(missing)", "image": "yes" if safe_get(images, i) else "no", "availability": safe_get(avails, i) or "(missing)"}
    for i in missing_link_idx if safe_get(ids, i)
]
show_issue_table("Missing Product URL (by product ID)", missing_link_rows, sample_show)

missing_img_rows = [
    {"id": safe_get(ids, i), "link": safe_get(links, i), "primary_image": "(missing)"}
    for i in missing_img_idx if safe_get(ids, i)
]
show_issue_table("Missing Primary Image (by product ID)", missing_img_rows, sample_show)

missing_avail_rows = [
    {"id": safe_get(ids, i), "link": safe_get(links, i), "availability": "(missing)"}
    for i in missing_avail_idx if safe_get(ids, i)
]
show_issue_table("Missing Availability (by product ID)", missing_avail_rows, sample_show)

# PRICE details
missing_price_rows = [
    {"id": safe_get(ids, i) or "(missing id)", "link": safe_get(links, i), "raw_price": "(missing)"}
    for i in missing_price_idx if safe_get(ids, i)
]
show_issue_table("Missing Price (by product ID)", missing_price_rows, sample_show)

bad_price_rows = [
    {"id": safe_get(ids, i) or "(missing id)",
     "link": safe_get(links, i),
     "raw_price": safe_get(prices_raw, i) or "(missing)",
     "parsed_amount": safe_get(prices_amt, i),
    }
    for i in bad_price_idx
]
show_issue_table("Invalid Price (non-numeric or <= 0) — by product ID", bad_price_rows, sample_show)

invalid_format_rows = [
    {"id": safe_get(ids, i) or "(missing id)",
     "link": safe_get(links, i),
     "raw_price": safe_get(prices_raw, i) or "(missing)",
     "note": "Format not allowed by FAVI (dot-as-thousands/comma-as-thousands/etc.)"}
    for i in invalid_price_format_idx
]
show_issue_table("Price format violations (FAVI rules)", invalid_format_rows, sample_show)

overprecision_rows = [
    {"id": safe_get(ids, i) or "(missing id)",
     "link": safe_get(links, i),
     "raw_price": safe_get(prices_raw, i) or "(missing)",
     "note": "More than 2 decimals — FAVI rounds automatically"}
    for i in overprecision_price_idx
]
show_issue_table("Price over-precision (> 2 decimals) — informational", overprecision_rows, sample_show)

# Bad URL warnings (RAW)
bad_url_rows = [
    {"id": safe_get(ids, i) or "(missing id)", "raw_url": safe_get(raw_links, i), "encoded_url": safe_get(links, i)}
    for i in bad_url_idx
]
show_issue_table("Bad Product URLs (spaces/non-ASCII) — RAW view", bad_url_rows, sample_show)

bad_img_rows = [
    {"id": safe_get(ids, i) or "(missing id)", "raw_image_url": safe_get(raw_imgs, i), "encoded_image_url": safe_get(images, i)}
    for i in bad_img_idx
]
show_issue_table("Bad Image URLs (spaces/non-ASCII) — RAW view", bad_img_rows, sample_show)

# Duplicates (IDs)
dup_ids_map: Dict[str, List[int]] = defaultdict(list)
for old_i, new_i, pid in dup_id_pairs:
    dup_ids_map[pid].extend([old_i, new_i])

dup_id_rows = []
for pid, idxs in dup_ids_map.items():
    idxs_u = unique_preserve([str(x) for x in idxs])
    ex_links = unique_preserve([safe_get(links, int(i)) for i in idxs_u if safe_get(links, int(i))])[:3]
    dup_id_rows.append({
        "id": pid,
        "occurrences": len(unique_preserve([str(int(i)) for i in idxs])),
        "example_links": " | ".join(ex_links) if ex_links else ""
    })
dup_id_rows.sort(key=lambda r: (-r["occurrences"], r["id"]))
show_issue_table("Duplicate IDs (grouped)", dup_id_rows, sample_show)

# Duplicates (URLs)
url_to_ids: Dict[str, List[str]] = defaultdict(list)
for old_i, new_i, url in dup_link_pairs:
    oid = safe_get(ids, old_i)
    nid = safe_get(ids, new_i)
    if oid: url_to_ids[url].append(oid)
    if nid: url_to_ids[url].append(nid)

dup_url_rows = []
for url, idlist in url_to_ids.items():
    ids_u = unique_preserve(idlist)
    dup_url_rows.append({
        "url": url,
        "num_ids": len(ids_u),
        "ids": ", ".join(ids_u[:12]) + (" …" if len(ids_u) > 12 else "")
    })
dup_url_rows.sort(key=lambda r: (-r["num_ids"], r["url"]))
show_issue_table("Duplicate Product URLs (grouped, with IDs)", dup_url_rows, sample_show)

st.markdown("---")
st.caption(
    ("Scope: Sample first N items (streaming)" if use_sample_mode else
     f"Scope: Auto (parser: {'Streaming' if (is_gzip_path(src_path) or file_size>SMALL_SIZE_LIMIT) else 'DOM'})")
)
st.markdown("© 2025 Raul Bertoldini")
