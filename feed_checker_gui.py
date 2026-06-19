# feed_checker_gui.py
from __future__ import annotations
from typing import List, Tuple, Dict, Any, Iterable
import re
from collections import defaultdict
import os
import gzip
import tempfile
import json
import base64
import streamlit as st
from urllib.parse import urlparse

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

# The two helpers below were added recently to `feed_specs.py`.
# Import them if available, otherwise provide safe fallbacks so the
# Streamlit app does not fail on older deployments where the module
# hasn't been updated yet.
try:
    from feed_specs import is_favi_compatible, needs_conversion
except Exception:
    def is_favi_compatible(spec_name: str) -> bool:
        return True

    def needs_conversion(spec_name: str) -> tuple[bool, str]:
        return (False, "")

# Recommended-element coverage (description, category, delivery/shipping, …).
# Falls back to a no-op if feed_specs hasn't been updated on this deployment.
try:
    from feed_specs import RECOMMENDED_FIELDS, present_recommended_fields
except Exception:
    RECOMMENDED_FIELDS = []

    def present_recommended_fields(elem) -> set:
        return set()

# Shared FAVI look-and-feel (Work Sans, crimson banner, themed cards/pills).
from branding import inject_css, page_header, render_metric_row, FAVICON_URL

# Safe XML parsing (defusedxml if present)
try:
    from defusedxml import ElementTree as ET  # type: ignore
except Exception:
    import xml.etree.ElementTree as ET  # type: ignore

st.set_page_config(page_title="FAVI Feed Checker", page_icon=FAVICON_URL, layout="wide")
inject_css()
page_header(
    "Feed Checker",
    subtitle="Validate a product feed before FAVI import — detects the format, checks "
             "required fields and price formatting, and flags missing recommended elements.",
)
st.warning(
    "🚧 **Beta** — the Feed Checker is still in beta. More fixes and new "
    "functionality are coming soon."
)

# --------- Tuning ----------
SMALL_SIZE_LIMIT = 30 * 1024 * 1024   # 30 MB → DOM; above this → streaming
REQUEST_TIMEOUT = 120                 # seconds
STREAM_CHUNK = 1 << 20                # 1 MB
CLICKUP_FORM_URL = "https://forms.clickup.com/90151995362/f/2kyqmhz2-30675/FF5VMWEUZRGFU7QVFR"
CLICKUP_COUNTRIES = ["CZ", "SK", "RO", "HU", "HR", "PL", "IT", "BG", "SI", "GR"]
CLICKUP_FORMATS = ["CSV", "CENEO", "OTHER", "CENEJE", "GOOGLE", "LEGACY", "COMPARI", "SKROUTZ"]
COUNTRY_BY_TLD = {
    "cz": "CZ",
    "sk": "SK",
    "ro": "RO",
    "hu": "HU",
    "hr": "HR",
    "pl": "PL",
    "it": "IT",
    "bg": "BG",
    "si": "SI",
    "gr": "GR",
}

# ---------- UI helpers ----------
def summarize(pass_fail: Dict[str, Tuple[bool, bool, str]]):
    st.subheader("Summary")

    fails, warns, passes = [], [], []
    for k, (ok, warn, extra) in pass_fail.items():
        if not ok:
            fails.append((k, extra))
        elif warn:
            warns.append((k, extra))
        else:
            passes.append((k, extra))

    # Lead with what needs attention — errors first, then warnings.
    if fails or warns:
        lines = [f"- ❌ **{k}** {extra}".rstrip() for k, extra in fails]
        lines += [f"- ⚠️ **{k}** {extra}".rstrip() for k, extra in warns]
        st.markdown("\n".join(lines))
    else:
        st.success("No errors or warnings — everything checks out.")

    # Everything that's fine is tucked into a dropdown to keep the view scannable.
    if passes:
        with st.expander(f"✅ Passing checks ({len(passes)})"):
            st.markdown("\n".join(f"- ✅ **{k}** {extra}".rstrip() for k, extra in passes))

def clickup_card_metric(label: str, value: str):
    safe_label = str(label).replace("&", "&amp;").replace("<", "&lt;").replace(">", "&gt;")
    safe_value = str(value or "Not set").replace("&", "&amp;").replace("<", "&lt;").replace(">", "&gt;")
    st.markdown(
        f"""
        <div style="
            background:#f8f7ff;
            border:1px solid #ddd6fe;
            border-radius:14px;
            padding:12px 14px;
            margin-bottom:10px;
        ">
            <div style="
                font-size:12px;
                font-weight:700;
                letter-spacing:0.04em;
                text-transform:uppercase;
                color:#6d28d9;
                margin-bottom:4px;
            ">{safe_label}</div>
            <div style="
                font-size:14px;
                color:#1f2937;
                line-height:1.35;
                word-break:break-word;
            ">{safe_value}</div>
        </div>
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

MAX_ROWS = 1000   # cap rows rendered per issue list, to keep the page light

def show_issue_table(title: str, rows: List[Dict]):
    """Render an issue category as a collapsed, expandable list with its count
    in the label. Renders nothing when empty, so a clean feed stays uncluttered."""
    n = len(rows)
    if n == 0:
        return
    with st.expander(f"{title} — {n}"):
        st.dataframe(rows[:MAX_ROWS], width="stretch")
        if n > MAX_ROWS:
            st.caption(f"Showing the first {MAX_ROWS:,} of {n:,}.")

# ---------- Tag helpers ----------
def localname(tag: str) -> str:
    raw = tag.split('}', 1)[1] if '}' in tag else tag
    return raw.lower()

def localnames_from_item_paths(spec_name: str) -> set[str]:
    """Derive the set of valid item tag localnames from SPEC.item_paths for this spec."""
    paths = SPEC.get(spec_name, {}).get("item_paths", [])
    names = set()
    for p in paths:
        last = p.split("/")[-1].strip(".")  # e.g. "SHOPITEM", "{ns}entry", "product"
        if last:
            names.add(strip_ns(last).lower())
    return names

def _clean_host(host: str) -> str:
    host = (host or "").lower().strip()
    host = host.split("@")[-1].split(":")[0]
    return host[4:] if host.startswith("www.") else host

def infer_shop_name(source: str) -> str:
    candidate = (source or "").strip()
    if candidate.lower().startswith(("http://", "https://")):
        host = _clean_host(urlparse(candidate).netloc)
        if host:
            parts = [p for p in host.split(".") if p]
            if len(parts) >= 3 and parts[-2] in {"co", "com"}:
                base = parts[-3]
            elif len(parts) >= 2:
                base = parts[-2]
            else:
                base = parts[0]
            return re.sub(r"[-_]+", " ", base).strip().title()

    filename = os.path.basename(candidate)
    filename = re.sub(r"(\.xml)?(\.gz)?$", "", filename, flags=re.IGNORECASE)
    return re.sub(r"[-_]+", " ", filename).strip().title()

def infer_country(source_url: str) -> str:
    if not source_url.lower().startswith(("http://", "https://")):
        return ""
    host = _clean_host(urlparse(source_url).netloc)
    for tld, country in COUNTRY_BY_TLD.items():
        if host.endswith(f".{tld}"):
            return country
    return ""

def map_clickup_format(spec_name: str) -> str:
    if spec_name.startswith("Google Merchant"):
        return "GOOGLE"
    if spec_name.startswith("Compari"):
        return "COMPARI"
    if spec_name.startswith("Skroutz"):
        return "SKROUTZ"
    if spec_name.startswith("Ceneo"):
        return "CENEO"
    if spec_name.startswith("Jeftinije") or spec_name.startswith("Ceneje.si"):
        return "CENEJE"
    if spec_name.startswith("Heureka"):
        return "LEGACY"
    return "OTHER"

def build_problem_codes(
    xml_ok: bool,
    spec_name: str,
    conv_required: bool,
    missing_id_idx: List[int],
    missing_link_idx: List[int],
    missing_img_idx: List[int],
    missing_avail_idx: List[int],
    missing_price_idx: List[int],
    bad_price_idx: List[int],
    invalid_price_format_idx: List[int],
    bad_url_idx: List[int],
    bad_img_idx: List[int],
    dup_id_pairs: List[Tuple[int, int, str]],
    dup_link_pairs: List[Tuple[int, int, str]],
    recommended_missing: Dict[str, List[int]] | None = None,
) -> List[str]:
    codes: List[str] = []
    if not xml_ok:
        codes.append("XML_INVALID")
    if spec_name == "UNKNOWN":
        codes.append("FORMAT_UNKNOWN")
    elif conv_required:
        codes.append("FORMAT_NEEDS_CONVERSION")
    if missing_id_idx:
        codes.append("MISSING_ID")
    if missing_link_idx:
        codes.append("MISSING_PRODUCT_URL")
    if missing_img_idx:
        codes.append("MISSING_PRIMARY_IMAGE")
    if missing_avail_idx:
        codes.append("MISSING_AVAILABILITY")
    if missing_price_idx:
        codes.append("MISSING_PRICE")
    if bad_price_idx:
        codes.append("INVALID_PRICE")
    if invalid_price_format_idx:
        codes.append("INVALID_PRICE_FORMAT")
    if bad_url_idx or bad_img_idx:
        codes.append("URL_ENCODING_ISSUES")
    if dup_id_pairs:
        codes.append("DUPLICATE_IDS")
    if dup_link_pairs:
        codes.append("DUPLICATE_PRODUCT_URLS")
    if recommended_missing:
        if recommended_missing.get("description"):
            codes.append("MISSING_DESCRIPTION")
        if recommended_missing.get("delivery"):
            codes.append("MISSING_DELIVERY")
    return codes

def build_problem_summary(
    spec_name: str,
    conv_required: bool,
    conv_note: str,
    missing_link_idx: List[int],
    missing_img_idx: List[int],
    missing_price_idx: List[int],
    bad_price_idx: List[int],
    invalid_price_format_idx: List[int],
    bad_url_idx: List[int],
    bad_img_idx: List[int],
    dup_id_pairs: List[Tuple[int, int, str]],
    dup_link_pairs: List[Tuple[int, int, str]],
) -> str:
    parts: List[str] = []

    if spec_name == "UNKNOWN":
        parts.append("Transformation could not be identified")
    elif conv_required:
        parts.append(conv_note or f"{spec_name} needs conversion before FAVI import")

    if missing_link_idx:
        parts.append(f"missing product URLs ({len(missing_link_idx)})")
    if missing_img_idx:
        parts.append(f"missing primary images ({len(missing_img_idx)})")
    if missing_price_idx:
        parts.append(f"missing prices ({len(missing_price_idx)})")
    if bad_price_idx:
        parts.append(f"invalid prices ({len(bad_price_idx)})")
    if invalid_price_format_idx:
        parts.append(f"price format issues ({len(invalid_price_format_idx)})")

    url_issue_count = len(bad_url_idx) + len(bad_img_idx)
    if url_issue_count:
        parts.append(f"URLs needing encoding ({url_issue_count})")
    if dup_id_pairs:
        parts.append(f"duplicate IDs ({len(dup_id_pairs)})")
    if dup_link_pairs:
        parts.append(f"duplicate product URLs ({len(dup_link_pairs)})")

    if not parts:
        return "Feed checked successfully. No obvious issues were detected."
    return "; ".join(parts[:5]).strip().rstrip(".") + "."

def make_clickup_url(payload: Dict[str, Any]) -> str:
    raw = json.dumps(payload, ensure_ascii=False, separators=(",", ":"))
    token = base64.urlsafe_b64encode(raw.encode("utf-8")).decode("ascii").rstrip("=")
    return f"{CLICKUP_FORM_URL}#faviTicket={token}"

# ---------- I/O helpers ----------
_DOWNLOAD_HEADERS = {
    "User-Agent": "Mozilla/5.0 (compatible; FeedChecker/1.0; +https://favi.com)",
    "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
    "Accept-Language": "en-US,en;q=0.5",
    "Accept-Encoding": "gzip, deflate",
}

def download_to_tmp(url: str, chunk=STREAM_CHUNK) -> str:
    """Stream a URL to a temp file (no giant bytes in memory). Returns file path."""
    import requests
    with requests.get(url, stream=True, timeout=REQUEST_TIMEOUT, headers=_DOWNLOAD_HEADERS) as r:
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
            return f.read(2) == b"\x1f\x8b"
    except Exception:
        return False

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
# Flagged (a dot/comma is grouping thousands — FAVI wants spaces or nothing):
#  - "1.234,50" / "1,234.50" (both a dot AND a comma present)
#  - "1.234.567"             (same separator repeated)
#  - More than 2 decimals  → warn: over-precision (will be rounded)
def _extract_first_numeric_token(s: str) -> str:
    # capture the same token parse_price() would see, but keep punctuation to validate format
    m = re.search(r"\d{1,3}(?:[ .,\u00A0]\d{3})+(?:[.,]\d+)?|\d+(?:[.,]\d+)?", s)
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

    has_dot = "." in token
    has_comma = "," in token

    # Both present → a dot or comma is grouping thousands, which FAVI doesn't
    # accept (use spaces, or nothing). A lone dot OR a lone comma is an
    # unambiguous decimal separator and is fine.
    if has_dot and has_comma:
        return False, False, "uses both '.' and ',' — keep one decimal separator (spaces for thousands)"

    core = token.replace(" ", "")  # spaces are thousands grouping; drop them
    sep = "." if has_dot else ("," if has_comma else "")

    if sep:
        # A decimal separator appears once; repeated means it's grouping
        # thousands (e.g. "1.234.567").
        if core.count(sep) > 1:
            return False, False, f"'{sep}' used to group thousands — use spaces or nothing"
        int_part, dec_part = core.split(sep)
        if not int_part.isdigit() or not dec_part.isdigit():
            return False, False, "not a numeric value"
        return True, len(dec_part) > 2, ""

    if not core.isdigit():
        return False, False, "not a numeric value"
    return True, False, ""

# ---------- Form ----------
with st.form("input"):
    url = st.text_input("Feed URL", placeholder="https://example.com/feed.xml")
    up = st.file_uploader("…or upload an XML file (.xml or .xml.gz)", type=["xml", "gz"])

    # Power-user knobs tucked away so the default flow is just paste-and-check.
    with st.expander("Advanced options"):
        scope = st.selectbox("Processing scope", ["Auto (full)", "Sample first N items"])
        n_limit = None
        if scope == "Sample first N items":
            n_limit = st.number_input("Sample size (items)", min_value=100, max_value=200_000, value=5_000, step=500)
        stop_on_first_parse_error = st.checkbox("Stop on first XML parse error", value=True)

    submitted = st.form_submit_button("Check feed", type="primary", use_container_width=True)

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

# Recommended / content elements (description, category, delivery/shipping, …):
# per-field index lists, plus the set of items missing at least one.
recommended_missing: Dict[str, List[int]] = {f["key"]: [] for f in RECOMMENDED_FIELDS}
recommended_gap_idx: List[int] = []

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

    # Recommended / content elements (FAVI-documented; non-blocking)
    if RECOMMENDED_FIELDS:
        present_rec = present_recommended_fields(elem)
        missing_any = False
        for f in RECOMMENDED_FIELDS:
            if f["key"] not in present_rec:
                recommended_missing[f["key"]].append(index)
                missing_any = True
        if missing_any:
            recommended_gap_idx.append(index)

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
        with open_maybe_gzip(src_path) as fh:
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
def _detect_spec_from_prefix(path: str, prefix_bytes: int = 262144) -> str:
    """
    Read a small prefix of the file into a mini-DOM for spec detection.
    We wrap the snippet in a synthetic root if needed so ElementTree can parse it.
    Falls back to root-tag-only detection if the snippet is not well-formed.
    """
    import io
    with open_maybe_gzip(path) as fh:
        raw = fh.read(prefix_bytes)
    # Try full parse of the prefix (works if feed is tiny or prefix captures whole root)
    try:
        root = ET.fromstring(raw)
        return detect_spec(root) or "UNKNOWN"
    except ET.ParseError:
        pass
    # Prefix is incomplete XML — wrap it so we at least get the root + a few items
    try:
        # Close any open tags by appending a dummy close; won't be valid but gives
        # us enough structure for iterparse to yield several start events.
        context = ET.iterparse(io.BytesIO(raw), events=("start",))
        root_elem = None
        items_seen = 0
        for _, elem in context:
            if root_elem is None:
                root_elem = elem
            items_seen += 1
            if items_seen >= 200:   # enough children for reliable detection
                break
        if root_elem is not None:
            spec = detect_spec(root_elem)
            if spec and spec.upper() != "UNKNOWN":
                return spec
    except Exception:
        pass
    return "UNKNOWN"


def run_stream_path(limit: int | None):
    global xml_ok, spec_name, total_items, processed_items
    try:
        # Quick root/spec detection using a small prefix (captures root + a few items)
        spec_name_local = _detect_spec_from_prefix(src_path)
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

total_dups = len(dup_id_pairs) + len(dup_link_pairs)
url_issues = len(bad_url_idx) + len(bad_img_idx)
items_value = f"{processed_items} / {total_items}" if use_sample_mode else f"{total_items}"
short_format = spec_name.split(" (")[0].split(" / ")[0].strip() if spec_name != "UNKNOWN" else "UNKNOWN"

render_metric_row([
    ("Format", short_format, "brand" if spec_name != "UNKNOWN" else "muted"),
    ("Items", items_value, "default"),
    ("Duplicates", total_dups, "error" if total_dups > 0 else "ok"),
    ("Missing IDs", len(missing_id_idx), "error" if missing_id_idx else "ok"),
    ("URL issues", url_issues, "error" if url_issues > 0 else "ok"),
    ("Recommended gaps", len(recommended_gap_idx),
     "warn" if recommended_gap_idx else "ok",
     "items missing ≥1 element" if recommended_gap_idx else None),
])

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
pass_fail["Product URL validity (no spaces/special chars)"] = (
    len(bad_url_idx) == 0,
    False,
    f"(issues: {len(bad_url_idx)})" if len(bad_url_idx) > 0 else ""
)
pass_fail["Image URL validity (no spaces/special chars)"] = (
    len(bad_img_idx) == 0,
    False,
    f"(issues: {len(bad_img_idx)})" if len(bad_img_idx) > 0 else ""
)

# PRICE summary lines (all as WARNs, never FAIL)
pass_fail["Price present"] = (True, len(missing_price_idx) > 0, f"(missing: {len(missing_price_idx)})")
pass_fail["Price numeric > 0"] = (True, len(bad_price_idx) > 0, f"(invalid: {len(bad_price_idx)})")
pass_fail["Price format follows FAVI rules"] = (True, len(invalid_price_format_idx) > 0,
                                               f"(invalid format: {len(invalid_price_format_idx)})")
pass_fail["Price precision (<= 2 decimals)"] = (True, len(overprecision_price_idx) > 0,
                                               f"(over-precision: {len(overprecision_price_idx)})")

# Recommended / content elements (all WARN — never FAIL)
for _f in RECOMMENDED_FIELDS:
    _miss = recommended_missing.get(_f["key"], [])
    _tag = "FAVI required" if _f["required"] else "recommended"
    pass_fail[f"{_f['label']} present ({_tag})"] = (
        True, len(_miss) > 0, f"(missing: {len(_miss)})"
    )

# Check FAVI compatibility
favi_compat = is_favi_compatible(spec_name)
conv_required, conv_note = needs_conversion(spec_name)
pass_fail["FAVI direct compatibility"] = (favi_compat, not favi_compat and not conv_required, 
                                          conv_note if conv_required else "")

st.markdown("---")

# Show FAVI compatibility warning if needed
if conv_required:
    st.error(f"""
    **FAVI cannot parse this feed directly**
    
    **Detected format:** {spec_name}
    
    **Reason:** {conv_note}
    
    **Solution:** Use the AWS Lambda feed transformer to convert this feed to Google Shopping or element-based Ceneje format before importing to FAVI.
    """)
elif not favi_compat:
    st.warning(f"""
    **Feed format may not be fully compatible with FAVI**
    
    **Detected format:** {spec_name}
    
    Consider converting this feed to Google Shopping format for best compatibility.
    """)

# ---------- CLICKUP DRAFT ----------
problem_codes = build_problem_codes(
    xml_ok=xml_ok,
    spec_name=spec_name,
    conv_required=conv_required,
    missing_id_idx=missing_id_idx,
    missing_link_idx=missing_link_idx,
    missing_img_idx=missing_img_idx,
    missing_avail_idx=missing_avail_idx,
    missing_price_idx=missing_price_idx,
    bad_price_idx=bad_price_idx,
    invalid_price_format_idx=invalid_price_format_idx,
    bad_url_idx=bad_url_idx,
    bad_img_idx=bad_img_idx,
    dup_id_pairs=dup_id_pairs,
    dup_link_pairs=dup_link_pairs,
    recommended_missing=recommended_missing,
)

source_url = url.strip() if url.strip().lower().startswith(("http://", "https://")) else ""
draft_defaults = {
    "shop_name": infer_shop_name(source_url or src_label),
    "country": infer_country(source_url),
    "issue_request_crm": build_problem_summary(
        spec_name=spec_name,
        conv_required=conv_required,
        conv_note=conv_note,
        missing_link_idx=missing_link_idx,
        missing_img_idx=missing_img_idx,
        missing_price_idx=missing_price_idx,
        bad_price_idx=bad_price_idx,
        invalid_price_format_idx=invalid_price_format_idx,
        bad_url_idx=bad_url_idx,
        bad_img_idx=bad_img_idx,
        dup_id_pairs=dup_id_pairs,
        dup_link_pairs=dup_link_pairs,
    ),
    "input_xml_feed_url": source_url,
    "input_feed_format": map_clickup_format(spec_name),
}
draft_seed = json.dumps(
    {
        "src_label": src_label,
        "source_url": source_url,
        "spec_name": spec_name,
        "problem_codes": problem_codes,
        "processed_items": processed_items,
    },
    sort_keys=True,
)
if st.session_state.get("clickup_draft_seed") != draft_seed:
    for field_name, value in draft_defaults.items():
        st.session_state[f"clickup_{field_name}"] = value
    st.session_state["clickup_draft_seed"] = draft_seed
    st.session_state["clickup_editor_open"] = False

summary_col, clickup_col = st.columns([1.35, 1], gap="large")

with summary_col:
    summarize(pass_fail)

# ---------- DETAILS ----------
def safe_get(lst, i, default=""):
    try:
        return lst[i]
    except Exception:
        return default

has_detail_issues = any([
    missing_id_idx, missing_link_idx, missing_img_idx, missing_avail_idx,
    missing_price_idx, bad_price_idx, invalid_price_format_idx, overprecision_price_idx,
    bad_url_idx, bad_img_idx, dup_id_pairs, dup_link_pairs,
])
st.markdown("---")
if has_detail_issues:
    st.subheader("Details")
else:
    st.success("No issues in the core checks — IDs, URLs, images, prices, and availability all look good.")

# Missing fields
missing_id_rows = [
    {"id": "(missing)", "link": safe_get(links, i), "image": "yes" if safe_get(images, i) else "no", "availability": safe_get(avails, i) or "(missing)"}
    for i in missing_id_idx
]
show_issue_table("Missing ID (by example values)", missing_id_rows)

missing_link_rows = [
    {"id": safe_get(ids, i), "link": "(missing)", "image": "yes" if safe_get(images, i) else "no", "availability": safe_get(avails, i) or "(missing)"}
    for i in missing_link_idx if safe_get(ids, i)
]
show_issue_table("Missing Product URL (by product ID)", missing_link_rows)

missing_img_rows = [
    {"id": safe_get(ids, i), "link": safe_get(links, i), "primary_image": "(missing)"}
    for i in missing_img_idx if safe_get(ids, i)
]
show_issue_table("Missing Primary Image (by product ID)", missing_img_rows)

missing_avail_rows = [
    {"id": safe_get(ids, i), "link": safe_get(links, i), "availability": "(missing)"}
    for i in missing_avail_idx if safe_get(ids, i)
]
show_issue_table("Missing Availability (by product ID)", missing_avail_rows)

# PRICE details
missing_price_rows = [
    {"id": safe_get(ids, i) or "(missing id)", "link": safe_get(links, i), "raw_price": "(missing)"}
    for i in missing_price_idx if safe_get(ids, i)
]
show_issue_table("Missing Price (by product ID)", missing_price_rows)

bad_price_rows = [
    {"id": safe_get(ids, i) or "(missing id)",
     "link": safe_get(links, i),
     "raw_price": safe_get(prices_raw, i) or "(missing)",
     "parsed_amount": safe_get(prices_amt, i),
    }
    for i in bad_price_idx
]
show_issue_table("Invalid Price (non-numeric or <= 0) by product ID", bad_price_rows)

invalid_format_rows = [
    {"id": safe_get(ids, i) or "(missing id)",
     "link": safe_get(links, i),
     "raw_price": safe_get(prices_raw, i) or "(missing)",
     "note": "Format not allowed by FAVI (dot-as-thousands/comma-as-thousands/etc.)"}
    for i in invalid_price_format_idx
]
show_issue_table("Price format violations (FAVI rules)", invalid_format_rows)

overprecision_rows = [
    {"id": safe_get(ids, i) or "(missing id)",
     "link": safe_get(links, i),
     "raw_price": safe_get(prices_raw, i) or "(missing)",
     "note": "More than 2 decimals — FAVI rounds automatically"}
    for i in overprecision_price_idx
]
show_issue_table("Price over-precision (> 2 decimals) informational", overprecision_rows)

if bad_url_idx or bad_img_idx:
    st.markdown("### URL encoding issues")
    st.warning(
        "URLs with spaces or non-ASCII characters can cause feed rejection — they should be "
        "percent-encoded. The feed transformer normally handles this, but it's worth verifying the output."
    )

bad_url_rows = [
    {"id": safe_get(ids, i) or "(missing id)", 
     "raw_url": safe_get(raw_links, i), 
     "encoded_url": safe_get(links, i),
     "issue": "Non-ASCII chars" if not ascii_only.match(safe_get(raw_links, i)) else "Contains spaces"}
    for i in bad_url_idx
]
show_issue_table("Product URLs requiring encoding", bad_url_rows)

bad_img_rows = [
    {"id": safe_get(ids, i) or "(missing id)", 
     "raw_image_url": safe_get(raw_imgs, i), 
     "encoded_image_url": safe_get(images, i),
     "issue": "Non-ASCII chars" if not ascii_only.match(safe_get(raw_imgs, i)) else "Contains spaces"}
    for i in bad_img_idx
]
show_issue_table("Image URLs requiring encoding", bad_img_rows)

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
show_issue_table("Duplicate IDs (grouped)", dup_id_rows)

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
show_issue_table("Duplicate Product URLs (grouped, with IDs)", dup_url_rows)

# ---------- RECOMMENDED ELEMENTS ----------
if RECOMMENDED_FIELDS:
    st.markdown("---")
    st.subheader("Recommended elements")
    st.caption(
        "Beyond the core ID/URL/image/price checks, FAVI documents these element "
        "requirements at help.favionline.com/en/meanings-and-requirements-for-individual-elements. "
        "Items below are missing the element — '(FAVI required)' ones are mandatory for FAVI's "
        "native format, the rest are recommended to improve listing quality and conversions."
    )
    any_recommended_missing = False
    for f in RECOMMENDED_FIELDS:
        miss = recommended_missing.get(f["key"], [])
        if not miss:
            continue
        any_recommended_missing = True
        tag = "FAVI required" if f["required"] else "recommended"
        rec_rows = [
            {"id": safe_get(ids, i) or "(missing id)", "link": safe_get(links, i)}
            for i in miss
        ]
        show_issue_table(f"Missing {f['label']} ({tag})", rec_rows)
    if not any_recommended_missing:
        st.success("All recommended elements are present on every item checked.")

st.markdown("---")
st.caption(
    ("Scope: Sample first N items (streaming)" if use_sample_mode else
     f"Scope: Auto (parser: {'Streaming' if (is_gzip_path(src_path) or file_size>SMALL_SIZE_LIMIT) else 'DOM'})")
)
st.markdown("© 2025 Raul Bertoldini")
