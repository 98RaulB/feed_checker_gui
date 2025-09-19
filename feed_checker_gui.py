# feed_checker_gui.py
from __future__ import annotations
from typing import List, Tuple, Dict, Any, Iterable
import re
from collections import defaultdict
import os
import io
import gzip
import tempfile
import streamlit as st

# Shared rules/helpers from your feed_specs.py
from feed_specs import (
    detect_spec,
    get_item_nodes,               # used in DOM path
    read_id,
    read_link,
    read_availability,
    gather_primary_image,
    read_link_raw,                 # RAW (no percent-encoding) to warn on spaces/non-ASCII
    gather_primary_image_raw,      # RAW
)

# Safe XML parsing (defusedxml if present)
try:
    from defusedxml import ElementTree as ET  # type: ignore
except Exception:
    import xml.etree.ElementTree as ET  # type: ignore

st.set_page_config(page_title="Feed Checker (GUI)", layout="wide")
st.title("🧪 Feed Checker (GUI)")
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

# ---------- Tag matching helpers (prevents 'Items: 0' when tag name differs) ----------
def localname(tag: str) -> str:
    return tag.split('}', 1)[1] if '}' in tag else tag

# Very forgiving defaults
DEFAULT_ITEM_TAGS = {
    # lower/upper/common
    "item","ITEM","product","PRODUCT","offer","OFFER","entry","ENTRY",
    # marketplace/feed aliases
    "shopitem","SHOPITEM","offeritem","OFFERITEM","productitem","PRODUCTITEM",
    "shopItem","ShopItem","Product","Offer","Entry",
}

# Spec-to-tag mapping (tune as needed)
ITEM_TAGS_BY_SPEC = {
    # Heureka/Ceneo-like
    "HEUREKA": {"SHOPITEM","shopitem","ShopItem"},
    "CENEO": {"offer","Offer","OFFER","SHOPITEM","shopitem"},
    # Google/Atom/RSS
    "GOOGLE": {"item","entry"},
    "ATOM": {"entry","ENTRY"},
    "RSS": {"item","ITEM"},
    # Skroutz/Compari/Ceneje examples (adjust as encountered)
    "SKROUTZ": {"item","offer","product"},
    "COMPARI": {"product","offer","item"},
    "CENEJE": {"item","offer","product"},
    # Fallback
    "UNKNOWN": set(),
}

def guess_item_tag_set(spec_name: str) -> set[str]:
    base = set(ITEM_TAGS_BY_SPEC.get((spec_name or "UNKNOWN").upper(), set()))
    return (base | set(DEFAULT_ITEM_TAGS))

# ---------- I/O helpers ----------
def download_to_tmp(url: str, chunk=STREAM_CHUNK) -> str:
    """Stream a URL to a temp file (no giant bytes in memory). Returns file path."""
    import requests
    with requests.get(url, stream=True, timeout=REQUEST_TIMEOUT, headers={"User-Agent":"FeedChecker/GUI"}) as r:
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
def iter_items_stream(file_like, guess_item_tags: Iterable[str]):
    """
    Stream items with ET.iterparse. Yields (elem, root) for each end-event of an item-like tag.
    Caller should elem.clear() after processing to free memory.
    """
    want = set(guess_item_tags)
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
    if not url.lower().startswith(("http://", "https://")):
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

missing_id_idx: List[int] = []
missing_link_idx: List[int] = []
missing_img_idx: List[int] = []
missing_avail_idx: List[int] = []

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

    ids.append(pid); links.append(purl); images.append(pimg); avails.append(pav)
    raw_links.append(purl_raw); raw_imgs.append(pimg_raw)

    if not pid: missing_id_idx.append(index)
    if not purl: missing_link_idx.append(index)
    if not pimg: missing_img_idx.append(index)
    if not pav:  missing_avail_idx.append(index)

    if purl_raw and ((" " in purl_raw) or not ascii_only.match(purl_raw)):
        bad_url_idx.append(index)
    if pimg_raw and ((" " in pimg_raw) or not ascii_only.match(pimg_raw)):
        bad_img_idx.append(index)

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

        # Figure out which item tags to look for, and show a small hint
        item_tags = guess_item_tag_set(spec_name)
        st.caption("Looking for item tags: " + ", ".join(sorted(list(item_tags))[:8]) + ("…" if len(item_tags)>8 else ""))

        # Full streaming pass
        processed = 0
        with open_maybe_gzip(src_path) as fh2:
            for elem, root in iter_items_stream(fh2, guess_item_tags=item_tags):
                total_items += 1
                if limit is not None and processed >= limit:
                    # keep counting total_items beyond limit without processing
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



