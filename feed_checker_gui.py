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
st.caption("Streaming-safe version. Uses iterparse for huge feeds; small feeds still OK.")

# ---------- Small helpers ----------
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
    # color: green #16a34a, red #dc2626, gray #6b7280, amber #f59e0b
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

def download_to_tmp(url: str, chunk=1<<20) -> str:
    """Stream a URL to a temp file (no giant bytes in memory). Returns file path."""
    import requests
    with requests.get(url, stream=True, timeout=120, headers={"User-Agent":"FeedChecker/GUI"}) as r:
        r.raise_for_status()
        size = int(r.headers.get("Content-Length") or 0)
        if size and size > 200*1024*1024:
            st.warning(f"Large feed detected: ~{size/1024/1024:.0f} MB. Switching to streaming parser.")
        suffix = ".xml.gz" if r.headers.get("Content-Type","").lower().endswith("gzip") or url.lower().endswith(".gz") else ".xml"
        tmp = tempfile.NamedTemporaryFile(delete=False, suffix=suffix)
        for chunk_bytes in r.iter_content(chunk_size=chunk):
            if chunk_bytes:
                tmp.write(chunk_bytes)
        tmp.flush()
        tmp.close()
        return tmp.name

def filelike_from_upload(up) -> io.BufferedReader:
    """Persist an uploaded file to disk to allow iterparse to stream it."""
    # Try to infer .gz by magic header
    head = up.read(2)
    up.seek(0)
    is_gz = head == b"\x1f\x8b"
    suffix = ".xml.gz" if is_gz or (up.name and up.name.lower().endswith(".gz")) else ".xml"
    tmp = tempfile.NamedTemporaryFile(delete=False, suffix=suffix)
    tmp.write(up.read())
    tmp.flush(); tmp.close()
    up.seek(0)
    return open(tmp.name, "rb")

def open_maybe_gzip(path_or_fileobj):
    """Return a binary file-like object that yields decompressed bytes if .gz."""
    if isinstance(path_or_fileobj, (io.BytesIO, io.BufferedReader)):
        # Peek gzip magic
        pos = path_or_fileobj.tell()
        magic = path_or_fileobj.read(2)
        path_or_fileobj.seek(pos)
        if magic == b"\x1f\x8b":
            return gzip.open(path_or_fileobj, "rb")
        return path_or_fileobj
    # else it's a path
    if str(path_or_fileobj).lower().endswith(".gz"):
        return gzip.open(path_or_fileobj, "rb")
    return open(path_or_fileobj, "rb")

def iter_items_stream(file_like, guess_item_tags: Iterable[str]=("item","product","offer","entry")):
    """
    Stream items with ET.iterparse. Yields (elem, root) for each end-event of an item-like tag.
    Caller MUST use/read and then call elem.clear(), and occasionally root.clear().
    """
    context = ET.iterparse(file_like, events=("start","end"))
    event, root = next(context)  # grab root for spec detection and clearing
    # Detect item tags by suffix match (namespace-safe)
    def is_item_tag(tag: str) -> bool:
        if not tag:
            return False
        bare = tag.split('}',1)[1] if '}' in tag else tag
        return bare in guess_item_tags
    for event, elem in context:
        if event == "end" and is_item_tag(elem.tag):
            yield elem, root
            # free processed subtree
            elem.clear()
            # light-touch root clear to drop siblings already processed
            if len(root) > 0 and root[0] is not None:
                # stdlib ElementTree doesn't have getprevious(); safe to clear root periodically
                pass

# ---------- Form ----------
with st.form("input"):
    url = st.text_input("Feed URL (http/https)", placeholder="https://example.com/feed.xml")
    up = st.file_uploader("…or upload an XML file (.xml or .xml.gz)", type=["xml", "gz"])
    colA, colB, colC = st.columns(3)
    with colA:
        sample_show = st.number_input("Show up to N sample issues per category", 1, 50, 10)
    with colB:
        mode = st.selectbox("Parse mode", ["Lite (sample items)", "Full (process all items)"])
    with colC:
        stop_on_first_parse_error = st.checkbox("Stop on XML parse error", value=True)
    submitted = st.form_submit_button("Check feed")

if not submitted:
    st.markdown("© 2025 Raul Bertoldini")
    st.stop()

# 1) Obtain a file-like source (path or handle), streaming-friendly
src_label = None
path_or_handle: Any = None
if url.strip():
    if not url.lower().startswith(("http://", "https://")):
        st.error("URL must start with http:// or https://")
        st.stop()
    try:
        path_or_handle = download_to_tmp(url.strip())
        src_label = url.strip()
    except Exception as e:
        st.error(f"Failed to download URL: {e}")
        st.stop()
elif up is not None:
    try:
        fh = filelike_from_upload(up)
        path_or_handle = fh  # handle
        src_label = up.name
    except Exception as e:
        st.error(f"Failed to read uploaded file: {e}")
        st.stop()
else:
    st.warning("Provide a URL or upload a file.")
    st.stop()

st.write(f"**Source:** `{src_label}`")

# 2) Open (with transparent gzip if needed) and bootstrap XML
try:
    f = open_maybe_gzip(path_or_handle)
except Exception as e:
    st.error(f"Failed opening source: {e}")
    st.stop()

# We will:
#  - do a tiny, safe read to assert the XML is parseable;
#  - then stream with iterparse and compute metrics on the fly.

# ---- Bootstrap root + spec via a small iterparse priming ----
xml_ok = True
spec_name = "UNKNOWN"
total_items = 0

# Collections (same names as your previous code)
ids: List[str] = []
links: List[str] = []              # encoded product links
images: List[str] = []             # encoded primary image
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

# How many items to process in Lite mode (guardrail)
LITE_LIMIT = 30000

def process_item(elem, index: int, spec: str):
    """Extract and update all lists/counters for a single item element."""
    # Safe/encoded for core checks
    pid = (read_id(elem, spec) or "").strip()
    purl = (read_link(elem, spec) or "").strip()
    pav  = (read_availability(elem, spec) or "").strip()
    pimg = (gather_primary_image(elem, spec) or "").strip()
    # RAW values only for 'bad URL' warnings
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

try:
    # We need spec_name for read_* helpers. We'll peek root from a fresh iterparse
    # Note: create a new handle because iterparse consumes the stream.
    def open_again():
        if isinstance(path_or_handle, (str, os.PathLike)):
            return open_maybe_gzip(path_or_handle)
        # uploaded handle
        try:
            path_or_handle.seek(0)
        except Exception:
            pass
        return open_maybe_gzip(path_or_handle)

    # Pass 1: detect spec_name quickly
    with open_again() as fh1:
        context1 = ET.iterparse(fh1, events=("start",))
        _, root_first = next(context1)  # first start is root
        spec_name = detect_spec(root_first) or "UNKNOWN"
        # If we got here, XML is at least syntactically OK up to root
        st.success("XML syntax: OK")

    # Pass 2: real streaming pass to process items
    processed = 0
    limit = None if mode == "Full (process all items)" else LITE_LIMIT
    with open_again() as fh2:
        for elem, root in iter_items_stream(fh2):
            total_items += 1
            # Skip processing after limit in Lite mode, but keep counting items for display
            if limit is not None and processed >= limit:
                continue
            process_item(elem, processed, spec_name)
            processed += 1

except ET.ParseError as e:
    xml_ok = False
    st.error(f"XML syntax: ERROR — {e}")
    if stop_on_first_parse_error:
        st.stop()

# ---------- TOP ROW ----------
st.markdown("---")
c1, c2, c3, c4, c5 = st.columns([1, 1, 1, 1, 1], gap="large")

with c1:
    if spec_name != "UNKNOWN":
        status_pill(f"Transformation: {spec_name}", "#16a34a")
    else:
        status_pill("Transformation: UNKNOWN", "#6b7280")

with c2:
    # Show processed count (Lite) vs total seen
    label_items = f"{total_items}"
    if mode.startswith("Lite"):
        label_items = f"{min(total_items, len(ids))} / {total_items}"
    status_pill(f"Items: {label_items}", "#16a34a")

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

# When Lite mode is on, indices refer to processed set (0..processed-1)
# --- Missing fields ---
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

# --- Bad URL warnings (RAW) ---
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

# --- Duplicates (IDs) ---
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

# --- Duplicates (URLs) ---
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
st.caption(("Mode: **Lite** shows processed/total item counts and caps processing at "
            f"{LITE_LIMIT:,} items. Use **Full** to scan everything (may take long)."))

st.markdown("© 2025 Raul Bertoldini")



