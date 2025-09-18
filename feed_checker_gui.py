# feed_checker_gui.py
from __future__ import annotations
from typing import List, Tuple, Dict
import re
from collections import defaultdict
import streamlit as st

# Shared rules/helpers from your feed_specs.py
from feed_specs import (
    detect_spec,
    get_item_nodes,
    read_id,
    read_link,
    read_availability,
    gather_primary_image,
    read_link_raw,                 # NEW
    gather_primary_image_raw,      # NEW
)

# Safe XML parsing (defusedxml if present)
try:
    from defusedxml import ElementTree as ET  # type: ignore
except Exception:
    import xml.etree.ElementTree as ET  # type: ignore

st.set_page_config(page_title="Feed Checker (GUI)", layout="wide")
st.title("🧪 Feed Checker (GUI)")
st.caption("Uses the shared rules from feed_specs.py so Checker and Fixer always stay in sync.")

# ---------- Small helpers ----------
def fetch_bytes_from_url(u: str) -> bytes:
    import requests
    r = requests.get(u, headers={"User-Agent": "FeedChecker/GUI"}, timeout=45)
    r.raise_for_status()
    return r.content

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

def show_sample(title: str, indices: List[int], sample_n: int = 10, red: bool = False):
    """(Kept for reference; now we prefer ID-based tables below.)"""
    if not indices:
        st.write(f"**{title}:** none 🎉")
        return
    st.write(f"**{title}:** {len(indices)}")
    with st.expander(f"Show first {min(sample_n, len(indices))}"):
        subset = indices[:sample_n]
        if red:
            st.markdown(
                "<ul style='margin-top:0'>"
                + "".join(f"<li style='color:#dc2626'>item index {i}</li>" for i in subset)
                + "</ul>",
                unsafe_allow_html=True,
            )
        else:
            st.write(subset)

def is_bad_url(url: str) -> bool:
    """
    Warn if a URL contains spaces or non-ASCII characters (e.g., č, ę, кириллица).
    Note: We only warn; we don't fail the feed on this.
    """
    if not url:
        return False
    if re.search(r"\s", url):
        return True
    if any(ord(ch) > 127 for ch in url):
        return True
    return False

# New helpers for ID-based tables

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

# ---------- Form ----------
with st.form("input"):
    url = st.text_input("Feed URL (http/https)", placeholder="https://example.com/feed.xml")
    up = st.file_uploader("…or upload an XML file", type=["xml"])
    colA, colB = st.columns(2)
    with colA:
        sample_show = st.number_input("Show up to N sample issues per category", 1, 50, 10)
    with colB:
        stop_on_first_parse_error = st.checkbox("Stop on XML parse error", value=True)
    submitted = st.form_submit_button("Check feed")

if submitted:
    # 1) Load bytes
    xml_bytes = None
    src_label = None
    if url.strip():
        if not url.lower().startswith(("http://", "https://")):
            st.error("URL must start with http:// or https://")
            st.stop()
        try:
            xml_bytes = fetch_bytes_from_url(url.strip())
            src_label = url.strip()
        except Exception as e:
            st.error(f"Failed to download URL: {e}")
            st.stop()
    elif up is not None:
        xml_bytes = up.read()
        src_label = up.name
    else:
        st.warning("Provide a URL or upload a file.")
        st.stop()

    st.write(f"**Source:** `{src_label}`")

    # 2) Parse XML
    xml_ok = True
    try:
        root = ET.fromstring(xml_bytes)
        st.success("XML syntax: OK")
    except ET.ParseError as e:
        xml_ok = False
        st.error(f"XML syntax: ERROR — {e}")
        if stop_on_first_parse_error:
            st.stop()

    # 3) Detect transformation
    spec_name = detect_spec(root) if xml_ok else "UNKNOWN"

    # 4) Collect items & run checks
    items = get_item_nodes(root, spec_name) if spec_name != "UNKNOWN" else []
    total_items = len(items)

    ids: List[str] = []
    links: List[str] = []              # encoded product links (used for duplicates, display)
    images: List[str] = []             # encoded primary image (display)
    avails: List[str] = []             # availability

    missing_id_idx: List[int] = []
    missing_link_idx: List[int] = []
    missing_img_idx: List[int] = []
    missing_avail_idx: List[int] = []

    # NEW: raw URLs for warning checks
    raw_links: List[str] = []          # raw product links (for whitespace/non-ASCII warning)
    raw_imgs: List[str] = []           # raw primary image URLs (for whitespace/non-ASCII warning)
    bad_url_idx: List[int] = []        # indices with suspicious product URLs (raw)
    bad_img_idx: List[int] = []        # indices with suspicious image URLs (raw)

    # Track duplicates with first-seen index
    id_first_seen: Dict[str, int] = {}
    link_first_seen: Dict[str, int] = {}
    dup_id_pairs: List[Tuple[int, int, str]] = []
    dup_link_pairs: List[Tuple[int, int, str]] = []

    ascii_only = re.compile(r'^[\x00-\x7F]+$')  # ASCII-only check for warnings

    for i, it in enumerate(items):
        # encoded (safe) versions for normal checks
        pid = (read_id(it, spec_name) or "").strip()
        purl = (read_link(it, spec_name) or "").strip()
        pav  = (read_availability(it, spec_name) or "").strip()
        pimg = (gather_primary_image(it, spec_name) or "").strip()

        # RAW versions only for “bad URL” detection
        purl_raw = (read_link_raw(it, spec_name) or "").strip()
        pimg_raw = (gather_primary_image_raw(it, spec_name) or "").strip()

        ids.append(pid)
        links.append(purl)
        images.append(pimg)
        avails.append(pav)

        raw_links.append(purl_raw)
        raw_imgs.append(pimg_raw)

        if not pid:
            missing_id_idx.append(i)
        if not purl:
            missing_link_idx.append(i)
        if not pimg:
            missing_img_idx.append(i)
        if not pav:
            missing_avail_idx.append(i)

        # --- Bad URL warnings (spaces or non-ASCII) on RAW values only
        if purl_raw and ((" " in purl_raw) or not ascii_only.match(purl_raw)):
            bad_url_idx.append(i)
        if pimg_raw and ((" " in pimg_raw) or not ascii_only.match(pimg_raw)):
            bad_img_idx.append(i)

        # duplicates (id)
        if pid:
            if pid in id_first_seen:
                dup_id_pairs.append((id_first_seen[pid], i, pid))
            else:
                id_first_seen[pid] = i

        # duplicates (link) — use ENCODED links to avoid false dupes due to encoding
        if purl:
            if purl in link_first_seen:
                dup_link_pairs.append((link_first_seen[purl], i, purl))
            else:
                link_first_seen[purl] = i

    total_dups = len(dup_id_pairs) + len(dup_link_pairs)

    # >>> IMPORTANT: compute warnings AFTER lists are filled <<<
    total_warnings = (
        len(missing_link_idx)
        + len(missing_img_idx)
        + len(missing_avail_idx)
        + len(bad_url_idx)
        + len(bad_img_idx)
    )
    any_warnings = total_warnings > 0

    # ---------- TOP ROW ----------
    st.markdown("---")
    c1, c2, c3, c4, c5 = st.columns([1, 1, 1, 1, 1], gap="large")

    with c1:
        if spec_name != "UNKNOWN":
            status_pill(f"Transformation: {spec_name}", "#16a34a")  # green
        else:
            status_pill("Transformation: UNKNOWN", "#6b7280")       # gray

    with c2:
        status_pill(f"Items: {total_items}", "#16a34a")             # always green

    with c3:
        if total_dups > 0:
            status_pill(f"Duplicates: {total_dups}", "#dc2626")
        else:
            status_pill("Duplicates: 0", "#16a34a")

    with c4:
        if len(missing_id_idx) > 0:
            status_pill(f"Missing IDs: {len(missing_id_idx)}", "#dc2626")  # RED if any
        else:
            status_pill("Missing IDs: 0", "#16a34a")

    with c5:
        if any_warnings:
            status_pill(f"Warnings: {total_warnings}", "#f59e0b")   # amber/orange if any
        else:
            status_pill("Warnings: 0", "#16a34a")


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
    # New: URL validity warnings (bad = spaces or non-ASCII)
    pass_fail["Product URL validity"] = (True, len(bad_url_idx) > 0, f"(bad: {len(bad_url_idx)})")
    pass_fail["Image URL validity"] = (True, len(bad_img_idx) > 0, f"(bad: {len(bad_img_idx)})")

    st.markdown("---")
    summarize(pass_fail)

    # ---------- DETAILS (ID-based) ----------
    st.markdown("---")
    st.subheader("Details")

    # --- Missing fields ---
    # When ID is missing we can't show an ID; show link/image/availability to locate the row.
    missing_id_rows = [
        {
            "id": "(missing)",
            "link": links[i],
            "image": "yes" if images[i] else "no",
            "availability": avails[i] or "(missing)"
        }
        for i in missing_id_idx
    ]
    show_issue_table("Missing ID (by example values)", missing_id_rows, sample_show)

    missing_link_rows = [
        {"id": ids[i], "link": "(missing)", "image": "yes" if images[i] else "no", "availability": avails[i] or "(missing)"}
        for i in missing_link_idx if ids[i]
    ]
    show_issue_table("Missing Product URL (by product ID)", missing_link_rows, sample_show)

    missing_img_rows = [
        {"id": ids[i], "link": links[i], "primary_image": "(missing)"}
        for i in missing_img_idx if ids[i]
    ]
    show_issue_table("Missing Primary Image (by product ID)", missing_img_rows, sample_show)

    missing_avail_rows = [
        {"id": ids[i], "link": links[i], "availability": "(missing)"}
        for i in missing_avail_idx if ids[i]
    ]
    show_issue_table("Missing Availability (by product ID)", missing_avail_rows, sample_show)

    # --- Bad URL warnings (RAW) ---
    bad_url_rows = [
        {"id": ids[i] or "(missing id)", "raw_url": raw_links[i], "encoded_url": links[i]}
        for i in bad_url_idx
    ]
    show_issue_table("Bad Product URLs (spaces/non-ASCII) — RAW view", bad_url_rows, sample_show)

    bad_img_rows = [
        {"id": ids[i] or "(missing id)", "raw_image_url": raw_imgs[i], "encoded_image_url": images[i]}
        for i in bad_img_idx
    ]
    show_issue_table("Bad Image URLs (spaces/non-ASCII) — RAW view", bad_img_rows, sample_show)

    # --- Duplicates (IDs) -> show where they appear + example links ---
    dup_ids_map: Dict[str, List[int]] = defaultdict(list)
    for old_i, new_i, pid in dup_id_pairs:
        dup_ids_map[pid].extend([old_i, new_i])

    dup_id_rows = []
    for pid, idxs in dup_ids_map.items():
        idxs_u = unique_preserve([str(x) for x in idxs])
        # example links from occurrences
        ex_links = unique_preserve([links[int(i)] for i in idxs_u if int(i) < len(links) and links[int(i)]])[:3]
        dup_id_rows.append({
            "id": pid,
            "occurrences": len(unique_preserve([str(int(i)) for i in idxs])),
            "example_links": " | ".join(ex_links) if ex_links else ""
        })
    dup_id_rows.sort(key=lambda r: (-r["occurrences"], r["id"]))
    show_issue_table("Duplicate IDs (grouped)", dup_id_rows, sample_show)

    # --- Duplicates (URLs) -> show which IDs share the same URL ---
    url_to_ids: Dict[str, List[str]] = defaultdict(list)
    for old_i, new_i, url in dup_link_pairs:
        if old_i < len(ids) and ids[old_i]:
            url_to_ids[url].append(ids[old_i])
        if new_i < len(ids) and ids[new_i]:
            url_to_ids[url].append(ids[new_i])

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

st.markdown("© 2025 Raul Bertoldini")


