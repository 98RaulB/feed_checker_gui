# feed_checker_gui.py
from __future__ import annotations
import io
from typing import List, Tuple, Dict
import streamlit as st

# Use the shared spec/rules from feed_specs.py (your file)
from feed_specs import (
    NS,
    detect_spec,
    get_item_nodes,
    read_id,
    read_link,
    read_availability,
    gather_primary_image,
)

# Safe XML parsing (defusedxml if present)
try:
    from defusedxml import ElementTree as ET  # type: ignore
except Exception:
    import xml.etree.ElementTree as ET  # type: ignore

st.set_page_config(page_title="Feed Checker (GUI)", layout="wide")
st.title("🧪 Feed Checker (GUI)")
st.caption("Uses the shared rules from feed_specs.py so Checker and Fixer always stay in sync.")

with st.form("input"):
    url = st.text_input("Feed URL (http/https)", placeholder="https://example.com/feed.xml")
    up = st.file_uploader("…or upload an XML file", type=["xml"])
    colA, colB = st.columns(2)
    with colA:
        sample_show = st.number_input("Show up to N sample issues per category", 1, 50, 10)
    with colB:
        stop_on_first_parse_error = st.checkbox("Stop on XML parse error", value=True)
    submitted = st.form_submit_button("Check feed")

def fetch_bytes_from_url(u: str) -> bytes:
    import requests
    r = requests.get(u, headers={"User-Agent":"FeedChecker/GUI"}, timeout=45)
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
    rows = []
    for k, (ok, warn, extra) in pass_fail.items():
        rows.append(verdict_row(k, ok, warn, extra)[1])
    for k, (ok, warn, extra) in pass_fail.items():
        lbl, text = verdict_row(k, ok, warn, extra)
        st.write(f"- **{lbl}**: {text}")

if submitted:
    # 1) Load bytes
    xml_bytes = None
    src_label = None
    if url.strip():
        if not url.lower().startswith(("http://","https://")):
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

    # 3) Detect transformation (using shared rules)
    if xml_ok:
        spec_name = detect_spec(root)
    else:
        # If parse failed but we continue, detection is meaningless
        spec_name = "UNKNOWN"

    if spec_name == "UNKNOWN":
        st.error("Transformation detection: UNKNOWN (cannot match any strict signature).")
        # Even if unknown, try to show a hint about item counts
        items = list(root.iter()) if xml_ok else []
        st.stop()
    else:
        st.success(f"Transformation detection: **{spec_name}**")

    # 4) Collect items and run checks
    items = get_item_nodes(root, spec_name)
    total_items = len(items)
    st.write(f"Items found: **{total_items}**")

    ids: List[str] = []
    links: List[str] = []
    missing_id_idx: List[int] = []
    missing_link_idx: List[int] = []
    missing_img_idx: List[int] = []
    missing_avail_idx: List[int] = []

    # Track duplicates with first-seen index
    id_first_seen: Dict[str, int] = {}
    link_first_seen: Dict[str, int] = {}
    dup_id_pairs: List[Tuple[int, int, str]] = []
    dup_link_pairs: List[Tuple[int, int, str]] = []

    # Iterate items
    for i, it in enumerate(items):
        pid = (read_id(it, spec_name) or "").strip()
        purl = (read_link(it, spec_name) or "").strip()
        pav  = (read_availability(it, spec_name) or "").strip()
        pimg = (gather_primary_image(it, spec_name) or "").strip()

        ids.append(pid)
        links.append(purl)

        if not pid:
            missing_id_idx.append(i)
        if not purl:
            missing_link_idx.append(i)
        if not pimg:
            missing_img_idx.append(i)
        if not pav:
            missing_avail_idx.append(i)

        # duplicates (id)
        if pid:
            if pid in id_first_seen:
                dup_id_pairs.append((id_first_seen[pid], i, pid))
            else:
                id_first_seen[pid] = i

        # duplicates (link)
        if purl:
            if purl in link_first_seen:
                dup_link_pairs.append((link_first_seen[purl], i, purl))
            else:
                link_first_seen[purl] = i

    # 5) Build results
    pass_fail: Dict[str, Tuple[bool, bool, str]] = {}

    # XML syntax
    pass_fail["XML syntax"] = (xml_ok, False, "")

    # Transformation detection
    pass_fail["Transformation detected"] = (spec_name != "UNKNOWN", False, spec_name if spec_name != "UNKNOWN" else "")

    # IDs present (strict: fail if any missing)
    pass_fail["IDs present"] = (len(missing_id_idx) == 0, False, f"(missing: {len(missing_id_idx)})")

    # Duplicate IDs (fail if any)
    pass_fail["Duplicate IDs"] = (len(dup_id_pairs) == 0, False, f"(duplicates: {len(dup_id_pairs)})")

    # Duplicate Product URLs (fail if any)
    pass_fail["Duplicate Product URLs"] = (len(dup_link_pairs) == 0, False, f"(duplicates: {len(dup_link_pairs)})")

    # Product URL present (warn if missing)
    pass_fail["Product URL present"] = (True, len(missing_link_idx) > 0, f"(missing: {len(missing_link_idx)})")

    # Primary image present (warn if missing)
    pass_fail["Primary image present"] = (True, len(missing_img_idx) > 0, f"(missing: {len(missing_img_idx)})")

    # Availability present (warn if missing)
    pass_fail["Availability present"] = (True, len(missing_avail_idx) > 0, f"(missing: {len(missing_avail_idx)})")

    # 6) Summary
    st.markdown("---")
    summarize(pass_fail)

    # 7) Details
    st.markdown("---")
    st.subheader("Details")

    def show_sample(title: str, indices: List[int], sample_n: int = 10):
        if not indices:
            st.write(f"**{title}:** none 🎉")
            return
        st.write(f"**{title}:** {len(indices)}")
        with st.expander(f"Show first {min(sample_n, len(indices))}"):
            st.write(indices[:sample_n])

    show_sample("Missing ID (item indices)", missing_id_idx, sample_show)
    show_sample("Missing Product URL (item indices)", missing_link_idx, sample_show)
    show_sample("Missing Primary Image (item indices)", missing_img_idx, sample_show)
    show_sample("Missing Availability (item indices)", missing_avail_idx, sample_show)

    if dup_id_pairs:
        st.write(f"**Duplicate IDs:** {len(dup_id_pairs)}")
        with st.expander("Show first duplicates (old_index, new_index, id)"):
            st.write(dup_id_pairs[:sample_show])
    else:
        st.write("**Duplicate IDs:** none 🎉")

    if dup_link_pairs:
        st.write(f"**Duplicate Product URLs:** {len(dup_link_pairs)}")
        with st.expander("Show first duplicates (old_index, new_index, url)"):
            st.write(dup_link_pairs[:sample_show])
    else:
        st.write("**Duplicate Product URLs:** none 🎉")

    st.markdown("---")
st.markdown("© 2025 Raul Bertoldini")

