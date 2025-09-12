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

# ---------- Small UI helpers ----------
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
    for k, (ok, warn, extra) in pass_fail.items():
        lbl, text = verdict_row(k, ok, warn, extra)
        st.write(f"- **{lbl}**: {text}")

def status_pill(text: str, color: str = "#16a34a"):  # green default
    # color: green #16a34a, red #dc2626, gray #6b7280
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
        spec_name = "UNKNOWN"

    # 4) Collect items & run checks
    items = get_item_nodes(root, spec_name) if spec_name != "UNKNOWN" else []
    total_items = len(items)

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

    total_dups = len(dup_id_pairs) + len(dup_link_pairs)

        # ---------- TOP ROW ----------
    st.markdown("---")
    c1, c2, c3, c4 = st.columns([1,1,1,1], gap="large")
    
    with c1:
        if spec_name != "UNKNOWN":
            status_pill(f"Transformation: {spec_name}", "#16a34a")  # green
        else:
            status_pill("Transformation: UNKNOWN", "#6b7280")       # gray
    
    with c2:
        status_pill(f"Items: {total_items}", "#16a34a")             # always green
    
    with c3:
        if (len(dup_id_pairs) + len(dup_link_pairs)) > 0:
            status_pill(f"Duplicates: {len(dup_id_pairs) + len(dup_link_pairs)}", "#dc2626")
        else:
            status_pill("Duplicates: 0", "#16a34a")
    
    with c4:
        if len(missing_id_idx) > 0:
            status_pill(f"Missing IDs: {len(missing_id_idx)}", "#dc2626")  # RED if any
        else:
            status_pill("Missing IDs: 0", "#16a34a")

    # ---------- SUMMARY ----------
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

    st.markdown("---")
    summarize(pass_fail)

    # ---------- DETAILS ----------
    st.markdown("---")
    st.subheader("Details")

    def show_sample(title: str, indices: List[int], sample_n: int = 10, red: bool = False):
        if not indices:
            st.write(f"**{title}:** none 🎉")
            return
        st.write(f"**{title}:** {len(indices)}")
        with st.expander(f"Show first {min(sample_n, len(indices))}"):
            subset = indices[:sample_n]
            if red:
                # render as a red bullet list
                st.markdown(
                    "<ul style='margin-top:0'>"
                    + "".join(f"<li style='color:#dc2626'>item index {i}</li>" for i in subset)
                    + "</ul>",
                    unsafe_allow_html=True,
                )
            else:
                st.write(subset)
    
    # Use it like this:
    show_sample("Missing ID (item indices)", missing_id_idx, sample_show, red=True)
    show_sample("Missing Product URL (item indices)", missing_link_idx, sample_show)
    show_sample("Missing Primary Image (item indices)", missing_img_idx, sample_show)
    show_sample("Missing Availability (item indices)", missing_avail_idx, sample_show)

    if missing_id_idx:
        with st.expander("Show first offending rows (index, link, primary image present)"):
        rows = []
        for i in missing_id_idx[:sample_show]:
            link_i = links[i] if i < len(links) else ""
            # primary image presence
            prim_i = "yes" if i < len(items) and (gather_primary_image(items[i], spec_name) or "").strip() else "no"
            rows.append({"index": i, "link": link_i, "primary_image": prim_i})
        st.dataframe(rows, use_container_width=True)
        
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

