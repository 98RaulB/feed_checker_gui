# feed_fixer_gui.py
from __future__ import annotations
from typing import Dict, List, Tuple, Any, Set
import re
import html
import streamlit as st

# Reuse your spec/rules so checker & fixer stay in sync
from feed_specs import (
    NS,
    SPEC,
    detect_spec,
    get_item_nodes,
    read_id,
    read_link,
    read_availability,
    gather_primary_image,
    signature_tags,
    percent_encode_url,
)

# Safe XML parsing (defusedxml if present)
try:
    from defusedxml import ElementTree as ET  # type: ignore
except Exception:
    import xml.etree.ElementTree as ET  # type: ignore

# -------------------- App config --------------------
st.set_page_config(page_title="Feed Fixer → Google RSS", layout="wide")
st.title("🛠️ Feed Fixer")
st.caption("Detects known formats, merges mixed feeds, asks for mappings when needed, and outputs Google Merchant RSS.")

# -------------------- Session init --------------------
if "xml_bytes" not in st.session_state:
    st.session_state["xml_bytes"] = None
if "source_label" not in st.session_state:
    st.session_state["source_label"] = None
if "build_now" not in st.session_state:
    st.session_state["build_now"] = False

# -------------------- Small helpers --------------------
def fetch_bytes_from_url(u: str) -> bytes:
    import requests
    r = requests.get(u, headers={"User-Agent": "FeedFixer/GUI"}, timeout=60)
    r.raise_for_status()
    return r.content

def status_pill(text: str, color: str = "#16a34a"):
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

def strip_ns(tag: str) -> str:
    return tag.split('}', 1)[1] if isinstance(tag, str) and '}' in tag else tag

def to_localname_set(root: ET.Element) -> Set[str]:
    names: Set[str] = set()
    for e in root.iter():
        if isinstance(e.tag, str):
            names.add(strip_ns(e.tag).lower())
    return names

def top2_closest_specs(root: ET.Element) -> List[Tuple[str,int,int]]:
    present = to_localname_set(root)
    scored: List[Tuple[str,int,int]] = []
    for spec_name, cfg in SPEC.items():
        sigs = [s.lower() for s in cfg.get("signature_tags", [])]
        found = 0
        for s in sigs:
            parts = [p.strip() for p in s.split("|")]
            if any(p in present for p in parts if p):
                found += 1
        scored.append((spec_name, found, len(sigs)))
    scored.sort(key=lambda x: x[1], reverse=True)
    return scored[:2]

def cdata(text: str) -> str:
    if text is None:
        return ""
    safe = text.replace("]]>", "]]]]><![CDATA[>")
    return f"<![CDATA[{safe}]]>"

def as_price(val: str, default_currency: str = "EUR") -> str:
    v = (val or "").strip()
    if not v:
        return ""
    m = re.search(r"([0-9]+(?:[.,][0-9]+)?)", v)
    if not m:
        return ""
    num = m.group(1).replace(",", ".")
    m2 = re.search(r"([A-Z]{3})", v)
    cur = m2.group(1) if m2 else default_currency
    return f"{num} {cur}"

def is_urlish(s: str) -> bool:
    return isinstance(s, str) and (s.startswith("http://") or s.startswith("https://"))

def collect_all_item_kv(elem: ET.Element) -> Dict[str, List[str]]:
    out: Dict[str, List[str]] = {}
    def add(k: str, v: str):
        if not v:
            return
        out.setdefault(k, []).append(v)

    for child in list(elem):
        if not isinstance(child.tag, str):
            continue
        lname = strip_ns(child.tag).lower()

        v = (child.text or "").strip()
        if v:
            add(lname, v)

        for _, a in (child.attrib or {}).items():
            if is_urlish(a):
                add(lname, a)

        for gchild in list(child):
            if not isinstance(gchild.tag, str):
                continue
            glname = strip_ns(gchild.tag).lower()
            gv = (gchild.text or "").strip()
            if gv:
                add(glname, gv)
            for _, a2 in (gchild.attrib or {}).items():
                if is_urlish(a2):
                    add(glname, a2)
    return out

G_FIELDS = [
    "id","item_group_id","title","description","link","image_link","additional_image_link",
    "price","availability","brand","mpn","gtin","condition",
    "google_product_category","product_type","color","material","size"
]

SYNONYMS: Dict[str, str] = {
    # ids
    "id":"id","item_id":"id","identifier":"id","productid":"id","product_id":"id","itemid":"id",
    "{http://base.google.com/ns/1.0}id":"id","g:id":"id",
    # group id
    "itemgroup_id":"item_group_id","item_group_id":"item_group_id","groupid":"item_group_id",
    # title/name
    "title":"title","name":"title","productname":"title",
    # desc
    "description":"description","desc":"description","content":"description",
    # link
    "link":"link","url":"link","product_url":"link","u":"link",
    "{http://base.google.com/ns/1.0}link":"link","g:link":"link",
    # images (primary)
    "image":"image_link","img":"image_link","imgurl":"image_link","image_url":"image_link","image_link":"image_link",
    "mainimage":"image_link","main":"image_link","imgurl_main":"image_link","img_main":"image_link",
    # gallery
    "image_url_2":"additional_image_link","image2":"additional_image_link","img2":"additional_image_link",
    "image_url_3":"additional_image_link","image3":"additional_image_link","img3":"additional_image_link",
    "image_url_4":"additional_image_link","image4":"additional_image_link","img4":"additional_image_link",
    "image_url_5":"additional_image_link","image5":"additional_image_link","img5":"additional_image_link",
    "imgurl_alternative":"additional_image_link","moreimages":"additional_image_link","gallery":"additional_image_link",
    "imgs":"additional_image_link","mainurl":"image_link",
    # price
    "price":"price","price_vat":"price","price_with_vat":"price",
    "{http://base.google.com/ns/1.0}price":"price","g:price":"price",
    # availability
    "availability":"availability","stock":"availability","in_stock":"availability",
    "avail":"availability","availability_status":"availability","delivery_date":"availability","delivery":"availability",
    # brand
    "brand":"brand","manufacturer":"brand","producer":"brand",
    # mpn, gtin
    "mpn":"mpn","ean":"gtin","gtin":"gtin",
    # condition
    "condition":"condition",
    # categories
    "google_product_category":"google_product_category","categorytext":"google_product_category","cat":"google_product_category",
    "category":"product_type","product_type":"product_type","category_full":"product_type",
    # attributes
    "color":"color","colour":"color","farba":"color","kolor":"color",
    "material":"material","materiál":"material",
    "size":"size","velikost":"size","rozmiar":"size","größe":"size",
}

def auto_merge_to_google(item: ET.Element, spec_name: str, default_currency: str) -> Tuple[Dict[str, Any], Dict[str, List[str]]]:
    g: Dict[str, Any] = {k: "" for k in G_FIELDS}
    g["additional_image_link"] = []

    # Spec readers (priority)
    pid = (read_id(item, spec_name) or "").strip()
    plink = (read_link(item, spec_name) or "").strip()
    pimg  = (gather_primary_image(item, spec_name) or "").strip()
    pav   = (read_availability(item, spec_name) or "").strip()

    if pid: g["id"] = pid
    if plink: g["link"] = percent_encode_url(plink)
    if pimg: g["image_link"] = percent_encode_url(pimg)
    if pav: g["availability"] = pav

    # Generic
    bag = collect_all_item_kv(item)
    unmapped: Dict[str, List[str]] = {}

    def push(k: str, val: str):
        if not val:
            return
        if k == "additional_image_link":
            if val not in g["additional_image_link"]:
                g["additional_image_link"].append(percent_encode_url(val))
        elif k in ("link","image_link"):
            g[k] = percent_encode_url(val)
        else:
            if not g.get(k):
                g[k] = val

    for src_tag, values in bag.items():
        tgt = SYNONYMS.get(src_tag)
        if tgt:
            for v in values:
                if tgt == "price":
                    push(tgt, as_price(v, default_currency))
                else:
                    push(tgt, v)
        else:
            if "image" in src_tag or "img" in src_tag:
                for v in values:
                    if is_urlish(v):
                        push("additional_image_link", v)
                continue
            unmapped[src_tag] = values

    if g.get("price"):
        g["price"] = as_price(str(g["price"]), default_currency)

    for k in ["title","description","brand","mpn","gtin","condition","google_product_category","product_type","color","material","size"]:
        g[k] = (g.get(k) or "").strip()

    return g, unmapped

def build_google_rss(entries: List[Dict[str, Any]], shop_title: str = "FeedFixer Output") -> bytes:
    def esc(s: str) -> str:
        return html.escape(s or "")

    lines: List[str] = []
    lines.append('<?xml version="1.0" encoding="UTF-8"?>')
    lines.append('<rss version="2.0" xmlns:g="http://base.google.com/ns/1.0">')
    lines.append("  <channel>")
    lines.append(f"    <title>{esc(shop_title)}</title>")
    lines.append(f"    <link>https://example.com/</link>")
    lines.append(f"    <description>{esc(shop_title)} feed</description>")

    for e in entries:
        pid = (e.get("id") or "").strip()
        if not pid:
            continue
        lines.append("    <item>")
        lines.append(f"      <g:id>{esc(pid)}</g:id>")

        if e.get("title"):
            lines.append(f"      <title>{cdata(e['title'])}</title>")
        if e.get("description"):
            lines.append(f"      <description>{cdata(e['description'])}</description>")

        if e.get("link"):
            lines.append(f"      <link>{esc(e['link'])}</link>")
        if e.get("image_link"):
            lines.append(f"      <g:image_link>{esc(e['image_link'])}</g:image_link>")
        for ai in e.get("additional_image_link", []) or []:
            lines.append(f"      <g:additional_image_link>{esc(ai)}</g:additional_image_link>")

        if e.get("price"):
            lines.append(f"      <g:price>{esc(e['price'])}</g:price>")
        if e.get("availability"):
            lines.append(f"      <g:availability>{esc(e['availability'])}</g:availability>")

        for k in ["item_group_id","brand","mpn","gtin","condition",
                  "google_product_category","product_type","color","material","size"]:
            v = (e.get(k) or "").strip()
            if v:
                lines.append(f"      <g:{k}>{esc(v)}</g:{k}>")

        lines.append("    </item>")

    lines.append("  </channel>")
    lines.append("</rss>")
    return "\n".join(lines).encode("utf-8")

# -------------------- Sidebar options (widgets persist automatically) --------------------
with st.sidebar:
    st.header("Output options")
    shop_title = st.text_input("Feed title", value="FeedFixer Output", key="shop_title")
    default_currency = st.text_input("Default currency", value="EUR", key="default_currency")
    if st.button("🔄 Reset / Load another feed"):
        for k in list(st.session_state.keys()):
            if k.startswith("map_"):
                del st.session_state[k]
        st.session_state["xml_bytes"] = None
        st.session_state["source_label"] = None
        st.session_state["build_now"] = False
        st.rerun()

# -------------------- Load form (one-time) --------------------
if st.session_state["xml_bytes"] is None:
    with st.form("input"):
        url = st.text_input("Feed URL (http/https)", placeholder="https://example.com/feed.xml")
        up = st.file_uploader("…or upload an XML file", type=["xml"])
        submitted = st.form_submit_button("Load")
    if submitted:
        try:
            if url.strip():
                if not url.lower().startswith(("http://","https://")):
                    st.error("URL must start with http:// or https://")
                    st.stop()
                xml_bytes = fetch_bytes_from_url(url.strip())
                st.session_state["xml_bytes"] = xml_bytes
                st.session_state["source_label"] = url.strip()
            elif up is not None:
                st.session_state["xml_bytes"] = up.read()
                st.session_state["source_label"] = up.name
            else:
                st.warning("Provide a URL or upload a file.")
                st.stop()
            st.success("Feed loaded. You can now map fields and build the RSS.")
            st.rerun()
        except Exception as e:
            st.error(f"Failed to load feed: {e}")
            st.stop()

# If still no feed, stop here
if st.session_state["xml_bytes"] is None:
    st.stop()

# -------------------- Parse & detect (from session_state) --------------------
xml_bytes = st.session_state["xml_bytes"]
src_label = st.session_state["source_label"]

st.write(f"**Source:** `{src_label}`")

try:
    root = ET.fromstring(xml_bytes)
    st.success("XML syntax: OK")
except ET.ParseError as e:
    st.error(f"XML syntax: ERROR — {e}")
    st.stop()

spec_name = detect_spec(root)
top2 = top2_closest_specs(root)

st.markdown("---")
c1, c2 = st.columns([2,3])
with c1:
    if spec_name != "UNKNOWN":
        status_pill(f"Detected: {spec_name}", "#16a34a")
    else:
        status_pill("Detected: UNKNOWN or MIXED", "#6b7280")
with c2:
    st.write("Closest formats:")
    for name, found, total in top2:
        pct = f"{(100.0*found/max(1,total)):.0f}%"
        st.write(f"- **{name}**: {found}/{total} signature tags ({pct})")

# -------------------- Extract items, auto-merge, collect unmapped --------------------
items = get_item_nodes(root, spec_name) if spec_name != "UNKNOWN" else []
st.write(f"Items found: **{len(items)}**")

merged: List[Dict[str, Any]] = []
all_unmapped: Dict[str, Set[str]] = {}
missing_ids = 0

for it in items:
    g, unmapped = auto_merge_to_google(it, spec_name, st.session_state.get("default_currency","EUR"))
    if not (g.get("id") or "").strip():
        missing_ids += 1
    for k, vals in unmapped.items():
        if not vals:
            continue
        ex = next((v for v in vals if v.strip()), "")
        if ex:
            all_unmapped.setdefault(k, set()).add(ex)
    merged.append(g)

if missing_ids > 0:
    st.error(f"{missing_ids} item(s) missing ID. They will be dropped unless you map a tag to g:id below.")

# -------------------- Mapping UI (stateful) --------------------
st.markdown("---")
st.subheader("Map unknown tags to Google fields (optional, keeps data)")

if not all_unmapped:
    st.write("No unknown fields found 🎉")
    tag_map: Dict[str, str] = {}
else:
    st.caption("Pick where each INPUT tag should go. Unmapped tags will be ignored.")
    TARGETS = ["(ignore)"] + G_FIELDS

    cols = st.columns(3)
    i = 0
    selection: Dict[str, str] = {}
    for tag, examples in sorted(all_unmapped.items()):
        with cols[i % 3]:
            example = next(iter(examples))
            # pull previous selection from session_state (if any)
            prev = st.session_state.get(f"map_{tag}", "(ignore)")
            choice = st.selectbox(
                f"{tag}  \n`e.g. {example[:80]}{'…' if len(example)>80 else ''}`",
                TARGETS,
                index=(TARGETS.index(prev) if prev in TARGETS else 0),
                key=f"map_{tag}",
            )
            selection[tag] = choice
        i += 1
    tag_map = {k:v for k,v in selection.items() if v and v != "(ignore)"}

# -------------------- Apply mapping to merged entries --------------------
if tag_map:
    updated: List[Dict[str, Any]] = []
    for it, g in zip(items, merged):
        bag = collect_all_item_kv(it)
        for src_tag, tgt in tag_map.items():
            vals = bag.get(src_tag, [])
            if not vals:
                continue
            if tgt == "additional_image_link":
                for v in vals:
                    if is_urlish(v) and v not in g["additional_image_link"]:
                        g["additional_image_link"].append(percent_encode_url(v))
            elif tgt in ("link","image_link"):
                last = next((v for v in reversed(vals) if v.strip()), "")
                if last:
                    g[tgt] = percent_encode_url(last)
            elif tgt == "price":
                last = next((v for v in reversed(vals) if v.strip()), "")
                if last:
                    g["price"] = as_price(last, st.session_state.get("default_currency","EUR"))
            else:
                if not g.get(tgt):
                    g[tgt] = vals[0]
        updated.append(g)
    merged = updated

# -------------------- Generate Google RSS (stateful build) --------------------
st.markdown("---")
st.subheader("Generate Google Merchant RSS")

usable = [e for e in merged if (e.get("id") or "").strip()]
dropped = len(merged) - len(usable)
if dropped > 0:
    st.warning(f"Dropping {dropped} item(s) without ID from the output (IDs are mandatory).")

col_build, _ = st.columns([1,4])
with col_build:
    if st.button("Build RSS"):
        st.session_state["build_now"] = True
        st.rerun()

# If the user clicked Build RSS, produce output now (on this rerun)
if st.session_state["build_now"]:
    out = build_google_rss(usable, shop_title=st.session_state.get("shop_title","FeedFixer Output"))
    st.success(f"Generated {len(usable)} items.")
    st.download_button("⬇️ Download feed.xml", data=out, file_name="feed.xml", mime="application/rss+xml")


