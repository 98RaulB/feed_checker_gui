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

st.set_page_config(page_title="Feed Fixer", layout="wide")
st.title("🛠️ Feed Fixer")
st.caption("Detects known formats, merges mixed feeds, lets you map unknown tags, and outputs Google Merchant RSS (or Heureka if chosen).")

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
    """
    Normalize prices to 'N.NN CUR'. If currency missing, use default_currency.
    """
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
    """
    Collect direct (and obvious nested) child texts and URL-like attributes.
    Returns: localname -> [values...], lowercased keys.
    """
    out: Dict[str, List[str]] = {}
    for child in list(elem):
        if not isinstance(child.tag, str):
            continue
        lname = strip_ns(child.tag).lower()

        v = (child.text or "").strip()
        if v:
            out.setdefault(lname, []).append(v)

        for _, a in (child.attrib or {}).items():
            if is_urlish(a):
                out.setdefault(lname, []).append(a)

        for gchild in list(child):
            if not isinstance(gchild.tag, str):
                continue
            glname = strip_ns(gchild.tag).lower()
            gv = (gchild.text or "").strip()
            if gv:
                out.setdefault(glname, []).append(gv)
            for _, a2 in (gchild.attrib or {}).items():
                if is_urlish(a2):
                    out.setdefault(glname, []).append(a2)
    return out

# Google Merchant target fields (subset that’s most useful)
G_FIELDS = [
    "id","item_group_id","title","description","link","image_link","additional_image_link",
    "price","availability","brand","mpn","gtin","condition",
    "google_product_category","product_type","color","material","size"
]

# Common synonyms to help auto-map unknown keys → Google fields
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
    # gallery → additional_image_link
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
    """
    Map to Google fields using (1) spec readers and (2) generic collection + synonyms.
    Returns (google_fields, unmapped_source_fields)
    """
    g: Dict[str, Any] = {k: "" for k in G_FIELDS}
    g["additional_image_link"] = []  # list

    # 1) Spec readers (highest priority)
    pid = (read_id(item, spec_name) or "").strip()
    plink = (read_link(item, spec_name) or "").strip()
    pimg  = (gather_primary_image(item, spec_name) or "").strip()
    pav   = (read_availability(item, spec_name) or "").strip()

    if pid: g["id"] = pid
    if plink: g["link"] = percent_encode_url(plink)
    if pimg: g["image_link"] = percent_encode_url(pimg)
    if pav: g["availability"] = pav

    # 2) Generic collection + synonyms
    bag = collect_all_item_kv(item)
    unmapped: Dict[str, List[str]] = {}

    def push(k: str, val: str):
        if not val:
            return
        if k == "additional_image_link":
            enc = percent_encode_url(val)
            if enc not in g["additional_image_link"]:
                g["additional_image_link"].append(enc)
        elif k in ("link", "image_link"):
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

    # Ensure price normalized if present
    if g.get("price"):
        g["price"] = as_price(str(g["price"]), default_currency)

    # Cleanup whitespace
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
            continue  # IDs mandatory
        lines.append("    <item>")
        lines.append(f"      <g:id>{esc(pid)}</g:id>")

        if e.get("title"):
            lines.append(f"      <title>{cdata(e['title'])}</title>")
        if e.get("description"):
            lines.append(f"      <description>{cdata(e['description'])}</description>")

        if e.get("link"):
            lines.append(f"      <link>{esc(percent_encode_url(e['link']))}</link>")
        if e.get("image_link"):
            lines.append(f"      <g:image_link>{esc(percent_encode_url(e['image_link']))}</g:image_link>")
        for ai in e.get("additional_image_link", []) or []:
            lines.append(f"      <g:additional_image_link>{esc(percent_encode_url(ai))}</g:additional_image_link>")

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

def build_heureka(entries: List[Dict[str, Any]]) -> bytes:
    """
    Minimal, clean Heureka output from the unified entries.
    Using:
      ITEM_ID, PRODUCTNAME, URL, IMGURL, PRICE_VAT, MANUFACTURER, DESCRIPTION
    Falls back when fields are missing; drops items with no ID.
    """
    def esc(s: str) -> str:
        return html.escape(s or "")

    lines: List[str] = []
    lines.append('<?xml version="1.0" encoding="UTF-8"?>')
    lines.append('<SHOP>')
    for e in entries:
        pid = (e.get("id") or "").strip()
        if not pid:
            continue
        name = (e.get("title") or "").strip()
        url  = percent_encode_url(e.get("link") or "")
        img  = percent_encode_url(e.get("image_link") or "")
        price = (e.get("price") or "").split()[0] if e.get("price") else ""
        brand = (e.get("brand") or "").strip()
        desc  = (e.get("description") or "").strip()

        lines.append("  <SHOPITEM>")
        lines.append(f"    <ITEM_ID>{esc(pid)}</ITEM_ID>")
        if name:
            lines.append(f"    <PRODUCTNAME>{cdata(name)}</PRODUCTNAME>")
        if url:
            lines.append(f"    <URL>{esc(url)}</URL>")
        if img:
            lines.append(f"    <IMGURL>{esc(img)}</IMGURL>")
        if price:
            lines.append(f"    <PRICE_VAT>{esc(price)}</PRICE_VAT>")
        if brand:
            lines.append(f"    <MANUFACTURER>{cdata(brand)}</MANUFACTURER>")
        if desc:
            lines.append(f"    <DESCRIPTION>{cdata(desc)}</DESCRIPTION>")
        # simple availability mapping (optional)
        if e.get("availability"):
            lines.append(f"    <DELIVERY_DATE>{'0' if e['availability'].lower().startswith('in stock') else '7'}</DELIVERY_DATE>")
        # gallery
        for ai in e.get("additional_image_link", []) or []:
            lines.append(f"    <IMGURL_ALTERNATIVE>{esc(percent_encode_url(ai))}</IMGURL_ALTERNATIVE>")
        lines.append("  </SHOPITEM>")
    lines.append("</SHOP>")
    return "\n".join(lines).encode("utf-8")

# -------------------- Sidebar options --------------------
with st.sidebar:
    st.header("Output options")
    shop_title = st.text_input("Feed title", value="FeedFixer Output")
    default_currency = st.text_input("Default currency (for price parsing)", value="EUR")
    st.caption("IDs are mandatory: items without ID will be dropped from the output.")

# -------------------- Input form --------------------
with st.form("input"):
    url = st.text_input("Feed URL (http/https)", placeholder="https://example.com/feed.xml")
    up = st.file_uploader("…or upload an XML file", type=["xml"])
    sample_show = st.number_input("Show up to N sample issues per category", 1, 50, 10)
    submitted = st.form_submit_button("Load")

if not submitted:
    st.stop()

# -------------------- Load bytes --------------------
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

# -------------------- Parse --------------------
try:
    root = ET.fromstring(xml_bytes)
    st.success("XML syntax: OK")
except ET.ParseError as e:
    st.error(f"XML syntax: ERROR — {e}")
    st.stop()

# -------------------- Detect & score --------------------
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

merged: List[Dict[str, Any]] = []
all_unmapped: Dict[str, Set[str]] = {}
missing_ids = 0

for it in items:
    g, unmapped = auto_merge_to_google(it, spec_name, default_currency)
    if not (g.get("id") or "").strip():
        missing_ids += 1
    for k, vals in unmapped.items():
        if not vals:
            continue
        ex = next((v for v in vals if v.strip()), "")
        if ex:
            all_unmapped.setdefault(k, set()).add(ex)
    merged.append(g)

# ---------- persist parsing state so UI actions don't reset ----------
st.session_state.parsed_items = items
st.session_state.merged_entries = merged
st.session_state.all_unmapped = all_unmapped
st.session_state.spec_name = spec_name
st.session_state.shop_title = shop_title
st.session_state.default_currency = default_currency

if missing_ids > 0:
    st.error(f"{missing_ids} item(s) missing ID. They will be dropped unless you map a tag to g:id below.")

# -------------------- Mapping UI for unknown tags --------------------
st.markdown("---")
st.subheader("Map unknown tags to Google fields (optional, keeps data)")

if "tag_map" not in st.session_state:
    st.session_state.tag_map = {}

if not all_unmapped:
    st.write("No unknown fields found 🎉")
else:
    st.caption("Pick where each INPUT tag should go. Unmapped tags will be ignored.")
    TARGETS = ["(ignore)"] + G_FIELDS

    cols = st.columns(3)
    i = 0
    for tag, examples in sorted(all_unmapped.items()):
        with cols[i % 3]:
            example = next(iter(examples))
            choice = st.selectbox(
                f"{tag}  \n`e.g. {example[:80]}{'…' if len(example)>80 else ''}`",
                TARGETS,
                index=TARGETS.index(st.session_state.tag_map.get(tag, "(ignore)"))
                if st.session_state.tag_map.get(tag) in TARGETS else 0,
                key=f"map_{tag}",
            )
            if choice and choice != "(ignore)":
                st.session_state.tag_map[tag] = choice
            elif tag in st.session_state.tag_map:
                # user reset to ignore
                del st.session_state.tag_map[tag]
        i += 1

# -------------------- Apply mapping to merged entries --------------------
if st.session_state.tag_map:
    updated: List[Dict[str, Any]] = []
    for it, g in zip(st.session_state.parsed_items, st.session_state.merged_entries):
        bag = collect_all_item_kv(it)
        for src_tag, tgt in st.session_state.tag_map.items():
            vals = bag.get(src_tag, [])
            if not vals:
                continue
            if tgt == "additional_image_link":
                for v in vals:
                    if is_urlish(v):
                        enc = percent_encode_url(v)
                        if enc not in g["additional_image_link"]:
                            g["additional_image_link"].append(enc)
            elif tgt in ("link","image_link"):
                last = next((v for v in reversed(vals) if v.strip()), "")
                if last:
                    g[tgt] = percent_encode_url(last)
            elif tgt == "price":
                last = next((v for v in reversed(vals) if v.strip()), "")
                if last:
                    g["price"] = as_price(last, st.session_state.default_currency)
            else:
                if not g.get(tgt):
                    g[tgt] = vals[0]
        updated.append(g)
    st.session_state.merged_entries = updated

# -------------------- Duplicate handling (ask user) --------------------
st.markdown("---")
st.subheader("Duplicates")

# compute duplicates by id and by link (encoded)
id_first: Dict[str, int] = {}
link_first: Dict[str, int] = {}
dup_ids: List[Tuple[int, int, str]] = []
dup_links: List[Tuple[int, int, str]] = []

for idx, e in enumerate(st.session_state.merged_entries):
    pid = (e.get("id") or "").strip()
    purl = (e.get("link") or "").strip()
    if pid:
        if pid in id_first:
            dup_ids.append((id_first[pid], idx, pid))
        else:
            id_first[pid] = idx
    if purl:
        if purl in link_first:
            dup_links.append((link_first[purl], idx, purl))
        else:
            link_first[purl] = idx

dup_total = len(dup_ids) + len(dup_links)
if dup_total > 0:
    st.warning(f"Found duplicates: IDs={len(dup_ids)}, Links={len(dup_links)}")
    policy = st.radio(
        "When duplicates are present, choose an action:",
        options=["Keep first only (drop later duplicates)", "Keep all"],
        index=0,
        horizontal=False,
        key="dup_policy",
    )
else:
    st.write("No duplicates found 🎉")
    st.session_state.dup_policy = "Keep all"

# Apply policy preview
def apply_duplicates_policy(entries: List[Dict[str, Any]], policy: str) -> List[Dict[str, Any]]:
    if policy != "Keep first only (drop later duplicates)":
        return entries
    seen_ids: Set[str] = set()
    seen_links: Set[str] = set()
    out: List[Dict[str, Any]] = []
    for e in entries:
        pid = (e.get("id") or "").strip()
        purl = (e.get("link") or "").strip()
        # if either ID or link is duplicate, skip (keep first only)
        if pid and pid in seen_ids:
            continue
        if purl and purl in seen_links:
            continue
        out.append(e)
        if pid:
            seen_ids.add(pid)
        if purl:
            seen_links.add(purl)
    return out

# -------------------- Output format choice --------------------
st.markdown("---")
st.subheader("Generate output")

output_fmt = "Google RSS"
if st.session_state.spec_name == "Heureka strict":
    output_fmt = st.radio("Output format", ["Google RSS", "Heureka"], index=0, horizontal=True)

if st.button("Build feed"):
    # enforce ID rule and duplicates policy
    usable = [e for e in st.session_state.merged_entries if (e.get("id") or "").strip()]
    usable = apply_duplicates_policy(usable, st.session_state.dup_policy)

    if output_fmt == "Heureka":
        out = build_heureka(usable)
        st.success(f"Generated {len(usable)} items (Heureka).")
        st.download_button("⬇️ Download feed_heureka.xml", data=out, file_name="feed_heureka.xml", mime="application/xml")
    else:
        out = build_google_rss(usable, shop_title=st.session_state.shop_title)
        st.success(f"Generated {len(usable)} items (Google RSS).")
        st.download_button("⬇️ Download feed.xml", data=out, file_name="feed.xml", mime="application/rss+xml")
