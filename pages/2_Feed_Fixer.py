from __future__ import annotations
from typing import Dict, Any, List, Tuple
import streamlit as st

# Safe XML parsing (fallback if defusedxml missing)
try:
    from defusedxml import ElementTree as ET  # type: ignore
except Exception:
    import xml.etree.ElementTree as ET  # type: ignore


# -------------------- helpers --------------------
def strip_ns(tag: str) -> str:
    return tag.split('}', 1)[1] if isinstance(tag, str) and '}' in tag else tag

def percent_encode_url(url: str) -> str:
    if not url:
        return url
    from urllib.parse import urlsplit, urlunsplit, quote
    parts = urlsplit(url)
    path = quote(parts.path or "", safe="/:@&+$,;=-._~")
    query = quote(parts.query or "", safe="=/?&:+,;@-._~")
    frag  = quote(parts.fragment or "", safe="-._~")
    return urlunsplit((parts.scheme, parts.netloc, path, query, frag))

def first_text(elem, xpaths: List[str]) -> str:
    ns = {"g":"http://base.google.com/ns/1.0", "content":"http://purl.org/rss/1.0/modules/content/"}
    for xp in xpaths:
        n = elem.find(xp, namespaces=ns)
        if n is not None:
            t = (n.text or "").strip()
            if t:
                return t
    return ""

def first_local_text(elem, localnames: List[str]) -> str:
    want = {n.lower() for n in localnames}
    for child in list(elem):
        loc = strip_ns(child.tag).lower()
        if loc in want:
            t = (child.text or "").strip()
            if t:
                return t
    return ""

def all_local_texts(elem, localname: str) -> List[str]:
    out: List[str] = []
    ln = localname.lower()
    for child in list(elem):
        loc = strip_ns(child.tag).lower()
        if loc == ln:
            t = (child.text or "").strip()
            if t:
                out.append(t)
    return out

def normalize_availability(raw: str) -> str:
    v = (raw or "").strip().lower()
    if v in {"in stock","instock","available","yes","true","1","on"}:      return "in stock"
    if v in {"out of stock","outofstock","unavailable","no","false","0","off"}: return "out of stock"
    if v in {"preorder","pre-order"}: return "preorder"
    if v in {"backorder","back-order"}: return "backorder"
    return raw or ""

def warn_if_price_missing_currency(price: str) -> bool:
    # returns True if looks like just a number (no currency code/symbol)
    if not price:
        return False
    has_alpha = any(c.isalpha() for c in price)
    has_symbol = any(c in "€$£ Kčzł₺₴₽¥₪" for c in price)
    return not (has_alpha or has_symbol)


# -------------------- source detection --------------------
def detect_source(root: ET.Element) -> str:
    # Very lightweight: if it has SHOP/SHOPITEM, call it HEUREKA
    if root.find(".//SHOPITEM") is not None:
        return "HEUREKA"
    # fallbacks (we keep it minimal here)
    if root.find(".//item") is not None:
        return "GENERIC"
    if any(strip_ns(e.tag).lower()=="entry" for e in root.iter()):
        return "GENERIC"
    if root.find(".//product") is not None or root.find(".//Product") is not None:
        return "GENERIC"
    return "GENERIC"


# -------------------- HEUREKA extractor (precise mapping) --------------------
def extract_heureka(root: ET.Element) -> List[Dict[str, Any]]:
    items: List[Dict[str, Any]] = []
    for it in root.findall(".//SHOPITEM"):
        # direct children by exact tag name (case-sensitive in many exports, but we also use localname helpers)
        item_id = first_local_text(it, ["ITEM_ID"]) or first_text(it, ["./ITEM_ID"])
        title   = first_local_text(it, ["PRODUCTNAME"]) or first_text(it, ["./PRODUCTNAME"])
        link    = first_local_text(it, ["URL"]) or first_text(it, ["./URL"])
        img     = first_local_text(it, ["IMGURL"]) or first_text(it, ["./IMGURL"])
        price   = first_local_text(it, ["PRICE_VAT"]) or first_text(it, ["./PRICE_VAT"])
        brand   = first_local_text(it, ["MANUFACTURER"]) or first_text(it, ["./MANUFACTURER"])
        ean     = first_local_text(it, ["EAN"]) or first_text(it, ["./EAN"])
        mpn     = first_local_text(it, ["PRODUCTNO"]) or first_text(it, ["./PRODUCTNO"])
        cond    = first_local_text(it, ["CONDITION"]) or first_text(it, ["./CONDITION"])
        desc    = first_local_text(it, ["DESCRIPTION"]) or first_text(it, ["./DESCRIPTION"])
        group   = first_local_text(it, ["ITEMGROUP_ID"]) or first_text(it, ["./ITEMGROUP_ID"])
        deliv   = first_local_text(it, ["DELIVERY_DATE"]) or first_text(it, ["./DELIVERY_DATE"])

        # multiple alternative images
        gallery = all_local_texts(it, "IMGURL_ALTERNATIVE")
        # normalize URLs
        link = percent_encode_url(link)
        img  = percent_encode_url(img)
        gallery = [percent_encode_url(u) for u in gallery if u]

        # availability rule from DELIVERY_DATE: "< 3 = in stock"
        availability = ""
        if deliv and deliv.strip().isdigit():
            if int(deliv.strip()) < 3:
                availability = "in stock"

        items.append({
            "id": (item_id or "").strip(),
            "title": title or "",
            "description": desc or "",
            "link": link or "",
            "images": [x for x in [img] if x] + gallery,
            "price": price or "",
            "availability": availability,  # may be empty if rule didn’t apply
            "brand": brand or "",
            "mpn": mpn or "",
            "gtin": ean or "",
            "item_group_id": group or "",
            # optional: you can add category/attrs later if needed
            "attrs": {},  # reserved for future Heureka params
        })
    return items


# -------------------- generic rich extractor (fallback) --------------------
CORE_TAGS = {
    "id","identifier","productid","item_id",
    "title","name","productname",
    "description","content","encoded","desc","short_description","long_description","longdesc",
    "link","url","product_url",
    "image","image_url","imgurl","image_link","imgs","mainimage",
    "price","price_with_vat","value",
    "availability","in_stock","stock","availability_status","avail",
    "brand","manufacturer","mpn","gtin","ean","barcode","sku",
    "category","categorytext","product_type","google_product_category",
    "item_group_id"
}

def kv_from_params(it) -> List[Tuple[str,str]]:
    pairs: List[Tuple[str,str]] = []
    for xp in [
        "./param", "./parameter", "./parameters/parameter", "./params/param",
        "./attribute", "./attributes/attribute", "./size_list/size", "./variant/param"
    ]:
        for node in it.findall(xp):
            name = (node.get("name") or node.get("Name") or "").strip()
            if not name:
                name = first_text(node, ["./name","./Name"])
            if not name:
                continue
            val = (node.text or "").strip() or first_text(node, ["./value","./Value","./val"])
            if val:
                pairs.append((name, val))
    return pairs

def all_images_generic(it) -> List[str]:
    imgs: List[str] = []
    primary = first_text(it, [
        "./image","./Image","./image_url","./Image_url","./IMGURL","./mainImage",
        "./{http://base.google.com/ns/1.0}image_link","./g:image_link","./imgs/main"
    ]) or first_local_text(it, ["image_link"])
    if primary:
        imgs.append(primary)
    gallery = []
    for nm in ["imgs/img","images/image","gallery/image","gallery/img","image_list/image",
               "{http://base.google.com/ns/1.0}additional_image_link","g:additional_image_link"]:
        gallery += [ (n.text or "").strip() for n in it.findall("./" + nm) if (n.text or "").strip() ]
    out = []
    for u in (imgs + gallery):
        enc = percent_encode_url(u)
        if enc and enc not in out:
            out.append(enc)
    return out

def extract_generic(root: ET.Element) -> List[Dict[str, Any]]:
    items: List[Dict[str, Any]] = []
    cand = []
    cand += root.findall(".//product")
    cand += root.findall(".//Product")
    cand += root.findall(".//item")
    cand += [e for e in root.iter() if strip_ns(e.tag).lower()=="entry"]
    cand += root.findall(".//SHOPITEM")
    cand += root.findall(".//o")  # Ceneo
    if not cand:
        cand = list(root)
    for it in cand:
        idv = first_text(it, ["./id","./ID","./Identifier","./identifier","./ProductId","./productid",
                              "./ITEM_ID","./{http://base.google.com/ns/1.0}id","./g:id"])
        title = first_text(it, ["./title","./Title","./name","./Name","./PRODUCTNAME","./productname"])
        link  = first_text(it, ["./link","./Link","./url","./URL","./product_url","./Product_url",
                                "./{http://base.google.com/ns/1.0}link","./g:link"])
        desc  = first_text(it, ["./description","./Description","./content:encoded"]) \
                or first_local_text(it, ["encoded","desc","short_description","long_description","longdesc"])
        price = first_text(it, ["./price","./PRICE","./value","./price_with_vat","./Price"])
        avail = first_text(it, ["./availability","./in_stock","./stock","./availability_status","./AVAILABILITY","./avail",
                                "./{http://base.google.com/ns/1.0}availability","./g:availability"])
        brand = first_text(it, ["./brand","./Brand","./manufacturer","./Manufacturer","./g:brand"])
        mpn   = first_text(it, ["./mpn","./MPN","./code","./sku","./SKU","./g:mpn"])
        gtin  = first_text(it, ["./gtin","./GTIN","./ean","./EAN","./barcode","./Barcode","./g:gtin"])
        group = first_text(it, ["./item_group_id","./ITEMGROUP_ID","./g:item_group_id"])
        images = all_images_generic(it)

        attrs: Dict[str,str] = {}
        for k, v in kv_from_params(it):
            lk = k.strip().lower()
            if lk and v and lk not in attrs:
                attrs[lk] = v

        # fold extra simple children as attrs
        for child in list(it):
            if not isinstance(child.tag, str):
                continue
            local = strip_ns(child.tag).lower()
            if local in CORE_TAGS:
                continue
            t = (child.text or "").strip()
            if t and local not in attrs:
                attrs[local] = t

        items.append({
            "id": (idv or "").strip(),
            "title": title or "",
            "description": desc or "",
            "link": percent_encode_url(link) if link else "",
            "images": images,
            "price": price or "",
            "availability": normalize_availability(avail),
            "brand": brand or "",
            "mpn": mpn or "",
            "gtin": gtin or "",
            "item_group_id": group or "",
            "attrs": attrs,
        })
    return items


# -------------------- emitters (Google + optional others) --------------------
def emit_google_rss(rows: List[Dict[str, Any]]) -> bytes:
    from xml.sax.saxutils import escape
    out = [
        '<?xml version="1.0" encoding="UTF-8"?>',
        '<rss version="2.0" xmlns:g="http://base.google.com/ns/1.0">',
        "<channel>",
        "<title>Fixed Feed</title>",
        "<link>https://feeds.example</link>",
        "<description>Auto-transformed</description>",
    ]
    for r in rows:
        out.append("<item>")
        out.append(f"<g:id>{escape(r['id'])}</g:id>")
        if r["title"]: out.append(f"<title>{escape(r['title'])}</title>")
        if r["description"]: out.append(f"<description>{escape(r['description'])}</description>")
        if r["link"]: out.append(f"<link>{escape(r['link'])}</link>")
        if r.get("images"):
            out.append(f"<g:image_link>{escape(r['images'][0])}</g:image_link>")
            for u in r["images"][1:]:
                out.append(f"<g:additional_image_link>{escape(u)}</g:additional_image_link>")
        if r["price"]: out.append(f"<g:price>{escape(r['price'])}</g:price>")
        if r["availability"]: out.append(f"<g:availability>{escape(r['availability'])}</g:availability>")
        if r["brand"]: out.append(f"<g:brand>{escape(r['brand'])}</g:brand>")
        if r["mpn"]: out.append(f"<g:mpn>{escape(r['mpn'])}</g:mpn>")
        if r["gtin"]: out.append(f"<g:gtin>{escape(r['gtin'])}</g:gtin>")
        if r["item_group_id"]: out.append(f"<g:item_group_id>{escape(r['item_group_id'])}</g:item_group_id>")
        out.append("</item>")
    out.append("</channel></rss>")
    return ("\n".join(out)).encode("utf-8")

def emit_google_atom(rows: List[Dict[str, Any]]) -> bytes:
    from xml.sax.saxutils import escape
    out = [
        '<?xml version="1.0" encoding="UTF-8"?>',
        '<feed xmlns="http://www.w3.org/2005/Atom" xmlns:g="http://base.google.com/ns/1.0">',
        "<title>Fixed Feed</title>",
        '<link rel="self" href="https://feeds.example"/>',
    ]
    for r in rows:
        out.append("<entry>")
        out.append(f"<g:id>{escape(r['id'])}</g:id>")
        if r["title"]: out.append(f"<title>{escape(r['title'])}</title>")
        if r["description"]: out.append(f"<content type=\"html\">{escape(r['description'])}</content>")
        if r["link"]: out.append(f"<g:link>{escape(r['link'])}</g:link>")
        if r.get("images"):
            out.append(f"<g:image_link>{escape(r['images'][0])}</g:image_link>")
            for u in r["images"][1:]:
                out.append(f"<g:additional_image_link>{escape(u)}</g:additional_image_link>")
        if r["price"]: out.append(f"<g:price>{escape(r['price'])}</g:price>")
        if r["availability"]: out.append(f"<g:availability>{escape(r['availability'])}</g:availability>")
        if r["brand"]: out.append(f"<g:brand>{escape(r['brand'])}</g:brand>")
        if r["mpn"]: out.append(f"<g:mpn>{escape(r['mpn'])}</g:mpn>")
        if r["gtin"]: out.append(f"<g:gtin>{escape(r['gtin'])}</g:gtin>")
        if r["item_group_id"]: out.append(f"<g:item_group_id>{escape(r['item_group_id'])}</g:item_group_id>")
        out.append("</entry>")
    out.append("</feed>")
    return ("\n".join(out)).encode("utf-8")

EMITTERS = {
    "Google RSS": emit_google_rss,
    "Google Atom": emit_google_atom,
    # You can add back Skroutz/Compari emitters if needed
}


# -------------------- UI --------------------
st.set_page_config(page_title="Feed Fixer (Preview)", layout="wide")
st.title("🔧 Feed Fixer (Preview)")
st.caption("Strict IDs. Heureka → Google mapping preserved (description, multi-images, brand/MPN/GTIN, item_group_id, availability from DELIVERY_DATE).")

with st.form("input"):
    src_url  = st.text_input("Source feed URL (http/https)", placeholder="https://example.com/feed.xml")
    src_file = st.file_uploader("…or upload an XML file", type=["xml"])
    target   = st.selectbox("Target specification", list(EMITTERS.keys()), index=0)
    st.divider()
    c1, c2 = st.columns(2)
    with c1:
        do_avail_norm = st.checkbox("Normalize textual availability (generic fallback)", value=True)
    with c2:
        do_dedupe = st.checkbox("De-duplicate by (id, link)", value=True)
    limit_items = st.number_input("Limit items (0 = all)", min_value=0, value=0, step=50)
    submitted = st.form_submit_button("Transform")

if submitted:
    feed_bytes = None
    label = None
    if src_url:
        if not src_url.lower().startswith(("http://","https://")):
            st.error("URL must start with http:// or https://")
        else:
            try:
                import requests
                r = requests.get(src_url, headers={"User-Agent":"FeedFixerPreview/1.3"}, timeout=40)
                r.raise_for_status()
                feed_bytes = r.content
                label = src_url
            except Exception as e:
                st.error(f"Failed to fetch URL: {e}")
    elif src_file is not None:
        feed_bytes = src_file.read()
        label = src_file.name
    else:
        st.warning("Provide a URL or upload a file.")

    if feed_bytes:
        st.write(f"**Input:** `{label}`")
        try:
            root = ET.fromstring(feed_bytes)
        except ET.ParseError as e:
            st.error(f"XML parse error: {e}")
            st.stop()

        # detect + extract
        src = detect_source(root)
        if src == "HEUREKA":
            rows = extract_heureka(root)
        else:
            rows = extract_generic(root)

        # strict IDs
        missing_ids = [i for i, r in enumerate(rows) if not (r.get("id") or "").strip()]
        if missing_ids:
            st.error(f"Missing ID on {len(missing_ids)} items. Fix the source feed (IDs are mandatory).")
            with st.expander("Show first missing-ID indices"):
                st.write(missing_ids[:50])
            st.stop()

        # availability normalization (textual fallback only, Heureka delivery_date already applied)
        if do_avail_norm:
            for r in rows:
                if r.get("availability"):
                    r["availability"] = normalize_availability(r["availability"])

        # dedupe
        if do_dedupe:
            seen = set(); deduped = []
            for r in rows:
                key = (r.get("id",""), r.get("link",""))
                if key in seen:
                    continue
                seen.add(key); deduped.append(r)
            rows = deduped

        # limit
        total_before = len(rows)
        if limit_items and limit_items > 0:
            rows = rows[:limit_items]
        total_after = len(rows)

        # metrics + warnings
        missing_link = sum(1 for r in rows if not r.get("link"))
        missing_img  = sum(1 for r in rows if not r.get("images"))
        c1, c2, c3, c4 = st.columns(4)
        c1.metric("Items (source)", total_before)
        c2.metric("Items (output)", total_after)
        c3.metric("Missing link", missing_link)
        c4.metric("Missing primary image", missing_img)

        # price currency warnings (Google likes currency)
        if any(warn_if_price_missing_currency(r.get("price","")) for r in rows if r.get("price")):
            st.warning("Some prices do not include currency (e.g., '529' instead of '529 CZK'). Google requires a currency. Consider appending a currency code.")

        # emit
        xml_bytes = EMITTERS[target](rows)

        with st.expander("Preview (first lines)"):
            preview = xml_bytes.decode("utf-8", errors="replace").splitlines()
            st.code("\n".join(preview[:150]))

        dl_name = f"fixed_{target.lower().replace(' ','_')}_google.xml"
        st.download_button("⬇️ Download fixed feed", xml_bytes, file_name=dl_name, mime="application/xml")

        st.success(f"Detected source: {src}. Mapped fields including description, multi-images, brand/MPN/GTIN, item_group_id. "
                   f"Applied availability from DELIVERY_DATE (<3 → 'in stock').")


