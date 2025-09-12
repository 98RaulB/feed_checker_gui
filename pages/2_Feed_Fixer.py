from __future__ import annotations
import hashlib
from typing import Dict, Any, List, Tuple
import streamlit as st

# Safe XML parsing (fallback if defusedxml missing)
try:
    from defusedxml import ElementTree as ET  # type: ignore
except Exception:
    import xml.etree.ElementTree as ET  # type: ignore


# -------------------- Helpers --------------------
def strip_ns(tag: str) -> str:
    return tag.split('}', 1)[1] if '}' in tag else tag

def first_text(elem, xpaths: List[str]) -> str:
    for xp in xpaths:
        n = elem.find(xp, namespaces={"g": "http://base.google.com/ns/1.0"})
        if n is not None:
            t = (n.text or "").strip()
            if t:
                return t
    return ""

def all_texts(elem, xpaths: List[str]) -> List[str]:
    out: List[str] = []
    for xp in xpaths:
        for n in elem.findall(xp, namespaces={"g": "http://base.google.com/ns/1.0"}):
            t = (n.text or "").strip()
            if t:
                out.append(t)
    return out

def percent_encode_url(url: str) -> str:
    if not url:
        return url
    from urllib.parse import urlsplit, urlunsplit, quote
    parts = urlsplit(url)
    path = quote(parts.path, safe="/:@&+$,;=-._~")
    query = quote(parts.query, safe="=/?&:+,;@-._~")
    frag  = quote(parts.fragment, safe="-._~")
    return urlunsplit((parts.scheme, parts.netloc, path, query, frag))

def synth_id_from_url(u: str) -> str:
    return hashlib.sha1((u or "").encode("utf-8")).hexdigest()[:16]

def normalize_availability(raw: str) -> str:
    v = (raw or "").strip().lower()
    if v in {"in stock","instock","available","yes","true","1","on"}:      return "in stock"
    if v in {"out of stock","outofstock","unavailable","no","false","0","off"}: return "out of stock"
    return raw or ""


# -------------------- Neutral extractor (rich) --------------------
CORE_TAGS = {
    # common logical fields (lowercased) to avoid duplicating into attrs
    "id","identifier","productid","item_id",
    "title","name","productname",
    "description","content",
    "link","url","product_url",
    "image","image_url","imgurl","image_link","imgs","mainimage",
    "price","price_with_vat","value",
    "availability","in_stock","stock","availability_status","avail",
    "brand","manufacturer","mpn","gtin","ean","barcode","sku",
    "category","categorytext","product_type","google_product_category"
}

def kv_from_params(it) -> List[Tuple[str,str]]:
    pairs: List[Tuple[str,str]] = []
    # Common parameter patterns across feeds
    for xp in [
        "./param", "./parameter", "./parameters/parameter", "./params/param",
        "./attribute", "./attributes/attribute", "./size_list/size", "./variant/param"
    ]:
        for node in it.findall(xp):
            # name may be in attribute or subnode
            name = (node.get("name") or node.get("Name") or "").strip()
            if not name:
                # try <name>child</name>
                name = first_text(node, ["./name","./Name"])
            if not name:
                continue
            val = (node.text or "").strip() or first_text(node, ["./value","./Value","./val"])
            if val:
                pairs.append((name, val))
    return pairs

def all_images(it) -> List[str]:
    # collect many possible image fields; primary should be first
    imgs = []
    # obvious singletons
    primary = first_text(it, [
        "./image","./Image","./image_url","./Image_url","./IMGURL","./mainImage",
        "./{http://base.google.com/ns/1.0}image_link","./g:image_link",
        "./imgs/main"
    ])
    if primary:
        imgs.append(primary)
    # gallery-like
    gallery = all_texts(it, [
        "./imgs/img","./images/image","./gallery/image","./gallery/img","./image_list/image",
        "./{http://base.google.com/ns/1.0}additional_image_link","./g:additional_image_link"
    ])
    for u in gallery:
        if u and u not in imgs:
            imgs.append(u)
    # fallbacks (any child that looks like an image URL)
    for child in list(it):
        tag = strip_ns(child.tag).lower() if isinstance(child.tag, str) else ""
        if tag in CORE_TAGS:  # already handled
            continue
        t = (child.text or "").strip()
        if t and ("http://" in t or "https://" in t) and any(x in t.lower() for x in [".jpg",".jpeg",".png",".webp",".gif"]):
            if t not in imgs:
                imgs.append(t)
    # percent-encode & dedupe
    out = []
    for u in imgs:
        enc = percent_encode_url(u)
        if enc and enc not in out:
            out.append(enc)
    return out

def extract_items_neutral(root) -> List[Dict[str, Any]]:
    items: List[Dict[str, Any]] = []
    candidates = []
    candidates += root.findall(".//product")
    candidates += root.findall(".//Product")
    candidates += root.findall(".//item")
    candidates += [e for e in root.iter() if strip_ns(e.tag).lower() == "entry"]
    candidates += root.findall(".//SHOPITEM")
    candidates += root.findall(".//o")  # Ceneo
    if not candidates:
        candidates = list(root)

    for it in candidates:
        idv = first_text(it, [
            "./id","./ID","./Identifier","./identifier","./ProductId","./productid",
            "./ITEM_ID","./{http://base.google.com/ns/1.0}id","./g:id"
        ])
        link = first_text(it, [
            "./link","./Link","./url","./URL","./product_url","./Product_url",
            "./{http://base.google.com/ns/1.0}link","./g:link"
        ])
        title = first_text(it, ["./title","./Title","./name","./Name","./PRODUCTNAME","./productname"])
        desc  = first_text(it, ["./description","./Description","./content"])
        price = first_text(it, ["./price","./PRICE","./value","./price_with_vat","./Price"])
        avail = first_text(it, [
            "./availability","./in_stock","./stock","./availability_status","./AVAILABILITY","./avail",
            "./{http://base.google.com/ns/1.0}availability","./g:availability"
        ])
        brand = first_text(it, ["./brand","./Brand","./manufacturer","./Manufacturer",
                                "./{http://base.google.com/ns/1.0}brand","./g:brand"])
        mpn   = first_text(it, ["./mpn","./MPN","./code","./sku","./SKU",
                                "./{http://base.google.com/ns/1.0}mpn","./g:mpn"])
        gtin  = first_text(it, ["./gtin","./GTIN","./ean","./EAN","./barcode","./Barcode",
                                "./{http://base.google.com/ns/1.0}gtin","./g:gtin"])
        category = first_text(it, ["./category","./categorytext","./Category","./CategoryText"])
        product_type = first_text(it, ["./product_type","./ProductType",
                                       "./{http://base.google.com/ns/1.0}product_type","./g:product_type"])
        gpc = first_text(it, ["./google_product_category","./GoogleProductCategory",
                              "./{http://base.google.com/ns/1.0}google_product_category","./g:google_product_category"])

        images = all_images(it)

        # generic attributes (params, attributes, etc.)
        attrs: Dict[str, str] = {}
        for k, v in kv_from_params(it):
            lk = k.strip().lower()
            if lk and v and lk not in attrs:
                attrs[lk] = v

        # also pick up simple child tags not in core set (best-effort)
        for child in list(it):
            if not isinstance(child.tag, str):
                continue
            local = strip_ns(child.tag).lower()
            if local in CORE_TAGS:
                continue
            t = (child.text or "").strip()
            if t and local not in attrs and not t.startswith("<"):
                attrs[local] = t

        # normalize & synthesize
        link = percent_encode_url(link) if link else ""
        if not idv and link:
            idv = synth_id_from_url(link)

        items.append({
            "id": idv or "",
            "title": title or "",
            "description": desc or "",
            "link": link or "",
            "price": price or "",
            "availability": avail or "",
            "brand": brand or "",
            "mpn": mpn or "",
            "gtin": gtin or "",
            "category": category or "",
            "product_type": product_type or "",
            "google_product_category": gpc or "",
            "images": images,       # list
            "attrs": attrs          # dict[str,str]
        })
    return items


# -------------------- Emitters (rich) --------------------
def emit_skroutz(rows: List[Dict[str, Any]], include_extras: bool) -> bytes:
    from xml.sax.saxutils import escape
    out = ['<?xml version="1.0" encoding="UTF-8"?>', "<products>"]
    for r in rows:
        out.append("<product>")
        if r["id"]:    out.append(f"<id>{escape(r['id'])}</id>")
        if r["title"]: out.append(f"<name>{escape(r['title'])}</name>")
        if r["link"]:  out.append(f"<link>{escape(r['link'])}</link>")
        # primary image
        if r.get("images"):
            out.append(f"<image>{escape(r['images'][0])}</image>")
            # additional images
            if len(r["images"]) > 1:
                out.append("<images>")
                for u in r["images"][1:]:
                    out.append(f"<image>{escape(u)}</image>")
                out.append("</images>")
        if r["price"]: out.append(f"<price_with_vat>{escape(r['price'])}</price_with_vat>")
        if r["availability"]: out.append(f"<availability>{escape(r['availability'])}</availability>")
        if r["brand"]: out.append(f"<brand>{escape(r['brand'])}</brand>")
        if r["mpn"]:   out.append(f"<mpn>{escape(r['mpn'])}</mpn>")
        if r["gtin"]:  out.append(f"<gtin>{escape(r['gtin'])}</gtin>")
        if r["category"]: out.append(f"<category>{escape(r['category'])}</category>")
        if include_extras and r["attrs"]:
            out.append("<Extras>")
            for k, v in r["attrs"].items():
                out.append(f"<{escape(k)}>{escape(v)}</{escape(k)}>")
            out.append("</Extras>")
        out.append("</product>")
    out.append("</products>")
    return ("\n".join(out)).encode("utf-8")


def emit_compari(rows: List[Dict[str, Any]], include_extras: bool) -> bytes:
    from xml.sax.saxutils import escape
    out = ['<?xml version="1.0" encoding="UTF-8"?>', "<Products>"]
    for r in rows:
        out.append("<Product>")
        if r["id"]:    out.append(f"<Identifier>{escape(r['id'])}</Identifier>")
        if r["title"]: out.append(f"<Name>{escape(r['title'])}</Name>")
        if r["link"]:  out.append(f"<Product_url>{escape(r['link'])}</Product_url>")
        # primary image + extras
        if r.get("images"):
            out.append(f"<Image_url>{escape(r['images'][0])}</Image_url>")
            if len(r["images"]) > 1:
                out.append("<Gallery>")
                for u in r["images"][1:]:
                    out.append(f"<Image>{escape(u)}</Image>")
                out.append("</Gallery>")
        if r["price"]: out.append(f"<Price>{escape(r['price'])}</Price>")
        if r["availability"]: out.append(f"<availability>{escape(r['availability'])}</availability>")
        if r["brand"]: out.append(f"<Manufacturer>{escape(r['brand'])}</Manufacturer>")
        if r["category"]: out.append(f"<Category>{escape(r['category'])}</Category>")
        # neutral parameters block (many channels will ignore if unknown)
        if include_extras and r["attrs"]:
            out.append("<Parameters>")
            for k, v in r["attrs"].items():
                out.append(f"<Parameter name=\"{escape(k)}\">{escape(v)}</Parameter>")
            out.append("</Parameters>")
        out.append("</Product>")
    out.append("</Products>")
    return ("\n".join(out)).encode("utf-8")


def emit_google_rss(rows: List[Dict[str, Any]], include_extras: bool) -> bytes:
    from xml.sax.saxutils import escape
    # Map some common attrs to official Google fields
    def g_from_attrs(attrs: Dict[str,str], key: str) -> str:
        return attrs.get(key, "") or attrs.get(key.replace("_"," "), "")
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
        if r["id"]:            out.append(f"<g:id>{escape(r['id'])}</g:id>")
        if r["title"]:         out.append(f"<title>{escape(r['title'])}</title>")
        if r["description"]:   out.append(f"<description>{escape(r['description'])}</description>")
        if r["link"]:          out.append(f"<link>{escape(r['link'])}</link>")
        # images
        if r.get("images"):
            out.append(f"<g:image_link>{escape(r['images'][0])}</g:image_link>")
            for u in r["images"][1:]:
                out.append(f"<g:additional_image_link>{escape(u)}</g:additional_image_link>")
        if r["price"]:         out.append(f"<g:price>{escape(r['price'])}</g:price>")
        if r["availability"]:  out.append(f"<g:availability>{escape(r['availability'])}</g:availability>")
        if r["brand"]:         out.append(f"<g:brand>{escape(r['brand'])}</g:brand>")
        if r["mpn"]:           out.append(f"<g:mpn>{escape(r['mpn'])}</g:mpn>")
        if r["gtin"]:          out.append(f"<g:gtin>{escape(r['gtin'])}</g:gtin>")
        if r["product_type"]:  out.append(f"<g:product_type>{escape(r['product_type'])}</g:product_type>")
        if r["google_product_category"]:
            out.append(f"<g:google_product_category>{escape(r['google_product_category'])}</g:google_product_category>")
        # Map popular attrs into Google keys if present
        for key in ["color","size","material","age_group","gender"]:
            val = g_from_attrs(r["attrs"], key)
            if val:
                out.append(f"<g:{key}>{escape(val)}</g:{key}>")
        # Optionally include neutral <extras> (ignored by Google)
        if include_extras and r["attrs"]:
            out.append("<extras>")
            for k, v in r["attrs"].items():
                if k in {"color","size","material","age_group","gender"}:  # already mapped
                    continue
                out.append(f"<{escape(k)}>{escape(v)}</{escape(k)}>")
            out.append("</extras>")
        out.append("</item>")
    out.append("</channel></rss>")
    return ("\n".join(out)).encode("utf-8")


def emit_google_atom(rows: List[Dict[str, Any]], include_extras: bool) -> bytes:
    from xml.sax.saxutils import escape
    def g_from_attrs(attrs: Dict[str,str], key: str) -> str:
        return attrs.get(key, "") or attrs.get(key.replace("_"," "), "")
    out = [
        '<?xml version="1.0" encoding="UTF-8"?>',
        '<feed xmlns="http://www.w3.org/2005/Atom" xmlns:g="http://base.google.com/ns/1.0">',
        "<title>Fixed Feed</title>",
        '<link rel="self" href="https://feeds.example"/>',
    ]
    for r in rows:
        out.append("<entry>")
        if r["id"]:            out.append(f"<g:id>{escape(r['id'])}</g:id>")
        if r["title"]:         out.append(f"<title>{escape(r['title'])}</title>")
        if r["description"]:   out.append(f"<content type=\"html\">{escape(r['description'])}</content>")
        if r["link"]:          out.append(f"<g:link>{escape(r['link'])}</g:link>")
        if r.get("images"):
            out.append(f"<g:image_link>{escape(r['images'][0])}</g:image_link>")
            for u in r["images"][1:]:
                out.append(f"<g:additional_image_link>{escape(u)}</g:additional_image_link>")
        if r["price"]:         out.append(f"<g:price>{escape(r['price'])}</g:price>")
        if r["availability"]:  out.append(f"<g:availability>{escape(r['availability'])}</g:availability>")
        if r["brand"]:         out.append(f"<g:brand>{escape(r['brand'])}</g:brand>")
        if r["mpn"]:           out.append(f"<g:mpn>{escape(r['mpn'])}</g:mpn>")
        if r["gtin"]:          out.append(f"<g:gtin>{escape(r['gtin'])}</g:gtin>")
        if r["product_type"]:  out.append(f"<g:product_type>{escape(r['product_type'])}</g:product_type>")
        if r["google_product_category"]:
            out.append(f"<g:google_product_category>{escape(r['google_product_category'])}</g:google_product_category>")
        for key in ["color","size","material","age_group","gender"]:
            val = g_from_attrs(r["attrs"], key)
            if val:
                out.append(f"<g:{key}>{escape(val)}</g:{key}>")
        if include_extras and r["attrs"]:
            out.append("<extras>")
            for k, v in r["attrs"].items():
                if k in {"color","size","material","age_group","gender"}:
                    continue
                out.append(f"<{escape(k)}>{escape(v)}</{escape(k)}>")
            out.append("</extras>")
        out.append("</entry>")
    out.append("</feed>")
    return ("\n".join(out)).encode("utf-8")


EMITTERS = {
    "Skroutz": emit_skroutz,
    "Compari": emit_compari,
    "Google RSS": emit_google_rss,
    "Google Atom": emit_google_atom,
}


# -------------------- UI --------------------
st.set_page_config(page_title="Feed Fixer (Preview)", layout="wide")
st.title("🔧 Feed Fixer (Preview)")
st.caption("Transforms feeds while preserving descriptions, multi-images, identifiers and attributes. Download to test.")

with st.form("input"):
    src_url = st.text_input("Source feed URL (http/https)", placeholder="https://example.com/feed.xml")
    src_file = st.file_uploader("...or upload an XML file", type=["xml"])
    target = st.selectbox("Target specification", list(EMITTERS.keys()), index=0)
    st.divider()
    c1, c2, c3 = st.columns(3)
    with c1:
        do_avail_norm = st.checkbox("Normalize availability values", value=True)
    with c2:
        do_dedupe = st.checkbox("De-duplicate by (id, link)", value=True)
    with c3:
        include_extras = st.checkbox("Include neutral Extras block", value=True,
                                     help="Adds <Extras>/<Parameters> with all attributes for testing; ignored by channels.")
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
                r = requests.get(src_url, headers={"User-Agent": "FeedFixerPreview/1.1"}, timeout=40)
                r.raise_for_status()
                feed_bytes = r.content
                label = src_url
            except Exception as e:
                st.error(f"Failed to fetch URL: {e}")
    elif src_file is not None:
        feed_bytes = src_file.read()
        label = src_file.name
    else:
        st.warning("Please provide a URL or upload a file.")

    if feed_bytes:
        st.write(f"**Input:** `{label}`")
        try:
            root = ET.fromstring(feed_bytes)
        except ET.ParseError as e:
            st.error(f"XML parse error: {e}")
            st.stop()

        rows = extract_items_neutral(root)
        total_before = len(rows)

        if do_avail_norm:
            for r in rows:
                r["availability"] = normalize_availability(r.get("availability",""))

        if do_dedupe:
            seen = set(); deduped = []
            for r in rows:
                key = (r.get("id",""), r.get("link",""))
                if key in seen:
                    continue
                seen.add(key); deduped.append(r)
            rows = deduped

        if limit_items and limit_items > 0:
            rows = rows[:limit_items]

        total_after = len(rows)
        missing_url = sum(1 for r in rows if not r.get("link"))
        missing_img = sum(1 for r in rows if not r.get("images"))

        c1, c2, c3, c4 = st.columns(4)
        c1.metric("Items (before)", total_before)
        c2.metric("Items (after)", total_after)
        c3.metric("Missing URL", missing_url)
        c4.metric("Missing primary image", missing_img)

        xml_bytes = EMITTERS[target](rows, include_extras)

        with st.expander("Preview (first lines)", expanded=False):
            preview = xml_bytes.decode("utf-8", errors="replace").splitlines()
            st.code("\n".join(preview[:150]))

        dl_name = f"fixed_{target.lower().replace(' ','_')}_rich.xml"
        st.download_button("⬇️ Download fixed feed", xml_bytes, file_name=dl_name, mime="application/xml")

        st.info("This preserves extra data where the target spec supports it, and adds a neutral block otherwise. "
                "When DEV gives you /fixed/ write access, we can add a “Publish” button to upload to a stable URL.")
