from __future__ import annotations
from typing import Dict, Any, List, Tuple
import streamlit as st

# Safe XML parsing (fallback if defusedxml missing)
try:
    from defusedxml import ElementTree as ET  # type: ignore
except Exception:
    import xml.etree.ElementTree as ET  # type: ignore

# -------------------- Namespaces --------------------
NS = {
    "g": "http://base.google.com/ns/1.0",
    "content": "http://purl.org/rss/1.0/modules/content/",
    "atom": "http://www.w3.org/2005/Atom",
}

# -------------------- Utils --------------------
def strip_ns(tag: str) -> str:
    return tag.split('}', 1)[1] if isinstance(tag, str) and '}' in tag else tag

def text_of(node) -> str:
    return (node.text or "").strip()

def percent_encode_url(url: str) -> str:
    if not url:
        return url
    from urllib.parse import urlsplit, urlunsplit, quote
    parts = urlsplit(url)
    path = quote(parts.path or "", safe="/:@&+$,;=-._~")
    query = quote(parts.query or "", safe="=/?&:+,;@-._~")
    frag  = quote(parts.fragment or "", safe="-._~")
    return urlunsplit((parts.scheme, parts.netloc, path, query, frag))

def find_first_text(elem: ET.Element, paths: List[str]) -> str:
    for p in paths:
        n = elem.find(p, namespaces=NS)
        if n is not None:
            t = text_of(n)
            if t:
                return t
    return ""

def find_all_texts(elem: ET.Element, paths: List[str]) -> List[str]:
    out: List[str] = []
    for p in paths:
        for n in elem.findall(p, namespaces=NS):
            t = text_of(n)
            if t:
                out.append(t)
    return out

def find_direct_local(elem: ET.Element, localnames: List[str]) -> str:
    wants = {n.lower() for n in localnames}
    for child in list(elem):
        if not isinstance(child.tag, str):
            continue
        if strip_ns(child.tag).lower() in wants:
            t = text_of(child)
            if t:
                return t
    return ""

def find_all_direct_local(elem: ET.Element, localname: str) -> List[str]:
    ln = localname.lower()
    out: List[str] = []
    for child in list(elem):
        if not isinstance(child.tag, str):
            continue
        if strip_ns(child.tag).lower() == ln:
            t = text_of(child)
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
    if not price:
        return False
    has_alpha = any(c.isalpha() for c in price)
    has_symbol = any(c in "€$£ Kčzł₺₴₽¥₪" for c in price)
    return not (has_alpha or has_symbol)

def collect_simple_attrs(item: ET.Element, core_keys: set) -> Dict[str, str]:
    attrs: Dict[str, str] = {}
    for child in list(item):
        if not isinstance(child.tag, str):
            continue
        local = strip_ns(child.tag).lower()
        if local in core_keys:
            continue
        t = text_of(child)
        if t and local not in attrs:
            attrs[local] = t
    return attrs

# -------------------- Core field maps (tightened) --------------------
# Keys we know how to map to the neutral model
CORE_TAGS = {
    "id","identifier","productid","item_id",
    "title","name","productname",
    "description","content","encoded","desc","short_description","long_description","longdesc",
    "link","url","product_url",
    "image","image_url","imgurl","image_link","imgs","mainimage",
    "price","price_vat","price_with_vat","value",
    "availability","in_stock","stock","availability_status","avail","delivery_date",
    "brand","manufacturer","mpn","gtin","ean","barcode","sku",
    "category","categorytext","product_type","google_product_category",
    "item_group_id","condition",
    "imgurl_alternative","images","gallery",
}

FIELD_MAPS: Dict[str, Dict[str, List[str]]] = {
    # --- Deterministic signatures below ---
    "HEUREKA": {
        "id": ["./ITEM_ID"],
        "title": ["./PRODUCTNAME"],
        "link": ["./URL"],
        "description": ["./DESCRIPTION"],
        "price": ["./PRICE_VAT"],
        "availability_hint": ["./DELIVERY_DATE"],   # < 3 => in stock
        "brand": ["./MANUFACTURER"],
        "mpn": ["./PRODUCTNO"],
        "gtin": ["./EAN"],
        "image_primary": ["./IMGURL"],
        "image_gallery": ["./IMGURL_ALTERNATIVE"],
        "item_group_id": ["./ITEMGROUP_ID"],
    },
    "COMPARI": {
        "id": ["./Identifier"],
        "title": ["./Name"],
        "link": ["./Product_url"],
        "description": ["./Description"],
        "price": ["./Price"],
        "availability": ["./availability"],
        "brand": ["./Manufacturer"],
        "image_primary": ["./Image_url"],
        "image_gallery": ["./Gallery/Image"],
        "category": ["./Category"],
    },
    "SKROUTZ": {
        "id": ["./id"],
        "title": ["./name"],
        "link": ["./link"],
        "description": ["./description"],
        "price": ["./price_with_vat"],
        "availability": ["./availability"],
        "brand": ["./brand"],
        "mpn": ["./mpn"],
        "gtin": ["./gtin"],
        "image_primary": ["./image"],
        "image_gallery": ["./images/image"],
    },
    "JEFTINIJE": {  # Ceneje / Jeftinije family
        "id": ["./Id","./ID","./id"],
        "title": ["./Name","./TITLE","./title","./PRODUCTNAME"],
        "link": ["./Url","./URL","./link","./ProductUrl","./Product_url"],
        "description": ["./Description","./description"],
        "price": ["./Price","./PRICE","./price","./price_with_vat"],
        "availability": ["./Availability","./availability"],
        "brand": ["./Manufacturer","./Brand","./MANUFACTURER"],
        "gtin": ["./EAN","./ean","./gtin"],
        "mpn": ["./Code","./code","./mpn","./PRODUCTNO"],
        "image_primary": ["./Image","./IMGURL","./Image_url"],
        "image_gallery": ["./Images/Image","./Gallery/Image","./IMGURL_ALTERNATIVE"],
        "item_group_id": ["./ItemGroupId","./ITEMGROUP_ID"],
    },
    "CENEO": {  # offers/o
        "id": ["./id","./ID","./offer_id"],
        "title": ["./name","./NAME","./title","./Title","./PRODUCTNAME"],
        "link": ["./url","./URL","./link"],
        "description": ["./desc","./DESCRIPTION","./description"],
        "price": ["./price","./PRICE","./price_with_vat"],
        "availability": ["./availability"],
        "brand": ["./producer","./Brand","./MANUFACTURER"],
        "gtin": ["./ean","./EAN","./gtin"],
        "mpn": ["./code","./mpn","./PRODUCTNO"],
        "image_primary": ["./imgs/main","./image","./IMGURL"],
        "image_gallery": ["./imgs/img","./images/image","./IMGURL_ALTERNATIVE"],
    },
    "GOOGLE_RSS": {
        "id": ["./g:id","./{http://base.google.com/ns/1.0}id"],
        "title": ["./title"],
        "link": ["./link","./g:link","./{http://base.google.com/ns/1.0}link"],
        "description": ["./description","./content:encoded"],
        "price": ["./g:price","./{http://base.google.com/ns/1.0}price"],
        "availability": ["./g:availability","./{http://base.google.com/ns/1.0}availability"],
        "brand": ["./g:brand","./{http://base.google.com/ns/1.0}brand"],
        "mpn": ["./g:mpn","./{http://base.google.com/ns/1.0}mpn"],
        "gtin": ["./g:gtin","./{http://base.google.com/ns/1.0}gtin"],
        "image_primary": ["./g:image_link","./{http://base.google.com/ns/1.0}image_link","./image_link"],
        "image_gallery": ["./g:additional_image_link","./{http://base.google.com/ns/1.0}additional_image_link"],
        "item_group_id": ["./g:item_group_id","./{http://base.google.com/ns/1.0}item_group_id"],
        "product_type": ["./g:product_type","./{http://base.google.com/ns/1.0}product_type"],
        "google_product_category": ["./g:google_product_category","./{http://base.google.com/ns/1.0}google_product_category"],
        "condition": ["./g:condition","./{http://base.google.com/ns/1.0}condition"],
    },
    "GOOGLE_ATOM": {
        "id": ["./g:id","./{http://base.google.com/ns/1.0}id"],
        "title": ["./title"],
        "link": ["./g:link","./{http://base.google.com/ns/1.0}link"],
        "description": ["./content","./content:encoded"],
        "price": ["./g:price","./{http://base.google.com/ns/1.0}price"],
        "availability": ["./g:availability","./{http://base.google.com/ns/1.0}availability"],
        "brand": ["./g:brand","./{http://base.google.com/ns/1.0}brand"],
        "mpn": ["./g:mpn","./{http://base.google.com/ns/1.0}mpn"],
        "gtin": ["./g:gtin","./{http://base.google.com/ns/1.0}gtin"],
        "image_primary": ["./g:image_link","./{http://base.google.com/ns/1.0}image_link"],
        "image_gallery": ["./g:additional_image_link","./{http://base.google.com/ns/1.0}additional_image_link"],
        "item_group_id": ["./g:item_group_id","./{http://base.google.com/ns/1.0}item_group_id"],
        "product_type": ["./g:product_type","./{http://base.google.com/ns/1.0}product_type"],
        "google_product_category": ["./g:google_product_category","./{http://base.google.com/ns/1.0}google_product_category"],
        "condition": ["./g:condition","./{http://base.google.com/ns/1.0}condition"],
    },
}

ITEM_PATHS = {
    "HEUREKA": ".//SHOPITEM",
    "COMPARI": ".//Product",
    "SKROUTZ": ".//product",
    "JEFTINIJE": ".//Item",
    "CENEO": ".//o",
    "GOOGLE_RSS": ".//item",
    "GOOGLE_ATOM": ".//{http://www.w3.org/2005/Atom}entry|.//entry",
}

CRITICAL_KEYS = {"id","title","link","image_primary","price","description"}

# -------------------- Deterministic detection --------------------
def detect_source(root: ET.Element) -> str:
    # Root/child signature checks (strict order)
    if root.find(".//SHOPITEM") is not None:
        return "HEUREKA"
    if root.find(".//Product") is not None and strip_ns(root.tag).lower() in {"products","root","xml","export"} or root.find(".//Product") is not None:
        return "COMPARI"
    if root.find(".//product") is not None and strip_ns(root.tag).lower() in {"products","mywebstore","xml","export"} or root.find(".//product") is not None:
        return "SKROUTZ"
    # Ceneje/Jeftinije: CNJExport or <Item> nodes under a CNJ-like root
    if strip_ns(root.tag).lower() in {"cnjexport","cnejexport","jeftinije","ceneje"} or root.find(".//Item") is not None:
        # but make sure it's not a Ceneo <o> feed
        if root.find(".//o") is None and root.find(".//Item") is not None:
            return "JEFTINIJE"
    # Ceneo: offers/o is very characteristic
    if root.find(".//o") is not None:
        return "CENEO"
    # Google Atom vs RSS
    if any(strip_ns(e.tag).lower()=="entry" for e in root.iter()):
        return "GOOGLE_ATOM"
    if root.find(".//item") is not None:
        return "GOOGLE_RSS"
    return "UNKNOWN"

# -------------------- Extraction engine --------------------
def gather_images(item: ET.Element, fm: Dict[str,List[str]]) -> List[str]:
    prim = find_first_text(item, fm.get("image_primary", []))
    gallery = find_all_texts(item, fm.get("image_gallery", []))
    out = []
    if prim:
        out.append(prim)
    for u in gallery:
        if u and u not in out:
            out.append(u)
    enc = []
    for u in out:
        pu = percent_encode_url(u)
        if pu and pu not in enc:
            enc.append(pu)
    return enc

def extract_by_map(root: ET.Element, spec: str) -> List[Dict[str,Any]]:
    items: List[Dict[str,Any]] = []
    path = ITEM_PATHS.get(spec)
    if not path:
        return items

    # support alternation (Atom entry)
    nodes: List[ET.Element] = []
    if "|" in path:
        p1, p2 = path.split("|", 1)
        nodes = root.findall(p1, namespaces=NS) + root.findall(p2, namespaces=NS)
    else:
        nodes = root.findall(path, namespaces=NS)

    fm = FIELD_MAPS[spec]
    for it in nodes:
        row: Dict[str,Any] = {}
        # simple fields
        for key in ["id","title","link","description","price","availability",
                    "brand","mpn","gtin","item_group_id","category",
                    "product_type","google_product_category","condition"]:
            row[key] = find_first_text(it, fm.get(key, [])) or ""

        # images
        row["images"] = gather_images(it, fm)

        # Heureka special: DELIVERY_DATE < 3 => in stock
        if spec == "HEUREKA" and not row.get("availability"):
            hint = find_first_text(it, fm.get("availability_hint", []))
            if hint and hint.strip().isdigit() and int(hint.strip()) < 3:
                row["availability"] = "in stock"

        # sanitize URL
        row["link"] = percent_encode_url(row.get("link",""))

        # generic attributes bucket (non-core children)
        attrs = collect_simple_attrs(it, CORE_TAGS)
        row["attrs"] = attrs

        items.append(row)
    return items

# -------------------- Unknown spec fallback (scoring) --------------------
def candidate_item_nodes(root: ET.Element) -> List[ET.Element]:
    guesses = [".//SHOPITEM",".//Product",".//product",".//Item",".//o",".//item",
               ".//entry",".//{http://www.w3.org/2005/Atom}entry"]
    out: List[ET.Element] = []
    for g in guesses:
        out += root.findall(g, namespaces=NS)
    if not out:
        out = list(root)
    return out

def observed_local_tags(item_nodes: List[ET.Element]) -> set:
    tags = set()
    for it in item_nodes[:50]:
        for ch in list(it):
            if isinstance(ch.tag, str):
                tags.add(strip_ns(ch.tag).lower())
    return tags

def score_similarity(seen_tags: set, fmap: Dict[str,List[str]], item_tag_hint: str = "") -> float:
    # expected local tag names
    expected = set()
    for paths in fmap.values():
        for p in paths:
            local = strip_ns(p.split("/")[-1]).lower().strip(".")
            if local:
                expected.add(local)

    if not expected:
        return 0.0
    inter = len(seen_tags & expected)
    union = len(seen_tags | expected)
    jaccard = inter / union if union else 0.0

    # weight critical keys
    crit_hits = 0
    for k in CRITICAL_KEYS:
        paths = fmap.get(k, [])
        hit = any(strip_ns(px.split("/")[-1]).lower() in seen_tags for px in paths)
        if hit:
            crit_hits += 1
    crit_weight = crit_hits / max(1, len(CRITICAL_KEYS))

    # big bonus for container match (prevents JEFTINIJE vs CENEO confusion)
    bonus = 0.0
    if item_tag_hint:
        hint = item_tag_hint.lower()
        # map container preferences per spec
        preferred = {
            "HEUREKA": {"shopitem"},
            "COMPARI": {"product"},
            "SKROUTZ": {"product"},
            "JEFTINIJE": {"item"},
            "CENEO": {"o"},
            "GOOGLE_RSS": {"item"},
            "GOOGLE_ATOM": {"entry"},
        }
        for spec, fav in preferred.items():
            if hint in fav and fmap is FIELD_MAPS.get(spec):
                bonus = 0.2  # strong bias
                break

    return 0.65 * jaccard + 0.3 * crit_weight + bonus

# -------------------- Emitters --------------------
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
        if r.get("title"): out.append(f"<title>{escape(r['title'])}</title>")
        if r.get("description"): out.append(f"<description>{escape(r['description'])}</description>")
        if r.get("link"): out.append(f"<link>{escape(r['link'])}</link>")
        imgs = r.get("images") or []
        if imgs:
            out.append(f"<g:image_link>{escape(imgs[0])}</g:image_link>")
            for u in imgs[1:]:
                out.append(f"<g:additional_image_link>{escape(u)}</g:additional_image_link>")
        if r.get("price"): out.append(f"<g:price>{escape(r['price'])}</g:price>")
        if r.get("availability"): out.append(f"<g:availability>{escape(r['availability'])}</g:availability>")
        if r.get("brand"): out.append(f"<g:brand>{escape(r['brand'])}</g:brand>")
        if r.get("mpn"): out.append(f"<g:mpn>{escape(r['mpn'])}</g:mpn>")
        if r.get("gtin"): out.append(f"<g:gtin>{escape(r['gtin'])}</g:gtin>")
        if r.get("item_group_id"): out.append(f"<g:item_group_id>{escape(r['item_group_id'])}</g:item_group_id>")
        if r.get("product_type"): out.append(f"<g:product_type>{escape(r['product_type'])}</g:product_type>")
        if r.get("google_product_category"): out.append(f"<g:google_product_category>{escape(r['google_product_category'])}</g:google_product_category>")
        if r.get("condition"): out.append(f"<g:condition>{escape(r['condition'])}</g:condition>")
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
        if r.get("title"): out.append(f"<title>{escape(r['title'])}</title>")
        if r.get("description"): out.append(f"<content type=\"html\">{escape(r['description'])}</content>")
        if r.get("link"): out.append(f"<g:link>{escape(r['link'])}</g:link>")
        imgs = r.get("images") or []
        if imgs:
            out.append(f"<g:image_link>{escape(imgs[0])}</g:image_link>")
            for u in imgs[1:]:
                out.append(f"<g:additional_image_link>{escape(u)}</g:additional_image_link>")
        if r.get("price"): out.append(f"<g:price>{escape(r['price'])}</g:price>")
        if r.get("availability"): out.append(f"<g:availability>{escape(r['availability'])}</g:availability>")
        if r.get("brand"): out.append(f"<g:brand>{escape(r['brand'])}</g:brand>")
        if r.get("mpn"): out.append(f"<g:mpn>{escape(r['mpn'])}</g:mpn>")
        if r.get("gtin"): out.append(f"<g:gtin>{escape(r['gtin'])}</g:gtin>")
        if r.get("item_group_id"): out.append(f"<g:item_group_id>{escape(r['item_group_id'])}</g:item_group_id>")
        if r.get("product_type"): out.append(f"<g:product_type>{escape(r['product_type'])}</g:product_type>")
        if r.get("google_product_category"): out.append(f"<g:google_product_category>{escape(r['google_product_category'])}</g:google_product_category>")
        if r.get("condition"): out.append(f"<g:condition>{escape(r['condition'])}</g:condition>")
        out.append("</entry>")
    out.append("</feed>")
    return ("\n".join(out)).encode("utf-8")

def emit_skroutz(rows: List[Dict[str, Any]]) -> bytes:
    from xml.sax.saxutils import escape
    out = ['<?xml version="1.0" encoding="UTF-8"?>', "<products>"]
    for r in rows:
        out.append("<product>")
        out.append(f"<id>{escape(r['id'])}</id>")
        if r.get("title"): out.append(f"<name>{escape(r['title'])}</name>")
        if r.get("link"): out.append(f"<link>{escape(r['link'])}</link>")
        imgs = r.get("images") or []
        if imgs:
            out.append(f"<image>{escape(imgs[0])}</image>")
            if len(imgs) > 1:
                out.append("<images>")
                for u in imgs[1:]:
                    out.append(f"<image>{escape(u)}</image>")
                out.append("</images>")
        if r.get("price"): out.append(f"<price_with_vat>{escape(r['price'])}</price_with_vat>")
        if r.get("availability"): out.append(f"<availability>{escape(r['availability'])}</availability>")
        if r.get("brand"): out.append(f"<brand>{escape(r['brand'])}</brand>")
        if r.get("mpn"):   out.append(f"<mpn>{escape(r['mpn'])}</mpn>")
        if r.get("gtin"):  out.append(f"<gtin>{escape(r['gtin'])}</gtin>")
        out.append("</product>")
    out.append("</products>")
    return ("\n".join(out)).encode("utf-8")

def emit_compari(rows: List[Dict[str, Any]]) -> bytes:
    from xml.sax.saxutils import escape
    out = ['<?xml version="1.0" encoding="UTF-8"?>', "<Products>"]
    for r in rows:
        out.append("<Product>")
        out.append(f"<Identifier>{escape(r['id'])}</Identifier>")
        if r.get("title"): out.append(f"<Name>{escape(r['title'])}</Name>")
        if r.get("link"):  out.append(f"<Product_url>{escape(r['link'])}</Product_url>")
        imgs = r.get("images") or []
        if imgs:
            out.append(f"<Image_url>{escape(imgs[0])}</Image_url>")
            if len(imgs) > 1:
                out.append("<Gallery>")
                for u in imgs[1:]:
                    out.append(f"<Image>{escape(u)}</Image>")
                out.append("</Gallery>")
        if r.get("price"): out.append(f"<Price>{escape(r['price'])}</Price>")
        if r.get("availability"): out.append(f"<availability>{escape(r['availability'])}</availability>")
        if r.get("brand"): out.append(f"<Manufacturer>{escape(r['brand'])}</Manufacturer>")
        if r.get("category"): out.append(f"<Category>{escape(r['category'])}</Category>")
        out.append("</Product>")
    out.append("</Products>")
    return ("\n".join(out)).encode("utf-8")

EMITTERS = {
    "Google RSS": emit_google_rss,
    "Google Atom": emit_google_atom,
    "Skroutz": emit_skroutz,
    "Compari": emit_compari,
}

# -------------------- UI --------------------
st.set_page_config(page_title="Feed Fixer (Preview)", layout="wide")
st.title("🔧 Feed Fixer (Preview)")
st.caption("Strict IDs. Accurate spec detection (Heureka / Compari / Skroutz / Ceneje-Jeftinije / Ceneo / Google). Fallback fixes unknown feeds with closest map.")

with st.form("input"):
    src_url  = st.text_input("Source feed URL (http/https)", placeholder="https://example.com/feed.xml")
    src_file = st.file_uploader("…or upload an XML file", type=["xml"])
    target   = st.selectbox("Target specification", list(EMITTERS.keys()), index=0)
    st.divider()
    c1, c2, c3 = st.columns(3)
    with c1:
        do_avail_norm = st.checkbox("Normalize textual availability", value=True)
    with c2:
        do_dedupe = st.checkbox("De-duplicate by (id, link)", value=True)
    with c3:
        item_limit = st.number_input("Limit items (0 = all)", min_value=0, value=0, step=50)
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
                r = requests.get(src_url, headers={"User-Agent":"FeedFixer/2.1"}, timeout=45)
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

        detected = detect_source(root)

        if detected in FIELD_MAPS:
            rows = extract_by_map(root, detected)
        else:
            # Unknown → score and choose closest map
            items_guess = candidate_item_nodes(root)
            seen = observed_local_tags(items_guess)
            hint_tag = strip_ns(items_guess[0].tag) if items_guess else ""
            scored: List[Tuple[str,float]] = []
            for name, fmap in FIELD_MAPS.items():
                scored.append((name, score_similarity(seen, fmap, hint_tag)))
            scored.sort(key=lambda x: x[1], reverse=True)

            st.warning("Specification not clearly recognized. Closest candidates:")
            st.table([{"candidate": n, "score": round(s*100,1)} for n, s in scored[:2]])

            best = scored[0][0]
            rows = extract_by_map(root, best)
            detected = f"UNKNOWN → using closest: {best}"

        # Strict IDs
        missing = [i for i, r in enumerate(rows) if not (r.get("id") or "").strip()]
        if missing:
            st.error(f"Missing ID on {len(missing)} items. IDs are mandatory; fix the source feed.")
            with st.expander("Show first missing-ID indices"):
                st.write(missing[:50])
            st.stop()

        # Availability normalizer
        if do_avail_norm:
            for r in rows:
                if r.get("availability"):
                    r["availability"] = normalize_availability(r["availability"])

        # De-duplicate
        if do_dedupe:
            seen_pairs = set(); deduped = []
            for r in rows:
                key = (r.get("id",""), r.get("link",""))
                if key in seen_pairs:
                    continue
                seen_pairs.add(key)
                deduped.append(r)
            rows = deduped

        # Limit
        total_before = len(rows)
        if item_limit and item_limit > 0:
            rows = rows[:item_limit]
        total_after = len(rows)

        # Metrics
        missing_link = sum(1 for r in rows if not r.get("link"))
        missing_img  = sum(1 for r in rows if not (r.get("images") or []))
        c1, c2, c3, c4 = st.columns(4)
        c1.metric("Items (source)", total_before)
        c2.metric("Items (output)", total_after)
        c3.metric("Missing link", missing_link)
        c4.metric("Missing primary image", missing_img)

        if any(warn_if_price_missing_currency(r.get("price","")) for r in rows if r.get("price")):
            st.warning("Some prices appear without currency. Google requires a currency (e.g., '529 CZK').")

        # Emit
        xml_bytes = EMITTERS[target](rows)

        with st.expander("Preview (first lines)"):
            preview = xml_bytes.decode("utf-8", errors="replace").splitlines()
            st.code("\n".join(preview[:150]))

        filename = f"fixed_{target.lower().replace(' ','_')}.xml"
        st.download_button("⬇️ Download fixed feed", xml_bytes, file_name=filename, mime="application/xml")

        st.success(f"Detected source: {detected}")

