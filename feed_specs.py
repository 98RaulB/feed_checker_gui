# feed_specs.py
from __future__ import annotations
from typing import Dict, List, Tuple, Any

# Safe XML parsing
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

# -------------------- Small helpers --------------------
def strip_ns(tag: str) -> str:
    return tag.split('}', 1)[1] if isinstance(tag, str) and '}' in tag else tag

# ---- RAW versions for validation (no percent-encoding) ----
def read_link_raw(elem: ET.Element, spec_name: str) -> str:
    return _first(elem, SPEC.get(spec_name, {}).get("link_paths", []))

def _looks_like_google_without_ns(root: ET.Element) -> bool:
    """
    Heuristic: <rss> root, has <item>, and inside <item> we see common
    Google tag names (id, link, image_link, price, availability) WITHOUT the g: namespace.
    Also ensure the Google namespace string is NOT present.
    """
    try:
        root_xml = ET.tostring(root, encoding="utf-8", method="xml")
    except Exception:
        root_xml = b""

    if b"base.google.com/ns/1.0" in root_xml:
        return False  # it actually has g:, so not this case

    root_local = strip_ns(root.tag).lower()
    if root_local != "rss":
        return False

    first_item = root.find(".//item")
    if first_item is None:
        return False

    # collect child local tag names of the first item
    locals_ = {strip_ns(c.tag).lower() for c in list(first_item)}
    googleish = {"id", "link", "image_link", "price", "availability", "product_type", "title", "description"}
    # require at least a minimal core set
    core = {"id", "link", "image_link"}
    return (len(locals_ & googleish) >= 3) and (core <= locals_)

def gather_primary_image_raw(elem: ET.Element, spec_name: str) -> str:
    paths = SPEC.get(spec_name, {}).get("image_primary_paths", [])
    return _first(elem, paths) if paths else ""

def _exists_local(root: ET.Element, localname: str) -> bool:
    lname = localname.lower()
    for e in root.iter():
        if isinstance(e.tag, str) and strip_ns(e.tag).lower() == lname:
            return True
    return False

def _first_local(root: ET.Element, localname: str) -> ET.Element | None:
    lname = localname.lower()
    for e in root.iter():
        if isinstance(e.tag, str) and strip_ns(e.tag).lower() == lname:
            return e
    return None

def _text(n) -> str:
    return (n.text or "").strip()

def _select_value(elem: ET.Element, path: str) -> str:
    """
    Lightweight selector:
      - '@id'                          -> elem.get('id')
      - './node/@url'                  -> elem.find('./node', NS).get('url')
      - './node' or 'node'             -> text of that node (first)
    """
    path = path.strip()
    if not path:
        return ""
    # direct attribute of current element
    if path.startswith("@"):
        return (elem.get(path[1:]) or "").strip()

    # attribute of a found child: '.../@attr'
    if "/@" in path:
        node_path, attr = path.rsplit("/@", 1)
        n = elem.find(node_path, namespaces=NS)
        if n is not None:
            return (n.get(attr) or "").strip()
        return ""

    # normal element text
    n = elem.find(path, namespaces=NS)
    return (n.text or "").strip() if n is not None and n.text else ""

def _first(elem: ET.Element, paths: List[str]) -> str:
    for p in paths:
        t = _select_value(elem, p)
        if t:
            return t
    return ""

def _all(elem: ET.Element, paths: List[str]) -> List[str]:
    out: List[str] = []
    for p in paths:
        # collect attributes if '/@' is used, else element texts
        if "/@" in p:
            node_path, attr = p.rsplit("/@", 1)
            for n in elem.findall(node_path, namespaces=NS):
                v = (n.get(attr) or "").strip()
                if v:
                    out.append(v)
        elif p.startswith("@"):
            v = (elem.get(p[1:]) or "").strip()
            if v:
                out.append(v)
        else:
            for n in elem.findall(p, namespaces=NS):
                v = (n.text or "").strip()
                if v:
                    out.append(v)
    return out

def percent_encode_url(url: str) -> str:
    if not url:
        return url
    from urllib.parse import urlsplit, urlunsplit, quote
    parts = urlsplit(url)
    path = quote(parts.path or "", safe="/:@&+$,;=-._~")
    query = quote(parts.query or "", safe="=/?&:+,;@-._~")
    frag  = quote(parts.fragment or "", safe="-._~")
    return urlunsplit((parts.scheme, parts.netloc, path, query, frag))

# -------------------- EXACT mirror of your checker SPECS --------------------
SPEC: Dict[str, Dict[str, Any]] = {
    "Google Merchant (g:) RSS": {
        "item_paths": [".//item"],
        "id_paths": ["./{http://base.google.com/ns/1.0}id", "./g:id"],
        "link_paths": ["./link", "./{http://base.google.com/ns/1.0}link", "./g:link"],
        "image_primary_paths": ["./{http://base.google.com/ns/1.0}image_link", "./g:image_link"],
        "required_fields": ["title", "description", "link", "image_link"],
        "availability_paths": ["./{http://base.google.com/ns/1.0}availability", "./g:availability"],
        "availability_aliases": ["availability"],
        "signature_tags": [
            "title","description","link","id","image_link","price","availability","brand","mpn",
            "gtin","condition","google_product_category","product_type","shipping"
        ],
        "expected_root_locals": ["rss"],
        "required_ns_fragments": ["base.google.com/ns/1.0"],
    },
    
    "Google Merchant (g:) Atom": {
        "item_paths": [".//{http://www.w3.org/2005/Atom}entry", ".//entry"],
        "id_paths": ["./{http://base.google.com/ns/1.0}id", "./g:id"],
        # ⬇️ Replace your current link_paths with this list
        "link_paths": [
            "./atom:link[@rel='alternate']/@href",     # preferred Atom form
            "./atom:link/@href",                       # any Atom link with href
            "./{http://www.w3.org/2005/Atom}link/@href",
            "./atom:link",                             # (less common) text inside <link>...</link>
            "./{http://www.w3.org/2005/Atom}link",
            "./link",                                  # bad feeds that drop the ns
            "./g:link",                                # very rare, but harmless fallback
            "./{http://base.google.com/ns/1.0}link",
        ],
        "image_primary_paths": ["./{http://base.google.com/ns/1.0}image_link", "./g:image_link"],
        "required_fields": ["id", "link", "image_link"],
        "availability_paths": ["./{http://base.google.com/ns/1.0}availability", "./g:availability"],
        "availability_aliases": ["availability"],
        "signature_tags": [
            "id","title","description","link","image_link","price","availability","brand","mpn",
            "gtin","condition","google_product_category","product_type","shipping"
        ],
        "expected_root_locals": ["feed"],
        "required_ns_fragments": ["base.google.com/ns/1.0"],
    },

    "Google Merchant (no-namespace) RSS": {
    "item_paths": [".//item"],
    "id_paths": ["./id"],
    "link_paths": ["./link"],
    "image_primary_paths": ["./image_link"],
    "required_fields": ["title", "description", "link", "image_link"],
    "availability_paths": ["./availability"],
    "availability_aliases": ["availability"],
    "signature_tags": [
        "title","description","link","id","image_link","price","availability",
        "brand","mpn","gtin","condition","google_product_category","product_type","shipping"
    ],
    "expected_root_locals": ["rss"],
    # helps GUI show a hint
    "required_ns_fragments": [],  # intentionally empty (no g:)
    },
    
    "Heureka strict": {
        "item_paths": [".//SHOPITEM"],
        "id_paths": ["./ITEM_ID"],
        "link_paths": ["./URL"],
        "image_primary_paths": ["./IMGURL"],
        "required_fields": ["item_id", "productname", "url", "imgurl"],
        "availability_paths": ["./AVAILABILITY", "./DELIVERY", "./delivery", "./AVAILABILITY_DESC", "./DELIVERY_DATE"],
        "availability_aliases": ["availability", "delivery", "availability_desc", "delivery_date"],
        "signature_tags": [
            "item_id","productname","description","url","imgurl","price","manufacturer",
            "categorytext","availability","delivery","delivery_time"
        ],
        "expected_root_locals": ["shop"],
        # Special rule from your checker’s behavior: DELIVERY_DATE < 3 => “in stock”
        "special": {"heureka_delivery_date_to_availability": True},
    },
    "Compari / Árukereső / Pazaruvaj (case-insensitive)": {
        "item_paths": [".//product"],
        "id_paths": ["./Identifier", "./identifier", "./ProductId", "./productid", "./id"],
        "link_paths": ["./Product_url", "./product_url"],
        "image_primary_paths": ["./Image_url", "./image_url"],
        "required_fields": ["identifier|productid", "name", "product_url", "price", "image_url", "category", "description"],
        "availability_paths": ["./availability", "./in_stock", "./stock", "./availability_status", "./Delivery_time"],
        "availability_aliases": ["availability", "in_stock", "stock", "availability_status", "Delivery_time"],
        "signature_tags": [
            "identifier","productid","name","product_url","price","old_price","image_url","category",
            "category_full","manufacturer","description","delivery_time","stock","in_stock"
        ],
        "expected_root_locals": ["products"],
    },
    "Skroutz strict": {
        "item_paths": [".//product"],
        "id_paths": ["./id"],
        "link_paths": ["./link"],
        "image_primary_paths": ["./image"],
        "required_fields": ["id", "name", "link", "image", "price_with_vat"],
        "availability_paths": ["./availability", "./in_stock", "./stock"],
        "availability_aliases": ["availability", "in_stock", "stock"],
        "signature_tags": [
            "id","name","link","image","price_with_vat","category","category_id","brand","availability"
        ],
        "expected_root_locals": ["products"],
    },
    "Jeftinije / Ceneje strict": {
        "item_paths": [".//Item"],
        "id_paths": ["./ID", "./id"],
        "link_paths": ["./link"],
        "image_primary_paths": ["./mainImage", "./image"],
        "required_fields": ["id", "name", "link", "mainimage|image", "price"],
        "availability_paths": ["./availability", "./in_stock", "./stock"],
        "availability_aliases": ["availability", "in_stock", "stock"],
        "signature_tags": [
            "id","name","link","mainimage","image","price","brand","category","availability","description"
        ],
        "expected_root_locals": ["items","products","shop"],
    },
    "Ceneo strict": {
    "item_paths": [".//o"],             # unchanged
    # read from attributes on <o>
    "id_paths": ["@id"],
    "link_paths": ["@url"],
    # keep element fallback if some feeds use child nodes
    "image_primary_paths": ["./imgs/main/@url", "./image"],
    # (optional) gallery if present as <imgs><img url="..."/>
    "image_gallery_paths": ["./imgs/img/@url"],
    "required_fields": ["name", "price", "cat", "url"],
    # availability commonly in attribute 'avail' on <o>
    "availability_paths": ["@avail", "@availability", "@stock"],
    "availability_aliases": ["availability", "stock", "avail"],
    "signature_tags": [
        "id","name","price","cat","url","imgs","main","desc","avail","availability","stock"
    ],
    "expected_root_locals": ["offers"],
},
}

# -------------------- Detection = mirror of your detectors --------------------
def _exists(root, xpath: str) -> bool:
    return root.find(xpath, namespaces=NS) is not None

def detect_spec(root: ET.Element) -> str:
    root_xml = ET.tostring(root, encoding="utf-8", method="xml")

    # Google Atom
    if any(strip_ns(e.tag).lower() == "entry" for e in root.iter()):
        if b"base.google.com/ns/1.0" in root_xml:
            return "Google Merchant (g:) Atom"
    # Google RSS
    if _exists(root, ".//item"):
        if b"base.google.com/ns/1.0" in root_xml:
            return "Google Merchant (g:) RSS"
            
    # Google RSS (no g: namespace) — must come BEFORE marketplace checks
    if _looks_like_google_without_ns(root):
        return "Google Merchant (no-namespace) RSS"

    # Heureka: SHOPITEM (case-insensitive)
    if _exists(root, ".//SHOPITEM") or _exists_local(root, "shopitem"):
        return "Heureka strict"

    # CENEO: <o> anywhere
    if _exists(root, ".//o") or _exists_local(root, "o"):
        return "Ceneo strict"

    # CENEJE / JEFTINIJE: <Item> or <item> anywhere
    if _exists(root, ".//Item") or _exists_local(root, "item"):
        return "Jeftinije / Ceneje strict"

    # Compari / Skroutz: look for <product> (case-insensitive). Skroutz has price_with_vat.
    if _exists(root, ".//product") or _exists_local(root, "product"):
        sample = root.find(".//product", namespaces=NS) or _first_local(root, "product")
        if sample is not None and (sample.find("./price_with_vat", namespaces=NS) is not None or
                                   any(strip_ns(c.tag).lower()=="price_with_vat" for c in list(sample))):
            return "Skroutz strict"
        return "Compari / Árukereső / Pazaruvaj (case-insensitive)"

    # Fallbacks for odd Google
    if any(strip_ns(e.tag).lower() == "entry" for e in root.iter()) and b"base.google.com/ns/1.0" in root_xml:
        return "Google Merchant (g:) Atom"
    if (_exists(root, ".//item") or _exists_local(root, "item")) and b"base.google.com/ns/1.0" in root_xml:
        return "Google Merchant (g:) RSS"

    return "UNKNOWN"

# -------------------- Shared image & field accessors --------------------
def gather_primary_image(elem: ET.Element, spec_name: str, do_percent_encode: bool = True) -> str:
    paths = SPEC.get(spec_name, {}).get("image_primary_paths", [])
    prim = _first(elem, paths) if paths else ""
    if do_percent_encode and prim:
        prim = percent_encode_url(prim)
    return prim

def gather_gallery(elem: ET.Element, spec_name: str, do_percent_encode: bool = True) -> List[str]:
    """
    Your checker spec list doesn’t include explicit gallery paths,
    so by default we return an empty list here. If you want to add
    gallery support spec-by-spec, add an `"image_gallery_paths": [...]`
    list to the SPEC entry, and we’ll read it.
    """
    paths = SPEC.get(spec_name, {}).get("image_gallery_paths", [])
    out: List[str] = _all(elem, paths) if paths else []
    if do_percent_encode:
        out = [percent_encode_url(u) for u in out if u]
    # de-dup & keep order
    seen, dedup = set(), []
    for u in out:
        if u and u not in seen:
            seen.add(u); dedup.append(u)
    return dedup

def get_item_nodes(root: ET.Element, spec_name: str) -> List[ET.Element]:
    paths = SPEC.get(spec_name, {}).get("item_paths", [])
    nodes: List[ET.Element] = []
    # 1) Try configured XPaths (fast path)
    for p in paths:
        nodes += root.findall(p, namespaces=NS)
    if nodes:
        return nodes

    # 2) Case-insensitive fallback by localname (from last path token)
    desired: List[str] = []
    for p in paths:
        last = p.split("/")[-1]  # e.g., "product", "Item", "{ns}entry", "o"
        last = last.strip(".")
        last_local = strip_ns(last).lower()
        if last_local:
            desired.append(last_local)

    if not desired:
        return nodes

    out: List[ET.Element] = []
    want = set(desired)
    for e in root.iter():
        if isinstance(e.tag, str) and strip_ns(e.tag).lower() in want:
            out.append(e)
    return out

def read_id(elem: ET.Element, spec_name: str) -> str:
    return _first(elem, SPEC.get(spec_name, {}).get("id_paths", []))

def read_link(elem: ET.Element, spec_name: str) -> str:
    val = _first(elem, SPEC.get(spec_name, {}).get("link_paths", []))
    return percent_encode_url(val) if val else ""

def read_availability(elem: ET.Element, spec_name: str) -> str:
    # First try explicit availability paths
    paths = SPEC.get(spec_name, {}).get("availability_paths", [])
    val = _first(elem, paths) if paths else ""
    if val:
        return val

    # Heureka special: if DELIVERY_DATE is present and < 3 -> "in stock"
    if spec_name == "Heureka strict":
        dd = _first(elem, ["./DELIVERY_DATE"])
        if dd and dd.isdigit() and int(dd) < 3:
            return "in stock"
    return ""

def required_fields(spec_name: str) -> List[str]:
    return SPEC.get(spec_name, {}).get("required_fields", [])

def signature_tags(spec_name: str) -> List[str]:
    return SPEC.get(spec_name, {}).get("signature_tags", [])

def expected_root_locals(spec_name: str) -> List[str]:
    return SPEC.get(spec_name, {}).get("expected_root_locals", [])

def requires_google_ns(spec_name: str) -> bool:
    frags = SPEC.get(spec_name, {}).get("required_ns_fragments", [])
    return bool(frags)
