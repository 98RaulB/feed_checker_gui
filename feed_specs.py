# feed_specs.py
from __future__ import annotations
from typing import Dict, List, Tuple, Any
import re  # needed for price parsing

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
    "hz": "http://www.zbozi.cz/ns/offer/1.0",
}

# -------------------- Small helpers --------------------
def strip_ns(tag: str) -> str:
    return tag.split('}', 1)[1] if isinstance(tag, str) and '}' in tag else tag

# ---- RAW versions for validation (no percent-encoding) ----
def read_link_raw(elem: ET.Element, spec_name: str) -> str:
    return _read_first_ci(elem, SPEC.get(spec_name, {}).get("link_paths", []))

# ---------- PRICE HELPERS (no currency) ----------
# One shared analysis is used by BOTH the amount parser and the FAVI format
# validator, so they can never disagree about the same string.
#
# FAVI accepts: "8000", "8,000", "8 000", "8000.70", "8000,70" — comma or
# space may group thousands, dot is ONLY a decimal separator, and >2 decimals
# are auto-rounded (warn, not reject).

# NBSP / narrow NBSP / thin space: CLDR formatters emit these as thousands
# separators for cs/fr/pl locales.
_SPACE_SEPS = "\u00A0\u202F\u2009"

# The full contiguous numeric region (digits + any separators), NOT just the
# longest well-formed prefix — so malformed grouping like "1234 567" is seen
# whole and rejected instead of being silently truncated to "1234".
_num_run_re = re.compile(r"[+-]?\d[\d .,\u00A0\u202F\u2009]*")


def _first_numeric_run(txt: str) -> str:
    m = _num_run_re.search(txt or "")
    if not m:
        return ""
    run = m.group(0)
    for ch in _SPACE_SEPS:
        run = run.replace(ch, " ")
    # trailing separators belong to surrounding prose ("8 000, incl. VAT")
    return run.strip(" .,")


def _grouped_ok(parts: List[str]) -> bool:
    """Thousands grouping: 1-3 digit head, then groups of exactly 3."""
    return (
        bool(parts)
        and parts[0].isdigit() and 1 <= len(parts[0]) <= 3
        and all(p.isdigit() and len(p) == 3 for p in parts[1:])
    )


def analyze_price_text(raw_text: str) -> Tuple[float | None, bool, bool, str]:
    """
    Returns (amount, format_valid, overprecision, reason).
      amount        parsed value, or None when it can't be read confidently
      format_valid  obeys FAVI grouping/decimal rules
      overprecision >2 decimals (FAVI rounds; warn only)
      reason        explanation when invalid
    """
    if raw_text is None or not str(raw_text).strip():
        return None, False, False, "missing"
    body = _first_numeric_run(str(raw_text))
    if not body:
        return None, False, False, "no numeric token"
    sign = -1.0 if body.startswith("-") else 1.0
    body = body.lstrip("+-")
    if not body or not re.fullmatch(r"[\d .,]+", body):
        return None, False, False, "not a numeric value"

    has_space = " " in body
    n_dots = body.count(".")
    n_commas = body.count(",")
    group_sep = ""   # "." / "," when that char groups thousands
    dec_part = ""

    if n_dots and n_commas:
        # Both present: the rightmost one is the decimal separator.
        dec_sep = "." if body.rfind(".") > body.rfind(",") else ","
        group_sep = "," if dec_sep == "." else "."
        head, _, dec_part = body.rpartition(dec_sep)
        if dec_sep in head or not dec_part.isdigit():
            return None, False, False, "malformed number"
    elif n_dots or n_commas:
        sep = "." if n_dots else ","
        if (n_dots or n_commas) > 1:
            # Repeated separator can only be thousands grouping.
            group_sep, head = sep, body
        else:
            head0, _, tail = body.rpartition(sep)
            if (
                len(tail) == 3 and tail.isdigit()
                and not has_space
                and head0.isdigit() and 1 <= len(head0) <= 3
            ):
                # "8,000" / "8.000" style: separator groups thousands.
                group_sep, head = sep, body
            else:
                head, dec_part = head0, tail
                if not dec_part.isdigit():
                    return None, False, False, "malformed number"
    else:
        head = body

    parts = re.split(r"[ " + re.escape(group_sep) + r"]" if group_sep else r"[ ]", head)
    if any(not p.isdigit() for p in parts):
        return None, False, False, "malformed number"
    if len(parts) > 1 and not _grouped_ok(parts):
        return None, False, False, "digit grouping is malformed (thousands groups must be 3 digits)"

    amount = sign * float("".join(parts) + ("." + dec_part if dec_part else ""))
    overprec = len(dec_part) > 2

    if group_sep == ".":
        # FAVI: dot must never group thousands (dot is only a decimal separator).
        return amount, False, overprec, (
            "dot used to group thousands — FAVI reads '.' only as a decimal "
            "separator (use spaces, commas, or nothing)"
        )
    return amount, True, overprec, ""


def parse_price_text(raw_text: str) -> float | None:
    """Return amount parsed from raw text like '1 234,50'."""
    return analyze_price_text(raw_text)[0]

def read_price_text(elem: ET.Element, spec_name: str) -> str:
    """
    Return the raw textual price as found in the element, with spec-specific paths first,
    then generic fallbacks. Does NOT normalize; use parse_price_text() for that.
    """
    spec = SPEC.get(spec_name, {})
    paths: List[str] = spec.get("price_paths", [])

    # Generic fallbacks are used ONLY when the spec defines no price paths of
    # its own (unknown/unconfigured formats). For a known spec they would mask
    # a missing required VAT price — e.g. a Heureka item with only a net
    # <PRICE> (no PRICE_VAT) must be reported as missing, not silently read.
    fallback_paths = [] if paths else [
        "g:sale_price", "g:price",        # Google Merchant
        "PRICE_VAT", "Price", "price",    # Heureka/Compari/etc.
        "price_with_vat",                 # Skroutz
        "@price",                         # Ceneo-style attribute
    ]

    for p in paths + fallback_paths:
        txt = _first(elem, [p])
        if txt:
            return txt

    # Attribute-style composite nodes, e.g. <price amount="123.45" .../>
    node = _first_node(elem, paths + fallback_paths)
    if node is not None:
        a = (node.get("amount") or "").strip()
        if a:
            return a

    # Case-insensitive fallback (e.g. feed uses <PRICE> where the spec lists
    # ./price / ./Price). Last resort, after exact paths and amount-attr nodes.
    return _value_by_localname_ci(elem, _aliases_from_paths(paths + fallback_paths))

def read_price(elem: ET.Element, spec_name: str) -> tuple[float | None, str]:
    """
    Unified price reader for GUI:
      returns (amount_float_or_None, raw_text_as_found)
    """
    raw = read_price_text(elem, spec_name)
    amt = parse_price_text(raw)
    return amt, raw

def _looks_like_google_without_ns(root: ET.Element) -> bool:
    try:
        root_xml = ET.tostring(root, encoding="utf-8", method="xml")
    except Exception:
        root_xml = b""
    if b"base.google.com/ns/1.0" in root_xml:
        return False
    # Google Shopping without the g: namespace ships in more than one wrapper:
    # the canonical <rss><channel>, but also a bare <items> root (Channable
    # exports, e.g. vidaXL.cz). Don't gate on the root tag — the item-level
    # field signature below (id+link+image_link, all Google-specific) is the
    # real discriminator, and it keeps marketplace formats out: Jeftinije uses
    # <Item> with <mainImage>/<slikaVelika>, Ceneo <o>, Heureka <SHOPITEM> —
    # none carry an <image_link> child, so none satisfy `core` below.
    items = root.findall(".//item")[:5]  # look at up to 5 items
    if not items:
        return False
    googleish = {"id","link","image_link","price","availability","product_type","title","description"}
    core = {"id","link","image_link"}
    for it in items:
        locals_ = {strip_ns(c.tag).lower() for c in list(it)}
        if core <= locals_ and len(locals_ & googleish) >= 3:
            return True
    return False

def gather_primary_image_raw(elem: ET.Element, spec_name: str) -> str:
    paths = SPEC.get(spec_name, {}).get("image_primary_paths", [])
    return _read_first_ci(elem, paths)

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

def _first_node(elem: ET.Element, paths: List[str]) -> ET.Element | None:
    """
    Return the first matching node for any of the given paths (ignoring '/@attr' tail if present).
    """
    for p in paths:
        p = p.strip()
        if not p:
            continue
        node_path = p
        if p.startswith("@"):
            # attribute on current node → the node is elem itself
            return elem
        if "/@" in p:
            node_path, _ = p.rsplit("/@", 1)
        n = elem.find(node_path, namespaces=NS)
        if n is not None:
            return n
    return None

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
    try:
        parts = urlsplit(url)
    except ValueError:
        # e.g. unclosed '[' parses as a broken IPv6 host — leave the URL
        # untouched; the validity checks flag it, and one bad URL must not
        # abort the whole run.
        return url
    # '%' is kept safe so an already-encoded URL passes through unchanged
    # (idempotent) instead of being double-encoded to %25xx.
    path = quote(parts.path or "", safe="%/:@&+$,;=-._~")
    query = quote(parts.query or "", safe="%=/?&:+,;@-._~")
    frag  = quote(parts.fragment or "", safe="%-._~")
    return urlunsplit((parts.scheme, parts.netloc, path, query, frag))

# -------------------- SPEC definitions --------------------
SPEC: Dict[str, Dict[str, Any]] = {
    "Google Merchant (g:) RSS": {
        "item_paths": [".//item"],
        "id_paths": ["./{http://base.google.com/ns/1.0}id", "./g:id"],
        "link_paths": ["./link", "./{http://base.google.com/ns/1.0}link", "./g:link"],
        "image_primary_paths": ["./{http://base.google.com/ns/1.0}image_link", "./g:image_link"],
        "price_paths": [
            "./{http://base.google.com/ns/1.0}sale_price", "./g:sale_price",
            "./{http://base.google.com/ns/1.0}price", "./g:price",
        ],
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
        "link_paths": [
            "./atom:link[@rel='alternate']/@href",
            "./atom:link/@href",
            "./{http://www.w3.org/2005/Atom}link/@href",
            "./atom:link",
            "./{http://www.w3.org/2005/Atom}link",
            "./link",
            "./g:link",
            "./{http://base.google.com/ns/1.0}link",
        ],
        "image_primary_paths": ["./{http://base.google.com/ns/1.0}image_link", "./g:image_link"],
        "price_paths": [
            "./{http://base.google.com/ns/1.0}sale_price", "./g:sale_price",
            "./{http://base.google.com/ns/1.0}price", "./g:price",
        ],
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
        "price_paths": ["./sale_price", "./price"],
        "required_fields": ["title", "description", "link", "image_link"],
        "availability_paths": ["./availability"],
        "availability_aliases": ["availability"],
        "signature_tags": [
            "title","description","link","id","image_link","price","availability",
            "brand","mpn","gtin","condition","google_product_category","product_type","shipping"
        ],
        # no-namespace Google arrives under <rss><channel>, a bare <items> root
        # (Channable exports), or a lone <channel> — all carry <item> children.
        "expected_root_locals": ["rss", "items", "channel"],
        "required_ns_fragments": [],
    },

    "Heureka strict": {
        "item_paths": [
            ".//hz:SHOPITEM",
            ".//SHOPITEM", ".//shopitem", ".//ShopItem"
        ],
        # NOTE: ITEMGROUP_ID must NOT be an id fallback — it is shared across
        # variants, so using it masks missing ITEM_IDs and fabricates
        # duplicate-ID errors for every variant group.
        "id_paths": [
            "./hz:ITEM_ID", "./hz:item_id", "./hz:ItemId",
            "./ITEM_ID", "./item_id", "./ItemId"
        ],
        "link_paths": [
            "./hz:URL",
            "./URL", "./Url", "./url"
        ],
        "image_primary_paths": [
            "./hz:IMGURL",
            "./IMGURL", "./ImgUrl", "./imgurl"
        ],
        "image_gallery_paths": [
            "./hz:IMGURL_ALTERNATIVE",
            "./IMGURL_ALTERNATIVE", "./ImgUrl_Alternative", "./imgurl_alternative"
        ],
        "price_paths": [
            "./hz:PRICE_VAT",
            "./PRICE_VAT", "./price_vat"
        ],
        "required_fields": ["ITEM_ID|item_id", "PRODUCTNAME|productname", "URL|url", "IMGURL|imgurl"],
        "availability_paths": [
            "./hz:AVAILABILITY", "./hz:availability",
            "./hz:DELIVERY", "./hz:delivery",
            "./hz:AVAILABILITY_DESC", "./hz:availability_desc",
            "./hz:DELIVERY_DATE", "./hz:delivery_date",
            "./AVAILABILITY", "./availability",
            "./DELIVERY", "./delivery",
            "./AVAILABILITY_DESC", "./availability_desc",
            "./DELIVERY_DATE", "./delivery_date"
        ],
        "availability_aliases": ["availability", "delivery", "availability_desc", "delivery_date"],
        "signature_tags": [
            "item_id","productname","description","url","imgurl","imgurl_alternative","price",
            "manufacturer","categorytext","availability","delivery","delivery_time","delivery_date"
        ],
        "expected_root_locals": ["shop"],
        "special": {"heureka_delivery_date_to_availability": True},
    },

    "Compari / Árukereső / Pazaruvaj (case-insensitive)": {
        "item_paths": [".//product"],
        "id_paths": ["./Identifier", "./identifier", "./ProductId", "./productid", "./id"],
        "link_paths": ["./Product_url", "./product_url", "./ProductUrl", "./producturl"],
        "image_primary_paths": ["./Image_url", "./image_url", "./ImageUrl", "./imageurl"],
        "price_paths": ["./Price", "./price"],
        "required_fields": ["identifier|productid", "name", "product_url", "price", "image_url", "category", "description"],
        "availability_paths": ["./availability", "./in_stock", "./stock", "./availability_status", "./Delivery_time", "./DeliveryTime", "./deliverytime"],
        "availability_aliases": ["availability", "in_stock", "stock", "availability_status", "Delivery_time", "DeliveryTime", "deliverytime"],
        "signature_tags": [
            "identifier","productid","name","product_url","producturl","image_url","imageurl","category",
            "category_full","manufacturer","description","delivery_time","deliverytime","stock","in_stock"
        ],
        "expected_root_locals": ["products"],
    },

    "Skroutz strict": {
        "item_paths": [".//product"],
        "id_paths": ["./id"],
        "link_paths": ["./link"],
        "image_primary_paths": ["./image"],
        "price_paths": ["./price_with_vat"],
        "required_fields": ["id", "name", "link", "image", "price_with_vat"],
        "availability_paths": ["./availability", "./in_stock", "./stock"],
        "availability_aliases": ["availability", "in_stock", "stock"],
        "signature_tags": [
            "id","name","link","image","price_with_vat","category","category_id","brand","availability"
        ],
        # Skroutz's canonical feed wraps <products> in <mywebstore>.
        "expected_root_locals": ["mywebstore", "products"],
    },

    "Jeftinije / Ceneje (element-based)": {
        "item_paths": [".//Item"],
        "id_paths": ["./ID", "./id"],
        "link_paths": ["./link"],
        "image_primary_paths": ["./mainImage", "./image", "./slikaVelika", "./slikaMala"],
        "price_paths": ["./price", "./Price"],
        "required_fields": ["id", "name", "link", "image", "price"],
        "availability_paths": ["./availability", "./in_stock", "./stock"],
        "availability_aliases": ["availability", "in_stock", "stock"],
        "signature_tags": [
            "id","name","link","mainimage","image","price","brand","category","availability","description"
        ],
        "expected_root_locals": ["cnjexport","items","products","shop"],
        "favi_compatible": True,
    },

    "Ceneje.si (attribute-based)": {
        "item_paths": [".//Item"],
        "id_paths": ["@ID", "@id"],
        "link_paths": ["@link"],
        "image_primary_paths": ["@slikaVelika", "@slikaMala", "@image", "@mainImage"],
        "price_paths": ["@price"],
        "required_fields": ["id", "name", "link", "image", "price"],
        "availability_paths": ["@in_stock", "@availability", "@stock"],
        "availability_aliases": ["availability", "in_stock", "stock"],
        "signature_tags": [
            "id","name","link","mainimage","image","price","brand","category","availability","description"
        ],
        "expected_root_locals": ["cnjexport", "items", "products"],
        "favi_compatible": False,
        "conversion_required": True,
        "conversion_note": "Attribute-based Ceneje.si format - FAVI requires element-based format. Use Lambda transformer to convert.",
    },

    "Ceneo strict": {
        "item_paths": [".//o"],
        "id_paths": ["@id"],
        "link_paths": ["@url"],
        "image_primary_paths": ["./imgs/main/@url", "./image"],
        # Ceneo gallery images are <imgs><i url="..."/> (not <img>).
        "image_gallery_paths": ["./imgs/i/@url", "./imgs/img/@url"],
        "price_paths": ["@price", "./price"],  # attribute first
        "required_fields": ["name", "price", "cat", "url"],
        "availability_paths": ["@avail", "@availability", "@stock"],
        "availability_aliases": ["availability", "stock", "avail"],
        "signature_tags": [
            "id","name","price","cat","url","imgs","main","desc","avail","availability","stock"
        ],
        "expected_root_locals": ["offers"],
    },
}

# -------------------- Detection --------------------
def _exists(root, xpath: str) -> bool:
    return root.find(xpath, namespaces=NS) is not None

def _root_local(root: ET.Element) -> str:
    return strip_ns(root.tag).lower() if isinstance(root.tag, str) else ""

def _child_localnames(elem: ET.Element) -> set[str]:
    return {
        strip_ns(child.tag).lower()
        for child in list(elem)
        if isinstance(child.tag, str)
    }

def _attr_names(elem: ET.Element) -> set[str]:
    return {str(k).lower() for k in elem.attrib.keys()}

def _matches_expected_root(root: ET.Element, spec_name: str) -> bool:
    expected = {name.lower() for name in expected_root_locals(spec_name)}
    return not expected or _root_local(root) in expected

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
        sample = root.find(".//o")
        if sample is None:
            sample = _first_local(root, "o")
        if sample is not None:
            attr_names = _attr_names(sample)
            child_names = _child_localnames(sample)
            ceneo_hits = len(attr_names & {"id", "price", "url", "avail", "availability", "stock"})
            ceneo_hits += len(child_names & {"name", "price", "cat", "imgs", "desc"})
            if _matches_expected_root(root, "Ceneo strict") and ceneo_hits >= 3:
                return "Ceneo strict"

    # CENEJE / JEFTINIJE: <Item> or <item> anywhere
    # Distinguish between attribute-based (Ceneje.si) and element-based (Jeftinije)
    if _exists(root, ".//Item") or _exists_local(root, "item"):
        sample = root.find(".//Item")
        if sample is None:
            sample = _first_local(root, "item")
        if sample is not None:
            child_names = _child_localnames(sample)
            attr_names = _attr_names(sample)

            # Check if it uses attributes (Ceneje.si style) or child elements (Jeftinije style)
            has_attr_id = sample.get("ID") is not None or sample.get("id") is not None
            has_attr_price = sample.get("price") is not None
            has_attr_link = sample.get("link") is not None
            has_elem_id = sample.find("./ID") is not None or sample.find("./id") is not None

            attr_hits = len(attr_names & {
                "id", "link", "price", "slikavelika", "slikamala", "image", "mainimage"
            })
            elem_hits = len(child_names & {
                "id", "name", "link", "mainimage", "image", "slikavelika", "slikamala",
                "price", "availability", "description"
            })

            if (
                (has_attr_id or has_attr_price or has_attr_link)
                and _matches_expected_root(root, "Ceneje.si (attribute-based)")
                and attr_hits >= 2
            ):
                return "Ceneje.si (attribute-based)"
            elif (
                has_elem_id
                and _matches_expected_root(root, "Jeftinije / Ceneje (element-based)")
                and elem_hits >= 3
            ):
                return "Jeftinije / Ceneje (element-based)"

    # Compari / Skroutz: look for <product>. Skroutz has price_with_vat.
    if _exists(root, ".//product") or _exists_local(root, "product"):
        sample = root.find(".//product", namespaces=NS)
        if sample is None:
            sample = _first_local(root, "product")
        if sample is not None:
            child_names = _child_localnames(sample)

            skroutz_hits = len(child_names & {"id", "name", "link", "image", "price_with_vat", "category"})
            if (
                "price_with_vat" in child_names
                and _matches_expected_root(root, "Skroutz strict")
                and skroutz_hits >= 3
            ):
                return "Skroutz strict"

            compari_hits = len(child_names & {
                "identifier", "productid", "name", "product_url", "image_url",
                "price", "category", "description"
            })
            # Require at least one Compari-DISTINCTIVE tag. Generic fields
            # (name/price/category/description) alone would misclassify any
            # generic <product> feed as Compari, whose path table then reads
            # <link>/<image> as empty and mass-flags missing URLs/images.
            compari_distinctive = child_names & {
                "identifier", "productid", "product_url", "producturl",
                "image_url", "imageurl",
            }
            if (
                _matches_expected_root(root, "Compari / Árukereső / Pazaruvaj (case-insensitive)")
                and compari_hits >= 3
                and compari_distinctive
            ):
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
    prim = _read_first_ci(elem, paths)
    if do_percent_encode and prim:
        prim = percent_encode_url(prim)
    return prim

def gather_gallery(elem: ET.Element, spec_name: str, do_percent_encode: bool = True) -> List[str]:
    """
    Return gallery images if configured for the spec.
    """
    paths = SPEC.get(spec_name, {}).get("image_gallery_paths", [])
    out: List[str] = _all(elem, paths) if paths else []
    if not out and paths:
        out = _values_by_localname_ci(elem, _aliases_from_paths(paths))
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
    return _read_first_ci(elem, SPEC.get(spec_name, {}).get("id_paths", []))

def read_link(elem: ET.Element, spec_name: str) -> str:
    val = _read_first_ci(elem, SPEC.get(spec_name, {}).get("link_paths", []))
    return percent_encode_url(val) if val else ""

def _value_by_localname_ci(elem: ET.Element, aliases: List[str]) -> str:
    """Case-insensitive value lookup by child/attribute localname.

    Returns the value of the first direct child (or item attribute) whose
    localname equals one of `aliases`, compared case-insensitively. Aliases are
    tried in order, so a spec's preferred field still wins. This tolerates
    tag-casing drift in real feeds — e.g. a ProductsUp export emitting
    <Delivery_Time> when the Compari spec's availability_paths only list the
    case-sensitive ./Delivery_time / ./DeliveryTime / ./deliverytime, which
    ElementTree.find() would never match.
    """
    if not aliases:
        return ""
    child_vals: Dict[str, str] = {}
    for child in list(elem):
        if not isinstance(child.tag, str):
            continue
        local = strip_ns(child.tag).lower()
        txt = (child.text or "").strip()
        if txt and local not in child_vals:
            child_vals[local] = txt
    attr_vals: Dict[str, str] = {
        str(k).lower(): (v or "").strip()
        for k, v in (elem.attrib or {}).items()
        if (v or "").strip()
    }
    for alias in aliases:
        key = alias.lower()
        if child_vals.get(key):
            return child_vals[key]
        if attr_vals.get(key):
            return attr_vals[key]
    return ""


def _values_by_localname_ci(elem: ET.Element, aliases: List[str]) -> List[str]:
    """All direct-child text values whose localname matches one of `aliases`
    (case-insensitive). Multi-value sibling of _value_by_localname_ci, for
    gallery image lists."""
    if not aliases:
        return []
    wanted = {a.lower() for a in aliases}
    out: List[str] = []
    for child in list(elem):
        if isinstance(child.tag, str) and strip_ns(child.tag).lower() in wanted:
            txt = (child.text or "").strip()
            if txt:
                out.append(txt)
    return out


def _alias_from_simple_path(path: str) -> str | None:
    """Localname implied by a *simple* SPEC path: a direct child (./name,
    ./{ns}name, ./pfx:name) or an item attribute (@name). Returns None for
    nested or predicated paths (e.g. ./imgs/main/@url) that a flat localname
    match can't resolve, so we never derive a misleading alias from them."""
    p = path.strip()
    if not p:
        return None
    if p.startswith("@"):
        p = p[1:]
        return p.lower() if p and "/" not in p else None
    if p.startswith("./"):
        p = p[2:]
    if not p or "/" in p or "[" in p or "@" in p:
        return None
    p = strip_ns(p)
    if ":" in p:
        p = p.split(":", 1)[1]
    return p.lower() or None


def _aliases_from_paths(paths: List[str]) -> List[str]:
    """Deduped localnames derived from a spec's *_paths, used as the
    case-insensitive fallback set. Every field reuses the same casing /
    namespace-prefix tolerance without a separately hand-maintained alias list."""
    out: List[str] = []
    seen = set()
    for p in paths:
        a = _alias_from_simple_path(p)
        if a and a not in seen:
            seen.add(a)
            out.append(a)
    return out


def _read_first_ci(elem: ET.Element, paths: List[str]) -> str:
    """Exact-path read with a case-insensitive localname fallback derived from
    the same paths. The fallback fires only when the exact (case-sensitive)
    XPaths miss, so it can only turn a false 'missing' into the real value — it
    never overrides a successful exact match."""
    val = _first(elem, paths) if paths else ""
    if val:
        return val
    return _value_by_localname_ci(elem, _aliases_from_paths(paths))


def read_availability(elem: ET.Element, spec_name: str) -> str:
    # First try explicit availability paths
    paths = SPEC.get(spec_name, {}).get("availability_paths", [])
    val = _first(elem, paths) if paths else ""
    if val:
        return val

    # Case-insensitive fallback. The availability_paths above are matched by
    # ElementTree.find(), which is case-sensitive, so a one-letter casing
    # difference (feed's <Delivery_Time> vs spec's ./Delivery_time) reads as
    # "missing" even though the spec name promises case-insensitivity. Fall
    # back to matching availability_aliases by localname so casing drift in
    # real feeds doesn't produce false "missing availability" reports.
    return _value_by_localname_ci(elem, SPEC.get(spec_name, {}).get("availability_aliases", []))

def required_fields(spec_name: str) -> List[str]:
    return SPEC.get(spec_name, {}).get("required_fields", [])

def signature_tags(spec_name: str) -> List[str]:
    return SPEC.get(spec_name, {}).get("signature_tags", [])

def expected_root_locals(spec_name: str) -> List[str]:
    return SPEC.get(spec_name, {}).get("expected_root_locals", [])

def requires_google_ns(spec_name: str) -> bool:
    frags = SPEC.get(spec_name, {}).get("required_ns_fragments", [])
    return bool(frags)

def is_favi_compatible(spec_name: str) -> bool:
    """Check if the feed format can be parsed directly by FAVI (element-based formats only)."""
    spec = SPEC.get(spec_name, {})
    # Default to True for most formats; only attribute-based formats set this to False
    return spec.get("favi_compatible", True)

def needs_conversion(spec_name: str) -> Tuple[bool, str]:
    """Check if the feed needs conversion before FAVI can use it."""
    spec = SPEC.get(spec_name, {})
    if spec.get("conversion_required", False):
        note = spec.get("conversion_note", "Conversion required for FAVI compatibility")
        return True, note
    return False, ""


# -------------------- Recommended / content elements --------------------
# FAVI documents these element requirements at
# help.favionline.com/en/meanings-and-requirements-for-individual-elements.
# The core checker validates ID / URL / image / availability / price; these
# are the *other* elements FAVI cares about. `required=True` marks elements
# FAVI lists as mandatory for its native format; the rest are recommended
# (they improve listing quality and conversions).
#
# Detection is alias-based over each item's direct-child tag localnames and
# its attribute names — deliberately spec-agnostic, so it works for every
# format in SPEC (element- and attribute-based alike) without enumerating
# exact XPaths per spec. Aliases are matched case-insensitively.
RECOMMENDED_FIELDS: List[Dict[str, Any]] = [
    {
        "key": "title", "label": "Product name", "required": True,
        "aliases": {"title", "productname", "product_name", "name"},
    },
    {
        "key": "description", "label": "Description", "required": True,
        # "summary" is Google Merchant's documented Atom description element.
        "aliases": {"description", "desc", "summary"},
    },
    {
        "key": "category", "label": "Category", "required": True,
        "aliases": {
            "categorytext", "category", "categories", "category_full",
            "cat", "fileunder", "google_product_category", "product_type",
        },
    },
    {
        "key": "delivery", "label": "Delivery / shipping", "required": False,
        "aliases": {
            "delivery", "shipping", "delivery_price", "delivery_cost",
            "deliverycost", "shipping_cost", "shipping_price",
        },
    },
    {
        "key": "brand", "label": "Manufacturer / brand", "required": False,
        # "producent" is Ceneo's Polish attribute name.
        "aliases": {"manufacturer", "brand", "producer", "producent", "vendor"},
    },
    {
        "key": "gtin", "label": "EAN / GTIN", "required": False,
        "aliases": {"ean", "gtin", "gtin13", "gtin14", "ean13", "ean_code", "barcode"},
    },
]


# Containers whose children carry name/value parameter pairs:
# Ceneo <attrs><a name="EAN">v</a>, Compari <Attributes><Attribute>,
# Jeftinije <attributes><attribute><name>/<values>.
_PARAM_CONTAINER_LOCALS = {"attrs", "attributes", "params"}
_PARAM_NAME_LOCALS = {"param_name", "name", "attribute_name"}
_PARAM_VALUE_LOCALS = {"val", "value", "values", "attribute_value"}


def _named_param_values(elem: ET.Element) -> Dict[str, str]:
    """name→value pairs (names lowercased) from parameter containers and
    Heureka-style repeated <PARAM> children. This is where Ceneo feeds keep
    brand ("Producent") and EAN, so presence checks must look inside."""
    out: Dict[str, str] = {}

    def _pair_from(sub: ET.Element) -> Tuple[str, str]:
        nm = (sub.get("name") or "").strip().lower()
        val = (sub.text or "").strip()
        if not nm:
            for g in list(sub):
                if not isinstance(g.tag, str):
                    continue
                g_local = strip_ns(g.tag).lower()
                if g_local in _PARAM_NAME_LOCALS and not nm:
                    nm = (g.text or "").strip().lower()
                elif g_local in _PARAM_VALUE_LOCALS and not val:
                    val = (g.text or "").strip()
        return nm, val

    for child in list(elem):
        if not isinstance(child.tag, str):
            continue
        local = strip_ns(child.tag).lower()
        if local in _PARAM_CONTAINER_LOCALS:
            for sub in list(child):
                if not isinstance(sub.tag, str):
                    continue
                nm, val = _pair_from(sub)
                if nm and val and nm not in out:
                    out[nm] = val
        elif local == "param":
            nm, val = _pair_from(child)
            if nm and val and nm not in out:
                out[nm] = val
    return out


def _present_value_localnames(elem: ET.Element) -> set[str]:
    """Localnames (lowercased) that carry a value on this item element.

    A direct child counts when it has non-empty text, sub-children (container
    elements such as Heureka <DELIVERY> or Google <g:shipping>), or its own
    attributes. Item attributes with a non-empty value count too — this is how
    attribute-based specs (Ceneje.si, Ceneo) expose their fields. Named
    parameters inside attrs/attributes containers count under their name.
    """
    present: set[str] = set()
    for child in list(elem):
        if not isinstance(child.tag, str):
            continue
        if (child.text or "").strip() or len(child) > 0 or child.attrib:
            present.add(strip_ns(child.tag).lower())
    for k, v in (elem.attrib or {}).items():
        if (v or "").strip():
            present.add(str(k).lower())
    present |= set(_named_param_values(elem).keys())
    return present


def read_recommended_value(elem: ET.Element, key: str) -> str:
    """Value of a RECOMMENDED_FIELDS entry (e.g. 'gtin', 'description',
    'category') read the same alias-based way presence is detected — direct
    children / item attributes first, then named parameters."""
    field = next((f for f in RECOMMENDED_FIELDS if f["key"] == key), None)
    if field is None:
        return ""
    aliases = sorted(field["aliases"])
    val = _value_by_localname_ci(elem, aliases)
    if val:
        return val
    named = _named_param_values(elem)
    for a in aliases:
        if named.get(a):
            return named[a]
    return ""


def is_valid_gtin(code: str) -> bool:
    """FAVI EAN/GTIN rule: 8, 12, 13 or 14 digits passing the GS1 checksum."""
    c = re.sub(r"[\s   -]", "", code or "")
    if not c.isdigit() or len(c) not in (8, 12, 13, 14):
        return False
    total = sum(int(d) * (3 if i % 2 == 0 else 1) for i, d in enumerate(reversed(c[:-1])))
    return (10 - total % 10) % 10 == int(c[-1])


def present_recommended_fields(elem: ET.Element) -> set[str]:
    """Return the set of RECOMMENDED_FIELDS keys present (non-empty) on this item."""
    present_locals = _present_value_localnames(elem)
    return {
        field["key"]
        for field in RECOMMENDED_FIELDS
        if present_locals & field["aliases"]
    }
