from __future__ import annotations
import re
import json
from collections import Counter
from typing import List, Optional, Callable, Dict, Any

import streamlit as st

# Prefer defusedxml but be robust if it’s missing some attrs
try:
    from defusedxml import ElementTree as ET  # type: ignore
except Exception:
    import xml.etree.ElementTree as ET  # type: ignore


# -------------------- Helpers & data structures --------------------
def strip_ns(tag: str) -> str:
    return tag.split('}', 1)[1] if '}' in tag else tag

def iter_local_children_tags(elem) -> List[str]:
    return [strip_ns(c.tag).lower() for c in list(elem) if isinstance(c.tag, str)]

def union_child_localnames(items, limit: int = 200) -> set[str]:
    s: set[str] = set()
    for it in items[:limit]:
        s.update(iter_local_children_tags(it))
    return s

def jaccard(a: set[str], b: set[str]) -> float:
    if not a and not b:
        return 1.0
    if not a or not b:
        return 0.0
    inter = len(a & b)
    union = len(a | b)
    return inter / union if union else 0.0


class StrictSpec:
    def __init__(
        self,
        name: str,
        detect_fn: Callable,            # (root) -> bool
        items_getter: Callable,         # (root) -> List[Element]
        id_xpath: List[str],
        url_xpath: List[str],
        image_xpath: List[str],
        required_item_children: List[str],  # case-insensitive, 'a|b' alias allowed
        availability_xpath: List[str],
        availability_aliases: List[str],
        # NEW: richer diagnostics for "closest spec"
        signature_tags: List[str],               # typical local child tags for an item in this spec
        expected_root_locals: List[str] = None,  # acceptable root localnames (e.g., ["rss"], ["feed"])
        required_ns_fragments: List[str] = None, # substrings to hint spec namespaces (e.g., ["base.google.com/ns/1.0"])
    ):
        self.name = name
        self.detect_fn = detect_fn
        self.items_getter = items_getter
        self.id_xpath = id_xpath
        self.url_xpath = url_xpath
        self.image_xpath = image_xpath
        self.required_item_children = required_item_children
        self.availability_xpath = availability_xpath
        self.availability_aliases = [a.lower() for a in availability_aliases]
        self.signature_tags = set(t.lower() for t in signature_tags)
        self.expected_root_locals = [x.lower() for x in (expected_root_locals or [])]
        self.required_ns_fragments = required_ns_fragments or []


def get_text(elem, paths: List[str]) -> Optional[str]:
    for xp in paths:
        found = elem.find(xp, namespaces={"g": "http://base.google.com/ns/1.0"})
        if found is not None and (found.text or "").strip():
            return (found.text or "").strip()
    return None


def make_xpath_items_getter(xpath: str) -> Callable:
    def _get(root):
        return root.findall(xpath, namespaces={"g": "http://base.google.com/ns/1.0"})
    return _get


def atom_entry_items_getter(root):
    # Return all elements whose localname is 'entry' (handles Atom default ns)
    return [e for e in root.iter() if strip_ns(e.tag).lower() == "entry"]


# -------------------- Detectors --------------------
def detect_google_rss(root) -> bool:
    if strip_ns(root.tag).lower() != "rss":
        return False
    has_item = any(strip_ns(e.tag).lower() == "item" for e in root.iter())
    has_g = any("base.google.com/ns/1.0" in (t if isinstance(t, str) else "") for t in [c.tag for c in root.iter()])
    return has_item and has_g

def detect_google_atom(root) -> bool:
    if strip_ns(root.tag).lower() != "feed":
        return False
    has_g_ns = any("base.google.com/ns/1.0" in (t if isinstance(t, str) else "") for t in [c.tag for c in root.iter()])
    if not has_g_ns:
        return False
    entries = atom_entry_items_getter(root)
    return len(entries) > 0

def detect_heureka(root) -> bool:
    return root.find(".//SHOPITEM") is not None

def detect_compari_ci(root) -> bool:
    products = root.findall(".//product")
    if not products:
        return False
    needed_any = {"identifier", "productid"}
    needed_all = {"name", "product_url", "price", "image_url"}
    for p in products:
        tags = set(strip_ns(c.tag).lower() for c in p)
        if (needed_any & tags) and needed_all.issubset(tags):
            return True
    return False

def detect_skroutz_strict(root) -> bool:
    products = root.findall(".//product")
    if not products:
        return False
    for p in products:
        tags = set(strip_ns(c.tag).lower() for c in p)
        if {"price_with_vat", "image", "link"}.issubset(tags):
            return True
    return False

def detect_ceneje(root) -> bool:
    return root.find(".//Item") is not None or root.find(".//item") is not None

def detect_ceneo(root) -> bool:
    return root.find(".//o") is not None


# -------------------- Specs (order still matters for detection; diagnostics ignores order) --------------------
SPECS = [
    StrictSpec(
        "Google Merchant (g:) RSS",
        detect_google_rss,
        make_xpath_items_getter(".//item"),
        ["./{http://base.google.com/ns/1.0}id", "./g:id"],
        ["./link", "./{http://base.google.com/ns/1.0}link", "./g:link"],
        ["./{http://base.google.com/ns/1.0}image_link", "./g:image_link"],
        ["title", "description", "link", "image_link"],
        availability_xpath=["./{http://base.google.com/ns/1.0}availability", "./g:availability"],
        availability_aliases=["availability"],
        signature_tags=[
            "title","description","link","id","image_link","price","availability","brand","mpn",
            "gtin","condition","google_product_category","product_type","shipping"
        ],
        expected_root_locals=["rss"],
        required_ns_fragments=["base.google.com/ns/1.0"],
    ),
    StrictSpec(
        "Google Merchant (g:) Atom",
        detect_google_atom,
        atom_entry_items_getter,
        ["./{http://base.google.com/ns/1.0}id", "./g:id"],
        ["./{http://base.google.com/ns/1.0}link", "./g:link"],
        ["./{http://base.google.com/ns/1.0}image_link", "./g:image_link"],
        ["id", "link", "image_link"],
        availability_xpath=["./{http://base.google.com/ns/1.0}availability", "./g:availability"],
        availability_aliases=["availability"],
        signature_tags=[
            "id","title","description","link","image_link","price","availability","brand","mpn",
            "gtin","condition","google_product_category","product_type","shipping"
        ],
        expected_root_locals=["feed"],
        required_ns_fragments=["base.google.com/ns/1.0"],
    ),
    StrictSpec(
        "Heureka strict",
        detect_heureka,
        make_xpath_items_getter(".//SHOPITEM"),
        ["./ITEM_ID"], ["./URL"], ["./IMGURL"],
        ["item_id", "productname", "url", "imgurl"],
        availability_xpath=["./AVAILABILITY", "./DELIVERY", "./delivery", "./AVAILABILITY_DESC"],
        availability_aliases=["availability", "delivery", "availability_desc"],
        signature_tags=[
            "item_id","productname","description","url","imgurl","price","manufacturer",
            "categorytext","availability","delivery","delivery_time"
        ],
        expected_root_locals=["shop"],
    ),
    StrictSpec(
        "Compari / Árukereső / Pazaruvaj (case-insensitive)",
        detect_compari_ci,
        make_xpath_items_getter(".//product"),
        ["./Identifier", "./identifier", "./ProductId", "./productid", "./id"],
        ["./Product_url", "./product_url", "./URL", "./url", "./link"],
        ["./Image_url", "./image_url", "./image", "./imgurl"],
        ["identifier|productid", "name", "product_url", "price", "image_url", "category", "description"],
        availability_xpath=["./availability", "./in_stock", "./stock", "./availability_status"],
        availability_aliases=["availability", "in_stock", "stock", "availability_status"],
        signature_tags=[
            "identifier","productid","name","product_url","price","old_price","image_url","category",
            "category_full","manufacturer","description","delivery_time","stock","in_stock"
        ],
        expected_root_locals=["products"],
    ),
    StrictSpec(
        "Skroutz strict",
        detect_skroutz_strict,
        make_xpath_items_getter(".//product"),
        ["./id"], ["./link"], ["./image"],
        ["id", "name", "link", "image", "price_with_vat"],
        availability_xpath=["./availability", "./in_stock", "./stock"],
        availability_aliases=["availability", "in_stock", "stock"],
        signature_tags=[
            "id","name","link","image","price_with_vat","category","category_id","brand","availability"
        ],
        expected_root_locals=["products"],
    ),
    StrictSpec(
        "Jeftinije / Ceneje strict",
        detect_ceneje,
        make_xpath_items_getter(".//Item"),
        ["./ID", "./id"], ["./link"], ["./mainImage", "./image"],
        ["id", "name", "link", "mainimage|image", "price"],
        availability_xpath=["./availability", "./in_stock", "./stock"],
        availability_aliases=["availability", "in_stock", "stock"],
        signature_tags=[
            "id","name","link","mainimage","image","price","brand","category","availability","description"
        ],
        expected_root_locals=["items","products","shop"],
    ),
    StrictSpec(
        "Ceneo strict",
        detect_ceneo,
        make_xpath_items_getter(".//o"),
        ["./id"], ["./url"], ["./imgs/main", "./image"],
        ["name", "price", "cat", "url"],
        availability_xpath=["./availability", "./stock", "./avail"],
        availability_aliases=["availability", "stock", "avail"],
        signature_tags=[
            "id","name","price","cat","url","imgs","main","desc","avail","availability","stock"
        ],
        expected_root_locals=["offers"],
    ),
]


# -------------------- Availability helper --------------------
def has_availability(it, spec: StrictSpec) -> bool:
    # 1) Try explicit paths
    txt = get_text(it, spec.availability_xpath)
    if txt:
        return True
    # 2) Fallback: presence of an alias tag name among children (any value)
    child_locals_lower = set(iter_local_children_tags(it))
    return any(alias in child_locals_lower for alias in spec.availability_aliases)


# -------------------- Closest-spec (improved) --------------------
def score_spec_for_diagnostics(root, spec: StrictSpec) -> Dict[str, Any]:
    """
    Heuristic score of how 'close' this feed looks to a known spec.
    Uses multiple signals to avoid bias toward any single tag like <product>.
    """
    root_local = strip_ns(root.tag).lower()

    # Items matched by *this spec's* item getter (not strict detector)
    items = spec.items_getter(root)
    n_items = len(items)

    # Union of local child tags across a sample of items
    union_tags = union_child_localnames(items, limit=200)

    # Signature Jaccard similarity
    sig_sim = jaccard(union_tags, spec.signature_tags)

    # Required coverage: how many required groups have at least one alias present in union
    req_alias_sets = [[a.strip().lower() for a in req.split("|")] for req in spec.required_item_children]
    req_hits = sum(1 for aliases in req_alias_sets if any(a in union_tags for a in aliases))
    req_cov = req_hits / max(len(req_alias_sets), 1)

    # Field retrievability sample
    sample = items[:200]
    id_ok = sum(1 for it in sample if get_text(it, spec.id_xpath))
    url_ok = sum(1 for it in sample if get_text(it, spec.url_xpath))
    img_ok = sum(1 for it in sample if get_text(it, spec.image_xpath))
    avail_ok = sum(1 for it in sample if has_availability(it, spec))
    denom = max(len(sample), 1)
    id_rate = id_ok / denom
    url_rate = url_ok / denom
    img_rate = img_ok / denom
    avail_rate = avail_ok / denom

    # Root expectation
    root_bonus = 0.0
    if spec.expected_root_locals:
        root_bonus = 1.0 if root_local in spec.expected_root_locals else 0.0

    # Namespace hints
    # Get namespaces used anywhere (rough heuristic)
    ns_present = set()
    for e in root.iter():
        if isinstance(e.tag, str) and e.tag.startswith("{"):
            ns_present.add(e.tag.split("}", 1)[0][1:])
    ns_bonus = 0.0
    if spec.required_ns_fragments:
        hits = sum(1 for frag in spec.required_ns_fragments if any(frag in ns for ns in ns_present))
        ns_bonus = hits / len(spec.required_ns_fragments)

    # Core score terms (weights tuned to spread the field)
    score = (
        min(n_items, 2000) * 0.05 +          # small weight for item count
        sig_sim * 100.0 +                    # strong weight for signature tags fit
        req_cov * 80.0 +                     # strong weight for required coverage
        (id_rate + url_rate + img_rate) * 35.0 +  # field retrievability
        avail_rate * 25.0 +                  # availability presence
        root_bonus * 10.0 +                  # root matches expectation
        ns_bonus * 10.0                      # namespaces contain expected fragments
    )

    # Penalty: if many items but very poor signature fit, reduce confidence
    if n_items >= 50 and sig_sim < 0.10:
        score *= 0.6

    return {
        "name": spec.name,
        "score": round(score, 2),
        "items_found": n_items,
        "signature_similarity": round(sig_sim, 3),
        "required_groups_present": req_hits,
        "required_groups_total": len(req_alias_sets),
        "id_rate": round(id_rate, 3),
        "url_rate": round(url_rate, 3),
        "img_rate": round(img_rate, 3),
        "availability_rate": round(avail_rate, 3),
        "root_match": bool(root_bonus),
        "ns_hint_match_fraction": round(ns_bonus, 3),
        "union_tags_sample": sorted(list(union_tags))[:25],  # show a small preview
    }


def closest_spec_hint(root) -> Dict[str, Any]:
    scores = [score_spec_for_diagnostics(root, s) for s in SPECS]
    scores.sort(key=lambda d: d["score"], reverse=True)
    return scores[0] if scores else {"name": None, "score": 0}


# -------------------- Analyzer --------------------
def analyze_feed(feed_bytes: bytes):
    out = {
        "xml_ok": False,
        "detected": None,
        "items": 0,
        "missing_required": {},
        "ids": [], "urls": [], "imgs": [],
        "duplicates": {"ids": {}, "urls": {}},
        "missing_img_count": 0,
        "missing_availability_count": 0,
        "severity": "PASS",
        "notes": [],
        "unrecognized_details": None,
    }
    # Parse
    try:
        root = ET.fromstring(feed_bytes)
        out["xml_ok"] = True
    except ET.ParseError as e:
        out["severity"] = "FAIL"
        out["notes"].append(f"XML parse error: {e}")
        return out

    # Detect (strict)
    spec = next((s for s in SPECS if s.detect_fn(root)), None)
    if not spec:
        # Diagnostics + closest-spec hint (improved)
        details = {
            "root": root.tag,
            "local_root": strip_ns(root.tag),
            "top_local_tags": Counter(strip_ns(e.tag) for e in root.iter()).most_common(12),
        }
        hint = closest_spec_hint(root)
        details["closest_spec"] = hint
        out["unrecognized_details"] = details
        out["detected"] = "Unrecognized"
        out["severity"] = "FAIL"
        return out

    out["detected"] = spec.name
    items = spec.items_getter(root)
    out["items"] = len(items)
    if not items:
        out["severity"] = "FAIL"
        out["notes"].append("No items found for detected transformation.")
        return out

    # Structural validation — WARN (never FAIL)
    missing_required = Counter()
    for it in items:
        child_locals_lower = set(iter_local_children_tags(it))
        for req in spec.required_item_children:
            aliases = [a.strip().lower() for a in req.split('|')]
            if not any(a in child_locals_lower for a in aliases):
                missing_required[req] += 1
    out["missing_required"] = dict(missing_required)

    # Extract core fields
    ids, urls, imgs = [], [], []
    for it in items:
        ids.append(get_text(it, spec.id_xpath) or "")
        urls.append(get_text(it, spec.url_xpath) or "")
        imgs.append(get_text(it, spec.image_xpath) or "")
    out["ids"] = ids
    out["urls"] = urls
    out["imgs"] = imgs

    # Availability presence — WARN
    avail_ok_flags = [has_availability(it, spec) for it in items]
    out["missing_availability_count"] = sum(1 for ok in avail_ok_flags if not ok)

    # Duplicates — FAIL if any non-empty duplicates exist
    def dup_map(values):
        vals = [v for v in values if v]
        c = Counter(vals)
        return {k: c for k, c in c.items() if c > 1}

    id_dupes = dup_map(ids)
    url_dupes = dup_map(urls)
    out["duplicates"]["ids"] = id_dupes
    out["duplicates"]["urls"] = url_dupes
    if id_dupes or url_dupes:
        out["severity"] = "FAIL"

    # Missing images / availability — WARN (never FAIL)
    out["missing_img_count"] = sum(1 for x in imgs if not x)

    # Elevate to WARN if needed (and not already FAIL)
    if out["severity"] != "FAIL":
        if out["missing_required"] or out["missing_img_count"] > 0 or out["missing_availability_count"] > 0:
            out["severity"] = "WARN"
        else:
            out["severity"] = "PASS"

    return out


# -------------------- Streamlit UI --------------------
st.set_page_config(page_title="Feed Checker GUI", layout="wide")
st.title("🧪 Feed Checker GUI (no-URL-probing)")
st.caption("Detects transformation (incl. Google Atom), validates structure, finds duplicates, checks availability, and flags missing primary images.")

with st.form("feed_input"):
    url = st.text_input("Feed URL (http/https)", placeholder="https://example.com/feed.xml")
    file = st.file_uploader("...or upload an XML file", type=["xml"])
    submitted = st.form_submit_button("Run checks")

if submitted:
    feed_bytes = None
    label = None
    if url and re.match(r"^https?://", url, flags=re.I):
        try:
            import requests
        except ImportError:
            st.error("To fetch from URL, install `requests` first: pip install requests")
        else:
            try:
                resp = requests.get(url, headers={"User-Agent": "FeedCheckerGUI/2.4"}, timeout=25)
                resp.raise_for_status()
                feed_bytes = resp.content
                label = url
            except Exception as e:
                st.error(f"Failed to fetch URL: {e}")
    elif file is not None:
        feed_bytes = file.read()
        label = file.name
    else:
        st.warning("Please provide a URL or upload a file.")

    if feed_bytes:
        st.write(f"**Input:** `{label}`")
        result = analyze_feed(feed_bytes)

        # Overall status
        if result["severity"] == "FAIL":
            st.error("Overall: **FAIL** (duplicates or unrecognized/parse error).")
        elif result["severity"] == "WARN":
            st.warning("Overall: **WARN** (parsed with issues like missing tags/images/availability).")
        else:
            st.success("Overall: **PASS**")

        # Summary row
        c1, c2, c3, c4 = st.columns(4)
        c1.metric("Transformation", result["detected"] or "Unrecognized")
        c2.metric("Items", result["items"])
        c3.metric("Duplicate IDs", len(result["duplicates"]["ids"]))
        c4.metric("Duplicate URLs", len(result["duplicates"]["urls"]))

        # Missing required (WARN)
        with st.expander("Structural required tags (WARN)", expanded=False):
            if result["missing_required"]:
                st.write("Count of items missing each required tag (case-insensitive):")
                st.json(result["missing_required"], expanded=False)
            else:
                st.write("All required tags present.")

        # Missing images (WARN)
        with st.expander("Primary image presence (WARN)", expanded=False):
            st.write(f"Items missing a primary image: **{result['missing_img_count']} / {result['items']}**")
            if result["missing_img_count"]:
                missing_idx = [i for i, x in enumerate(result["imgs"]) if not x][:50]
                st.caption("First 50 item indices missing image:")
                st.code(", ".join(map(str, missing_idx)) or "None")

        # Availability presence (WARN)
        with st.expander("Availability presence (WARN)", expanded=False):
            st.write(f"Items missing availability: **{result['missing_availability_count']} / {result['items']}**")

        # Duplicate details (FAIL)
        with st.expander("Duplicate IDs (FAIL if present)", expanded=False):
            if result["duplicates"]["ids"]:
                st.json(dict(list(result["duplicates"]["ids"].items())[:100]), expanded=False)
            else:
                st.write("No duplicate IDs.")
        with st.expander("Duplicate URLs (FAIL if present)", expanded=False):
            if result["duplicates"]["urls"]:
                st.json(dict(list(result["duplicates"]["urls"].items())[:100]), expanded=False)
            else:
                st.write("No duplicate URLs.")

        # Unrecognized diagnostics + closest-spec hint (improved)
        if result["detected"] == "Unrecognized" and result.get("unrecognized_details"):
            with st.expander("Why unrecognized? (diagnostics & closest spec hint)", expanded=True):
                d = result["unrecognized_details"]
                st.write(f"**Root tag:** `{d.get('root')}` (local: `{d.get('local_root')}`)")
                top_tags = d.get("top_local_tags") or []
                if top_tags:
                    st.write("**Top local tag frequencies (approx):**")
                    st.code("\n".join([f"{t}: {c}" for t, c in top_tags]))
                hint = d.get("closest_spec", {})
                if hint and hint.get("name"):
                    st.write("**Closest known transformation (heuristic):**")
                    st.json(hint, expanded=False)

        # Download JSON report
        report = json.dumps(result, indent=2, ensure_ascii=False)
        st.download_button("⬇️ Download JSON report", report, file_name="feed_report.json", mime="application/json")
        st.markdown("---")
st.markdown("© 2025 Raul Bertoldini")

