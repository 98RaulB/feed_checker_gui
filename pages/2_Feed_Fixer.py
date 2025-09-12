from __future__ import annotations
import hashlib
from typing import Dict, Any, List
import streamlit as st

# Safe XML parsing (fallback if defusedxml missing)
try:
    from defusedxml import ElementTree as ET  # type: ignore
except Exception:
    import xml.etree.ElementTree as ET  # type: ignore


# ---------- Helpers ----------
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


def percent_encode_url(url: str) -> str:
    if not url:
        return url
    from urllib.parse import urlsplit, urlunsplit, quote
    parts = urlsplit(url)
    path = quote(parts.path, safe="/:@&+$,;=-._~")
    query = quote(parts.query, safe="=/?&:+,;@-._~")
    frag = quote(parts.fragment, safe="-._~")
    return urlunsplit((parts.scheme, parts.netloc, path, query, frag))


def synth_id_from_url(u: str) -> str:
    return hashlib.sha1((u or "").encode("utf-8")).hexdigest()[:16]


def normalize_availability(raw: str) -> str:
    v = (raw or "").strip().lower()
    if v in {"in stock", "instock", "available", "yes", "true", "1", "on"}:
        return "in stock"
    if v in {"out of stock", "outofstock", "unavailable", "no", "false", "0", "off"}:
        return "out of stock"
    return raw or ""


# ---------- Neutral extractor ----------
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
            "./id", "./ID", "./Identifier", "./identifier", "./ProductId", "./productid",
            "./ITEM_ID", "./{http://base.google.com/ns/1.0}id", "./g:id",
        ])
        link = first_text(it, [
            "./link", "./Link", "./url", "./URL", "./product_url", "./Product_url",
            "./{http://base.google.com/ns/1.0}link", "./g:link",
        ])
        image = first_text(it, [
            "./image", "./Image", "./image_url", "./Image_url", "./IMGURL", "./imgs/main", "./mainImage",
            "./{http://base.google.com/ns/1.0}image_link", "./g:image_link",
        ])
        title = first_text(it, ["./title", "./Title", "./name", "./Name", "./PRODUCTNAME", "./productname"])
        desc = first_text(it, ["./description", "./Description"])
        price = first_text(it, ["./price", "./PRICE", "./value", "./price_with_vat", "./Price"])
        avail = first_text(it, [
            "./availability", "./in_stock", "./stock", "./availability_status", "./AVAILABILITY", "./avail",
            "./{http://base.google.com/ns/1.0}availability", "./g:availability",
        ])

        link = percent_encode_url(link) if link else ""
        image = percent_encode_url(image) if image else ""
        if not idv and link:
            idv = synth_id_from_url(link)

        items.append({
            "id": idv or "",
            "title": title or "",
            "description": desc or "",
            "link": link or "",
            "image": image or "",
            "price": price or "",
            "availability": avail or "",
        })
    return items


# ---------- Emitters ----------
def emit_skroutz(rows: List[Dict[str, Any]]) -> bytes:
    from xml.sax.saxutils import escape
    out = ['<?xml version="1.0" encoding="UTF-8"?>', "<products>"]
    for r in rows:
        out.append("<product>")
        if r["id"]:
            out.append(f"<id>{escape(r['id'])}</id>")
        if r["title"]:
            out.append(f"<name>{escape(r['title'])}</name>")
        if r["link"]:
            out.append(f"<link>{escape(r['link'])}</link>")
        if r["image"]:
            out.append(f"<image>{escape(r['image'])}</image>")
        if r["price"]:
            out.append(f"<price_with_vat>{escape(r['price'])}</price_with_vat>")
        if r["availability"]:
            out.append(f"<availability>{escape(r['availability'])}</availability>")
        out.append("</product>")
    out.append("</products>")
    return ("\n".join(out)).encode("utf-8")


def emit_compari(rows: List[Dict[str, Any]]) -> bytes:
    from xml.sax.saxutils import escape
    out = ['<?xml version="1.0" encoding="UTF-8"?>', "<Products>"]
    for r in rows:
        out.append("<Product>")
        if r["id"]:
            out.append(f"<Identifier>{escape(r['id'])}</Identifier>")
        if r["title"]:
            out.append(f"<Name>{escape(r['title'])}</Name>")
        if r["link"]:
            out.append(f"<Product_url>{escape(r['link'])}</Product_url>")
        if r["image"]:
            out.append(f"<Image_url>{escape(r['image'])}</Image_url>")
        if r["price"]:
            out.append(f"<Price>{escape(r['price'])}</Price>")
        if r["availability"]:
            out.append(f"<availability>{escape(r['availability'])}</availability>")
        out.append("</Product>")
    out.append("</Products>")
    return ("\n".join(out)).encode("utf-8")


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
        if r["id"]:
            out.append(f"<g:id>{escape(r['id'])}</g:id>")
        if r["title"]:
            out.append(f"<title>{escape(r['title'])}</title>")
        if r["description"]:
            out.append(f"<description>{escape(r['description'])}</description>")
        if r["link"]:
            out.append(f"<link>{escape(r['link'])}</link>")
        if r["image"]:
            out.append(f"<g:image_link>{escape(r['image'])}</g:image_link>")
        if r["price"]:
            out.append(f"<g:price>{escape(r['price'])}</g:price>")
        if r["availability"]:
            out.append(f"<g:availability>{escape(r['availability'])}</g:availability>")
        out.append("</item>")
    out.append("</channel></rss>")
    return ("\n".join(out)).encode("utf-8")


def emit_google_atom(rows: List[Dict[str, Any]]) -> bytes:
    from xml.sax.saxutils import escape
    out = [
        '<?xml version="1.0" encoding="UTF-8"?>',
        '<feed xmlns="http://www.w3.org/2005/Atom" xmlns:g="http://base.google.com/ns/1.0">',
        '<title>Fixed Feed</title>',
        '<link rel="self" href="https://feeds.example"/>',
    ]
    for r in rows:
        out.append("<entry>")
        if r["id"]:
            out.append(f"<g:id>{escape(r['id'])}</g:id>")
        if r["title"]:
            out.append(f"<title>{escape(r['title'])}</title>")
        if r["description"]:
            out.append(f"<content type=\"html\">{escape(r['description'])}</content>")
        if r["link"]:
            out.append(f"<g:link>{escape(r['link'])}</g:link>")
        if r["image"]:
            out.append(f"<g:image_link>{escape(r['image'])}</g:image_link>")
        if r["price"]:
            out.append(f"<g:price>{escape(r['price'])}</g:price>")
        if r["availability"]:
            out.append(f"<g:availability>{escape(r['availability'])}</g:availability>")
        out.append("</entry>")
    out.append("</feed>")
    return ("\n".join(out)).encode("utf-8")


EMITTERS = {
    "Skroutz": emit_skroutz,
    "Compari": emit_compari,
    "Google RSS": emit_google_rss,
    "Google Atom": emit_google_atom,
}


# ---------- UI ----------
st.set_page_config(page_title="Feed Fixer (Preview)", layout="wide")
st.title("🔧 Feed Fixer (Preview)")
st.caption("Transform a source feed to a chosen specification and download the fixed XML.")

with st.form("input"):
    src_url = st.text_input("Source feed URL (http/https)", placeholder="https://example.com/feed.xml")
    src_file = st.file_uploader("...or upload an XML file", type=["xml"])
    target = st.selectbox("Target specification", list(EMITTERS.keys()), index=0)
    st.divider()
    col1, col2, col3 = st.columns(3)
    with col1:
        do_avail_norm = st.checkbox("Normalize availability values", value=True)
    with col2:
        do_dedupe = st.checkbox("De-duplicate by (id, link)", value=True)
    with col3:
        limit_items = st.number_input("Limit items (0 = all)", min_value=0, value=0, step=50)
    submitted = st.form_submit_button("Transform")

if submitted:
    feed_bytes = None
    label = None
    if src_url:
        if not src_url.lower().startswith(("http://", "https://")):
            st.error("URL must start with http:// or https://")
        else:
            try:
                import requests
                r = requests.get(src_url, headers={"User-Agent": "FeedFixerPreview/1.0"}, timeout=40)
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
                r["availability"] = normalize_availability(r.get("availability", ""))

        if do_dedupe:
            seen = set()
            deduped = []
            for r in rows:
                key = (r.get("id", ""), r.get("link", ""))
                if key in seen:
                    continue
                seen.add(key)
                deduped.append(r)
            rows = deduped

        if limit_items and limit_items > 0:
            rows = rows[:limit_items]

        total_after = len(rows)
        missing_url = sum(1 for r in rows if not r.get("link"))
        missing_img = sum(1 for r in rows if not r.get("image"))

        c1, c2, c3, c4 = st.columns(4)
        c1.metric("Items (before)", total_before)
        c2.metric("Items (after)", total_after)
        c3.metric("Missing URL", missing_url)
        c4.metric("Missing Image", missing_img)

        xml_bytes = EMITTERS[target](rows)

        with st.expander("Preview (first lines)"):
            preview = xml_bytes.decode("utf-8", errors="replace").splitlines()
            st.code("\n".join(preview[:120]))

        dl_name = f"fixed_{target.lower().replace(' ', '_')}.xml"
        st.download_button("⬇️ Download fixed feed", xml_bytes, file_name=dl_name, mime="application/xml")

        st.info("Local preview only. When ready, publish the XML to a stable URL (e.g., https://www.s.favi.<tld>/fixed/<shop>.xml).")

      st.markdown("---")
st.caption("© 2025 Raul Bertoldini")

