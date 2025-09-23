 # feed_checker_gui.py
 from __future__ import annotations
 from typing import List, Tuple, Dict, Any, Iterable
 import re
 from collections import defaultdict
 import os
 import gzip
 import tempfile
 import streamlit as st
 
 # Shared rules/helpers from your feed_specs.py
 from feed_specs import (
     SPEC,
     strip_ns,
     detect_spec,
     get_item_nodes,               # used in DOM path
     read_id,
     read_link,
     read_availability,
+    read_price,
     gather_primary_image,
     read_link_raw,                 # RAW (no percent-encoding) to warn on spaces/non-ASCII
     gather_primary_image_raw,      # RAW
 )
 
 # Safe XML parsing (defusedxml if present)
 try:
     from defusedxml import ElementTree as ET  # type: ignore
 except Exception:
     import xml.etree.ElementTree as ET  # type: ignore
 
 st.set_page_config(page_title="FAVI Feed Checker", layout="wide")
 st.title("🧪 FAVI Feed Checker")
 st.caption("Auto mode for small vs large feeds; optional Sample mode to process only the first N items (streaming & low RAM).")
 
 # --------- Tuning ----------
 SMALL_SIZE_LIMIT = 30 * 1024 * 1024   # 30 MB → DOM; above this → streaming
 REQUEST_TIMEOUT = 120                 # seconds
 STREAM_CHUNK = 1 << 20                # 1 MB
 
 # ---------- UI helpers ----------
 def verdict_row(label: str, ok: bool, warn: bool = False, extra: str = "") -> Tuple[str, str]:
     if ok and not warn:
         return (label, f"✅ PASS {extra}".strip())
     if warn and ok:
diff --git a/feed_checker_gui.py b/feed_checker_gui.py
index 7404277edb68107dc0d4a5bf62d15026409706f4..c42f6129225a3914e9378d6029e7c9b8ed2f3bea 100644
--- a/feed_checker_gui.py
+++ b/feed_checker_gui.py
@@ -207,83 +208,163 @@ elif up is not None:
     try:
         src_path = persist_upload(up)
         src_label = up.name
     except Exception as e:
         st.error(f"Failed to read uploaded file: {e}"); st.stop()
 else:
     st.warning("Provide a URL or upload a file."); st.stop()
 
 st.write(f"**Source:** `{src_label}`")
 
 # Decide parsing strategy for Auto vs Sample
 file_size = os.path.getsize(src_path) if os.path.exists(src_path) else 0
 auto_force_streaming = is_gzip_path(src_path) or (file_size > SMALL_SIZE_LIMIT)
 use_sample_mode = (scope == "Sample first N items")
 
 # Data buckets (shared)
 xml_ok = True
 spec_name = "UNKNOWN"
 total_items = 0                 # total encountered (streamed) or len(items) in DOM
 processed_items = 0             # for sample mode display
 
 ids: List[str] = []
 links: List[str] = []
 images: List[str] = []
 avails: List[str] = []
+prices: List[str] = []
 
 missing_id_idx: List[int] = []
 missing_link_idx: List[int] = []
 missing_img_idx: List[int] = []
 missing_avail_idx: List[int] = []
+missing_price_idx: List[int] = []
 
 raw_links: List[str] = []
 raw_imgs: List[str] = []
 bad_url_idx: List[int] = []
 bad_img_idx: List[int] = []
+bad_price_idx: List[int] = []
+price_issue_notes: Dict[int, str] = {}
 
 id_first_seen: Dict[str, int] = {}
 link_first_seen: Dict[str, int] = {}
 dup_id_pairs: List[Tuple[int, int, str]] = []
 dup_link_pairs: List[Tuple[int, int, str]] = []
 
 ascii_only = re.compile(r'^[\x00-\x7F]+$')
 
+PRICE_NEEDS_CURRENCY = {
+    "Google Merchant (g:) RSS",
+    "Google Merchant (g:) Atom",
+    "Google Merchant (no-namespace) RSS",
+}
+
+_CURRENCY_SIGNS = set("€$£¥₽₺₪₫₭₮₴₸₨₩₱₦₡₲₳₵₣₤₥₧₰")
+
+def _has_currency_token(raw: str) -> bool:
+    if any(ch.isalpha() for ch in raw):
+        return True
+    return any(sign in raw for sign in _CURRENCY_SIGNS)
+
+def _extract_numeric_price(raw: str) -> float | None:
+    if not raw:
+        return None
+    text = raw.replace("\xa0", " ").strip()
+    if not re.search(r"\d", text):
+        return None
+
+    negative = text.startswith("-")
+    if negative:
+        text = text[1:].lstrip()
+
+    candidate = re.sub(r"[^0-9,.-]", "", text)
+    if not candidate or not re.search(r"\d", candidate):
+        return None
+
+    candidate = candidate.replace("-", "")
+
+    if candidate.count(",") and candidate.count("."):
+        if candidate.rfind(".") > candidate.rfind(","):
+            candidate = candidate.replace(",", "")
+        else:
+            candidate = candidate.replace(".", "")
+            candidate = candidate.replace(",", ".")
+    else:
+        if candidate.count(",") > 1:
+            candidate = candidate.replace(",", "")
+        elif candidate.count(",") == 1 and candidate.count(".") == 0:
+            candidate = candidate.replace(",", ".")
+
+        if candidate.count(".") > 1:
+            candidate = candidate.replace(".", "")
+
+    candidate = candidate.strip()
+    if not candidate:
+        return None
+
+    try:
+        value = float(candidate)
+    except ValueError:
+        return None
+
+    return -value if negative else value
+
+def price_issue_reason(raw: str, spec: str) -> str | None:
+    value = _extract_numeric_price(raw)
+    if value is None:
+        return "not numeric"
+    if value <= 0:
+        return "non-positive value"
+    if spec in PRICE_NEEDS_CURRENCY and not _has_currency_token(raw):
+        return "missing currency"
+    return None
+
 def process_item(elem, index: int, spec: str):
     pid = (read_id(elem, spec) or "").strip()
     purl = (read_link(elem, spec) or "").strip()
     pav  = (read_availability(elem, spec) or "").strip()
     pimg = (gather_primary_image(elem, spec) or "").strip()
+    pprice = (read_price(elem, spec) or "").strip()
     purl_raw = (read_link_raw(elem, spec) or "").strip()
     pimg_raw = (gather_primary_image_raw(elem, spec) or "").strip()
 
-    ids.append(pid); links.append(purl); images.append(pimg); avails.append(pav)
+    ids.append(pid); links.append(purl); images.append(pimg); avails.append(pav); prices.append(pprice)
     raw_links.append(purl_raw); raw_imgs.append(pimg_raw)
 
     if not pid: missing_id_idx.append(index)
     if not purl: missing_link_idx.append(index)
     if not pimg: missing_img_idx.append(index)
     if not pav:  missing_avail_idx.append(index)
+    if SPEC.get(spec, {}).get("price_paths"):
+        if not pprice:
+            missing_price_idx.append(index)
+            price_issue_notes[index] = "missing price"
+        else:
+            reason = price_issue_reason(pprice, spec)
+            if reason:
+                bad_price_idx.append(index)
+                price_issue_notes[index] = reason
 
     if purl_raw and ((" " in purl_raw) or not ascii_only.match(purl_raw)):
         bad_url_idx.append(index)
     if pimg_raw and ((" " in pimg_raw) or not ascii_only.match(pimg_raw)):
         bad_img_idx.append(index)
 
     if pid:
         if pid in id_first_seen:
             dup_id_pairs.append((id_first_seen[pid], index, pid))
         else:
             id_first_seen[pid] = index
 
     if purl:
         if purl in link_first_seen:
             dup_link_pairs.append((link_first_seen[purl], index, purl))
         else:
             link_first_seen[purl] = index
 
 # ---------- DOM path (Auto + small, non-gz, and only if NOT sample mode) ----------
 def run_dom_path() -> bool:
     global xml_ok, spec_name, total_items, processed_items
     try:
         with open(src_path, "rb") as fh:
             xml_bytes = fh.read()
         root = ET.fromstring(xml_bytes)
diff --git a/feed_checker_gui.py b/feed_checker_gui.py
index 7404277edb68107dc0d4a5bf62d15026409706f4..c42f6129225a3914e9378d6029e7c9b8ed2f3bea 100644
--- a/feed_checker_gui.py
+++ b/feed_checker_gui.py
@@ -380,107 +461,134 @@ else:
 st.markdown("---")
 c1, c2, c3, c4, c5 = st.columns([1, 1, 1, 1, 1], gap="large")
 
 with c1:
     status_pill(f"Transformation: {spec_name if spec_name!='UNKNOWN' else 'UNKNOWN'}",
                 "#16a34a" if spec_name!="UNKNOWN" else "#6b7280")
 
 with c2:
     if use_sample_mode:
         status_pill(f"Items: {processed_items} / {total_items}", "#16a34a")
     else:
         status_pill(f"Items: {total_items}", "#16a34a")
 
 with c3:
     total_dups = len(dup_id_pairs) + len(dup_link_pairs)
     status_pill(f"Duplicates: {total_dups}", "#dc2626" if total_dups > 0 else "#16a34a")
 
 with c4:
     status_pill(f"Missing IDs: {len(missing_id_idx)}", "#dc2626" if len(missing_id_idx)>0 else "#16a34a")
 
 with c5:
     total_warnings = (
         len(missing_link_idx)
         + len(missing_img_idx)
         + len(missing_avail_idx)
+        + len(missing_price_idx)
         + len(bad_url_idx)
         + len(bad_img_idx)
+        + len(bad_price_idx)
     )
     any_warnings = total_warnings > 0
     status_pill(f"Warnings: {total_warnings}", "#f59e0b" if any_warnings else "#16a34a")
 
 # ---------- SUMMARY ----------
 pass_fail: Dict[str, Tuple[bool, bool, str]] = {}
 pass_fail["XML syntax"] = (xml_ok, False, "")
 pass_fail["Transformation detected"] = (spec_name != "UNKNOWN", False, spec_name if spec_name != "UNKNOWN" else "")
 pass_fail["IDs present"] = (len(missing_id_idx) == 0, False, f"(missing: {len(missing_id_idx)})")
 pass_fail["Duplicate IDs"] = (len(dup_id_pairs) == 0, False, f"(duplicates: {len(dup_id_pairs)})")
 pass_fail["Duplicate Product URLs"] = (len(dup_link_pairs) == 0, False, f"(duplicates: {len(dup_link_pairs)})")
 pass_fail["Product URL present"] = (True, len(missing_link_idx) > 0, f"(missing: {len(missing_link_idx)})")
 pass_fail["Primary image present"] = (True, len(missing_img_idx) > 0, f"(missing: {len(missing_img_idx)})")
 pass_fail["Availability present"] = (True, len(missing_avail_idx) > 0, f"(missing: {len(missing_avail_idx)})")
+if SPEC.get(spec_name, {}).get("price_paths"):
+    pass_fail["Price present"] = (True, len(missing_price_idx) > 0, f"(missing: {len(missing_price_idx)})")
+    pass_fail["Price validity"] = (True, len(bad_price_idx) > 0, f"(suspect: {len(bad_price_idx)})")
 pass_fail["Product URL validity"] = (True, len(bad_url_idx) > 0, f"(bad: {len(bad_url_idx)})")
 pass_fail["Image URL validity"] = (True, len(bad_img_idx) > 0, f"(bad: {len(bad_img_idx)})")
 
 st.markdown("---")
 summarize(pass_fail)
 
 # ---------- DETAILS ----------
 st.markdown("---")
 st.subheader("Details")
 
 def safe_get(lst, i, default=""):
     try:
         return lst[i]
     except Exception:
         return default
 
 # Missing fields
 missing_id_rows = [
     {"id": "(missing)", "link": safe_get(links, i), "image": "yes" if safe_get(images, i) else "no", "availability": safe_get(avails, i) or "(missing)"}
     for i in missing_id_idx
 ]
 show_issue_table("Missing ID (by example values)", missing_id_rows, sample_show)
 
 missing_link_rows = [
     {"id": safe_get(ids, i), "link": "(missing)", "image": "yes" if safe_get(images, i) else "no", "availability": safe_get(avails, i) or "(missing)"}
     for i in missing_link_idx if safe_get(ids, i)
 ]
 show_issue_table("Missing Product URL (by product ID)", missing_link_rows, sample_show)
 
 missing_img_rows = [
     {"id": safe_get(ids, i), "link": safe_get(links, i), "primary_image": "(missing)"}
     for i in missing_img_idx if safe_get(ids, i)
 ]
 show_issue_table("Missing Primary Image (by product ID)", missing_img_rows, sample_show)
 
 missing_avail_rows = [
     {"id": safe_get(ids, i), "link": safe_get(links, i), "availability": "(missing)"}
     for i in missing_avail_idx if safe_get(ids, i)
 ]
 show_issue_table("Missing Availability (by product ID)", missing_avail_rows, sample_show)
 
+if SPEC.get(spec_name, {}).get("price_paths"):
+    missing_price_rows = [
+        {
+            "id": safe_get(ids, i) or "(missing id)",
+            "link": safe_get(links, i),
+            "price": "(missing)",
+        }
+        for i in missing_price_idx
+    ]
+    show_issue_table("Missing Price (by product ID)", missing_price_rows, sample_show)
+
+    bad_price_rows = [
+        {
+            "id": safe_get(ids, i) or "(missing id)",
+            "link": safe_get(links, i),
+            "price": safe_get(prices, i),
+            "note": price_issue_notes.get(i, "suspect value"),
+        }
+        for i in bad_price_idx
+    ]
+    show_issue_table("Suspicious Price values (by product ID)", bad_price_rows, sample_show)
+
 # Bad URL warnings (RAW)
 bad_url_rows = [
     {"id": safe_get(ids, i) or "(missing id)", "raw_url": safe_get(raw_links, i), "encoded_url": safe_get(links, i)}
     for i in bad_url_idx
 ]
 show_issue_table("Bad Product URLs (spaces/non-ASCII) — RAW view", bad_url_rows, sample_show)
 
 bad_img_rows = [
     {"id": safe_get(ids, i) or "(missing id)", "raw_image_url": safe_get(raw_imgs, i), "encoded_image_url": safe_get(images, i)}
     for i in bad_img_idx
 ]
 show_issue_table("Bad Image URLs (spaces/non-ASCII) — RAW view", bad_img_rows, sample_show)
 
 # Duplicates (IDs)
 dup_ids_map: Dict[str, List[int]] = defaultdict(list)
 for old_i, new_i, pid in dup_id_pairs:
     dup_ids_map[pid].extend([old_i, new_i])
 
 dup_id_rows = []
 for pid, idxs in dup_ids_map.items():
     idxs_u = unique_preserve([str(x) for x in idxs])
     ex_links = unique_preserve([safe_get(links, int(i)) for i in idxs_u if safe_get(links, int(i))])[:3]
     dup_id_rows.append({
         "id": pid,
         "occurrences": len(unique_preserve([str(int(i)) for i in idxs])),

# Bad URL warnings (RAW)
bad_url_rows = [
    {"id": safe_get(ids, i) or "(missing id)", "raw_url": safe_get(raw_links, i), "encoded_url": safe_get(links, i)}
    for i in bad_url_idx
]
show_issue_table("Bad Product URLs (spaces/non-ASCII) — RAW view", bad_url_rows, sample_show)

bad_img_rows = [
    {"id": safe_get(ids, i) or "(missing id)", "raw_image_url": safe_get(raw_imgs, i), "encoded_image_url": safe_get(images, i)}
    for i in bad_img_idx
]
show_issue_table("Bad Image URLs (spaces/non-ASCII) — RAW view", bad_img_rows, sample_show)

# Duplicates (IDs)
dup_ids_map: Dict[str, List[int]] = defaultdict(list)
for old_i, new_i, pid in dup_id_pairs:
    dup_ids_map[pid].extend([old_i, new_i])

dup_id_rows = []
for pid, idxs in dup_ids_map.items():
    idxs_u = unique_preserve([str(x) for x in idxs])
    ex_links = unique_preserve([safe_get(links, int(i)) for i in idxs_u if safe_get(links, int(i))])[:3]
    dup_id_rows.append({
        "id": pid,
        "occurrences": len(unique_preserve([str(int(i)) for i in idxs])),
        "example_links": " | ".join(ex_links) if ex_links else ""
    })
dup_id_rows.sort(key=lambda r: (-r["occurrences"], r["id"]))
show_issue_table("Duplicate IDs (grouped)", dup_id_rows, sample_show)

# Duplicates (URLs)
url_to_ids: Dict[str, List[str]] = defaultdict(list)
for old_i, new_i, url in dup_link_pairs:
    oid = safe_get(ids, old_i)
    nid = safe_get(ids, new_i)
    if oid: url_to_ids[url].append(oid)
    if nid: url_to_ids[url].append(nid)

dup_url_rows = []
for url, idlist in url_to_ids.items():
    ids_u = unique_preserve(idlist)
    dup_url_rows.append({
        "url": url,
        "num_ids": len(ids_u),
        "ids": ", ".join(ids_u[:12]) + (" …" if len(ids_u) > 12 else "")
    })
dup_url_rows.sort(key=lambda r: (-r["num_ids"], r["url"]))
show_issue_table("Duplicate Product URLs (grouped, with IDs)", dup_url_rows, sample_show)

st.markdown("---")
st.caption(
    ("Scope: Sample first N items (streaming)" if use_sample_mode else
     f"Scope: Auto (parser: {'Streaming' if (is_gzip_path(src_path) or file_size>SMALL_SIZE_LIMIT) else 'DOM'})")
)
st.markdown("© 2025 Raul Bertoldini")
