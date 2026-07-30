# pages/1_Feed_Filter.py
# Live feed filter for Account Managers — build AND/OR rules against a feed and
# see, in real time, how many products they would remove. Standalone from the
# validator: it drives feed_filter.py (pure engine) and never touches the
# single-shot checker flow, so it cannot destabilise that beta.
#
# Interactivity is cheap because the parsed table is cached (st.cache_data)
# keyed by a feed signature — a filter change reruns the script but hits the
# cache instead of re-downloading/re-parsing. The table is capped
# (feed_filter.DEFAULT_ITEM_CAP) so it can't OOM a ~1 GB host.
from __future__ import annotations

import io
import json
import os
import tempfile

import requests
import streamlit as st

from branding import inject_css, page_header, render_metric_row, FAVICON_URL
import feed_filter as ff

REQUEST_TIMEOUT = 120
STREAM_CHUNK = 1 << 20

st.set_page_config(page_title="FAVI Feed Filter", page_icon=FAVICON_URL, layout="wide")
inject_css()
page_header(
    "Feed Filter",
    subtitle="Build live AND/OR rules over a product feed and see exactly how many "
             "products each rule removes — before asking for a feed change.",
)
st.warning(
    "🧪 **Experimental** — filters a snapshot of up to "
    f"{ff.DEFAULT_ITEM_CAP:,} items. Counts within that snapshot are exact; "
    "full-feed filtering is coming with the Cloud Run dashboard."
)


# --------------------------------------------------------------------------- #
# Feed loading (download/upload -> temp file -> cached parse)
# --------------------------------------------------------------------------- #
def _download_to_tmp(url: str) -> str:
    suffix = ".xml.gz" if url.lower().split("?", 1)[0].endswith(".gz") else ".xml"
    fd, path = tempfile.mkstemp(suffix=suffix)
    with os.fdopen(fd, "wb") as out:
        with requests.get(url, stream=True, timeout=REQUEST_TIMEOUT) as r:
            r.raise_for_status()
            for chunk in r.iter_content(STREAM_CHUNK):
                if chunk:
                    out.write(chunk)
    return path


def _persist_upload(up) -> str:
    suffix = ".xml.gz" if up.name.lower().endswith(".gz") else ".xml"
    fd, path = tempfile.mkstemp(suffix=suffix)
    with os.fdopen(fd, "wb") as out:
        out.write(up.getbuffer())
    return path


@st.cache_data(show_spinner="Parsing feed…", max_entries=3)
def _cached_extract(signature: str, cap: int, index_params: bool, _path: str) -> ff.FeedTable:
    # `signature` (+ cap + index_params) is the cache key; `_path` is ignored
    # for hashing (leading underscore) so a stable feed reuses the parsed table
    # across reruns — that is what makes filtering feel live.
    return ff.extract(_path, cap, index_params=index_params)


def _set_feed(path: str, label: str, size: int, index_params: bool) -> None:
    st.session_state.update(
        ff_src_path=path,
        ff_src_label=label,
        ff_index_params=index_params,
        ff_signature=f"{label}::{size}::p{int(index_params)}",
    )


# Hand-off from the Feed Checker page: reuse the feed just validated there
# (session_state is shared across pages; temp files persist — delete=False).
shared_path = st.session_state.get("shared_feed_path")
if shared_path and os.path.exists(shared_path):
    shared_label = st.session_state.get("shared_feed_label", "last checked feed")
    if st.button(f"↪︎ Use the feed I just checked: {shared_label}", use_container_width=True):
        _set_feed(
            shared_path, shared_label,
            st.session_state.get("shared_feed_size") or os.path.getsize(shared_path),
            st.session_state.get("ff_index_params_cb", False),
        )

# Kept OUTSIDE the form so both the "use last checked" button and a fresh load
# read the same current value.
st.checkbox(
    "Also index product parameters (enables the Product-parameter filter — uses a bit more memory)",
    key="ff_index_params_cb",
)

with st.form("filter_input"):
    url = st.text_input("Feed URL", placeholder="https://example.com/feed.xml")
    up = st.file_uploader("…or upload an XML file (.xml or .xml.gz)", type=["xml", "gz"])
    load = st.form_submit_button("Load feed", type="primary", use_container_width=True)

if load:
    try:
        if url.strip():
            if not url.lower().startswith(("http://", "https://")):
                st.error("URL must start with http:// or https://")
                st.stop()
            path, label = _download_to_tmp(url.strip()), url.strip()
        elif up is not None:
            path, label = _persist_upload(up), up.name
        else:
            st.warning("Provide a URL or upload a file.")
            st.stop()
    except Exception as e:  # noqa: BLE001 - surface any load failure to the AM
        st.error(f"Could not load feed: {e}")
        st.stop()
    size = os.path.getsize(path) if os.path.exists(path) else 0
    _set_feed(path, label, size, st.session_state.get("ff_index_params_cb", False))

signature = st.session_state.get("ff_signature")
if not signature:
    st.info("Load a feed to start filtering.")
    st.stop()

table = _cached_extract(
    signature, ff.DEFAULT_ITEM_CAP,
    st.session_state.get("ff_index_params", False),
    st.session_state["ff_src_path"],
)
st.write(f"**Source:** `{st.session_state['ff_src_label']}`  ·  format: **{table.spec}**")

if table.spec == "UNKNOWN":
    st.error(
        f"Format not recognized — {table.total_seen:,} item-like elements were counted "
        "but no field table applies, so there is nothing to filter on. Identify/convert "
        "the feed format first (the Feed Checker page will tell you which)."
    )
    st.stop()
if table.n == 0:
    st.warning("No items were found in this feed.")
    st.stop()
if table.truncated:
    st.info(
        f"Loaded the first **{table.n:,}** of **{table.total_seen:,}** items. "
        "Percentages below are exact for the snapshot and a good estimate for the full feed."
    )


# --------------------------------------------------------------------------- #
# Rule builder (rules live in session_state as a list of stable ids)
# --------------------------------------------------------------------------- #
st.session_state.setdefault("ff_rules", [])
st.session_state.setdefault("ff_next_id", 0)


def _add_rule(field: str = "price", op: str = "", value: str = "", value2: str = "",
             set_mode: str | None = None, set_combine: str | None = None) -> None:
    rid = st.session_state["ff_next_id"]
    st.session_state["ff_next_id"] += 1
    st.session_state["ff_rules"].append(rid)
    ftype = ff.FIELD_TYPE[field]
    st.session_state[f"ff_field_{rid}"] = field
    if op:
        st.session_state[f"ff_op_{rid}_{ftype}"] = op
    st.session_state[f"ff_val_{rid}"] = value
    st.session_state[f"ff_val2_{rid}"] = value2
    # Quick-start recipes flip the keep/remove and AND/OR toggles so the button
    # does exactly what its label says.
    if set_mode:
        st.session_state["ff_mode"] = set_mode
    if set_combine:
        st.session_state["ff_combine"] = set_combine


def _clear_rules() -> None:
    st.session_state["ff_rules"] = []


st.subheader("Filter rules")

qc1, qc2, qc3, qc4 = st.columns(4)
qc1.button("➕ Add rule", use_container_width=True, on_click=_add_rule)
qc2.button("🗑 Clear all", use_container_width=True, on_click=_clear_rules)
qc3.button("Remove out-of-stock", use_container_width=True, on_click=_add_rule,
           kwargs=dict(field="availability", op="is empty", set_mode="remove", set_combine="OR"))
qc4.button("Remove missing image", use_container_width=True, on_click=_add_rule,
           kwargs=dict(field="has_image", op="is false", set_mode="remove", set_combine="OR"))

fld1, fld2 = st.columns(2)
combine = fld1.radio("Combine rules with", ["AND", "OR"], horizontal=True, key="ff_combine",
                     help="AND = product must match every rule · OR = any rule")
mode = fld2.radio("Rules select products to", ["keep", "remove"], horizontal=True, key="ff_mode",
                  help="keep = resulting feed is the matches · remove = drop the matches")

# Only offer "param" when the feed was indexed with parameters.
_field_keys = [f["key"] for f in ff.FIELDS if f["key"] != "param" or table.index_params]
# If params were switched off while a rule still pointed at "param", retarget it
# so the selectbox doesn't error on a value that is no longer an option.
for _rid in st.session_state["ff_rules"]:
    if st.session_state.get(f"ff_field_{_rid}") == "param" and not table.index_params:
        st.session_state[f"ff_field_{_rid}"] = _field_keys[0]

rules: list[dict] = []
for rid in list(st.session_state["ff_rules"]):
    c_field, c_op, c_val, c_del = st.columns([3, 3, 4, 1])
    field = c_field.selectbox(
        "Field", _field_keys,
        format_func=lambda k: ff.FIELD_LABEL[k], key=f"ff_field_{rid}",
        label_visibility="collapsed",
    )
    ftype = ff.FIELD_TYPE[field]
    # Op key carries the field type so switching field to a different type
    # gives a fresh op widget instead of erroring on a now-invalid stored value.
    op = c_op.selectbox(
        "Operator", ff.OPS_BY_TYPE[ftype], key=f"ff_op_{rid}_{ftype}",
        label_visibility="collapsed",
    )
    value = value2 = ""
    if ftype == "param":
        # Two inputs: the parameter NAME (stored in value2) and the match text.
        c_name, c_pval = c_val.columns(2)
        value2 = c_name.text_input("param name", key=f"ff_pname_{rid}",
                                   label_visibility="collapsed", placeholder="param name e.g. Color")
        if op in ("is empty", "is not empty"):
            c_pval.markdown("&nbsp;", unsafe_allow_html=True)
        else:
            value = c_pval.text_input("value", key=f"ff_val_{rid}",
                                     label_visibility="collapsed", placeholder="value")
    elif op == "between":
        v1, v2 = c_val.columns(2)
        value = v1.text_input("min", key=f"ff_val_{rid}", label_visibility="collapsed",
                              placeholder="min")
        value2 = v2.text_input("max", key=f"ff_val2_{rid}", label_visibility="collapsed",
                               placeholder="max")
    elif op in ("is empty", "is not empty", "is true", "is false"):
        c_val.markdown("&nbsp;", unsafe_allow_html=True)
    else:
        ph = "e.g. 200" if ftype == "number" else (
            "brandA, brandB" if op in ("in list", "not in list") else "text…")
        value = c_val.text_input("Value", key=f"ff_val_{rid}", label_visibility="collapsed",
                                placeholder=ph)
    c_del.button("✕", key=f"ff_del_{rid}",
                 on_click=lambda r=rid: st.session_state["ff_rules"].remove(r))
    rules.append({"field": field, "op": op, "value": value, "value2": value2})

if not rules:
    st.caption("No rules yet — add one, or use a quick-start button above.")


# --------------------------------------------------------------------------- #
# Live result
# --------------------------------------------------------------------------- #
result = ff.apply_rules(table, rules, combine=combine, mode=mode)
pct_removed = (result["removed"] / result["n"] * 100) if result["n"] else 0

st.markdown("---")
render_metric_row([
    ("Loaded", f"{result['n']:,}", "default",
     f"of {result['total_seen']:,} total" if table.truncated else "full feed"),
    ("Match rules", f"{result['matched']:,}", "brand"),
    ("Resulting feed", f"{result['kept']:,}", "ok"),
    ("Removed", f"{result['removed']:,}", "error" if result["removed"] else "ok",
     f"{pct_removed:.1f}% of snapshot"),
])

if result["per_rule"]:
    st.caption("Each rule on its own would match:")
    for pr in result["per_rule"]:
        st.markdown(f"- `{ff.rule_text(pr['rule'])}` → **{pr['matched']:,}** products")

# Resulting-feed preview (first 200 surviving rows).
PREVIEW_COLS = ["id", "title", "price", "availability", "brand", "category",
                "category_depth", "image_count", "has_ean"]
preview, taken = [], 0
for i in range(table.n):
    if result["keep_mask"][i]:
        preview.append({c: table.columns[c][i] for c in PREVIEW_COLS})
        taken += 1
        if taken >= 200:
            break
with st.expander(f"Preview resulting feed (first {len(preview)} of {result['kept']:,})",
                 expanded=bool(preview)):
    st.dataframe(preview, use_container_width=True, hide_index=True)


# --------------------------------------------------------------------------- #
# Hand-off: exactly what to tell Raul / run server-side
# --------------------------------------------------------------------------- #
st.subheader("Hand-off")
st.markdown("Copy this to describe the change, or download the artefacts:")
st.code(ff.describe(rules, combine, mode, result), language="text")

kept_ids = [table.columns["id"][i] for i in range(table.n) if result["keep_mask"][i]]
removed_ids = [table.columns["id"][i] for i in range(table.n) if not result["keep_mask"][i]]
d1, d2, d3 = st.columns(3)
d1.download_button("⬇︎ Resulting IDs (CSV)",
                   "id\n" + "\n".join(kept_ids), file_name="kept_ids.csv",
                   mime="text/csv", use_container_width=True)
d2.download_button("⬇︎ Removed IDs (CSV)",
                   "id\n" + "\n".join(removed_ids), file_name="removed_ids.csv",
                   mime="text/csv", use_container_width=True)
d3.download_button("⬇︎ Filter spec (JSON)",
                   json.dumps(ff.to_spec(rules, combine, mode), indent=2),
                   file_name="filter_spec.json", mime="application/json",
                   use_container_width=True)

st.markdown("© 2025 Raul Bertoldini")
