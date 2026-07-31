# filter_page.py — Feed Filter page; mounted by feed_checker_gui.py.
# Live feed filter for Account Managers — build AND/OR rules against a feed and
# see, in real time, how many products they would remove. Standalone from the
# validator: it drives feed_filter.py (pure engine) and never touches the
# single-shot checker flow, so it cannot destabilise that beta.
#
# Interactivity is cheap because the parsed table lives in session state and is
# keyed by a content signature — filter changes rerun the script without
# re-downloading or re-parsing. The table is capped
# (feed_filter.DEFAULT_ITEM_CAP) so a large feed cannot fill application memory.
from __future__ import annotations

import hashlib
import json
import os
import fcntl
import tempfile
import time
from urllib.parse import urljoin, urlparse

import requests
import streamlit as st

from branding import inject_css, page_header, render_metric_row
import feed_filter as ff
from safe_http import public_session

REQUEST_TIMEOUT = 120
STREAM_CHUNK = 1 << 20
MAX_REDIRECTS = 5


def _positive_env_int(name: str, default: int) -> int:
    try:
        return max(1, int(os.getenv(name, str(default))))
    except ValueError:
        return default


MAX_DOWNLOAD_SECONDS = _positive_env_int(
    "FAVI_FILTER_MAX_DOWNLOAD_SECONDS", 300
)
MAX_DOWNLOAD_BYTES = (
    _positive_env_int("FAVI_FILTER_MAX_DOWNLOAD_MB", 512) * 1024 * 1024
)
TEMP_FILE_TTL_SECONDS = _positive_env_int(
    "FAVI_FILTER_TEMP_TTL_SECONDS", 6 * 60 * 60
)
TEMP_DIR = os.path.join(
    tempfile.gettempdir(),
    f"favi-feed-filter-{getattr(os, 'getuid', lambda: 0)()}",
)
_DOWNLOAD_HEADERS = {
    "User-Agent": "Mozilla/5.0 (compatible; FeedChecker/1.0; +https://favi.com)",
    "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
    "Accept-Language": "en-US,en;q=0.5",
    "Accept-Encoding": "gzip, deflate",
}
_HTTP_SESSION = public_session()


def _cleanup_stale_temp_files() -> None:
    """Bound abandoned per-session downloads without touching other temp data."""
    os.makedirs(TEMP_DIR, mode=0o700, exist_ok=True)
    cutoff = time.time() - TEMP_FILE_TTL_SECONDS
    try:
        entries = list(os.scandir(TEMP_DIR))
    except OSError:
        return
    for entry in entries:
        try:
            if (
                entry.name.startswith("feed-")
                and entry.is_file(follow_symlinks=False)
                and entry.stat(follow_symlinks=False).st_mtime < cutoff
            ):
                with open(entry.path, "rb") as candidate:
                    try:
                        fcntl.flock(
                            candidate.fileno(),
                            fcntl.LOCK_EX | fcntl.LOCK_NB,
                        )
                    except BlockingIOError:
                        continue
                    os.unlink(entry.path)
        except OSError:
            pass


def _new_temp_path(suffix: str) -> tuple[int, str]:
    os.makedirs(TEMP_DIR, mode=0o700, exist_ok=True)
    return tempfile.mkstemp(prefix="feed-", suffix=suffix, dir=TEMP_DIR)


def _lease_temp_path(path: str):
    """Keep an owned source locked so the TTL janitor cannot delete it."""
    lease = open(path, "rb")
    try:
        fcntl.flock(lease.fileno(), fcntl.LOCK_SH | fcntl.LOCK_NB)
    except Exception:
        lease.close()
        raise
    return lease


_cleanup_stale_temp_files()

inject_css()
page_header(
    "Feed Filter",
    subtitle="Browse a product feed, find what you need, and build precise "
             "filter rules before asking for a feed change.",
)
st.warning(
    "🧪 **Experimental** — filters a snapshot of up to "
    f"{ff.DEFAULT_ITEM_CAP:,} items. Counts within that snapshot are exact; "
    "full-feed filtering is coming with the Cloud Run dashboard."
)


# --------------------------------------------------------------------------- #
# Feed loading (download/upload -> temp file -> session-owned parse)
# --------------------------------------------------------------------------- #
def _download_to_tmp(url: str) -> tuple[str, int, str]:
    current_url = url
    started = time.monotonic()

    for redirect_count in range(MAX_REDIRECTS + 1):
        try:
            response = _HTTP_SESSION.get(
                current_url,
                stream=True,
                timeout=(15, REQUEST_TIMEOUT),
                headers=_DOWNLOAD_HEADERS,
                allow_redirects=False,
            )
        except requests.RequestException as exc:
            raise ff.FeedDownloadError(
                f"Could not connect to {_display_source(current_url)}."
            ) from exc

        with response:
            if response.status_code in (301, 302, 303, 307, 308):
                location = response.headers.get("Location")
                if not location:
                    raise ff.FeedDownloadError(
                        "The feed URL redirected without a destination."
                    )
                if redirect_count == MAX_REDIRECTS:
                    raise ff.FeedDownloadError(
                        f"The feed URL exceeded {MAX_REDIRECTS} redirects."
                    )
                current_url = urljoin(current_url, location)
                continue

            if response.status_code >= 400:
                raise ff.FeedDownloadError(
                    f"The feed server at {_display_source(current_url)} "
                    f"returned HTTP {response.status_code}."
                )
            declared = response.headers.get("Content-Length")
            if declared:
                try:
                    declared_bytes = int(declared)
                except ValueError:
                    declared_bytes = 0
                if declared_bytes > MAX_DOWNLOAD_BYTES:
                    raise ff.FeedDownloadError(
                        f"The feed is larger than the "
                        f"{MAX_DOWNLOAD_BYTES // (1024 * 1024):,} MB download limit."
                    )

            suffix = (
                ".xml.gz"
                if current_url.lower().split("?", 1)[0].endswith(".gz")
                else ".xml"
            )
            fd, path = _new_temp_path(suffix)
            written = 0
            digest = hashlib.sha256()
            try:
                with os.fdopen(fd, "wb") as out:
                    for chunk in response.iter_content(STREAM_CHUNK):
                        if not chunk:
                            continue
                        written += len(chunk)
                        if written > MAX_DOWNLOAD_BYTES:
                            raise ff.FeedDownloadError(
                                f"The feed exceeded the "
                                f"{MAX_DOWNLOAD_BYTES // (1024 * 1024):,} MB "
                                "download limit."
                            )
                        if time.monotonic() - started > MAX_DOWNLOAD_SECONDS:
                            raise ff.FeedDownloadError(
                                f"The feed took longer than "
                                f"{MAX_DOWNLOAD_SECONDS:,} seconds to download."
                            )
                        digest.update(chunk)
                        out.write(chunk)
            except requests.RequestException as exc:
                try:
                    os.unlink(path)
                except OSError:
                    pass
                raise ff.FeedDownloadError(
                    f"The connection to {_display_source(current_url)} "
                    "was interrupted while downloading."
                ) from exc
            except Exception:
                try:
                    os.unlink(path)
                except OSError:
                    pass
                raise
            return path, written, digest.hexdigest()

    raise ff.FeedDownloadError("The feed could not be downloaded.")


def _persist_upload(up) -> tuple[str, int, str]:
    content = up.getbuffer()
    size = len(content)
    if size > MAX_DOWNLOAD_BYTES:
        raise ff.FeedDownloadError(
            f"The file is larger than the "
            f"{MAX_DOWNLOAD_BYTES // (1024 * 1024):,} MB limit."
        )
    is_gzip = bytes(content[:2]) == b"\x1f\x8b"
    suffix = ".xml.gz" if is_gzip else ".xml"
    fd, path = _new_temp_path(suffix)
    try:
        with os.fdopen(fd, "wb") as out:
            out.write(content)
    except Exception:
        try:
            os.unlink(path)
        except OSError:
            pass
        raise
    return path, size, hashlib.sha256(content).hexdigest()


def _fingerprint_path(path: str) -> tuple[int, str]:
    size = 0
    digest = hashlib.sha256()
    with open(path, "rb") as fh:
        for chunk in iter(lambda: fh.read(STREAM_CHUNK), b""):
            size += len(chunk)
            digest.update(chunk)
    return size, digest.hexdigest()


def _display_source(label: str) -> str:
    value = str(label or "feed")
    parsed = urlparse(value)
    if parsed.scheme in ("http", "https") and parsed.hostname:
        path = parsed.path or "/"
        if len(path) > 64:
            path = f"{path[:61]}…"
        return f"{parsed.hostname}{path}"
    name = os.path.basename(value)
    return name if len(name) <= 80 else f"{name[:77]}…"


def _set_feed(
    path: str,
    label: str,
    size: int,
    content_hash: str,
    index_params: bool,
    *,
    owned_path: bool,
) -> None:
    st.session_state.pop("ff_prepared_exports", None)
    st.session_state.pop("ff_category_facets", None)
    st.session_state.pop("ff_browse_request", None)
    st.session_state.pop("ff_browse_query", None)
    st.session_state.pop("ff_browse_categories", None)
    st.session_state.pop("ff_browse_scope", None)
    st.session_state.pop("ff_browse_page_size", None)
    st.session_state.pop("ff_browse_page", None)
    for state_key in list(st.session_state):
        if (
            state_key.startswith("ff_group_rules_")
            or state_key.startswith("ff_group_combine_")
        ):
            st.session_state.pop(state_key, None)
    previous_lease = st.session_state.pop("ff_owned_lease", None)
    if previous_lease is not None:
        try:
            previous_lease.close()
        except OSError:
            pass
    previous_path = st.session_state.get("ff_src_path")
    if (
        st.session_state.get("ff_owned_path")
        and previous_path
        and previous_path != path
    ):
        try:
            os.unlink(previous_path)
        except OSError:
            pass

    owned_lease = _lease_temp_path(path) if owned_path else None
    st.session_state.update(
        ff_src_path=path,
        ff_src_label=label,
        ff_src_size=size,
        ff_content_hash=content_hash,
        ff_owned_path=owned_path,
        ff_owned_lease=owned_lease,
        ff_index_params=index_params,
        ff_signature=f"{content_hash}::p{int(index_params)}",
        ff_rules=[],
        ff_combine="AND",
        ff_group_ids=[],
        ff_next_group_id=0,
        ff_groups_combine="AND",
        ff_mode="keep",
    )


# Hand-off from the Feed Checker page: reuse the feed just validated there
# (session_state is shared across pages; temp files persist — delete=False).
feed_controls = (
    st.expander("Change feed or parameter indexing", expanded=False)
    if st.session_state.get("ff_signature")
    else st.container()
)
with feed_controls:
    shared_path = st.session_state.get("shared_feed_path")
    if shared_path and os.path.exists(shared_path):
        shared_label = st.session_state.get(
            "shared_feed_label",
            "last checked feed",
        )
        if st.button(
            f"↪︎ Use feed from Feed Checker: {_display_source(shared_label)}",
            width="stretch",
        ):
            try:
                with st.spinner("Preparing the checked feed…"):
                    shared_size, shared_hash = _fingerprint_path(shared_path)
                _set_feed(
                    shared_path,
                    shared_label,
                    shared_size,
                    shared_hash,
                    st.session_state.get("ff_index_params_cb", False),
                    owned_path=False,
                )
            except Exception as exc:  # noqa: BLE001
                st.error(f"Could not reuse the checked feed: {exc}")
                st.stop()

    # Kept OUTSIDE the form so both the checked-feed button and a fresh load
    # read the same current value.
    st.checkbox(
        "Index product parameters (re-loads the current feed and uses more memory)",
        key="ff_index_params_cb",
        help=(
            "Enable this only when you need to filter a named parameter "
            "such as Color or Material."
        ),
    )

    with st.form("filter_input"):
        url = st.text_input(
            "Feed URL",
            placeholder="https://example.com/feed.xml",
        )
        up = st.file_uploader(
            "…or upload an XML file (.xml or .xml.gz)",
            type=["xml", "gz"],
        )
        load = st.form_submit_button(
            "Load feed",
            type="primary",
            width="stretch",
        )

if load:
    try:
        if url.strip() and up is not None:
            st.error("Use either a feed URL or an uploaded file, not both.")
            st.stop()
        if url.strip():
            path, size, content_hash = _download_to_tmp(url.strip())
            label = url.strip()
        elif up is not None:
            path, size, content_hash = _persist_upload(up)
            label = up.name
        else:
            st.warning("Provide a URL or upload a file.")
            st.stop()
    except Exception as exc:  # noqa: BLE001 - surface any load failure to the AM
        st.error(f"Could not load feed: {exc}")
        st.stop()
    _set_feed(
        path,
        label,
        size,
        content_hash,
        st.session_state.get("ff_index_params_cb", False),
        owned_path=True,
    )

signature = st.session_state.get("ff_signature")
if not signature:
    st.info("Load a feed to start filtering.")
    st.stop()

if not st.session_state.get("ff_content_hash"):
    try:
        migrated_size, migrated_hash = _fingerprint_path(
            st.session_state["ff_src_path"]
        )
    except Exception as exc:  # noqa: BLE001
        st.error(f"The previously loaded feed is no longer available: {exc}")
        st.stop()
    st.session_state["ff_src_size"] = migrated_size
    st.session_state["ff_content_hash"] = migrated_hash
    signature = (
        f"{migrated_hash}"
        f"::p{int(st.session_state.get('ff_index_params', False))}"
    )
    st.session_state["ff_signature"] = signature

requested_param_index = st.session_state.get("ff_index_params_cb", False)
if requested_param_index != st.session_state.get("ff_index_params", False):
    st.session_state.pop("ff_prepared_exports", None)
    st.session_state["ff_index_params"] = requested_param_index
    signature = (
        f"{st.session_state['ff_content_hash']}"
        f"::p{int(requested_param_index)}"
    )
    st.session_state["ff_signature"] = signature

if st.session_state.get("ff_table_signature") != signature:
    try:
        with st.spinner("Parsing feed…"):
            table = ff.extract(
                st.session_state["ff_src_path"],
                ff.DEFAULT_ITEM_CAP,
                index_params=requested_param_index,
            )
    except Exception as exc:  # noqa: BLE001
        if st.session_state.get("ff_owned_path"):
            lease = st.session_state.pop("ff_owned_lease", None)
            if lease is not None:
                try:
                    lease.close()
                except OSError:
                    pass
            try:
                os.unlink(st.session_state["ff_src_path"])
            except OSError:
                pass
            st.session_state.pop("ff_src_path", None)
            st.session_state.pop("ff_signature", None)
        st.error(
            "The feed could not be parsed. Check that it is a valid XML or "
            f"XML.GZ product feed. Details: {exc}"
        )
        st.stop()
    st.session_state["ff_table"] = table
    st.session_state["ff_table_signature"] = signature
else:
    table = st.session_state["ff_table"]

st.write(
    f"**Source:** `{_display_source(st.session_state['ff_src_label'])}`"
    f"  ·  format: **{table.spec}**"
)

if table.spec == "UNKNOWN":
    item_count = (
        f"{table.total_seen:,}"
        if table.total_exact
        else f"at least {table.total_seen:,}"
    )
    st.error(
        f"Format not recognized — {item_count} item-like elements were counted "
        "but no field table applies, so there is nothing to filter on. Identify/convert "
        "the feed format first (the Feed Checker page will tell you which)."
    )
    st.stop()
if table.n == 0:
    st.warning("No items were found in this feed.")
    st.stop()
if table.truncated:
    observed_copy = (
        f"of **{table.total_seen:,}** items"
        if table.total_exact
        else "and confirmed that the feed contains more items"
    )
    st.warning(
        f"Loaded the first **{table.n:,}** items {observed_copy}. "
        "The feed-order prefix can be biased, so counts below apply only to this "
        "snapshot and must not be extrapolated to the rest of the feed."
    )


# --------------------------------------------------------------------------- #
# Category facets + grouped rule builder
# --------------------------------------------------------------------------- #
CATEGORY_OPTION_CAP = 5_000
MAX_RULE_GROUPS = 8
MAX_RULES_PER_GROUP = 20
MAX_TOTAL_RULES = 100


def _category_facets() -> dict:
    """Snapshot-derived exact category choices, cached by parsed feed."""
    cached = st.session_state.get("ff_category_facets")
    facet_signature = st.session_state.get("ff_table_signature")
    if isinstance(cached, dict) and cached.get("signature") == facet_signature:
        return cached

    all_facets = ff.category_facets(table)
    limited = len(all_facets) > CATEGORY_OPTION_CAP
    shown_facets = all_facets[:CATEGORY_OPTION_CAP]
    values = [facet["value"] for facet in shown_facets]
    facets = {
        "signature": facet_signature,
        "values": values,
        "counts": {
            facet["value"]: facet["count"]
            for facet in shown_facets
        },
        "distinct": len(all_facets),
        "limited": limited,
    }
    st.session_state["ff_category_facets"] = facets
    return facets


facets = _category_facets()
category_values: list[str] = facets["values"]
category_counts: dict[str, int] = facets["counts"]

st.session_state.setdefault("ff_rules", [])
st.session_state.setdefault("ff_next_id", 0)
st.session_state.setdefault("ff_next_group_id", 0)
st.session_state.setdefault("ff_groups_combine", "AND")


def _ensure_group_state() -> None:
    group_ids = st.session_state.get("ff_group_ids")
    if not isinstance(group_ids, list) or not group_ids:
        group_id = st.session_state["ff_next_group_id"]
        st.session_state["ff_next_group_id"] += 1
        legacy_rules = list(st.session_state.get("ff_rules", []))
        st.session_state["ff_group_ids"] = [group_id]
        st.session_state[f"ff_group_rules_{group_id}"] = legacy_rules
        st.session_state[f"ff_group_combine_{group_id}"] = (
            st.session_state.get("ff_combine", "AND")
        )
        group_ids = [group_id]
    for group_id in group_ids:
        st.session_state.setdefault(f"ff_group_rules_{group_id}", [])
        st.session_state.setdefault(f"ff_group_combine_{group_id}", "AND")

    # Keep the old flat state as an alias of the first group for existing
    # sessions and AppTests; all new evaluation uses the grouped model.
    first_group = group_ids[0]
    st.session_state["ff_rules"] = st.session_state[
        f"ff_group_rules_{first_group}"
    ]
    st.session_state["ff_combine"] = st.session_state[
        f"ff_group_combine_{first_group}"
    ]


def _drop_rule_state(rule_id: int) -> None:
    """Remove state owned by a rule that will no longer be rendered."""
    for state_key in (
        f"ff_field_{rule_id}",
        f"ff_cat_values_{rule_id}",
        f"ff_pname_{rule_id}",
        f"ff_val_{rule_id}",
        f"ff_val2_{rule_id}",
        f"ff_del_{rule_id}",
    ):
        st.session_state.pop(state_key, None)
    operator_prefix = f"ff_op_{rule_id}_"
    for state_key in list(st.session_state):
        if state_key.startswith(operator_prefix):
            st.session_state.pop(state_key, None)


def _drop_group_state(group_id: int) -> None:
    """Remove a group's rules and widget state after delete/reset."""
    rule_ids = list(
        st.session_state.get(f"ff_group_rules_{group_id}", [])
    )
    for rule_id in rule_ids:
        _drop_rule_state(rule_id)
    for state_key in (
        f"ff_group_rules_{group_id}",
        f"ff_group_combine_{group_id}",
        f"ff_add_rule_{group_id}",
        f"ff_del_group_{group_id}",
    ):
        st.session_state.pop(state_key, None)


def _reset_to_one_empty_group() -> int:
    _ensure_group_state()
    existing_group_ids = list(st.session_state["ff_group_ids"])
    first_group = existing_group_ids[0]
    for group_id in existing_group_ids:
        _drop_group_state(group_id)
    st.session_state["ff_group_ids"] = [first_group]
    st.session_state[f"ff_group_rules_{first_group}"] = []
    st.session_state[f"ff_group_combine_{first_group}"] = "AND"
    st.session_state["ff_groups_combine"] = "AND"
    st.session_state["ff_rules"] = st.session_state[
        f"ff_group_rules_{first_group}"
    ]
    st.session_state["ff_combine"] = "AND"
    return first_group


def _state_rule_count() -> int:
    return sum(
        len(st.session_state.get(f"ff_group_rules_{group_id}", []))
        for group_id in st.session_state.get("ff_group_ids", [])
    )


def _add_rule(
    field: str = "price",
    op: str = "",
    value: object = "",
    value2: str = "",
    set_mode: str | None = None,
    set_combine: str | None = None,
    replace_existing: bool = False,
    group_id: int | None = None,
) -> None:
    _ensure_group_state()
    if replace_existing:
        group_id = _reset_to_one_empty_group()
    if group_id not in st.session_state["ff_group_ids"]:
        group_id = st.session_state["ff_group_ids"][0]
    if (
        len(st.session_state[f"ff_group_rules_{group_id}"])
        >= MAX_RULES_PER_GROUP
        or _state_rule_count() >= MAX_TOTAL_RULES
    ):
        return

    rule_id = st.session_state["ff_next_id"]
    st.session_state["ff_next_id"] += 1
    st.session_state[f"ff_group_rules_{group_id}"].append(rule_id)
    st.session_state["ff_rules"] = st.session_state[
        f"ff_group_rules_{st.session_state['ff_group_ids'][0]}"
    ]

    field_type = ff.FIELD_TYPE[field]
    operator_key = "category" if field == "category" else field_type
    st.session_state[f"ff_field_{rule_id}"] = field
    if op:
        st.session_state[f"ff_op_{rule_id}_{operator_key}"] = op
    if field == "category":
        if isinstance(value, (list, tuple, set)):
            selected = [str(item) for item in value]
        else:
            selected = [
                part.strip() for part in str(value).split(",") if part.strip()
            ]
        st.session_state[f"ff_cat_values_{rule_id}"] = selected
    else:
        st.session_state[f"ff_val_{rule_id}"] = value
    st.session_state[f"ff_val2_{rule_id}"] = value2

    if set_mode:
        st.session_state["ff_mode"] = set_mode
    if set_combine:
        st.session_state[f"ff_group_combine_{group_id}"] = set_combine
        if group_id == st.session_state["ff_group_ids"][0]:
            st.session_state["ff_combine"] = set_combine


def _add_group() -> None:
    _ensure_group_state()
    if (
        len(st.session_state["ff_group_ids"]) >= MAX_RULE_GROUPS
        or _state_rule_count() >= MAX_TOTAL_RULES
        or not st.session_state[
            f"ff_group_rules_{st.session_state['ff_group_ids'][0]}"
        ]
    ):
        return
    group_id = st.session_state["ff_next_group_id"]
    st.session_state["ff_next_group_id"] += 1
    st.session_state["ff_group_ids"].append(group_id)
    st.session_state[f"ff_group_rules_{group_id}"] = []
    st.session_state[f"ff_group_combine_{group_id}"] = "AND"
    _add_rule(group_id=group_id)


def _delete_rule(group_id: int, rule_id: int) -> None:
    rule_ids = st.session_state.get(f"ff_group_rules_{group_id}", [])
    if rule_id in rule_ids:
        rule_ids.remove(rule_id)
        _drop_rule_state(rule_id)


def _delete_group(group_id: int) -> None:
    group_ids = st.session_state.get("ff_group_ids", [])
    if len(group_ids) <= 1 or group_id not in group_ids:
        return
    group_ids.remove(group_id)
    _drop_group_state(group_id)
    st.session_state["ff_rules"] = st.session_state[
        f"ff_group_rules_{group_ids[0]}"
    ]
    st.session_state["ff_combine"] = st.session_state[
        f"ff_group_combine_{group_ids[0]}"
    ]


def _clear_rules() -> None:
    st.session_state.pop("ff_prepared_exports", None)
    _reset_to_one_empty_group()
    st.session_state["ff_mode"] = "keep"


def _ui_operators(field: str) -> list[str]:
    if field == "category":
        return ["one of", "not one of", "is empty", "is not empty"]
    return list(ff.operators_for_field(field))


def _operator_label(operator: str) -> str:
    return {
        "one of": "is one of",
        "not one of": "is not one of",
    }.get(operator, operator)


_ensure_group_state()
st.subheader("Filter rules")

primary_group_id = st.session_state["ff_group_ids"][0]
primary_rule_count = len(
    st.session_state[f"ff_group_rules_{primary_group_id}"]
)
total_rule_count = _state_rule_count()
qc1, qc2, qc3, qc4, qc5 = st.columns([1.1, 1.35, 1, 1.8, 1.55])
qc1.button(
    (
        "➕ Add to group 1"
        if len(st.session_state["ff_group_ids"]) > 1
        else "➕ Add rule"
    ),
    width="stretch",
    on_click=_add_rule,
    disabled=(
        primary_rule_count >= MAX_RULES_PER_GROUP
        or total_rule_count >= MAX_TOTAL_RULES
    ),
)
qc2.button(
    "＋ Add rule group",
    width="stretch",
    on_click=_add_group,
    disabled=(
        primary_rule_count == 0
        or len(st.session_state["ff_group_ids"]) >= MAX_RULE_GROUPS
        or total_rule_count >= MAX_TOTAL_RULES
    ),
    help=(
        "Add at least one condition to the first group before creating "
        "another group."
        if primary_rule_count == 0
        else "Add another parenthesized rule group."
    ),
)
qc3.button("🗑 Clear all", width="stretch", on_click=_clear_rules)
qc4.button(
    "Preset: missing availability",
    width="stretch",
    on_click=_add_rule,
    kwargs=dict(
        field="availability",
        op="is empty",
        set_mode="remove",
        set_combine="OR",
        replace_existing=True,
    ),
)
qc5.button(
    "Preset: missing image",
    width="stretch",
    on_click=_add_rule,
    kwargs=dict(
        field="has_image",
        op="is false",
        set_mode="remove",
        set_combine="OR",
        replace_existing=True,
    ),
)
st.caption(
    "Start with one group. Add another only when you need parentheses such as "
    "“(Sofas OR Chairs) AND price under 500”. Presets start over."
)

group_ids = list(st.session_state["ff_group_ids"])
control_left, control_right = st.columns(2)
mode = control_left.radio(
    "When a product matches",
    ["keep", "remove"],
    horizontal=True,
    key="ff_mode",
    format_func=lambda value: "Keep it" if value == "keep" else "Remove it",
    help="Keep it = resulting feed contains the matches · Remove it = drop the matches",
)
if len(group_ids) > 1:
    groups_combine = control_right.radio(
        "A product must match",
        ["AND", "OR"],
        horizontal=True,
        key="ff_groups_combine",
        format_func=lambda value: (
            "All rule groups" if value == "AND" else "Any rule group"
        ),
        help="This combines the parenthesized rule groups below.",
    )
else:
    st.session_state["ff_groups_combine"] = "AND"
    groups_combine = "AND"
    control_right.caption(
        "One rule group is active. Add another group to combine grouped logic."
    )

if facets["limited"]:
    st.warning(
        f"This snapshot has {facets['distinct']:,} distinct categories. "
        f"Dropdowns show the {CATEGORY_OPTION_CAP:,} most-used categories."
    )
elif category_values:
    st.caption(
        f"Category dropdowns use {len(category_values):,} exact feed values; "
        "the number beside each value is its product count in this snapshot."
    )

field_keys = [field["key"] for field in ff.FIELDS]
groups: list[dict] = []

for group_position, group_id in enumerate(group_ids, start=1):
    with st.container(border=True):
        gh_title, gh_logic, gh_add, gh_delete = st.columns([2.4, 3, 1.4, 1.3])
        gh_title.markdown(f"**Rule group {group_position}**")
        group_combine = gh_logic.radio(
            "Match rules in this group",
            ["AND", "OR"],
            horizontal=True,
            key=f"ff_group_combine_{group_id}",
            format_func=lambda value: (
                "All rules" if value == "AND" else "Any rule"
            ),
            label_visibility="collapsed",
            help="All rules = AND · Any rule = OR",
        )
        gh_add.button(
            "＋ Condition",
            key=f"ff_add_rule_{group_id}",
            width="stretch",
            on_click=_add_rule,
            kwargs={"group_id": group_id},
            disabled=(
                len(st.session_state[f"ff_group_rules_{group_id}"])
                >= MAX_RULES_PER_GROUP
                or total_rule_count >= MAX_TOTAL_RULES
            ),
        )
        if len(group_ids) > 1:
            gh_delete.button(
                "Delete group",
                key=f"ff_del_group_{group_id}",
                help=f"Delete rule group {group_position}",
                icon=":material/delete:",
                type="tertiary",
                on_click=_delete_group,
                args=(group_id,),
            )

        group_rule_ids = list(
            st.session_state[f"ff_group_rules_{group_id}"]
        )
        group_rules: list[dict] = []
        if group_rule_ids:
            h_field, h_op, h_val, h_del = st.columns([3, 3, 4, 1.3])
            h_field.caption("FIELD")
            h_op.caption("CONDITION")
            h_val.caption("VALUE")
            h_del.caption("ACTION")

        for rule_position, rule_id in enumerate(group_rule_ids, start=1):
            c_field, c_op, c_val, c_del = st.columns([3, 3, 4, 1.3])
            field = c_field.selectbox(
                "Field",
                field_keys,
                format_func=lambda key: ff.FIELD_LABEL[key],
                key=f"ff_field_{rule_id}",
                label_visibility="collapsed",
            )
            field_type = ff.FIELD_TYPE[field]
            operator_key = "category" if field == "category" else field_type
            operator = c_op.selectbox(
                "Operator",
                _ui_operators(field),
                key=f"ff_op_{rule_id}_{operator_key}",
                format_func=_operator_label,
                label_visibility="collapsed",
            )

            value: object = ""
            value2 = ""
            if field == "category" and operator in ("one of", "not one of"):
                value = c_val.multiselect(
                    "Categories",
                    category_values,
                    key=f"ff_cat_values_{rule_id}",
                    format_func=lambda category: (
                        f"{category} ({category_counts.get(category, 0):,})"
                    ),
                    placeholder="Choose one or more categories",
                    label_visibility="collapsed",
                )
            elif field_type == "param":
                c_name, c_pval = c_val.columns(2)
                value2 = c_name.text_input(
                    "param name",
                    key=f"ff_pname_{rule_id}",
                    label_visibility="collapsed",
                    placeholder="param name e.g. Color",
                )
                if operator in ("is empty", "is not empty"):
                    c_pval.markdown("&nbsp;", unsafe_allow_html=True)
                else:
                    value = c_pval.text_input(
                        "value",
                        key=f"ff_val_{rule_id}",
                        label_visibility="collapsed",
                        placeholder="value",
                    )
            elif operator == "between":
                v1, v2 = c_val.columns(2)
                value = v1.text_input(
                    "min",
                    key=f"ff_val_{rule_id}",
                    label_visibility="collapsed",
                    placeholder="min",
                )
                value2 = v2.text_input(
                    "max",
                    key=f"ff_val2_{rule_id}",
                    label_visibility="collapsed",
                    placeholder="max",
                )
            elif operator in (
                "is empty",
                "is not empty",
                "is true",
                "is false",
            ):
                c_val.markdown("&nbsp;", unsafe_allow_html=True)
            else:
                placeholder = (
                    "e.g. 200"
                    if field_type == "number"
                    else "brandA, brandB"
                    if operator in ("in list", "not in list")
                    else "text…"
                )
                value = c_val.text_input(
                    "Value",
                    key=f"ff_val_{rule_id}",
                    label_visibility="collapsed",
                    placeholder=placeholder,
                )
            c_del.button(
                "Delete rule",
                key=f"ff_del_{rule_id}",
                help=(
                    f"Delete rule {rule_position} from rule group "
                    f"{group_position}"
                ),
                icon=":material/delete:",
                type="tertiary",
                on_click=_delete_rule,
                args=(group_id, rule_id),
            )

            rule = {
                "field": field,
                "op": operator,
                "value": value,
                "value2": value2,
            }
            group_rules.append(rule)
            error = ff.rule_error(rule, table)
            if error:
                st.caption(
                    f"⚠️ {ff.FIELD_LABEL[field]}: {error} "
                    "This rule is not applied yet."
                )

        if not group_rules:
            st.caption(
                "No conditions in this group yet. Add one when you need it; "
                "empty groups do not affect the preview, but they must be "
                "completed or deleted before hand-off."
            )
        groups.append({"combine": group_combine, "rules": group_rules})


# --------------------------------------------------------------------------- #
# Live result
# --------------------------------------------------------------------------- #
empty_group_count = sum(not group["rules"] for group in groups)
result = ff.apply_rule_groups(
    table,
    groups,
    group_combine=groups_combine,
    mode=mode,
)
pct_removed = (result["removed"] / result["n"] * 100) if result["n"] else 0

if result["incomplete_rule_count"]:
    st.info(
        f"{result['incomplete_rule_count']} incomplete "
        f"{'rule is' if result['incomplete_rule_count'] == 1 else 'rules are'} "
        "ignored in the preview until all required values are filled in. "
        "Hand-off stays disabled meanwhile."
    )
if empty_group_count and result["active_rule_count"]:
    st.info(
        f"{empty_group_count} empty rule "
        f"{'group is' if empty_group_count == 1 else 'groups are'} ignored in "
        "the preview. Add a condition or delete the empty group before hand-off."
    )

st.markdown("---")
if result["active_rule_count"]:
    match_metric = (
        "Matched to keep" if mode == "keep" else "Matched to remove",
        f"{result['matched']:,}",
        "brand",
    )
else:
    match_metric = (
        "Filter status",
        "No active rules",
        "default",
        "All loaded products remain",
    )
render_metric_row([
    ("Loaded", f"{result['n']:,}", "default",
     (
         f"of {result['total_seen']:,} total"
         if table.truncated and table.total_exact
         else "more items exist"
         if table.truncated
         else "full feed"
     )),
    match_metric,
    ("Resulting feed", f"{result['kept']:,}", "ok"),
    ("Removed", f"{result['removed']:,}", "error" if result["removed"] else "ok",
     f"{pct_removed:.1f}% of snapshot"),
])

if len(groups) > 1 and result.get("per_group"):
    st.caption("Each active rule group on its own would match:")
    for group_result in result["per_group"]:
        group_number = int(group_result.get("group_index", 0)) + 1
        st.markdown(
            f"- **Rule group {group_number}** → "
            f"**{group_result['matched']:,}** products"
        )

if result["per_rule"]:
    with st.expander("Rule match details", expanded=False):
        for per_rule in result["per_rule"]:
            st.markdown(
                f"- `{ff.rule_text(per_rule['rule'])}` → "
                f"**{per_rule['matched']:,}** products"
            )


# --------------------------------------------------------------------------- #
# Browse/search: preview-only controls, deliberately excluded from exports.
# --------------------------------------------------------------------------- #
PREVIEW_COLS = [
    "id",
    "title",
    "price",
    "availability",
    "brand",
    "category",
    "category_depth",
    "image_count",
    "has_ean",
    "url",
]
default_browse = {
    "query": "",
    "categories": [],
    "scope": "Resulting feed",
    "page_size": 50,
}
st.session_state.setdefault("ff_browse_query", "")
st.session_state.setdefault("ff_browse_categories", [])
st.session_state.setdefault("ff_browse_scope", "Resulting feed")
st.session_state.setdefault("ff_browse_page_size", 50)
st.session_state.setdefault("ff_browse_page", 0)


def _previous_browse_page() -> None:
    st.session_state["ff_browse_page"] = max(
        0,
        int(st.session_state.get("ff_browse_page", 0)) - 1,
    )


def _next_browse_page() -> None:
    st.session_state["ff_browse_page"] = (
        int(st.session_state.get("ff_browse_page", 0)) + 1
    )

with st.expander("Browse / verify products", expanded=True):
    st.caption(
        "Search is only for checking the snapshot. It never changes match "
        "counts, the resulting IDs, or the hand-off."
    )
    with st.form("ff_browse_form"):
        browse_query = st.text_input(
            "Search products",
            key="ff_browse_query",
            placeholder="ID, title, brand, category, URL…",
            help="All words must occur somewhere in the searchable product fields.",
        )
        browse_left, browse_middle, browse_right = st.columns([3, 2, 1.3])
        browse_categories = browse_left.multiselect(
            "Exact categories",
            category_values,
            key="ff_browse_categories",
            format_func=lambda category: (
                f"{category} ({category_counts.get(category, 0):,})"
            ),
            placeholder="All categories",
        )
        browse_scope = browse_middle.selectbox(
            "Search within",
            ["Resulting feed", "Removed products", "All loaded products"],
            key="ff_browse_scope",
        )
        browse_page_size = browse_right.selectbox(
            "Rows per page",
            [25, 50, 100],
            key="ff_browse_page_size",
        )
        browse_submitted = st.form_submit_button(
            "Show products",
            type="primary",
        )

    if browse_submitted:
        st.session_state["ff_browse_request"] = {
            "query": browse_query,
            "categories": list(browse_categories),
            "scope": browse_scope,
            "page_size": browse_page_size,
        }
        st.session_state["ff_browse_page"] = 0

    browse_request = st.session_state.get(
        "ff_browse_request",
        default_browse,
    )
    if browse_request["scope"] == "Removed products":
        browse_base_mask = [not keep for keep in result["keep_mask"]]
    elif browse_request["scope"] == "All loaded products":
        browse_base_mask = [True] * table.n
    else:
        browse_base_mask = result["keep_mask"]

    visible_mask = ff.browse_mask(
        table,
        query=browse_request["query"],
        categories=browse_request["categories"],
        base_mask=browse_base_mask,
    )
    visible_count = sum(visible_mask)
    page_size = int(browse_request.get("page_size", 50))
    page_count = max(1, (visible_count + page_size - 1) // page_size)
    browse_page = min(
        max(0, int(st.session_state.get("ff_browse_page", 0))),
        page_count - 1,
    )
    st.session_state["ff_browse_page"] = browse_page
    page_start = browse_page * page_size
    preview = []
    matched_so_far = 0
    for row_index, visible in enumerate(visible_mask):
        if not visible:
            continue
        if matched_so_far < page_start:
            matched_so_far += 1
            continue
        preview.append({
            column: table.columns[column][row_index]
            for column in PREVIEW_COLS
        })
        matched_so_far += 1
        if len(preview) >= page_size:
            break

    criteria = []
    if str(browse_request["query"]).strip():
        criteria.append(f"search “{browse_request['query']}”")
    if browse_request["categories"]:
        criteria.append(
            f"{len(browse_request['categories'])} selected "
            f"{'category' if len(browse_request['categories']) == 1 else 'categories'}"
        )
    criteria_copy = " · " + " · ".join(criteria) if criteria else ""
    if preview:
        shown_start = page_start + 1
        shown_end = page_start + len(preview)
        shown_range = (
            str(shown_start)
            if shown_start == shown_end
            else f"{shown_start:,}–{shown_end:,}"
        )
    else:
        shown_range = "0"
    st.caption(
        f"Showing {shown_range} of {visible_count:,} products within "
        f"{browse_request['scope'].lower()}{criteria_copy}."
    )
    if preview:
        st.dataframe(
            preview,
            width="stretch",
            hide_index=True,
            column_config={
                "url": st.column_config.LinkColumn(
                    "Product URL",
                    display_text="Open",
                ),
            },
        )
        if page_count > 1:
            nav_back, nav_status, nav_forward = st.columns([1, 2, 1])
            nav_back.button(
                "← Previous",
                key="ff_browse_previous",
                width="stretch",
                on_click=_previous_browse_page,
                disabled=browse_page == 0,
            )
            nav_status.caption(
                f"Page {browse_page + 1:,} of {page_count:,}"
            )
            nav_forward.button(
                "Next →",
                key="ff_browse_next",
                width="stretch",
                on_click=_next_browse_page,
                disabled=browse_page >= page_count - 1,
            )
    else:
        st.info("No products match this browse search.")


# --------------------------------------------------------------------------- #
# Hand-off: exactly what to tell Raul / run server-side
# --------------------------------------------------------------------------- #
handoff_ready = (
    result["active_rule_count"] > 0
    and result["incomplete_rule_count"] == 0
    and empty_group_count == 0
)
st.subheader("Hand-off")
if handoff_ready:
    st.markdown(
        "Copy this to describe the change, then prepare the supporting files:"
    )
    st.code(
        ff.describe_rule_groups(
            groups,
            groups_combine,
            mode,
            result,
            table=table,
        ),
        language="text",
    )
else:
    if result["active_rule_count"] == 0:
        handoff_guidance = (
            "Add and complete at least one rule to create an accurate hand-off "
            "and enable downloads."
        )
    elif empty_group_count:
        handoff_guidance = (
            "Add a condition to every rule group or delete empty groups before "
            "creating the hand-off."
        )
    else:
        handoff_guidance = (
            "Complete every rule to create an accurate hand-off and enable "
            "downloads."
        )
    st.info(handoff_guidance)
if table.truncated:
    st.warning(
        "The ID files below contain only the loaded prefix snapshot, not the "
        "unseen remainder of the feed."
    )

filter_spec = ff.to_group_spec(
    groups,
    groups_combine,
    mode,
    table=table,
)
filter_spec["snapshot"] = {
    "source": _display_source(st.session_state["ff_src_label"]),
    "sha256": st.session_state["ff_content_hash"],
    "loadedItems": table.n,
    "observedItems": table.total_seen,
    "observedItemsExact": table.total_exact,
    "truncated": table.truncated,
    "parameterIndexing": table.index_params,
}
scope_label = "Snapshot " if table.truncated else ""
scope_file = "snapshot_" if table.truncated else ""

all_ids: list[str] = []
missing_ids = duplicate_ids = formula_ids = 0
if handoff_ready:
    all_ids = [str(value) for value in table.columns["id"]]
    missing_ids = sum(not value.strip() for value in all_ids)
    nonblank_ids = [value for value in all_ids if value.strip()]
    duplicate_ids = len(nonblank_ids) - len(set(nonblank_ids))
    formula_ids = sum(
        value.lstrip().startswith(("=", "+", "-", "@"))
        for value in nonblank_ids
    )
id_integrity_ok = missing_ids == 0 and duplicate_ids == 0
if handoff_ready and not id_integrity_ok:
    problems = []
    if missing_ids:
        problems.append(f"{missing_ids:,} missing")
    if duplicate_ids:
        problems.append(f"{duplicate_ids:,} duplicate")
    st.warning(
        "ID downloads are disabled because the feed contains "
        + " and ".join(problems)
        + " product IDs. The filter specification remains available."
    )
elif handoff_ready and formula_ids:
    st.info(
        f"{formula_ids:,} ID values begin with spreadsheet formula characters, "
        "so the ID files will use JSON instead of CSV to preserve them safely."
    )

export_key = hashlib.sha256(
    json.dumps(filter_spec, sort_keys=True).encode("utf-8")
).hexdigest()
prepared = st.session_state.get("ff_prepared_exports")
exports_ready = bool(
    handoff_ready
    and isinstance(prepared, dict)
    and prepared.get("key") == export_key
)

if handoff_ready and st.button(
    "Prepare hand-off downloads",
    type="primary",
    width="stretch",
):
    kept_ids = [
        all_ids[i] for i in range(table.n) if result["keep_mask"][i]
    ]
    removed_ids = [
        all_ids[i] for i in range(table.n) if not result["keep_mask"][i]
    ]
    id_format = "json" if formula_ids else "csv"
    if id_integrity_ok and id_format == "csv":
        kept_data = ff.ids_csv(kept_ids)
        removed_data = ff.ids_csv(removed_ids)
    elif id_integrity_ok:
        kept_data = json.dumps(kept_ids, ensure_ascii=False, indent=2)
        removed_data = json.dumps(removed_ids, ensure_ascii=False, indent=2)
    else:
        kept_data = removed_data = ""
    prepared = {
        "key": export_key,
        "id_format": id_format,
        "kept": kept_data,
        "removed": removed_data,
        "spec": json.dumps(filter_spec, indent=2),
    }
    st.session_state["ff_prepared_exports"] = prepared
    exports_ready = True

if handoff_ready and not exports_ready:
    st.caption(
        "Downloads are generated only when requested, keeping live rule editing fast."
    )

id_format = prepared.get("id_format", "csv") if exports_ready else (
    "json" if formula_ids else "csv"
)
id_mime = "application/json" if id_format == "json" else "text/csv"
d1, d2, d3 = st.columns(3)
d1.download_button(
    f"⬇︎ {scope_label}resulting IDs ({id_format.upper()})",
    prepared.get("kept", "") if exports_ready else "",
    file_name=f"{scope_file}kept_ids.{id_format}",
    mime=id_mime,
    width="stretch",
    disabled=not exports_ready or not id_integrity_ok,
)
d2.download_button(
    f"⬇︎ {scope_label}removed IDs ({id_format.upper()})",
    prepared.get("removed", "") if exports_ready else "",
    file_name=f"{scope_file}removed_ids.{id_format}",
    mime=id_mime,
    width="stretch",
    disabled=not exports_ready or not id_integrity_ok,
)
d3.download_button(
    "⬇︎ Filter spec (JSON)",
    prepared.get("spec", "") if exports_ready else "",
    file_name="filter_spec.json",
    mime="application/json",
    width="stretch",
    disabled=not exports_ready,
)

st.markdown("© 2026 Raul Bertoldini")
