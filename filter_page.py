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


# The rule builder / results / browse / exports live in filter_view so the Feed
# Checker "Browse" tab can reuse them against a single loaded feed.
from filter_view import render_filter

render_filter()
