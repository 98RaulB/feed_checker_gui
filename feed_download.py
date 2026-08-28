# feed_download.py — shared, hardened feed acquisition for BOTH pages.
#
# One implementation of download/upload persistence so the Checker and the
# Filter can never drift apart on abuse limits:
#   * SSRF-safe transport (safe_http.public_session; every redirect hop
#     re-validated against the public-IP policy).
#   * Hard size cap (Content-Length pre-check AND per-chunk enforcement, so a
#     spoofed or absent header cannot bypass it) and a wall-clock cap (a
#     slow-loris server cannot hold a worker forever — timeout=(15, N) only
#     bounds each individual socket read, not the whole request).
#   * All files land in a per-uid 0o700 temp dir with a "feed-" prefix; a TTL
#     janitor reclaims abandoned files while fcntl leases protect live ones.
#
# Env knobs keep their historical FAVI_FILTER_* names (they now apply to the
# whole app, not just the Filter page): FAVI_FILTER_MAX_DOWNLOAD_MB,
# FAVI_FILTER_MAX_DOWNLOAD_SECONDS, FAVI_FILTER_TEMP_TTL_SECONDS.
# The shop-facing deployment should set these LOWER than the internal defaults.
#
# This module must stay free of streamlit imports — it is also the piece a
# future Cloud Run API wraps.
from __future__ import annotations

import fcntl
import hashlib
import os
import tempfile
import time
from urllib.parse import urljoin, urlparse

import requests

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


# Defaults sized for the INTERNAL app (real partner feeds reach 1-2 GB, the
# pipeline's own download cap is 2 GB). The shop entry point pins these DOWN
# via os.environ.setdefault before this module loads — see shop_checker.py.
MAX_DOWNLOAD_SECONDS = _positive_env_int(
    "FAVI_FILTER_MAX_DOWNLOAD_SECONDS", 900
)
MAX_DOWNLOAD_BYTES = (
    _positive_env_int("FAVI_FILTER_MAX_DOWNLOAD_MB", 2048) * 1024 * 1024
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


def cleanup_stale_temp_files() -> None:
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


def new_temp_path(suffix: str) -> tuple[int, str]:
    os.makedirs(TEMP_DIR, mode=0o700, exist_ok=True)
    return tempfile.mkstemp(prefix="feed-", suffix=suffix, dir=TEMP_DIR)


def lease_temp_path(path: str):
    """Keep an owned source locked so the TTL janitor cannot delete it."""
    lease = open(path, "rb")
    try:
        fcntl.flock(lease.fileno(), fcntl.LOCK_SH | fcntl.LOCK_NB)
    except Exception:
        lease.close()
        raise
    return lease


def display_source(label: str) -> str:
    value = str(label or "feed")
    parsed = urlparse(value)
    if parsed.scheme in ("http", "https") and parsed.hostname:
        path = parsed.path or "/"
        if len(path) > 64:
            path = f"{path[:61]}…"
        return f"{parsed.hostname}{path}"
    name = os.path.basename(value)
    return name if len(name) <= 80 else f"{name[:77]}…"


def download_to_tmp(url: str) -> tuple[str, int, str]:
    """Stream a URL into the managed temp dir over the pinned public session.
    Returns (path, size, sha256)."""
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
                f"Could not connect to {display_source(current_url)}."
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
                    f"The feed server at {display_source(current_url)} "
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

            ctype = response.headers.get("Content-Type", "").lower()
            suffix = (
                ".xml.gz"
                if (
                    "gzip" in ctype
                    or current_url.lower().split("?", 1)[0].endswith(".gz")
                )
                else ".xml"
            )
            fd, path = new_temp_path(suffix)
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
                    f"The connection to {display_source(current_url)} "
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


def persist_upload(up) -> tuple[str, int, str]:
    """Persist a Streamlit upload into the managed temp dir.
    Returns (path, size, sha256)."""
    content = up.getbuffer()
    size = len(content)
    if size > MAX_DOWNLOAD_BYTES:
        raise ff.FeedDownloadError(
            f"The file is larger than the "
            f"{MAX_DOWNLOAD_BYTES // (1024 * 1024):,} MB limit."
        )
    is_gzip = bytes(content[:2]) == b"\x1f\x8b"
    suffix = ".xml.gz" if is_gzip else ".xml"
    fd, path = new_temp_path(suffix)
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
