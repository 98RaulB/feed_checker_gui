# feed_filter.py
# Standalone, dependency-free filtering engine for FAVI product feeds.
#
# Why this lives apart from feed_checker_gui.py:
#   * The GUI script is single-shot (it `st.stop()`s unless the form was just
#     submitted), so it cannot host live, re-run-driven filtering. This module
#     has NO Streamlit dependency at all — it is pure Python over the same
#     feed_specs readers the validator uses, so the extracted values always
#     agree with what the checker reports.
#   * It is the piece that will move to Cloud Run: the Streamlit page is a thin
#     shell over `extract()` + `apply_rules()`.
#
# Memory model (the whole reason this is safe on a ~1 GB Streamlit host):
#   * COLUMNAR storage — one list per field, not one dict per product. That is
#     3-5x lighter than list-of-dicts and mirrors what the GUI already does with
#     its parallel `ids`/`links`/`prices_amt` arrays.
#   * Low-cardinality string columns (availability, brand, category) are
#     INTERNED so repeated values share one object.
#   * A hard item cap (`DEFAULT_ITEM_CAP`) bounds the table regardless of feed
#     size. Counts are EXACT within the loaded set; beyond the cap the caller is
#     told the table is a sample (full-feed exact counts are the Cloud Run job).
from __future__ import annotations

from typing import List, Dict, Any, Iterable, Optional, Tuple
import csv
import gzip
import io
import ipaddress
import math
import os
import re
import socket
from urllib.parse import urlparse
import xml.etree.ElementTree as StdET

try:
    from defusedxml import ElementTree as ET  # type: ignore
except Exception:  # pragma: no cover - defusedxml is optional
    import xml.etree.ElementTree as ET  # type: ignore

from feed_specs import (
    SPEC,
    strip_ns,
    detect_spec,
    read_id,
    read_link,
    read_availability,
    read_price,
    gather_primary_image,
    gather_gallery,
    read_recommended_value,
    is_valid_gtin,
    _named_param_values,   # name->value pairs from <PARAM>/<attrs> containers
)

# Hard ceiling on rows held in memory for interactive filtering.
DEFAULT_ITEM_CAP = 200_000
try:
    MAX_XML_BYTES = max(
        1, int(os.getenv("FAVI_FILTER_MAX_XML_MB", "512"))
    ) * 1024 * 1024
except ValueError:
    MAX_XML_BYTES = 512 * 1024 * 1024
try:
    MAX_ITEM_XML_BYTES = max(
        1, int(os.getenv("FAVI_FILTER_MAX_ITEM_XML_MB", "16"))
    ) * 1024 * 1024
except ValueError:
    MAX_ITEM_XML_BYTES = 16 * 1024 * 1024
try:
    MAX_ITEM_NODES = max(
        1, int(os.getenv("FAVI_FILTER_MAX_ITEM_NODES", "25000"))
    )
except ValueError:
    MAX_ITEM_NODES = 25_000
_ALLOWED_URL_SCHEMES = {"http", "https"}


class FeedDownloadError(ValueError):
    """A feed URL was unsafe or could not be resolved safely."""


class FeedParseLimitError(ValueError):
    """The uncompressed XML exceeded the interactive parser's safety limit."""


def _resolve_ips(host: str) -> List[str]:
    infos = socket.getaddrinfo(host, None)
    return [info[4][0] for info in infos]


def _ip_is_global(ip_str: str) -> bool:
    try:
        ip = ipaddress.ip_address(ip_str)
    except ValueError:
        return False
    if isinstance(ip, ipaddress.IPv6Address) and ip.ipv4_mapped is not None:
        return _ip_is_global(str(ip.ipv4_mapped))
    return bool(ip.is_global and not ip.is_multicast)


def assert_public_ip(ip_str: str, host: str = "feed host") -> None:
    """Apply the URL destination policy to an already connected peer."""
    if not _ip_is_global(ip_str):
        raise FeedDownloadError(
            f"Host '{host}' connected to a private or reserved address."
        )


def public_url_ips(url: str) -> List[str]:
    """Validate a feed URL and return the exact public addresses it may use."""
    parsed = urlparse(url)
    scheme = (parsed.scheme or "").lower()
    if scheme not in _ALLOWED_URL_SCHEMES:
        raise FeedDownloadError("Only http:// and https:// feed URLs are allowed.")

    host = parsed.hostname
    if not host:
        raise FeedDownloadError("The feed URL has no host.")

    try:
        ipaddress.ip_address(host)
    except ValueError:
        try:
            ips = _resolve_ips(host)
        except socket.gaierror as exc:
            raise FeedDownloadError(f"Could not resolve host '{host}'.") from exc
        if not ips:
            raise FeedDownloadError(f"Host '{host}' did not resolve to an address.")
        for ip_str in ips:
            if not _ip_is_global(ip_str):
                raise FeedDownloadError(
                    f"Host '{host}' resolves to a private or reserved address."
                )
    else:
        ips = [host]
        if not _ip_is_global(host):
            raise FeedDownloadError(
                f"Host '{host}' is a private or reserved address."
            )
    return list(dict.fromkeys(ips))


def assert_public_url(url: str) -> None:
    """Reject URL shapes and destinations that could reach internal services."""
    public_url_ips(url)

# ---- Field catalogue: what an AM can filter on, and each field's data type. ----
# `type` drives which operators the UI offers and how a rule is evaluated.
# The "param" field is special: it filters an arbitrary named product parameter
# and is only usable when the feed was extracted with index_params=True.
FIELDS: List[Dict[str, str]] = [
    {"key": "price",              "label": "Price (amount)",         "type": "number"},
    {"key": "category_depth",     "label": "Category depth (levels)","type": "number"},
    {"key": "title_length",       "label": "Title length (chars)",   "type": "number"},
    {"key": "description_length", "label": "Description length",      "type": "number"},
    {"key": "image_count",        "label": "Image count",            "type": "number"},
    {"key": "availability",       "label": "Availability (raw)",     "type": "text"},
    {"key": "category",           "label": "Category",               "type": "text"},
    {"key": "brand",              "label": "Brand / manufacturer",   "type": "text"},
    {"key": "title",              "label": "Product name",           "type": "text"},
    {"key": "id",                 "label": "Item ID",                "type": "text"},
    {"key": "url",                "label": "Product URL",            "type": "text"},
    {"key": "has_image",          "label": "Has primary image",      "type": "bool"},
    {"key": "has_ean",            "label": "Has valid EAN/GTIN",     "type": "bool"},
    {"key": "price_valid",        "label": "Price is valid (>0)",    "type": "bool"},
    {"key": "has_description",    "label": "Has description",        "type": "bool"},
    {"key": "has_brand",          "label": "Has brand",              "type": "bool"},
    {"key": "has_category",       "label": "Has category",           "type": "bool"},
    {"key": "param",              "label": "Product parameter",      "type": "param"},
]
FIELD_TYPE: Dict[str, str] = {f["key"]: f["type"] for f in FIELDS}
FIELD_LABEL: Dict[str, str] = {f["key"]: f["label"] for f in FIELDS}

TEXT_OPS = [
    "contains", "not contains", "equals", "not equals",
    "starts with", "in list", "not in list", "is empty", "is not empty",
]
NUMBER_OPS = ["<", "<=", ">", ">=", "==", "!=", "between", "is empty", "is not empty"]
BOOL_OPS = ["is true", "is false"]
CATEGORY_OPS = ["one of", "not one of", "is empty", "is not empty"]

OPS_BY_TYPE = {"text": TEXT_OPS, "number": NUMBER_OPS, "bool": BOOL_OPS, "param": TEXT_OPS}
ZERO_IS_EMPTY_FIELDS = {
    "category_depth", "title_length", "description_length", "image_count",
}
BROWSE_SEARCH_FIELDS = ("id", "title", "brand", "category", "url", "availability")
BROWSE_ROW_FIELDS = (
    "id", "title", "price", "availability", "brand", "category", "url",
)


def operators_for_field(field: str) -> List[str]:
    """Return the concise operator list the UI should show for ``field``.

    Categories intentionally use exact feed-derived selections.  The legacy
    text operators (including ``in list`` / ``not in list``) remain accepted
    by the engine so saved v1 rules continue to reproduce exactly.
    """
    if field == "category":
        return list(CATEGORY_OPS)
    typ = FIELD_TYPE.get(field)
    return list(OPS_BY_TYPE.get(typ, []))


# --------------------------------------------------------------------------- #
# Extraction
# --------------------------------------------------------------------------- #
class FeedTable:
    """Columnar snapshot of a feed, capped at `DEFAULT_ITEM_CAP` rows."""

    def __init__(self, spec: str, index_params: bool = False) -> None:
        self.spec = spec
        self.index_params = index_params
        self.columns: Dict[str, list] = {f["key"]: [] for f in FIELDS}
        self.total_seen = 0
        self.total_exact = True

    @property
    def n(self) -> int:              # rows actually loaded (<= cap)
        return len(self.columns["id"])

    @property
    def truncated(self) -> bool:
        return not self.total_exact or self.total_seen > self.n


def category_facets(
    table: FeedTable,
    include_empty: bool = False,
) -> List[Dict[str, Any]]:
    """Return case-insensitive category counts from the loaded snapshot.

    The first spelling found in the feed is retained for display, while case
    variants are counted together.  Blank categories are omitted unless
    ``include_empty`` is requested.
    """
    counts: Dict[str, int] = {}
    display: Dict[str, str] = {}
    column = table.columns.get("category", [])
    for index in range(table.n):
        raw_value = column[index] if index < len(column) else ""
        value = str(raw_value or "").strip()
        if not value and not include_empty:
            continue
        folded = value.casefold()
        display.setdefault(folded, value)
        counts[folded] = counts.get(folded, 0) + 1
    return [
        {
            "value": display[folded],
            "count": count,
            "label": (
                f"{display[folded]} ({count:,})"
                if display[folded]
                else f"(Missing category) ({count:,})"
            ),
        }
        for folded, count in sorted(
            counts.items(),
            key=lambda item: (-item[1], display[item[0]].casefold()),
        )
    ]


def _is_gzip_path(path: str) -> bool:
    try:
        with open(path, "rb") as fh:
            return fh.read(2) == b"\x1f\x8b"
    except OSError:
        return False


def _open_maybe_gzip(path: str):
    return gzip.open(path, "rb") if _is_gzip_path(path) else open(path, "rb")


class _BoundedReader:
    """File-like reader that caps bytes after transport-level decompression."""

    def __init__(self, raw, limit: Optional[int] = None) -> None:
        self.raw = raw
        self.limit = MAX_XML_BYTES if limit is None else limit
        self.consumed = 0
        self.item_start: Optional[int] = None
        self.item_limit = MAX_ITEM_XML_BYTES

    def read(self, size: int = -1) -> bytes:
        remaining = self.limit - self.consumed
        if self.item_start is not None:
            remaining = min(
                remaining,
                self.item_limit - (self.consumed - self.item_start),
            )
        request_size = (
            remaining + 1
            if size is None or size < 0
            else min(size, remaining + 1)
        )
        data = self.raw.read(request_size)
        self.consumed += len(data)
        if self.consumed > self.limit:
            raise FeedParseLimitError(
                "The uncompressed feed exceeds the "
                f"{self.limit // (1024 * 1024):,} MB interactive parsing limit."
            )
        if (
            self.item_start is not None
            and self.consumed - self.item_start > self.item_limit
        ):
            raise FeedParseLimitError(
                "A single product exceeds the "
                f"{self.item_limit // (1024 * 1024):,} MB interactive item limit."
            )
        return data

    def start_item(self) -> None:
        self.item_start = self.consumed

    def end_item(self) -> None:
        self.item_start = None

    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc, tb) -> None:
        self.raw.close()


def _open_bounded_xml(path: str) -> _BoundedReader:
    return _BoundedReader(_open_maybe_gzip(path))


def _localnames_from_item_paths(spec_name: str) -> set:
    """Item tag localnames for a spec (mirrors the GUI helper)."""
    names = set()
    for p in SPEC.get(spec_name, {}).get("item_paths", []):
        last = p.split("/")[-1].strip(".")
        if last:
            names.add(strip_ns(last).lower())
    return names


def _all_item_tags() -> set:
    tags = {"item", "entry", "offer", "product"}
    for spec_name in SPEC:
        tags |= _localnames_from_item_paths(spec_name)
    return tags


def _detect_spec_stream(path: str) -> Tuple[str, int, bool]:
    """Return (spec, item-like candidates seen, candidate count is exact)."""
    item_tags = _all_item_tags()
    with _open_bounded_xml(path) as fh:
        ctx = ET.iterparse(fh, events=("start", "end"))
        _event, root = next(ctx)
        initial = detect_spec(root) or "UNKNOWN"
        if initial.upper() != "UNKNOWN":
            return initial, 0, False

        root_tag = root.tag
        root_attrib = dict(root.attrib)
        root_local = (
            strip_ns(root.tag).lower()
            if isinstance(root.tag, str)
            else ""
        )
        open_item = 1 if root_local in item_tags else 0
        candidate_nodes = 1 if open_item else 0
        if open_item:
            fh.start_item()
        candidates = 0
        stack = [root]
        for event, elem in ctx:
            local = (
                strip_ns(elem.tag).lower()
                if isinstance(elem.tag, str)
                else ""
            )
            if event == "start":
                stack.append(elem)
                if open_item == 0 and local in item_tags:
                    candidate_nodes = 1
                    fh.start_item()
                elif open_item > 0:
                    candidate_nodes += 1
                    if candidate_nodes > MAX_ITEM_NODES:
                        raise FeedParseLimitError(
                            "A single product exceeds the "
                            f"{MAX_ITEM_NODES:,}-element interactive item limit."
                        )
                if local in item_tags:
                    open_item += 1
                continue

            parent = stack[-2] if len(stack) > 1 else None
            if local not in item_tags:
                if open_item == 0 and elem is not root:
                    elem.clear()
                    if parent is not None:
                        parent.remove(elem)
                stack.pop()
                continue

            # A product can legitimately contain another item-like tag (for
            # example Compari <product><offer>…). Only the outer candidate has
            # the complete product fields and should consume detection budget.
            if open_item > 1:
                open_item -= 1
                stack.pop()
                continue

            candidates += 1
            fh.end_item()
            if elem is root:
                probe = elem
            else:
                probe = StdET.Element(root_tag, root_attrib)
                probe.append(elem)
            spec = detect_spec(probe) or "UNKNOWN"
            if spec.upper() != "UNKNOWN":
                return spec, candidates, False
            elem.clear()
            if parent is not None:
                parent.remove(elem)
            open_item = max(0, open_item - 1)
            candidate_nodes = 0
            stack.pop()
            if candidates >= 5:
                return "UNKNOWN", candidates, False
    return "UNKNOWN", candidates, True


def _iter_items_stream(file_like, wanted_localnames: Iterable[str]):
    """Yield item elements via iterparse, clearing each after it is consumed so
    the full document never accumulates in RAM (same guard as the GUI)."""
    want = set(wanted_localnames)
    ctx = ET.iterparse(file_like, events=("start", "end"))
    _event, root = next(ctx)
    root_local = (
        strip_ns(root.tag).lower()
        if isinstance(root.tag, str)
        else ""
    )
    open_wanted = 1 if root_local in want else 0
    item_nodes = 1 if open_wanted else 0
    if open_wanted and hasattr(file_like, "start_item"):
        file_like.start_item()
    stack = [root]
    for event, elem in ctx:
        ln = strip_ns(elem.tag).lower() if isinstance(elem.tag, str) else ""
        if event == "start":
            stack.append(elem)
            if open_wanted == 0 and ln in want:
                item_nodes = 1
                if hasattr(file_like, "start_item"):
                    file_like.start_item()
            elif open_wanted > 0:
                item_nodes += 1
                if item_nodes > MAX_ITEM_NODES:
                    raise FeedParseLimitError(
                        "A single product exceeds the "
                        f"{MAX_ITEM_NODES:,}-element interactive item limit."
                    )
            if ln in want:
                open_wanted += 1
            continue
        parent = stack[-2] if len(stack) > 1 else None
        if ln in want:
            open_wanted -= 1
            if open_wanted > 0:
                stack.pop()
                continue
            if hasattr(file_like, "end_item"):
                file_like.end_item()
            if parent is not None:
                parent.remove(elem)
            yield elem
            elem.clear()
            item_nodes = 0
        elif open_wanted == 0:
            elem.clear()
            if parent is not None:
                parent.remove(elem)
        stack.pop()


_CAT_SPLIT_RE = re.compile(r"[>|/]")


def _add_row(table: FeedTable, elem, spec: str, interns: Dict[str, dict],
             index_params: bool) -> None:
    """Extract one item's filterable fields into the columnar table."""
    cols = table.columns

    def _intern(field: str, value: str) -> str:
        # Share one string object per distinct value in low-cardinality columns.
        cache = interns[field]
        got = cache.get(value)
        if got is None:
            cache[value] = value
            return value
        return got

    try:
        amt, _raw = read_price(elem, spec)
    except Exception:
        amt = None
    pid = (read_id(elem, spec) or "").strip()
    purl = (read_link(elem, spec) or "").strip()
    title = (read_recommended_value(elem, "title") or "").strip()
    avail = (read_availability(elem, spec) or "").strip()
    brand = (read_recommended_value(elem, "brand") or "").strip()
    cat = (read_recommended_value(elem, "category") or "").strip()
    desc = (read_recommended_value(elem, "description") or "").strip()
    ean = (read_recommended_value(elem, "gtin") or "").strip()
    primary_url = (gather_primary_image(
        elem, spec, do_percent_encode=False
    ) or "").strip()
    primary = bool(primary_url)
    try:
        gallery = gather_gallery(elem, spec, do_percent_encode=False)
    except Exception:
        gallery = []
    image_count = len(dict.fromkeys(
        url for url in [primary_url, *gallery] if url
    ))
    depth = len([p for p in _CAT_SPLIT_RE.split(cat) if p.strip()]) if cat else 0

    cols["id"].append(pid)
    cols["url"].append(purl)
    cols["title"].append(title)
    cols["availability"].append(_intern("availability", avail))
    cols["brand"].append(_intern("brand", brand))
    cols["category"].append(_intern("category", cat))
    cols["price"].append(amt)
    cols["category_depth"].append(depth)
    cols["title_length"].append(len(title))
    cols["description_length"].append(len(desc))
    cols["image_count"].append(image_count)
    cols["has_image"].append(primary)
    cols["has_ean"].append(bool(ean) and is_valid_gtin(ean))
    cols["price_valid"].append(amt is not None and amt > 0)
    cols["has_description"].append(bool(desc))
    cols["has_brand"].append(bool(brand))
    cols["has_category"].append(bool(cat))
    if index_params:
        params = _named_param_values(elem)  # {name_lower: value}
        cols["param"].append({k: _intern("param", v) for k, v in params.items()})
    else:
        cols["param"].append(None)


def extract(src_path: str, cap: int = DEFAULT_ITEM_CAP,
            index_params: bool = False) -> FeedTable:
    """Parse a feed into a capped columnar `FeedTable`.

    All inputs use the same bounded streaming path. Parsing stops after one
    item beyond the row cap, which marks the snapshot as truncated without
    scanning an arbitrarily large feed. When `index_params` is set, each row
    also carries its named product parameters (more memory).
    """
    interns: Dict[str, dict] = {"availability": {}, "brand": {}, "category": {}, "param": {}}

    spec, detection_items, detection_exact = _detect_spec_stream(src_path)
    table = FeedTable(spec, index_params)
    if spec == "UNKNOWN":
        # Detection already counted enough item-like candidates to explain the
        # failure; avoid a second full pass over an unrecognized feed.
        table.total_seen = detection_items
        table.total_exact = detection_exact
        return table

    item_tags = _localnames_from_item_paths(spec) or {"item", "entry", "offer"}
    with _open_bounded_xml(src_path) as fh:
        for elem in _iter_items_stream(fh, item_tags):
            table.total_seen += 1
            if table.total_seen > cap:
                table.total_exact = False
                break
            _add_row(table, elem, spec, interns, index_params)
    return table


# --------------------------------------------------------------------------- #
# Filtering
# --------------------------------------------------------------------------- #
def _to_float(s: str) -> Optional[float]:
    try:
        value = float(str(s).strip().replace(",", "."))
    except (TypeError, ValueError):
        return None
    return value if math.isfinite(value) else None


def _list_values(raw_value: Any) -> List[str]:
    """Normalise native multiselect values and legacy comma-separated text."""
    if isinstance(raw_value, (list, tuple)):
        values = raw_value
    elif isinstance(raw_value, (set, frozenset)):
        values = sorted(raw_value, key=lambda value: str(value).casefold())
    else:
        values = str(raw_value if raw_value is not None else "").split(",")
    return [
        str(value).strip()
        for value in values
        if value is not None and str(value).strip()
    ]


def _effective_op(field: str, op: str) -> str:
    """Map the category-friendly labels to their legacy exact operators."""
    if field == "category":
        if op == "one of":
            return "in list"
        if op == "not one of":
            return "not in list"
    return op


def rule_error(
    rule: Dict[str, Any],
    table: Optional[FeedTable] = None,
) -> Optional[str]:
    """Return why a rule is incomplete/invalid, or None when it is usable."""
    if not isinstance(rule, dict):
        return "Choose a field."
    key = str(rule.get("field", ""))
    typ = FIELD_TYPE.get(key)
    if typ is None:
        return "Choose a field."

    shown_op = str(rule.get("op", ""))
    op = _effective_op(key, shown_op)
    if op not in OPS_BY_TYPE[typ]:
        return "Choose a valid condition."

    if typ == "param":
        if table is not None and not table.index_params:
            return "Enable product-parameter indexing for this rule."
        param_name = rule.get("value2", "")
        if param_name is None or not str(param_name).strip():
            return "Enter the product parameter name."

    if typ == "bool" or op in ("is empty", "is not empty"):
        return None

    raw_val = rule.get("value", "")
    if raw_val is None:
        raw_val = ""
    if typ == "number":
        low = _to_float(raw_val)
        if low is None:
            return "Enter a number."
        if op == "between":
            high = _to_float(rule.get("value2", ""))
            if high is None:
                return "Enter both ends of the range."
            if low > high:
                return "Minimum cannot be greater than maximum."
        return None

    if op in ("in list", "not in list"):
        values = _list_values(raw_val)
        if not values:
            if key == "category" and shown_op in ("one of", "not one of"):
                return "Select at least one category."
            return "Enter at least one comma-separated value."
        return None

    if not str(raw_val).strip():
        return "Enter a value."
    return None


def valid_rules(
    rules: List[Dict[str, Any]],
    table: Optional[FeedTable] = None,
) -> List[Dict[str, Any]]:
    return [rule for rule in rules if rule_error(rule, table) is None]


def rule_mask(table: FeedTable, rule: Dict[str, Any]) -> List[bool]:
    """Boolean mask (length n) of rows this single rule matches. An
    unparseable/invalid rule matches nothing (returns all-False)."""
    if not isinstance(rule, dict):
        return [False] * table.n
    key = rule.get("field", "")
    op = _effective_op(str(key), str(rule.get("op", "")))
    raw_val = rule.get("value", "")
    raw_val2 = rule.get("value2", "")
    if raw_val is None:
        raw_val = ""
    if raw_val2 is None:
        raw_val2 = ""
    n = table.n
    col = table.columns.get(key)
    if col is None or rule_error(rule, table) is not None:
        return [False] * n
    typ = FIELD_TYPE.get(key, "text")

    if typ == "bool":
        if op not in BOOL_OPS:
            return [False] * n
        want = op == "is true"
        return [bool(col[i]) is want for i in range(n)]

    if typ == "param":
        # `col[i]` is {name_lower: value} (or None). The parameter name lives in
        # value2; the match text (for the value-taking ops) in value.
        name = str(raw_val2).strip().lower()
        if not name:
            return [False] * n

        def _pvs(i: int) -> List[str]:
            d = col[i]
            joined = (d.get(name) or "") if d else ""
            return [
                part.strip().casefold()
                for part in joined.split(" | ")
                if part.strip()
            ]

        if op == "is empty":
            return [not _pvs(i) for i in range(n)]
        if op == "is not empty":
            return [bool(_pvs(i)) for i in range(n)]
        if op in ("in list", "not in list"):
            wanted = {value.casefold() for value in _list_values(raw_val)}
            inside = [any(value in wanted for value in _pvs(i)) for i in range(n)]
            return inside if op == "in list" else [not x for x in inside]
        needle = str(raw_val).strip().casefold()
        if op == "contains":
            return [any(needle in value for value in _pvs(i)) for i in range(n)]
        if op == "not contains":
            return [all(needle not in value for value in _pvs(i)) for i in range(n)]
        if op == "equals":
            return [any(value == needle for value in _pvs(i)) for i in range(n)]
        if op == "not equals":
            return [all(value != needle for value in _pvs(i)) for i in range(n)]
        if op == "starts with":
            return [any(value.startswith(needle) for value in _pvs(i)) for i in range(n)]
        return [False] * n

    if typ == "number":
        def _empty(i: int) -> bool:
            value = col[i]
            return value is None or (
                key in ZERO_IS_EMPTY_FIELDS and value == 0
            )

        if op == "is empty":
            return [_empty(i) for i in range(n)]
        if op == "is not empty":
            return [not _empty(i) for i in range(n)]
        thr = _to_float(raw_val)
        if op == "between":
            hi = _to_float(raw_val2)
            if thr is None or hi is None:
                return [False] * n
            lo, hi = min(thr, hi), max(thr, hi)
            return [col[i] is not None and lo <= col[i] <= hi for i in range(n)]
        if thr is None:
            return [False] * n
        cmp = {
            "<":  lambda v: v <  thr,
            "<=": lambda v: v <= thr,
            ">":  lambda v: v >  thr,
            ">=": lambda v: v >= thr,
            "==": lambda v: v == thr,
            "!=": lambda v: v != thr,
        }.get(op)
        if cmp is None:
            return [False] * n
        return [col[i] is not None and cmp(col[i]) for i in range(n)]

    # text
    needle = str(raw_val).strip().casefold()
    if op == "is empty":
        return [not (col[i] or "").strip() for i in range(n)]
    if op == "is not empty":
        return [bool((col[i] or "").strip()) for i in range(n)]
    if op in ("in list", "not in list"):
        wanted = {value.casefold() for value in _list_values(raw_val)}
        inside = [
            (col[i] or "").strip().casefold() in wanted
            for i in range(n)
        ]
        return inside if op == "in list" else [not x for x in inside]
    pred = {
        "contains":     lambda v: needle in v,
        "not contains": lambda v: needle not in v,
        "equals":       lambda v: v == needle,
        "not equals":   lambda v: v != needle,
        "starts with":  lambda v: v.startswith(needle),
    }.get(op)
    if pred is None:
        return [False] * n
    return [pred((col[i] or "").strip().casefold()) for i in range(n)]


def apply_rules(
    table: FeedTable,
    rules: List[Dict[str, Any]],
    combine: str = "AND",
    mode: str = "keep",
) -> Dict[str, Any]:
    """Evaluate `rules` against the table.

    combine: "AND" / "OR" across rules.
    mode:    "keep"   -> resulting feed = rows that MATCH the rules
             "remove" -> resulting feed = rows that do NOT match the rules
    Returns counts plus a `keep_mask` and per-rule match counts.
    """
    if combine not in ("AND", "OR"):
        raise ValueError("combine must be 'AND' or 'OR'")
    if mode not in ("keep", "remove"):
        raise ValueError("mode must be 'keep' or 'remove'")

    n = table.n
    errors = [rule_error(rule, table) for rule in rules]
    active = [
        rule for rule, error in zip(rules, errors)
        if error is None
    ]

    if not active:
        match = [False] * n
        keep_mask = [True] * n
        masks: List[List[bool]] = []
    else:
        masks = [rule_mask(table, r) for r in active]
        if combine == "OR":
            match = [any(m[i] for m in masks) for i in range(n)]
        else:
            match = [all(m[i] for m in masks) for i in range(n)]
        keep_mask = match if mode == "keep" else [not x for x in match]

    matched = sum(match)
    kept = sum(keep_mask)

    per_rule = [
        {"rule": rule, "matched": sum(mask)}
        for rule, mask in zip(active, masks)
    ]
    return {
        "n": n,
        "total_seen": table.total_seen,
        "total_exact": table.total_exact,
        "truncated": table.truncated,
        "matched": matched,
        "kept": kept,
        "removed": n - kept,
        "keep_mask": keep_mask,
        "per_rule": per_rule,
        "active_rule_count": len(active),
        "incomplete_rule_count": len(rules) - len(active),
        "rule_errors": errors,
    }


def _validate_filter_enums(combine: str, mode: str, label: str) -> None:
    if combine not in ("AND", "OR"):
        raise ValueError(f"{label} must be 'AND' or 'OR'")
    if mode not in ("keep", "remove"):
        raise ValueError("mode must be 'keep' or 'remove'")


def _group_parts(
    group: Dict[str, Any],
    group_index: int,
) -> Tuple[str, List[Dict[str, Any]]]:
    if not isinstance(group, dict):
        raise ValueError(f"group {group_index + 1} must be an object")
    combine = group.get("combine", "AND")
    if combine not in ("AND", "OR"):
        raise ValueError(
            f"group {group_index + 1} combine must be 'AND' or 'OR'"
        )
    rules = group.get("rules", [])
    if rules is None:
        rules = []
    if not isinstance(rules, list):
        raise ValueError(f"group {group_index + 1} rules must be a list")
    return combine, rules


def apply_rule_groups(
    table: FeedTable,
    groups: List[Dict[str, Any]],
    group_combine: str = "AND",
    mode: str = "keep",
) -> Dict[str, Any]:
    """Evaluate one level of parenthesised rule groups.

    Each group has ``{"combine": "AND"|"OR", "rules": [...]}``; active
    group results are then joined by ``group_combine``. Invalid/incomplete
    rules and groups with no active rules are ignored for preview purposes.
    If nothing is active, every row is kept even in remove mode.
    """
    _validate_filter_enums(group_combine, mode, "group_combine")
    if not isinstance(groups, list):
        raise ValueError("groups must be a list")

    n = table.n
    root_match: Optional[List[bool]] = None
    per_group: List[Dict[str, Any]] = []
    group_diagnostics: List[Dict[str, Any]] = []
    per_rule: List[Dict[str, Any]] = []
    rule_errors: List[Optional[str]] = []
    active_rule_count = 0
    active_group_count = 0
    total_rule_count = 0

    for group_index, group in enumerate(groups):
        combine, rules = _group_parts(group, group_index)
        errors = [rule_error(rule, table) for rule in rules]
        rule_errors.extend(errors)
        total_rule_count += len(rules)
        active_count = 0
        group_mask: Optional[List[bool]] = None
        group_per_rule: List[Dict[str, Any]] = []
        for rule, error in zip(rules, errors):
            if error is not None:
                continue
            mask = rule_mask(table, rule)
            active_count += 1
            summary = {
                "group_index": group_index,
                "rule": rule,
                "matched": sum(mask),
            }
            group_per_rule.append(summary)
            per_rule.append(summary)
            if group_mask is None:
                group_mask = mask
            elif combine == "OR":
                for row_index, matched in enumerate(mask):
                    if matched:
                        group_mask[row_index] = True
            else:
                for row_index, matched in enumerate(mask):
                    if not matched:
                        group_mask[row_index] = False

        active_rule_count += active_count
        incomplete_count = len(rules) - active_count

        if group_mask is not None:
            matched = sum(group_mask)
            active_group_count += 1
            active_summary = {
                "group_index": group_index,
                "combine": combine,
                "matched": matched,
                "active_rule_count": active_count,
                "incomplete_rule_count": incomplete_count,
                "per_rule": group_per_rule,
            }
            per_group.append(active_summary)
            if root_match is None:
                root_match = group_mask
            elif group_combine == "OR":
                for row_index, group_matched in enumerate(group_mask):
                    if group_matched:
                        root_match[row_index] = True
            else:
                for row_index, group_matched in enumerate(group_mask):
                    if not group_matched:
                        root_match[row_index] = False
        else:
            matched = 0

        group_diagnostics.append({
            "group_index": group_index,
            "combine": combine,
            "active": group_mask is not None,
            "matched": matched,
            "active_rule_count": active_count,
            "incomplete_rule_count": incomplete_count,
            "rule_errors": errors,
            "per_rule": group_per_rule,
        })

    if root_match is None:
        match = [False] * n
        keep_mask = [True] * n
    else:
        match = root_match
        keep_mask = match if mode == "keep" else [not value for value in match]

    matched = sum(match)
    kept = sum(keep_mask)
    return {
        "n": n,
        "total_seen": table.total_seen,
        "total_exact": table.total_exact,
        "truncated": table.truncated,
        "matched": matched,
        "kept": kept,
        "removed": n - kept,
        "keep_mask": keep_mask,
        "per_rule": per_rule,
        "per_group": per_group,
        "group_diagnostics": group_diagnostics,
        "active_group_count": active_group_count,
        "active_rule_count": active_rule_count,
        "incomplete_rule_count": total_rule_count - active_rule_count,
        "rule_errors": rule_errors,
    }


# --------------------------------------------------------------------------- #
# Human- and machine-readable rule summaries (the "tell Raul exactly what to
# do" output an AM hands back).
# --------------------------------------------------------------------------- #
def rule_text(rule: Dict[str, Any]) -> str:
    op = rule.get("op", "?")
    if rule.get("field") == "param":
        name = rule.get("value2", "") or "?"
        if op in ("is empty", "is not empty"):
            return f"param '{name}' {op}"
        return f"param '{name}' {op} {rule.get('value','')}".rstrip()
    field = FIELD_LABEL.get(rule.get("field", ""), rule.get("field", "?"))
    if op in ("is empty", "is not empty", "is true", "is false"):
        return f"{field} {op}"
    if op == "between":
        return f"{field} between {rule.get('value','?')} and {rule.get('value2','?')}"
    value = rule.get("value", "")
    if isinstance(value, (list, tuple, set, frozenset)):
        value = ", ".join(_list_values(value))
    shown_op = {
        "one of": "is one of",
        "not one of": "is not one of",
    }.get(op, op)
    return f"{field} {shown_op} {value}".rstrip()


def describe(
    rules: List[Dict[str, Any]],
    combine: str,
    mode: str,
    result: Optional[Dict[str, Any]] = None,
    table: Optional[FeedTable] = None,
) -> str:
    active = valid_rules(rules, table)
    if not active:
        body = "no filters (all products kept)"
    else:
        joined = f" {combine} ".join(rule_text(r) for r in active)
        verb = "KEEP only" if mode == "keep" else "REMOVE"
        body = f"{verb} products where {joined}"
    if result:
        pct = (result["removed"] / result["n"] * 100) if result["n"] else 0
        scope = " (sample of first %d items)" % result["n"] if result["truncated"] else ""
        body += (
            f"\n→ {result['kept']:,} of {result['n']:,} products remain, "
            f"{result['removed']:,} removed ({pct:.1f}%){scope}"
        )
    return body


def to_spec(
    rules: List[Dict[str, Any]],
    combine: str,
    mode: str,
    table: Optional[FeedTable] = None,
) -> Dict[str, Any]:
    """Machine-readable filter spec (JSON-serialisable) to reproduce server-side."""
    if combine not in ("AND", "OR"):
        raise ValueError("combine must be 'AND' or 'OR'")
    if mode not in ("keep", "remove"):
        raise ValueError("mode must be 'keep' or 'remove'")
    return {
        "combine": combine,
        "mode": mode,
        "rules": [_serialise_rule(rule) for rule in valid_rules(rules, table)],
    }


def describe_rule_groups(
    groups: List[Dict[str, Any]],
    group_combine: str,
    mode: str,
    result: Optional[Dict[str, Any]] = None,
    table: Optional[FeedTable] = None,
) -> str:
    """Human-readable v2 summary with explicit group parentheses."""
    _validate_filter_enums(group_combine, mode, "group_combine")
    if not isinstance(groups, list):
        raise ValueError("groups must be a list")

    group_texts: List[str] = []
    for group_index, group in enumerate(groups):
        combine, rules = _group_parts(group, group_index)
        active = valid_rules(rules, table)
        if active:
            joined = f" {combine} ".join(rule_text(rule) for rule in active)
            group_texts.append(f"({joined})")

    if not group_texts:
        body = "no filters (all products kept)"
    else:
        joined_groups = f" {group_combine} ".join(group_texts)
        verb = "KEEP only" if mode == "keep" else "REMOVE"
        body = f"{verb} products where {joined_groups}"

    if result:
        pct = (result["removed"] / result["n"] * 100) if result["n"] else 0
        scope = (
            " (sample of first %d items)" % result["n"]
            if result["truncated"]
            else ""
        )
        body += (
            f"\n→ {result['kept']:,} of {result['n']:,} products remain, "
            f"{result['removed']:,} removed ({pct:.1f}%){scope}"
        )
    return body


def to_group_spec(
    groups: List[Dict[str, Any]],
    group_combine: str,
    mode: str,
    table: Optional[FeedTable] = None,
) -> Dict[str, Any]:
    """JSON-serialisable v2 spec preserving one level of rule grouping."""
    _validate_filter_enums(group_combine, mode, "group_combine")
    if not isinstance(groups, list):
        raise ValueError("groups must be a list")

    serialised_groups = []
    for group_index, group in enumerate(groups):
        combine, rules = _group_parts(group, group_index)
        serialised_rules = [
            _serialise_rule(rule)
            for rule in valid_rules(rules, table)
        ]
        if serialised_rules:
            serialised_groups.append({
                "combine": combine,
                "rules": serialised_rules,
            })

    return {
        "version": 2,
        "groupCombine": group_combine,
        "mode": mode,
        "groups": serialised_groups,
    }


def _serialise_rule(rule: Dict[str, Any]) -> Dict[str, Any]:
    """Copy a valid rule into its stable, JSON-safe hand-off shape."""
    serialised: Dict[str, Any] = {}
    for key in ("field", "op", "value", "value2"):
        value = rule.get(key)
        if value is None or value == "":
            continue
        if key == "value" and isinstance(
            value,
            (list, tuple, set, frozenset),
        ):
            # Native multiselects use lists, while engine callers may use any
            # of the collection types accepted by `_list_values`. Normalising
            # them here keeps exported specs deterministic and JSON-safe.
            value = _list_values(value)
        serialised[key] = value
    return serialised


def _column_value(table: FeedTable, field: str, index: int) -> Any:
    column = table.columns.get(field, [])
    return column[index] if index < len(column) else None


def _browse_categories(categories: Optional[Iterable[str]]) -> List[str]:
    if categories is None:
        return []
    if isinstance(categories, str):
        values: Iterable[Any] = [categories]
    else:
        values = categories
    return [
        str(value).strip()
        for value in values
        if value is not None and str(value).strip()
    ]


def browse_mask(
    table: FeedTable,
    query: str = "",
    categories: Optional[Iterable[str]] = None,
    base_mask: Optional[Iterable[bool]] = None,
) -> List[bool]:
    """Return a literal, case-insensitive product-browser mask.

    Whitespace-separated query tokens use AND semantics; each token may occur
    in any of ``BROWSE_SEARCH_FIELDS``. Category selections are exact
    case-insensitive matches. ``base_mask`` can scope the search to an existing
    result or exclusion mask.
    """
    n = table.n
    if base_mask is None:
        scoped = [True] * n
    else:
        scoped = [bool(value) for value in base_mask]
        if len(scoped) != n:
            raise ValueError("base_mask length must equal the loaded row count")

    tokens = [
        token.casefold()
        for token in str(query if query is not None else "").split()
        if token
    ]
    wanted_categories = {
        category.casefold()
        for category in _browse_categories(categories)
    }

    # The product browser is rendered inside a collapsed Streamlit expander,
    # whose body still executes on every rerun. Avoid rebuilding six-field
    # search haystacks when no text search is active.
    if not tokens and not wanted_categories:
        return scoped

    mask: List[bool] = []
    for index in range(n):
        if not scoped[index]:
            mask.append(False)
            continue
        category = str(
            _column_value(table, "category", index) or ""
        ).strip().casefold()
        if wanted_categories and category not in wanted_categories:
            mask.append(False)
            continue
        if not tokens:
            mask.append(True)
            continue
        haystack = "\n".join(
            str(_column_value(table, field, index) or "")
            for field in BROWSE_SEARCH_FIELDS
        ).casefold()
        mask.append(all(token in haystack for token in tokens))
    return mask


def browse_rows(
    table: FeedTable,
    query: str = "",
    categories: Optional[Iterable[str]] = None,
    base_mask: Optional[Iterable[bool]] = None,
    limit: int = 50,
) -> Dict[str, Any]:
    """Return browser matches and at most ``limit`` lightweight row dicts."""
    if not isinstance(limit, int) or isinstance(limit, bool) or limit < 0:
        raise ValueError("limit must be a non-negative integer")
    mask = browse_mask(table, query, categories, base_mask)
    matched_count = 0
    shown_indices: List[int] = []
    for index, matched in enumerate(mask):
        if not matched:
            continue
        matched_count += 1
        if len(shown_indices) < limit:
            shown_indices.append(index)
    rows = [
        {
            field: _column_value(table, field, index)
            for field in BROWSE_ROW_FIELDS
        }
        for index in shown_indices
    ]
    return {
        "matched": matched_count,
        "shown": len(rows),
        "truncated": matched_count > len(rows),
        "indices": shown_indices,
        "mask": mask,
        "rows": rows,
    }


def ids_csv(values: Iterable[str]) -> str:
    """Return an exact one-column CSV, rejecting spreadsheet formula cells."""
    rows = [str(value) for value in values]
    unsafe = [
        value
        for value in rows
        if value.lstrip().startswith(("=", "+", "-", "@"))
    ]
    if unsafe:
        raise ValueError(
            "ID export contains spreadsheet formula characters; use the "
            "filter specification instead."
        )
    output = io.StringIO(newline="")
    writer = csv.writer(output, lineterminator="\n", quoting=csv.QUOTE_ALL)
    writer.writerow(["id"])
    writer.writerows([value] for value in rows)
    return output.getvalue()
