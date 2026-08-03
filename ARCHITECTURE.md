# FAVI Feed Tools — architecture

A Streamlit app that lets Account Managers **validate** a product feed and
**browse/filter** it from one load, plus a pure-Python filtering engine destined
for Cloud Run. Deployed on **Streamlit Community Cloud (~1 GB RAM/app)** — memory
discipline (columnar + capped) is deliberate, not incidental.

## Module map

| File | Role |
|---|---|
| `feed_checker_gui.py` | Entry point / `st.navigation` router. Calls `st.set_page_config` **once** (before navigation), then mounts the two pages. |
| `checker_page.py` | **Unified page** (default). Opens check-only at 1200px: the load form with validation beneath it. Switching on the **Browse & filter panel** splits the page into two `st.columns` — **left** "① Load & check", **right** "② Browse & filter" (empty-state hint until a feed loads, then the shared filter UI) — at full window width, off ONE feed load. See "The Browse half is opt-in" below. Owns the validator: parse (DOM ≤30 MB / streaming), ~30 accumulators, `render_validation()`, and the ClickUp draft. A failed submit never `st.stop()`s — the error renders under the form so an already-loaded feed's panels stay on screen. |
| `filter_page.py` | Thin standalone "Feed Filter" page: its own loader (SSRF-safe download, gate, parse → `ff_table`) then `filter_view.render_filter()`. Kept as a page so `switch_page("filter_page.py")` in the tests still resolves. |
| `filter_view.py` | **Shared** rule builder → live counts → browse/preview → hand-off exports, wrapped in `@st.fragment render_filter()`. Reads `st.session_state["ff_table"]` + feed identity. Both the checker's Browse column and `filter_page.py` call it. |
| `feed_filter.py` | Pure engine (no Streamlit): `extract()` → capped columnar `FeedTable`; `apply_rule_groups()`, `category_facets()`, `browse_mask()`, `describe_rule_groups()`, `to_group_spec()`; SSRF helpers (`public_url_ips`, `assert_public_url`) and parse-size limits. The piece that moves to Cloud Run. |
| `safe_http.py` | `public_session()` → requests session with a `PinnedPublicAdapter` (resolve once, reject private/reserved IPs, pin the connection to that IP, verify the original TLS hostname, ignore env proxies). SSRF defense for feed downloads. |
| `feed_specs.py` | Feed-format detection + field readers (Heureka, Google, Ceneo, Compari, …). Shared by validator and engine so extracted values always agree with the checker. |
| `branding.py` | FAVI look-and-feel: `inject_css`, `page_header`, `render_metric_row`. |

## Why one page needs `st.fragment`

Streamlit re-runs the whole script on every interaction. The validator was
historically **single-shot** (`if not submitted: st.stop()`), so its results only
existed on the run right after submit — any later widget wiped them. Live
filtering is *nonstop* interaction, which is why it started as a separate page.

The unified page fixes this with two moves:
1. **Persist the load.** On submit the feed is stored in
   `st.session_state["loaded_feed"]`; a gate stops only when nothing is loaded.
   So any full re-run still shows results (this also fixes the old ClickUp-draft
   ephemerality).
2. **Isolate filtering.** `render_filter()` is an `@st.fragment`, so filter
   interactions re-run **only that fragment** — the validation view is never
   re-executed or blanked. Validation re-runs only on a genuine full re-run
   (new load, param-index toggle, ClickUp edit).

The Browse column is rendered **before** Validation in code so a fatal parse
error in validation (which still `st.stop()`s) cannot blank the Browse panel.
Feed-**load** failures (bad URL, download error) never `st.stop()` at all.

## The Browse half is opt-in

`st.columns` fixes its widths when it is created, so an expander *inside* the
right column would only collapse vertically — validation would stay stranded at
half width. Reclaiming the space means deciding the split up front, so the
`show_browse` toggle (`key="checker_show_browse"`, default **off** — checking is
the common case) renders **above** the columns and drives three things:

- the layout: `st.columns(2)` when on, a single `st.container()` when off;
- `.block-container` `max-width`: `100%` side by side, otherwise branding's
  `1200px` (full width alone would sprawl on a large monitor — the point of
  check-only is a readable measure on a laptop);
- whether the Browse block runs at all — hidden skips
  `_prepare_browse_table()`, so a check-only run never pays for the second parse.

The feed is therefore parsed for browsing **lazily, on first open**, not at load:
`ff_table` is absent while the panel is hidden. Measured on a 10 MB / 30k item
feed, the first open costs ~1.2 s over a steady-state rerun (3.1 s vs 1.9 s); it
is wrapped in a spinner because that grows with feed size. `ff_table` /
`ff_table_signature` are plain keys, so hiding the panel again keeps the parsed
table and every later reopen is free. `checker_index_params_pref` mirrors the
"index product parameters" checkbox for the same reason the toggle needs
`checker_browse_pref` — losing that widget key would both revert the AM's setting
and change the signature, re-parsing the whole feed for nothing.

Two consequences of the panel being off by default:

- **The choice must be remembered explicitly.** Streamlit drops widget state for
  widgets the current page didn't render, so after a hop to the Feed Filter page
  the toggle would re-default and silently close the panel. `show_browse` is
  mirrored into `checker_browse_pref`, a plain (non-widget) key that survives a
  page switch, and that is what feeds the toggle's `value=`.
- **`_sync_filter_feed()` runs on the load path, not in the panel.** It used to
  live in `_prepare_browse_table()`; with the panel hidden by default that let
  feed A's `ff_*` state (and the `ff_table` the Feed Filter page reads) survive a
  switch to feed B. It only clears keys — no parse — so calling it on every load
  is cheap. `test_new_feed_on_checker_clears_stale_filter_state` covers both
  panel states.

`loaded_feed` and `ff_rules` are plain session-state keys, so the loaded feed and
the rule list survive a collapse, and re-opening re-parses via the signature check.

## Sticky rule state

The rule rows are built from pure widget keys (`ff_field_N`, `ff_op_N_*`,
`ff_cat_values_N`, `ff_val_N`, `ff_pname_N`), and Streamlit discards widget state
for widgets a run didn't render. Closing the Browse panel (or switching page) used
to drop them while `ff_rules` still listed the rules — so the rows came back
**empty** and the AM's typed values were silently gone.

`filter_view` mirrors those keys into `ff_sticky_rule_state`, one plain key that
survives, and refills **only keys that are missing**. That last part is the whole
safety property: overwriting present keys would revert every edit made after a
reopen and make clearing a value back to empty impossible. Two consequences:

- Rule ids are never reused, so `_is_sticky()` scoping the mirror to *live* rule
  ids is what keeps a deleted rule from being resurrected.
- Anything that clears rule state for a new feed **must** call
  `forget_sticky_state()` first, or the previous feed's values come straight back.
  `checker_page._sync_filter_feed()` gets this free from its blanket `ff_*` wipe;
  `filter_page._set_feed()` calls it explicitly, before its own targeted pops.

## Feed loading & parsing

- One download per load. Uploads → temp file; URLs → temp file **over
  `safe_http.public_session()`** (SSRF-safe: pinned public IP, redirect hops
  re-validated). Temp files use `delete=False` (they must outlive the request;
  `filter_page.py` also runs a flock-leased temp-file janitor).
- On a **feed change** (new content hash), the checker's `_sync_filter_feed()`
  wipes all `ff_*` rule/browse state so rules/category multiselects built against
  the previous feed can't carry over (a stale category would crash the Browse
  multiselect), then `_prepare_browse_table()` sets the full, consistent feed
  identity (`ff_signature`/`ff_src_path`/`ff_content_hash`=real sha256/…).
- **Two parses of the same file**: the validator's own parse (accumulators) and
  `feed_filter.extract()` (columnar `FeedTable`, cached by content signature in
  `st.session_state["ff_table"]`). One-time cost on load; filter interactions
  hit the cached table.
- Memory: `FeedTable` is columnar + interned + capped (`DEFAULT_ITEM_CAP`), so a
  full snapshot is well under the ~1 GB host budget. Counts are exact within the
  snapshot; beyond the cap the UI says it's a sample.

## Key `st.session_state` keys

- `loaded_feed` — the checker's persisted feed `{path,label,size,scope,n_limit,stop_on_first_parse_error}`.
- `ff_table` / `ff_table_signature` — the parsed `FeedTable` and its content signature (drives re-parse).
- `ff_src_label` / `ff_content_hash` — feed identity for the hand-off export snapshot.
- `ff_index_params_cb` — "index product parameters" toggle (changes the signature → re-parse).
- `ff_rules` / `ff_group_ids` / `ff_groups_combine` / `ff_mode` … — rule-builder state (owned by `render_filter`).
- `shared_feed_path` / `shared_feed_label` / `shared_feed_size` — cross-page hand-off so the standalone Feed Filter page can reuse a checked feed.

## Tests & CI

- `python -m unittest discover` (GitHub Actions `.github/workflows/ci.yml`, Python 3.12).
- `test_filter_app.py` drives the real app via `streamlit.testing.v1.AppTest`:
  boot `feed_checker_gui.py`, then either `switch_page("filter_page.py")` or inject
  `loaded_feed` to exercise the checker's two panels. Fragment reruns work under AppTest.
- `test_feed_filter.py` (engine), `test_safe_http.py` (SSRF adapter), `test_feed_specs.py`.

## Known follow-ups

- **Validation re-parses on every full re-run** (e.g. editing the ClickUp draft or
  toggling the param-index checkbox re-runs the whole script, since those widgets
  live outside the Browse `@st.fragment`). Correct output, but a repeated multi-
  second stall on very large feeds. Fix: memoize the parse/accumulators by content
  signature (the pattern `_prepare_browse_table` already uses). Filtering itself is
  fragment-isolated and does **not** trigger this.
