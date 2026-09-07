# FAVI Feed Tools

Streamlit tools for validating and filtering partner product feeds (Google
Merchant, Heureka, Ceneo, Compari, Skroutz and other marketplace XML formats),
plus a nightly quality audit of the feeds FAVI publishes.

One codebase, two deployments:

| Entry point | Audience | What it mounts |
|---|---|---|
| `feed_checker_gui.py` | FAVI account managers | Feed Checker (validate + browse/filter) and the standalone Feed Filter page |
| `shop_checker.py` | Partner shops, embedded in onboarding | Feed Checker only, in *shop mode*: no internal wording, tooling or hand-off features, tighter abuse limits |

Shop mode is decided by the entry point rather than configuration, so a
deployment cannot accidentally expose internal features. Validation logic is
identical in both, so what a shop sees is exactly what an account manager sees.
Design details are in [ARCHITECTURE.md](ARCHITECTURE.md).

## Quick start

```bash
python -m venv .venv && source .venv/bin/activate
pip install -r requirements.txt

streamlit run feed_checker_gui.py   # internal app
streamlit run shop_checker.py       # shop-facing app

python -m unittest discover -v      # engine, SSRF, spec and Streamlit AppTest suites
ruff check .                        # correctness lint (pyflakes rules), same gate as CI
```

Python 3.12 is what CI runs. The `.devcontainer/` gives the same setup in
GitHub Codespaces.

## Configuration

All configuration is by environment variable (on Streamlit Community Cloud:
app **Settings -> Secrets**). Nothing in this repository is deployment
specific, and no secret is ever committed.

| Variable | Default | Effect |
|---|---|---|
| `SLACK_WEBHOOK_URL` | unset (alerts off) | Incoming-webhook URL for best-effort Slack alerts on unexpected failures. One "started" ping per process proves it works. |
| `CLICKUP_FORM_URL` | unset (link hidden) | ClickUp intake form for the internal ticket draft. Internal app only. |
| `FAVI_FILTER_MAX_DOWNLOAD_MB` | 2048 internal / 256 shop | Hard cap on a downloaded feed. |
| `FAVI_FILTER_MAX_DOWNLOAD_SECONDS` | 900 internal / 180 shop | Wall-clock cap on a download. |
| `FAVI_FILTER_MAX_XML_MB` | 2048 internal / 512 shop | Cap on the uncompressed XML that is parsed. |
| `FAVI_CHECKER_MAX_ITEMS` | 500000 internal / 300000 shop | Cap on validated items. |

## Security model

- **SSRF-safe downloads.** Every feed URL is resolved once, private and
  reserved addresses are rejected, and the connection is pinned to the
  validated IP while TLS still verifies the original hostname
  (`safe_http.py`, `feed_filter.public_url_ips`).
- **Bounded resources.** Size and time caps are enforced before and during
  download and again at parse time; the shop entry point pins them lower.
- **Hardened XML.** All parsing goes through `defusedxml`.
- **No credentials in the repo.** Slack and ClickUp endpoints are injected per
  deployment. The audit's AWS access is a GitHub OIDC role scoped to this
  repository's `main` branch with read-only feed listing plus write access to
  one private S3 prefix (see [audit/iam/](audit/iam/README.md)).
- **Public-repo hygiene.** The nightly audit's logs, step summaries and
  artifacts identify feeds by id prefix only; the report with shop names is
  published to a private S3 prefix.

## Automation

| Workflow | Schedule | Purpose |
|---|---|---|
| `ci.yml` | push / PR | Correctness lint (`ruff`, pyflakes rules) and the full test suite on Python 3.12. |
| `feed-quality-audit.yml` | nightly 04:47 UTC | Advisory audit of every live published feed; fails only on a *new* feed-level blocker. |
| `keepalive.yml` | every 5 h | Visits the shop app in a headless browser so Community Cloud never shows partners its sleep screen; a failed run doubles as an uptime alert. |

## Layout

```
feed_checker_gui.py   internal entry point        shop_checker.py     shop-facing entry point
app_mode.py           SHOP flag set by the entry  branding.py         FAVI look and feel
checker_page.py       validator page              filter_page.py      standalone filter page
filter_view.py        shared filter UI            feed_filter.py      pure filtering engine
feed_specs.py         format detection + readers  feed_download.py    hardened download/upload
safe_http.py          SSRF-pinned HTTP adapter    error_reporting.py  Slack alerting
audit/                nightly audit script + IAM  test_*.py           test suites
```
