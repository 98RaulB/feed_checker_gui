# branding.py
"""Shared FAVI look-and-feel for the standalone Feed Checker app.

Mirrors the styling of the internal FAVI Admin Dashboard (Work Sans brand
font, crimson header banner, theme-aware cards/pills/buttons) so the public
checker reads as part of the same product. Each Streamlit page calls
`inject_css()` once (right after `st.set_page_config`) and `page_header(...)`
to render the banner.

Kept deliberately self-contained — no AWS/boto3 imports — so the checker
stays a zero-cloud-dependency app.
"""
from __future__ import annotations

from typing import Any

import streamlit as st

# Brand colors — taken from favi.it's <meta theme-color> / brand bar.
BRAND_PRIMARY = "#a61932"          # FAVI crimson
BRAND_PRIMARY_DARK = "#7a1224"     # darker crimson for the banner gradient
BRAND_PRIMARY_TINT = "#fdf2f4"     # very light blush for hover/highlight

# White FAVI logo SVG + favicon, served from the public CDN.
LOGO_WHITE_URL = (
    "https://s.favi.it/static/frontend/_global/images/favi-logo/"
    "favi-logo-white.19d6b1a8cc53f7ff081b90168348242f.svg"
)
FAVICON_URL = (
    "https://s.favi.it/static/frontend/favicons/"
    "favicon-32x32.42253f61a513379749b26edea886bccb.png"
)

_CSS = f"""
<style>
  /* Work Sans — FAVI's brand font (loaded by their public site too). */
  @import url('https://fonts.googleapis.com/css2?family=Work+Sans:wght@400;500;600;700&family=JetBrains+Mono:wght@400;500&display=swap');

  /* Scope font-family narrowly. NEVER use [class*="st-"] — that selector
     also hits Streamlit's Material Icon spans and breaks chevron icons. */
  .stApp,
  .stApp p, .stApp h1, .stApp h2, .stApp h3, .stApp h4, .stApp h5, .stApp h6,
  .stApp label, .stApp button, .stApp input, .stApp textarea, .stApp select,
  .stApp [data-testid="stMarkdownContainer"],
  .stApp [data-testid="stCaptionContainer"],
  .stApp [data-testid="stMetricLabel"],
  .stApp [data-testid="stMetricValue"] {{
    font-family: 'Work Sans', -apple-system, BlinkMacSystemFont, 'Segoe UI', sans-serif !important;
  }}

  /* Belt-and-braces — never override icon fonts. */
  .material-symbols-outlined, .material-symbols-rounded, .material-symbols-sharp,
  .material-icons, [data-testid="stIconMaterial"], [data-testid="stMaterialIcon"],
  i[class*="material"], span[class*="emotion"][class*="icon"] {{
    font-family: 'Material Symbols Outlined', 'Material Symbols Rounded',
                 'Material Icons', 'Material Icons Outlined' !important;
  }}

  code, pre, .stCodeBlock {{
    font-family: 'JetBrains Mono', ui-monospace, SFMono-Regular, Menlo, monospace !important;
  }}

  .block-container {{
    padding-top: 1rem !important;
    padding-bottom: 3rem !important;
    padding-left: 1.75rem !important;
    padding-right: 1.75rem !important;
    max-width: none !important;
    width: 100% !important;
  }}
  @media (max-width: 640px) {{
    .block-container {{ padding-left: 1rem !important; padding-right: 1rem !important; }}
  }}

  /* ── Page banner ── full-width crimson strip, white logo + page name. */
  .favi-banner {{
    background: linear-gradient(90deg, {BRAND_PRIMARY} 0%, {BRAND_PRIMARY_DARK} 100%);
    border-radius: 12px;
    padding: 1rem 1.5rem;
    display: flex;
    align-items: center;
    justify-content: space-between;
    gap: 1.5rem;
    margin: 0 0 1.5rem 0;
    box-shadow: 0 2px 8px rgba(166, 25, 50, 0.18);
  }}
  .favi-banner-logo {{ height: 32px; width: auto; display: block; }}
  .favi-banner-title {{
    color: #fff !important;
    font-size: 1.5rem;
    font-weight: 600;
    letter-spacing: -0.01em;
    line-height: 1.1;
    text-align: right;
    margin: 0;
  }}
  @media (max-width: 640px) {{
    .favi-banner {{ flex-direction: column; align-items: flex-start; gap: 0.5rem; }}
    .favi-banner-title {{ text-align: left; font-size: 1.25rem; }}
  }}

  h1, h2, h3, h4 {{ letter-spacing: -0.01em; font-weight: 600 !important; }}
  h1 {{ font-size: 1.75rem !important; margin-bottom: 0 !important; }}
  h2 {{ font-size: 1.25rem !important; margin-top: 1.5rem !important; }}
  h3 {{ font-size: 1rem !important; }}

  .favi-page-subtitle {{
    color: var(--text-color, #6b7280);
    opacity: 0.65;
    font-size: 0.9rem;
    margin-top: 0.35rem;
    margin-bottom: 1.5rem;
    line-height: 1.5;
  }}

  /* ── Metric cards ── backgrounds/text follow the active Streamlit theme. */
  .favi-card {{
    background: var(--secondary-background-color, #fff);
    border: 1px solid color-mix(in srgb, var(--text-color, #1f2937) 12%, transparent);
    border-radius: 12px;
    padding: 1rem 1.25rem;
    box-shadow: 0 1px 2px rgba(0, 0, 0, 0.04);
    transition: border-color 0.15s ease, box-shadow 0.15s ease;
    height: 100%;
  }}
  .favi-card:hover {{
    border-color: color-mix(in srgb, var(--text-color, #1f2937) 24%, transparent);
    box-shadow: 0 2px 6px rgba(0, 0, 0, 0.06);
  }}
  .favi-card-label {{
    font-size: 0.7rem;
    font-weight: 600;
    color: var(--text-color, #6b7280);
    opacity: 0.6;
    text-transform: uppercase;
    letter-spacing: 0.06em;
    margin: 0 0 0.4rem 0;
  }}
  .favi-card-value {{
    font-size: 1.875rem;
    font-weight: 600;
    color: var(--text-color, #1f2937);
    line-height: 1.1;
    margin: 0;
  }}
  .favi-card-delta {{
    font-size: 0.75rem;
    color: var(--text-color, #6b7280);
    opacity: 0.6;
    margin-top: 0.25rem;
  }}
  .favi-card.tone-error .favi-card-value {{ color: #ef4444; opacity: 1; }}
  .favi-card.tone-warn  .favi-card-value {{ color: #f59e0b; opacity: 1; }}
  .favi-card.tone-ok    .favi-card-value {{ color: #22c55e; opacity: 1; }}
  .favi-card.tone-muted .favi-card-value {{ color: var(--text-color, #6b7280); opacity: 0.55; }}
  .favi-card.tone-brand .favi-card-value {{ color: {BRAND_PRIMARY}; opacity: 1; }}

  /* ── Status pills ── translucent bg + bright fg, readable on light & dark. */
  .favi-pill {{
    display: inline-block;
    padding: 4px 12px;
    border-radius: 9999px;
    font-size: 0.78rem;
    font-weight: 600;
    line-height: 1.5;
    letter-spacing: 0.02em;
    vertical-align: middle;
  }}
  .favi-pill-ok      {{ background: rgba(34, 197, 94, 0.18);  color: #15803d; }}
  .favi-pill-error   {{ background: rgba(239, 68, 68, 0.18);  color: #b91c1c; }}
  .favi-pill-warn    {{ background: rgba(245, 158, 11, 0.18); color: #b45309; }}
  .favi-pill-muted   {{ background: rgba(107, 114, 128, 0.15); color: var(--text-color, #4b5563); }}
  .favi-pill-brand   {{ background: {BRAND_PRIMARY_TINT}; color: {BRAND_PRIMARY}; }}

  .favi-section-label {{
    font-size: 0.7rem;
    font-weight: 600;
    letter-spacing: 0.08em;
    text-transform: uppercase;
    color: var(--text-color, #6b7280);
    opacity: 0.6;
    margin: 0 0 0.5rem 0;
  }}

  section[data-testid="stSidebar"] {{
    border-right: 1px solid color-mix(in srgb, var(--text-color, #1f2937) 12%, transparent);
  }}

  /* Buttons — brand-tinted, theme-aware. */
  .stButton > button, .stFormSubmitButton > button, .stDownloadButton > button,
  .stLinkButton > a, [data-testid="stPopover"] > button {{
    border-radius: 8px;
    font-weight: 500;
    transition: all 0.12s ease;
  }}
  .stButton > button:not([kind="primary"]),
  .stFormSubmitButton > button:not([kind="primary"]),
  .stDownloadButton > button {{
    background: var(--secondary-background-color, #fff) !important;
    color: var(--text-color, #1f2937) !important;
    border-color: color-mix(in srgb, var(--text-color, #1f2937) 22%, transparent) !important;
  }}
  .stButton > button:hover:not(:disabled),
  .stFormSubmitButton > button:hover:not(:disabled),
  .stDownloadButton > button:hover:not(:disabled) {{
    transform: translateY(-1px);
    box-shadow: 0 2px 4px rgba(166, 25, 50, 0.08);
    border-color: color-mix(in srgb, var(--text-color, #1f2937) 34%, transparent) !important;
  }}
  .stButton > button[kind="primary"]:not(:disabled),
  .stFormSubmitButton > button[kind="primary"]:not(:disabled) {{
    background: {BRAND_PRIMARY} !important;
    border-color: {BRAND_PRIMARY} !important;
    color: #fff !important;
  }}
  .stButton > button[kind="primary"]:hover:not(:disabled),
  .stFormSubmitButton > button[kind="primary"]:hover:not(:disabled) {{
    background: {BRAND_PRIMARY_DARK} !important;
    border-color: {BRAND_PRIMARY_DARK} !important;
  }}

  /* Dataframe — rounded, contained. */
  [data-testid="stDataFrame"] {{
    border-radius: 12px;
    overflow: hidden;
    border: 1px solid color-mix(in srgb, var(--text-color, #1f2937) 12%, transparent);
  }}

  .stCodeBlock pre {{ font-size: 0.8rem !important; }}
  .stAlert {{ border-radius: 10px; }}
  footer {{ visibility: hidden; }}
</style>
"""


def inject_css() -> None:
    """Call once at the top of each page, right after `st.set_page_config`."""
    st.markdown(_CSS, unsafe_allow_html=True)


def page_header(title: str, *, subtitle: str | None = None) -> None:
    """Render the FAVI-crimson banner: white logo left, page name right."""
    st.markdown(
        f"""
        <div class="favi-banner">
          <img src="{LOGO_WHITE_URL}" alt="FAVI" class="favi-banner-logo"/>
          <h1 class="favi-banner-title">{title}</h1>
        </div>
        """,
        unsafe_allow_html=True,
    )
    if subtitle:
        st.markdown(
            f'<div class="favi-page-subtitle">{subtitle}</div>',
            unsafe_allow_html=True,
        )


def section_label(text: str) -> None:
    st.markdown(f'<div class="favi-section-label">{text}</div>', unsafe_allow_html=True)


# ── Metric cards ─────────────────────────────────────────────────────────────

def metric_card(label: str, value: Any, *, delta: str | None = None,
                tone: str = "default") -> str:
    """Return the HTML for a single metric card (render with unsafe_allow_html)."""
    tone_class = f" tone-{tone}" if tone != "default" else ""
    delta_html = f'<div class="favi-card-delta">{delta}</div>' if delta else ""
    return (
        f'<div class="favi-card{tone_class}">'
        f'<div class="favi-card-label">{label}</div>'
        f'<div class="favi-card-value">{value}</div>'
        f'{delta_html}'
        f'</div>'
    )


def render_metric_row(items: list[tuple]) -> None:
    """Render a row of metric cards. Each item is (label, value, tone[, delta])."""
    cols = st.columns(len(items))
    for col, item in zip(cols, items):
        label, value, tone, *rest = item
        delta = rest[0] if rest else None
        with col:
            st.markdown(metric_card(label, value, tone=tone, delta=delta),
                        unsafe_allow_html=True)


# ── Status pills ─────────────────────────────────────────────────────────────

def pill(text: str, tone: str = "ok") -> None:
    """Render a FAVI status pill. tone ∈ {ok, error, warn, muted, brand}."""
    safe = str(text).replace("&", "&amp;").replace("<", "&lt;").replace(">", "&gt;")
    st.markdown(f'<span class="favi-pill favi-pill-{tone}">{safe}</span>',
                unsafe_allow_html=True)
