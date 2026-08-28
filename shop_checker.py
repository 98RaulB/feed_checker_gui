# shop_checker.py
# SHOP-FACING entry point — deploy THIS file as the Streamlit main file for the
# partner/onboarding deployment. It mounts ONLY the Feed Checker page, in shop
# mode: no Feed Filter page, no ClickUp draft, no Browse & filter panel, no
# internal-pipeline wording. The internal app is feed_checker_gui.py, unchanged.
#
# Shop mode is decided here, before st.navigation executes the page script —
# see app_mode.py for why the entry point (not an env var) carries the flag.
#
# Launch: streamlit run shop_checker.py
import os

# Tight abuse limits for anonymous traffic, pinned BEFORE feed_download /
# feed_filter are imported (they read these at import time). setdefault, so an
# operator-set env var / Streamlit secret still wins. The internal app keeps
# the larger defaults defined in those modules (real partner feeds reach 2 GB).
os.environ.setdefault("FAVI_FILTER_MAX_DOWNLOAD_MB", "256")
os.environ.setdefault("FAVI_FILTER_MAX_DOWNLOAD_SECONDS", "180")
os.environ.setdefault("FAVI_FILTER_MAX_XML_MB", "512")
os.environ.setdefault("FAVI_CHECKER_MAX_ITEMS", "300000")

import streamlit as st

import app_mode

app_mode.SHOP = True

from branding import FAVICON_URL

st.set_page_config(page_title="FAVI Feed Check", page_icon=FAVICON_URL, layout="wide")

st.navigation([
    st.Page("checker_page.py", title="Feed Check", icon="🔎", default=True),
]).run()
