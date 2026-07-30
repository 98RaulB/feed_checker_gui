# feed_checker_gui.py
# Entry point / navigation router. The Feed Checker (validator) and the Feed
# Filter live as separate page scripts so the sidebar shows clean, named
# entries — "Feed Checker" / "Feed Filter" instead of the raw filename — and
# the two flows stay isolated. Launch (unchanged): streamlit run feed_checker_gui.py
#
# set_page_config must run once, before st.navigation, so it lives here and NOT
# in the page scripts (a second call would raise).
import streamlit as st

from branding import FAVICON_URL

st.set_page_config(page_title="FAVI Feed Tools", page_icon=FAVICON_URL, layout="wide")

st.navigation([
    st.Page("checker_page.py", title="Feed Checker", icon="🔎", default=True),
    st.Page("filter_page.py", title="Feed Filter", icon="🧮"),
]).run()
