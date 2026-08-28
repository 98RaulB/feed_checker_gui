# app_mode.py — which deployment is running this process.
#
# SHOP is set by the ENTRY POINT, not by configuration: feed_checker_gui.py
# (internal) sets it False, shop_checker.py (shop-facing) sets it True, both
# before st.navigation executes any page script. Deciding this by entry point
# rather than an env var is deliberate — a missing/mistyped env var on the
# shop deployment would silently expose internal features, while the entry
# file IS the deployment definition and cannot be absent.
#
# Both entries assign explicitly (never rely on the default): the test suite
# boots both apps inside one interpreter, where module state persists.

SHOP = False
