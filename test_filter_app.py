from __future__ import annotations

import hashlib
import fcntl
import json
import os
from pathlib import Path
import tempfile
import time
import unittest

from streamlit.testing.v1 import AppTest


ROOT = Path(__file__).resolve().parent
FIXTURE = b"""<rss xmlns:g="http://base.google.com/ns/1.0"><channel>
<item><g:id>1</g:id><title>Sofa</title><description>Red sofa</description>
<link>https://example.com/1</link><g:image_link>https://example.com/1.jpg</g:image_link>
<g:price>10 EUR</g:price><g:availability>in stock</g:availability>
<g:product_type>Living Room &gt; Sofas</g:product_type></item>
<item><g:id>2</g:id><title>Chair</title><description>Blue chair</description>
<link>https://example.com/2</link><g:image_link>https://example.com/2.jpg</g:image_link>
<g:price>20 EUR</g:price><g:availability>out of stock</g:availability>
<g:product_type>Dining Room &gt; Chairs</g:product_type></item>
<item><g:id>3</g:id><title>Lamp</title><description>Floor lamp</description>
<link>https://example.com/3</link><g:image_link>https://example.com/3.jpg</g:image_link>
<g:price>30 EUR</g:price>
<g:product_type>Lighting &gt; Lamps</g:product_type></item>
</channel></rss>"""


class FilterPageAppTests(unittest.TestCase):
    def setUp(self):
        fd, self.path = tempfile.mkstemp(suffix=".xml")
        with os.fdopen(fd, "wb") as fh:
            fh.write(FIXTURE)

    def tearDown(self):
        if os.path.exists(self.path):
            os.unlink(self.path)

    def _loaded_app(self) -> AppTest:
        digest = hashlib.sha256(FIXTURE).hexdigest()
        app = AppTest.from_file(str(ROOT / "feed_checker_gui.py")).run(timeout=20)
        app.switch_page("filter_page.py").run(timeout=20)
        app.session_state["ff_src_path"] = self.path
        app.session_state["ff_src_label"] = "fixture.xml"
        app.session_state["ff_src_size"] = len(FIXTURE)
        app.session_state["ff_content_hash"] = digest
        app.session_state["ff_owned_path"] = False
        app.session_state["ff_index_params"] = False
        app.session_state["ff_index_params_cb"] = False
        app.session_state["ff_signature"] = f"{digest}::p0"
        app.session_state["ff_rules"] = []
        app.session_state["ff_next_id"] = 0
        return app.run(timeout=20)

    def test_router_boots_both_pages_without_exceptions(self):
        app = AppTest.from_file(str(ROOT / "feed_checker_gui.py")).run(timeout=20)
        self.assertEqual(list(app.exception), [])
        # The Feed Checker page now loads a feed for BOTH validation and browsing.
        self.assertIn("Load feed", [button.label for button in app.button])
        app.switch_page("filter_page.py").run(timeout=20)
        self.assertEqual(list(app.exception), [])
        self.assertIn("Load feed", [button.label for button in app.button])

    def test_checker_page_validates_and_browses_one_loaded_feed(self):
        """A single load feeds BOTH tabs on the Feed Checker page: validation
        renders to completion and the shared filter UI renders, no exception."""
        app = AppTest.from_file(str(ROOT / "feed_checker_gui.py"))
        app.session_state["loaded_feed"] = {
            "path": self.path,
            "label": "fixture.xml",
            "size": len(FIXTURE),
            "scope": "Auto (full)",
            "n_limit": 5000,
            "stop_on_first_parse_error": True,
        }
        app.run(timeout=20)
        self.assertEqual(list(app.exception), [])

        markdown = " ".join(str(m.value) for m in app.markdown)
        captions = " ".join(str(c.value) for c in app.caption)
        buttons = [b.label for b in app.button]

        # Validation tab rendered from the loaded feed, through to its end caption.
        self.assertIn("**Source:**", markdown)
        self.assertIn("Scope:", captions)
        # Browse tab rendered the shared filter UI (the render_filter fragment).
        self.assertIn("🗑 Clear all", buttons)

    def test_new_feed_on_checker_clears_stale_filter_state(self):
        """Switching feeds on the Checker wipes rule/browse state built against the
        previous feed, so a stale category can't carry over or crash the Browse tab."""
        app = AppTest.from_file(str(ROOT / "feed_checker_gui.py"))
        app.session_state["loaded_feed"] = {
            "path": self.path, "label": "fixture.xml", "size": len(FIXTURE),
            "content_hash": "feedB", "scope": "Auto (full)",
            "n_limit": 5000, "stop_on_first_parse_error": True,
        }
        # Stale rule/browse state left over from a different feed ("feedA").
        app.session_state["ff_rules_feed"] = "feedA"
        app.session_state["ff_rules"] = [99]
        app.session_state["ff_group_ids"] = [0]
        app.session_state["ff_cat_values_0"] = ["Ghost category not in feed B"]
        app.session_state["ff_browse_categories"] = ["Ghost category not in feed B"]
        app.run(timeout=20)

        ss = app.session_state
        self.assertEqual(list(app.exception), [])
        # Feed changed → state re-synced to the new feed and the stale values dropped.
        self.assertEqual(ss["ff_rules_feed"], "feedB")
        stale = ["Ghost category not in feed B"]
        if "ff_cat_values_0" in ss:
            self.assertNotEqual(ss["ff_cat_values_0"], stale)
        if "ff_browse_categories" in ss:
            self.assertNotEqual(ss["ff_browse_categories"], stale)
        if "ff_rules" in ss:
            self.assertNotEqual(ss["ff_rules"], [99])
        # Browse tab still rendered (did not crash on the stale multiselect value).
        self.assertIn("🗑 Clear all", [b.label for b in app.button])

    def test_failed_submit_keeps_both_panels_alive(self):
        """A bad submit (typo'd URL) must not blank the page: the error renders
        under the form and the already-loaded feed's validation + browse panels
        stay on screen (no st.stop() on the load path)."""
        digest = hashlib.sha256(FIXTURE).hexdigest()
        app = AppTest.from_file(str(ROOT / "feed_checker_gui.py"))
        app.session_state["loaded_feed"] = {
            "path": self.path, "label": "fixture.xml", "size": len(FIXTURE),
            "content_hash": digest, "scope": "Auto (full)",
            "n_limit": 5000, "stop_on_first_parse_error": True,
        }
        app.run(timeout=20)
        app.text_input[0].set_value("ftp://bad.example/feed.xml")
        next(b for b in app.button if b.label == "Load feed").click()
        app.run(timeout=20)

        self.assertEqual(list(app.exception), [])
        self.assertTrue(any("http://" in e.value for e in app.error))
        markdown = " ".join(str(m.value) for m in app.markdown)
        self.assertIn("**Source:**", markdown)  # validation survived the bad submit
        self.assertIn("🗑 Clear all", [b.label for b in app.button])  # browse survived

    def test_collapsing_browse_panel_leaves_validation_full_width(self):
        """Switching the Browse panel off drops the right half entirely — no ②
        subheader, no filter UI — while validation renders to completion at the
        full page measure and the loaded feed stays loaded."""
        app = AppTest.from_file(str(ROOT / "feed_checker_gui.py"))
        app.session_state["loaded_feed"] = {
            "path": self.path, "label": "fixture.xml", "size": len(FIXTURE),
            "content_hash": hashlib.sha256(FIXTURE).hexdigest(),
            "scope": "Auto (full)", "n_limit": 5000, "stop_on_first_parse_error": True,
        }
        app.session_state["checker_show_browse"] = False
        app.run(timeout=20)

        self.assertEqual(list(app.exception), [])
        subheaders = [s.value for s in app.subheader]
        # Right half gone, and the left half loses its now-pointless ① marker.
        self.assertNotIn("② Browse & filter", subheaders)
        self.assertIn("Load & check", subheaders)
        self.assertNotIn("🗑 Clear all", [b.label for b in app.button])
        # Validation still ran end to end off the same loaded feed.
        self.assertIn("**Source:**", " ".join(str(m.value) for m in app.markdown))
        self.assertIn("Scope:", " ".join(str(c.value) for c in app.caption))
        # Collapsed reverts to the app's normal measure instead of sprawling.
        self.assertTrue(
            any("max-width:1200px" in str(m.value) for m in app.markdown),
            "collapsed layout should constrain .block-container to 1200px",
        )

    def test_browse_panel_toggle_defaults_to_on(self):
        """The toggle exists, defaults to on, and the side-by-side layout keeps
        using the full window width — collapsing is opt-in."""
        app = AppTest.from_file(str(ROOT / "feed_checker_gui.py")).run(timeout=20)
        self.assertEqual(list(app.exception), [])
        toggle = next(t for t in app.toggle if t.label == "Browse & filter panel")
        self.assertTrue(toggle.value)
        self.assertIn("② Browse & filter", [s.value for s in app.subheader])
        self.assertTrue(any("max-width:100%" in str(m.value) for m in app.markdown))

    def test_empty_submit_keeps_browse_placeholder(self):
        """Clicking Load feed with nothing filled shows a warning but keeps the
        right-half Browse placeholder visible (the page never half-disappears)."""
        app = AppTest.from_file(str(ROOT / "feed_checker_gui.py")).run(timeout=20)
        next(b for b in app.button if b.label == "Load feed").click()
        app.run(timeout=20)
        self.assertEqual(list(app.exception), [])
        self.assertTrue(any("Provide a URL" in w.value for w in app.warning))
        self.assertIn("② Browse & filter", [s.value for s in app.subheader])

    def test_blank_new_rule_is_ignored_and_downloads_stay_disabled(self):
        app = self._loaded_app()
        next(button for button in app.button if button.label == "➕ Add rule").click()
        app.run(timeout=20)
        self.assertEqual(list(app.exception), [])
        self.assertTrue(any("incomplete rule" in info.value for info in app.info))
        self.assertEqual(list(app.code), [])
        self.assertTrue(all(button.disabled for button in app.get("download_button")))

    def test_category_rule_uses_counted_feed_values_and_array_spec(self):
        app = self._loaded_app()
        next(
            button for button in app.button if button.label == "➕ Add rule"
        ).click()
        app.run(timeout=20)

        next(
            selectbox
            for selectbox in app.selectbox
            if selectbox.label == "Field"
        ).select("category")
        app.run(timeout=20)

        operator = next(
            selectbox
            for selectbox in app.selectbox
            if selectbox.label == "Operator"
        )
        self.assertEqual(
            operator.options,
            ["is one of", "is not one of", "is empty", "is not empty"],
        )
        categories = next(
            multiselect
            for multiselect in app.multiselect
            if multiselect.label == "Categories"
        )
        self.assertIn("Living Room > Sofas (1)", categories.options)
        categories.select("Living Room > Sofas")
        app.run(timeout=20)

        self.assertEqual(list(app.exception), [])
        self.assertIn("1 of 3 products remain", app.code[0].value)
        next(
            button
            for button in app.button
            if button.label == "Prepare hand-off downloads"
        ).click()
        app.run(timeout=20)
        specification = json.loads(
            app.session_state["ff_prepared_exports"]["spec"]
        )
        self.assertEqual(specification["version"], 2)
        self.assertEqual(
            specification["groups"][0]["rules"][0],
            {
                "field": "category",
                "op": "one of",
                "value": ["Living Room > Sofas"],
            },
        )
        next(
            selectbox
            for selectbox in app.selectbox
            if selectbox.label == "Operator"
        ).select("not one of")
        app.run(timeout=20)
        self.assertTrue(
            all(
                button.disabled
                for button in app.get("download_button")
            )
        )

    def test_rule_groups_preserve_parentheses_and_global_logic(self):
        app = self._loaded_app()
        next(
            button for button in app.button if button.label == "➕ Add rule"
        ).click()
        app.run(timeout=20)
        next(
            text_input
            for text_input in app.text_input
            if text_input.label == "Value"
        ).set_value("25")
        app.run(timeout=20)

        add_group = next(
            button
            for button in app.button
            if button.label == "＋ Add rule group"
        )
        self.assertFalse(add_group.disabled)
        add_group.click()
        app.run(timeout=20)

        field_selectors = [
            selectbox
            for selectbox in app.selectbox
            if selectbox.label == "Field"
        ]
        field_selectors[1].select("availability")
        app.run(timeout=20)
        operator_selectors = [
            selectbox
            for selectbox in app.selectbox
            if selectbox.label == "Operator"
        ]
        operator_selectors[1].select("is empty")
        app.run(timeout=20)

        global_logic = next(
            radio
            for radio in app.radio
            if radio.label == "A product must match"
        )
        global_logic.set_value("OR")
        app.run(timeout=20)
        self.assertEqual(list(app.exception), [])
        self.assertIn(") OR (", app.code[0].value)
        self.assertIn("3 of 3 products remain", app.code[0].value)

        next(
            radio
            for radio in app.radio
            if radio.label == "A product must match"
        ).set_value("AND")
        app.run(timeout=20)
        self.assertIn(") AND (", app.code[0].value)
        self.assertIn("0 of 3 products remain", app.code[0].value)

        group_delete_buttons = [
            button
            for button in app.button
            if str(button.key).startswith("ff_del_group_")
        ]
        self.assertEqual(len(group_delete_buttons), 2)
        group_delete_buttons[1].click()
        app.run(timeout=20)
        self.assertFalse(
            any(
                radio.label == "A product must match"
                for radio in app.radio
            )
        )
        self.assertIn("2 of 3 products remain", app.code[0].value)
        self.assertNotIn(") AND (", app.code[0].value)

    def test_empty_explicit_group_blocks_handoff_until_deleted(self):
        app = self._loaded_app()
        browser = next(
            expander
            for expander in app.expander
            if expander.label == "Browse / verify products"
        )
        self.assertTrue(browser.proto.expanded)
        next(
            button
            for button in app.button
            if button.label == "Preset: missing availability"
        ).click()
        app.run(timeout=20)
        next(
            button
            for button in app.button
            if button.label == "Prepare hand-off downloads"
        ).click()
        app.run(timeout=20)

        next(
            button
            for button in app.button
            if button.label == "＋ Add rule group"
        ).click()
        app.run(timeout=20)
        self.assertIn("➕ Add to group 1", [button.label for button in app.button])
        second_group = app.session_state["ff_group_ids"][1]
        second_rule = app.session_state[f"ff_group_rules_{second_group}"][0]
        delete_rule = next(
            button
            for button in app.button
            if button.key == f"ff_del_{second_rule}"
        )
        self.assertEqual(delete_rule.label, "Delete rule")
        delete_rule.click()
        app.run(timeout=20)

        self.assertEqual(
            app.session_state[f"ff_group_rules_{second_group}"],
            [],
        )
        self.assertTrue(
            any("empty rule group" in info.value for info in app.info)
        )
        self.assertEqual(list(app.code), [])
        self.assertTrue(
            all(button.disabled for button in app.get("download_button"))
        )

        delete_group = next(
            button
            for button in app.button
            if button.key == f"ff_del_group_{second_group}"
        )
        self.assertEqual(delete_group.label, "Delete group")
        delete_group.click()
        app.run(timeout=20)
        self.assertNotIn(
            f"ff_group_rules_{second_group}",
            app.session_state,
        )
        self.assertNotIn(
            f"ff_group_combine_{second_group}",
            app.session_state,
        )
        self.assertEqual(len(app.code), 1)
        self.assertTrue(
            all(not button.disabled for button in app.get("download_button"))
        )

    def test_browse_search_is_scoped_and_does_not_change_handoff(self):
        app = self._loaded_app()
        next(
            button
            for button in app.button
            if button.label == "Preset: missing availability"
        ).click()
        app.run(timeout=20)
        handoff_before = app.code[0].value

        next(
            text_input
            for text_input in app.text_input
            if text_input.label == "Search products"
        ).set_value("lamp")
        next(
            button
            for button in app.button
            if button.label == "Show products"
        ).click()
        app.run(timeout=20)
        self.assertTrue(
            any(
                "Showing 0 of 0 products within resulting feed"
                in caption.value
                for caption in app.caption
            )
        )
        self.assertEqual(app.code[0].value, handoff_before)

        next(
            selectbox
            for selectbox in app.selectbox
            if selectbox.label == "Search within"
        ).select("Removed products")
        next(
            button
            for button in app.button
            if button.label == "Show products"
        ).click()
        app.run(timeout=20)
        self.assertTrue(
            any(
                "Showing 1 of 1 products within removed products"
                in caption.value
                for caption in app.caption
            )
        )
        self.assertEqual(len(app.dataframe), 1)
        self.assertEqual(app.code[0].value, handoff_before)

    def test_clearing_remove_recipe_restores_identity_state(self):
        app = self._loaded_app()
        next(
            button
            for button in app.button
            if button.label == "Preset: missing availability"
        ).click()
        app.run(timeout=20)
        self.assertIn("1 removed", app.code[0].value)
        first_group = app.session_state["ff_group_ids"][0]
        first_rule = app.session_state[f"ff_group_rules_{first_group}"][0]
        next(
            button
            for button in app.button
            if button.label == "＋ Add rule group"
        ).click()
        app.run(timeout=20)
        second_group = app.session_state["ff_group_ids"][1]
        second_rule = app.session_state[f"ff_group_rules_{second_group}"][0]
        next(
            button for button in app.button if button.label == "🗑 Clear all"
        ).click()
        app.run(timeout=20)
        self.assertEqual(list(app.code), [])
        self.assertTrue(
            any(
                "Add and complete at least one rule" in info.value
                for info in app.info
            )
        )
        self.assertNotIn(f"ff_group_rules_{second_group}", app.session_state)
        for rule_id in (first_rule, second_rule):
            self.assertNotIn(f"ff_field_{rule_id}", app.session_state)
            self.assertNotIn(f"ff_val_{rule_id}", app.session_state)
        self.assertNotIn(f"ff_op_{first_rule}_text", app.session_state)
        self.assertNotIn(f"ff_op_{second_rule}_number", app.session_state)
        self.assertTrue(
            any(
                "No active rules" in markdown.value
                for markdown in app.markdown
            )
        )

    def test_completed_preset_prepares_all_downloads(self):
        app = self._loaded_app()
        next(
            button
            for button in app.button
            if button.label == "Preset: missing image"
        ).click()
        app.run(timeout=20)
        self.assertEqual(list(app.exception), [])
        self.assertEqual(len(app.code), 1)
        next(
            button
            for button in app.button
            if button.label == "Prepare hand-off downloads"
        ).click()
        app.run(timeout=20)
        self.assertEqual(list(app.exception), [])
        self.assertTrue(
            all(
                not button.disabled
                for button in app.get("download_button")
            )
        )

    def test_temp_janitor_preserves_leased_files(self):
        temp_dir = os.path.join(
            tempfile.gettempdir(),
            f"favi-feed-filter-{getattr(os, 'getuid', lambda: 0)()}",
        )
        os.makedirs(temp_dir, exist_ok=True)
        fd, path = tempfile.mkstemp(prefix="feed-", suffix=".xml", dir=temp_dir)
        os.close(fd)
        os.utime(path, (time.time() - 48 * 60 * 60,) * 2)
        lease = open(path, "rb")
        fcntl.flock(lease.fileno(), fcntl.LOCK_SH | fcntl.LOCK_NB)
        self.addCleanup(lambda: os.path.exists(path) and os.unlink(path))
        self.addCleanup(lease.close)

        app = AppTest.from_file(str(ROOT / "feed_checker_gui.py")).run(timeout=20)
        app.switch_page("filter_page.py").run(timeout=20)
        self.assertTrue(os.path.exists(path))

        lease.close()
        app.run(timeout=20)
        self.assertFalse(os.path.exists(path))


if __name__ == "__main__":
    unittest.main(verbosity=2)
