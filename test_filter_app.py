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
