# test_shop_mode.py — the shop-facing entry point (shop_checker.py) must never
# expose internal surfaces, and the Checker's own parse path must honor the
# feed_filter safety caps in both modes.
from __future__ import annotations

import os
from pathlib import Path
import tempfile
import types
import unittest
from unittest import mock

from streamlit.testing.v1 import AppTest

import app_mode
import feed_download as fdl
import feed_filter as ff
from test_filter_app import FIXTURE

ROOT = Path(__file__).resolve().parent


def _has_state_key(app: AppTest, key: str) -> bool:
    try:
        app.session_state[key]
        return True
    except KeyError:
        return False


def _all_text(app: AppTest) -> str:
    chunks = [str(m.value) for m in app.markdown]
    chunks += [str(c.value) for c in app.caption]
    chunks += [str(e.value) for e in app.error]
    chunks += [str(w.value) for w in app.warning]
    chunks += [str(i.value) for i in app.info]
    chunks += [str(s.value) for s in app.success]
    chunks += [str(getattr(e, "label", "")) for e in app.expander]
    chunks += [b.label for b in app.button]
    return " ".join(chunks)


class ShopModeAppTests(unittest.TestCase):
    def setUp(self):
        fd, self.path = tempfile.mkstemp(suffix=".xml")
        with os.fdopen(fd, "wb") as fh:
            fh.write(FIXTURE)

    def tearDown(self):
        if os.path.exists(self.path):
            os.unlink(self.path)
        # The entry points set the flag per app; never leak shop mode into
        # other test modules sharing this interpreter.
        app_mode.SHOP = False

    def _loaded_feed(self, scope: str = "Auto (full)") -> dict:
        return {
            "path": self.path,
            "label": "fixture.xml",
            "size": len(FIXTURE),
            "scope": scope,
            "n_limit": 5000,
            "stop_on_first_parse_error": False,
        }

    # ------------------------------------------------------------------ #
    # Shop entry point: trimmed surface
    # ------------------------------------------------------------------ #
    def test_shop_app_boots_with_single_trimmed_page(self):
        app = AppTest.from_file(str(ROOT / "shop_checker.py")).run(timeout=20)
        self.assertEqual(list(app.exception), [])
        self.assertIn("Load feed", [b.label for b in app.button])
        # No AM power tools: no Browse & filter toggle, no Advanced options —
        # and no TEXT telling a shop to use UI that doesn't exist for them.
        self.assertEqual(list(app.toggle), [])
        text = _all_text(app)
        self.assertNotIn("Advanced options", text)
        self.assertNotIn("Browse & filter panel", text)

    def test_shop_checker_hides_internal_surfaces_on_a_checked_feed(self):
        app = AppTest.from_file(str(ROOT / "shop_checker.py"))
        app.session_state["loaded_feed"] = self._loaded_feed()
        app.run(timeout=20)
        self.assertEqual(list(app.exception), [])

        text = _all_text(app)
        # Validation itself ran to the end (the footer renders last).
        self.assertIn("© 2026 FAVI", text)
        # Parser internals (Scope caption) are diagnostics — internal only.
        self.assertNotIn("Scope:", " ".join(str(c.value) for c in app.caption))
        # Internal-only surfaces are gone.
        self.assertNotIn("ClickUp", text)
        self.assertNotIn("AWS Lambda", text)
        self.assertNotIn("Raul Bertoldini", text)
        self.assertIn("© 2026 FAVI", text)
        # The ClickUp draft was not even computed/seeded.
        self.assertFalse(_has_state_key(app, "clickup_draft_seed"))
        # No hand-off state for the (unmounted) Feed Filter page.
        self.assertFalse(_has_state_key(app, "shared_feed_path"))

    def test_internal_app_keeps_its_internal_surfaces(self):
        app = AppTest.from_file(str(ROOT / "feed_checker_gui.py"))
        app.session_state["loaded_feed"] = self._loaded_feed()
        app.run(timeout=20)
        self.assertEqual(list(app.exception), [])

        text = _all_text(app)
        self.assertIn("ClickUp", text)
        self.assertIn("Raul Bertoldini", text)
        self.assertTrue(_has_state_key(app, "clickup_draft_seed"))
        self.assertTrue(
            any(t.label == "Browse & filter panel" for t in app.toggle)
        )

    def test_shop_unknown_format_is_a_single_clear_dead_end(self):
        # An unrecognized format must show ONE message — no metric row full of
        # zeros, no green "nothing checked" summary lines.
        fixture = b"<catalog><thing><a>1</a></thing><thing><a>2</a></thing></catalog>"
        fd, path = tempfile.mkstemp(suffix=".xml")
        with os.fdopen(fd, "wb") as fh:
            fh.write(fixture)
        self.addCleanup(lambda: os.path.exists(path) and os.unlink(path))

        app = AppTest.from_file(str(ROOT / "shop_checker.py"))
        app.session_state["loaded_feed"] = {
            "path": path,
            "label": "mystery.xml",
            "size": len(fixture),
            "scope": "Auto (full)",
            "n_limit": 5000,
            "stop_on_first_parse_error": False,
        }
        app.run(timeout=20)
        self.assertEqual(list(app.exception), [])

        text = _all_text(app)
        self.assertIn("was not identified as one FAVI accepts", text)
        self.assertIn("account manager", text)
        self.assertIn("© 2026 FAVI", text)
        # The confusing surfaces must NOT render.
        self.assertNotIn("Duplicates", text)          # metric row
        self.assertNotIn("Passing checks", text)      # green summary
        self.assertNotIn("No items were validated", text)
        # Internal app keeps the full diagnostic view for the same feed.
        internal = AppTest.from_file(str(ROOT / "feed_checker_gui.py"))
        internal.session_state["loaded_feed"] = {
            "path": path,
            "label": "mystery.xml",
            "size": len(fixture),
            "scope": "Auto (full)",
            "n_limit": 5000,
            "stop_on_first_parse_error": False,
        }
        internal.run(timeout=20)
        self.assertEqual(list(internal.exception), [])
        internal_text = _all_text(internal)
        self.assertIn("Duplicates", internal_text)
        self.assertIn("Transformation", internal_text)

    def test_shop_conversion_message_never_names_internal_tooling(self):
        # Attribute-based Ceneje.si triggers conversion_required, whose
        # feed_specs note literally says "Use Lambda transformer to convert."
        # — the shop error box and the summary line must both sanitize it.
        fixture = (
            b'<CNJExport>'
            b'<Item ID="1" link="https://example.com/1" price="10"'
            b' slikaVelika="https://example.com/1.jpg" in_stock="1"/>'
            b'<Item ID="2" link="https://example.com/2" price="20"'
            b' slikaVelika="https://example.com/2.jpg" in_stock="1"/>'
            b'</CNJExport>'
        )
        fd, path = tempfile.mkstemp(suffix=".xml")
        with os.fdopen(fd, "wb") as fh:
            fh.write(fixture)
        self.addCleanup(lambda: os.path.exists(path) and os.unlink(path))

        app = AppTest.from_file(str(ROOT / "shop_checker.py"))
        app.session_state["loaded_feed"] = {
            "path": path,
            "label": "ceneje.xml",
            "size": len(fixture),
            "scope": "Auto (full)",
            "n_limit": 5000,
            "stop_on_first_parse_error": False,
        }
        app.run(timeout=20)
        self.assertEqual(list(app.exception), [])

        text = _all_text(app)
        # The conversion error must render, in shop-safe wording only.
        self.assertIn("cannot import this feed format", text)
        self.assertIn("account manager", text)
        self.assertNotIn("Lambda", text)
        self.assertNotIn("transformer", text)
        self.assertNotIn("AWS", text)

    # ------------------------------------------------------------------ #
    # Checker parse path honors the feed_filter safety caps
    # ------------------------------------------------------------------ #
    def test_streaming_validation_respects_total_byte_cap(self):
        # Sample scope forces the streaming path; a 128-byte cap must stop the
        # fixture with a friendly error, not an exception.
        app = AppTest.from_file(str(ROOT / "shop_checker.py"))
        app.session_state["loaded_feed"] = self._loaded_feed(
            scope="Sample first N items"
        )
        with mock.patch.object(ff, "MAX_XML_BYTES", 128):
            app.run(timeout=20)
        self.assertEqual(list(app.exception), [])
        self.assertTrue(
            any("safety limits" in str(e.value) for e in app.error),
            [str(e.value) for e in app.error],
        )

    def test_streaming_validation_respects_item_node_cap(self):
        app = AppTest.from_file(str(ROOT / "shop_checker.py"))
        app.session_state["loaded_feed"] = self._loaded_feed(
            scope="Sample first N items"
        )
        with mock.patch.object(ff, "MAX_ITEM_NODES", 2):
            app.run(timeout=20)
        self.assertEqual(list(app.exception), [])
        self.assertTrue(
            any("safety limits" in str(e.value) for e in app.error),
            [str(e.value) for e in app.error],
        )


class FeedDownloadUnitTests(unittest.TestCase):
    def test_persist_upload_rejects_oversized_files(self):
        upload = types.SimpleNamespace(getbuffer=lambda: memoryview(b"x" * 64))
        with mock.patch.object(fdl, "MAX_DOWNLOAD_BYTES", 10):
            with self.assertRaises(ff.FeedDownloadError):
                fdl.persist_upload(upload)

    def test_persist_upload_lands_in_managed_temp_dir(self):
        upload = types.SimpleNamespace(getbuffer=lambda: memoryview(b"<a/>"))
        path, size, digest = fdl.persist_upload(upload)
        self.addCleanup(lambda: os.path.exists(path) and os.unlink(path))
        self.assertTrue(path.startswith(fdl.TEMP_DIR))
        self.assertTrue(os.path.basename(path).startswith("feed-"))
        self.assertEqual(size, 4)
        self.assertEqual(len(digest), 64)


if __name__ == "__main__":
    unittest.main()
