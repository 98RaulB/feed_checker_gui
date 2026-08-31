# test_error_reporting.py — the Slack reporter must be a no-op without the
# webhook secret, deliver once when configured, and mute repeats.
from __future__ import annotations

import unittest
from unittest import mock

import error_reporting as er


class _Response:
    status_code = 200


class ErrorReportingTests(unittest.TestCase):
    def setUp(self):
        er._last_sent.clear()

    def test_noop_without_webhook_env(self):
        with (
            mock.patch.dict("os.environ", {}, clear=False),
            mock.patch.object(er.requests, "post") as post,
        ):
            import os
            os.environ.pop(er.WEBHOOK_ENV, None)
            sent = er.report_error("ctx", RuntimeError("boom"), source="x")
        self.assertFalse(sent)
        post.assert_not_called()

    def test_posts_once_and_dedupes_repeats(self):
        env = {er.WEBHOOK_ENV: "https://hooks.slack.com/services/T/B/x"}
        with (
            mock.patch.dict("os.environ", env),
            mock.patch.object(er.requests, "post", return_value=_Response()) as post,
        ):
            first = er.report_error(
                "parser crashed", RuntimeError("boom"), source="feed.xml", shop=True,
            )
            repeat = er.report_error(
                "parser crashed", RuntimeError("boom"), source="feed.xml", shop=True,
            )
            different = er.report_error(
                "parser crashed", RuntimeError("other"), source="feed.xml",
            )
        self.assertTrue(first)
        self.assertFalse(repeat)
        self.assertTrue(different)
        self.assertEqual(post.call_count, 2)
        payload = post.call_args_list[0].kwargs["json"]["text"]
        self.assertIn("(shop)", payload)
        self.assertIn("parser crashed", payload)
        self.assertIn("feed.xml", payload)
        self.assertIn("RuntimeError", payload)

    def test_reporting_failure_never_raises(self):
        env = {er.WEBHOOK_ENV: "https://hooks.slack.com/services/T/B/x"}
        with (
            mock.patch.dict("os.environ", env),
            mock.patch.object(
                er.requests, "post", side_effect=OSError("network down")
            ),
        ):
            sent = er.report_error("ctx", RuntimeError("boom"))
        self.assertFalse(sent)


if __name__ == "__main__":
    unittest.main()
