from __future__ import annotations

import unittest
from unittest import mock

import requests
from requests.adapters import HTTPAdapter

import feed_filter as ff
import safe_http


class PinnedPublicAdapterTests(unittest.TestCase):
    def test_send_pins_resolved_ip_and_preserves_host_header(self):
        adapter = safe_http.PinnedPublicAdapter()
        request = requests.Request(
            "GET",
            "https://feed.example:8443/products.xml",
        ).prepare()
        response = requests.Response()
        response.status_code = 302
        response.headers["Content-Length"] = "0"

        with (
            mock.patch.object(
                ff,
                "public_url_ips",
                return_value=["93.184.216.34"],
            ) as resolve,
            mock.patch.object(
                HTTPAdapter,
                "send",
                return_value=response,
            ) as base_send,
        ):
            returned = adapter.send(request)

        self.assertIs(returned, response)
        resolve.assert_called_once_with(request.url)
        base_send.assert_called_once()
        self.assertEqual(request._favi_pinned_ip, "93.184.216.34")
        self.assertEqual(request.headers["Host"], "feed.example:8443")

    def test_send_falls_back_across_validated_public_ips(self):
        adapter = safe_http.PinnedPublicAdapter()
        request = requests.Request("GET", "http://feed.example/a").prepare()
        response = requests.Response()
        response.status_code = 200
        with (
            mock.patch.object(
                ff,
                "public_url_ips",
                return_value=["93.184.216.34", "93.184.216.35"],
            ),
            mock.patch.object(
                HTTPAdapter,
                "send",
                side_effect=[
                    requests.ConnectionError("first unavailable"),
                    response,
                ],
            ) as base_send,
        ):
            returned = adapter.send(request)
        self.assertIs(returned, response)
        self.assertEqual(base_send.call_count, 2)
        self.assertEqual(request._favi_pinned_ip, "93.184.216.35")

    def test_tls_pool_connects_to_ip_but_verifies_original_hostname(self):
        adapter = safe_http.PinnedPublicAdapter()
        adapter.poolmanager = mock.Mock()
        request = requests.Request(
            "GET",
            "https://feed.example/products.xml",
        ).prepare()
        request._favi_pinned_ip = "93.184.216.34"

        adapter.get_connection_with_tls_context(
            request,
            verify=True,
            proxies=None,
            cert=None,
        )

        kwargs = adapter.poolmanager.connection_from_host.call_args.kwargs
        self.assertEqual(kwargs["host"], "93.184.216.34")
        self.assertEqual(kwargs["scheme"], "https")
        self.assertEqual(
            kwargs["pool_kwargs"]["assert_hostname"],
            "feed.example",
        )
        self.assertEqual(
            kwargs["pool_kwargs"]["server_hostname"],
            "feed.example",
        )

    def test_public_session_ignores_environment_proxies(self):
        session = safe_http.public_session()
        self.addCleanup(session.close)
        self.assertFalse(session.trust_env)
        self.assertIsInstance(
            session.adapters["https://"],
            safe_http.PinnedPublicAdapter,
        )


if __name__ == "__main__":
    unittest.main(verbosity=2)
