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


class AddressFamilyFilterTests(unittest.TestCase):
    """A dual-stack feed host must not be pinned to an unreachable family.

    Regression: www.webareal.sk resolves to both 81.0.206.104 and
    2001:1528:114::6625. On a runtime without IPv6, pinning the AAAA failed
    inside getaddrinfo with glibc EAI_ADDRFAMILY ("[Errno -9] Address family
    for hostname not supported") instead of falling through to the A record.
    """

    DUAL_STACK = ["2001:1528:114::6625", "81.0.206.104"]

    def test_ipv6_candidate_dropped_when_runtime_has_no_ipv6(self):
        with mock.patch.object(safe_http.urllib3_connection, "HAS_IPV6", False):
            self.assertEqual(
                safe_http._connectable(self.DUAL_STACK),
                ["81.0.206.104"],
            )

    def test_both_candidates_kept_when_runtime_has_ipv6(self):
        with mock.patch.object(safe_http.urllib3_connection, "HAS_IPV6", True):
            self.assertEqual(
                safe_http._connectable(self.DUAL_STACK),
                self.DUAL_STACK,
            )

    def test_ipv6_only_host_is_still_attempted(self):
        """Keep the real connection error rather than reporting no address."""
        with mock.patch.object(safe_http.urllib3_connection, "HAS_IPV6", False):
            self.assertEqual(
                safe_http._connectable(["2001:1528:114::6625"]),
                ["2001:1528:114::6625"],
            )

    def test_send_skips_unreachable_ipv6_and_uses_the_a_record(self):
        adapter = safe_http.PinnedPublicAdapter()
        request = requests.Request(
            "GET",
            "https://www.webareal.sk/fotky15791/xml/heureka_sk.xml",
        ).prepare()
        response = requests.Response()
        response.status_code = 200

        with (
            mock.patch.object(safe_http.urllib3_connection, "HAS_IPV6", False),
            mock.patch.object(
                ff,
                "public_url_ips",
                return_value=list(self.DUAL_STACK),
            ),
            mock.patch.object(
                HTTPAdapter,
                "send",
                return_value=response,
            ) as base_send,
        ):
            returned = adapter.send(request)

        self.assertIs(returned, response)
        self.assertEqual(base_send.call_count, 1)
        self.assertEqual(request._favi_pinned_ip, "81.0.206.104")
        self.assertEqual(request.headers["Host"], "www.webareal.sk")

    def test_send_reports_the_reachable_family_error_not_ipv6_noise(self):
        """The old loop kept only last_error, so IPv6 noise masked real faults."""
        adapter = safe_http.PinnedPublicAdapter()
        request = requests.Request("GET", "https://feed.example/a.xml").prepare()

        with (
            mock.patch.object(safe_http.urllib3_connection, "HAS_IPV6", False),
            mock.patch.object(
                ff,
                "public_url_ips",
                return_value=list(self.DUAL_STACK),
            ),
            mock.patch.object(
                HTTPAdapter,
                "send",
                side_effect=requests.ConnectionError("ipv4 refused"),
            ) as base_send,
        ):
            with self.assertRaises(requests.ConnectionError) as caught:
                adapter.send(request)

        self.assertEqual(base_send.call_count, 1)
        self.assertIn("ipv4 refused", str(caught.exception))
        self.assertNotIn("2001:1528:114::6625", str(caught.exception))

    def test_public_url_ips_still_validates_every_resolved_address(self):
        """Filtering happens at connect time only; the SSRF policy is intact."""
        with (
            mock.patch.object(safe_http.urllib3_connection, "HAS_IPV6", False),
            mock.patch.object(
                ff,
                "_resolve_ips",
                return_value=["81.0.206.104", "fd00::1"],
            ),
        ):
            with self.assertRaises(ff.FeedDownloadError):
                ff.public_url_ips("https://feed.example/a.xml")


if __name__ == "__main__":
    unittest.main(verbosity=2)
