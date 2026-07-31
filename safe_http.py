"""Requests transport that pins each feed URL to pre-validated public IPs."""
from __future__ import annotations

from urllib.parse import urlparse

import requests
from requests.adapters import HTTPAdapter

import feed_filter as ff


def _host_header(url: str) -> str:
    parsed = urlparse(url)
    host = parsed.hostname or ""
    if ":" in host:
        host = f"[{host}]"
    port = parsed.port
    default_port = 443 if parsed.scheme.lower() == "https" else 80
    return f"{host}:{port}" if port and port != default_port else host


class PinnedPublicAdapter(HTTPAdapter):
    """Resolve once, reject non-public results, and connect to that exact IP."""

    def send(self, request, **kwargs):
        public_ips = ff.public_url_ips(request.url)
        request.headers["Host"] = _host_header(request.url)
        last_error = None
        for ip_str in public_ips:
            request._favi_pinned_ip = ip_str
            try:
                return super().send(request, **kwargs)
            except requests.ConnectionError as exc:
                last_error = exc
        if last_error is not None:
            raise last_error
        raise ff.FeedDownloadError("The feed host has no usable public address.")

    def get_connection_with_tls_context(
        self,
        request,
        verify,
        proxies=None,
        cert=None,
    ):
        host_params, pool_kwargs = self.build_connection_pool_key_attributes(
            request,
            verify,
            cert,
        )
        pinned_ip = getattr(request, "_favi_pinned_ip", None)
        if not pinned_ip:
            raise ff.FeedDownloadError(
                "The feed server connection was not pinned safely."
            )
        if host_params["scheme"] == "https":
            pool_kwargs["assert_hostname"] = host_params["host"]
            pool_kwargs["server_hostname"] = host_params["host"]
        return self.poolmanager.connection_from_host(
            scheme=host_params["scheme"],
            host=pinned_ip,
            port=host_params["port"],
            pool_kwargs=pool_kwargs,
        )


def public_session() -> requests.Session:
    """Build a direct-only session that never performs a second DNS lookup."""
    session = requests.Session()
    session.trust_env = False
    session.mount("http://", PinnedPublicAdapter(max_retries=0))
    session.mount("https://", PinnedPublicAdapter(max_retries=0))
    return session
