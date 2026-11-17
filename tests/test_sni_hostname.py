"""TLS host names omit the absolute-DNS root dot, without rewriting the URL."""

import ssl
from typing import Any

import pytest

import httpcore

RESPONSE = [b"HTTP/1.1 200 OK\r\nContent-Length: 0\r\n\r\n"]


class RecordingSyncStream(httpcore.MockStream):
    def __init__(self, buffer: list[bytes]):
        super().__init__(buffer)
        self.sni_names: list[str | None] = []

    def start_tls(
        self,
        ssl_context: ssl.SSLContext,
        server_hostname: str | None = None,
        timeout: float | None = None,
    ) -> httpcore.NetworkStream:
        self.sni_names.append(server_hostname)
        return self


class RecordingSyncBackend(httpcore.MockBackend):
    def __init__(self, buffer: list[bytes]):
        super().__init__(buffer)
        self.stream = RecordingSyncStream(buffer)
        self.tcp_hosts: list[str] = []

    def connect_tcp(
        self, host: str, port: int, **kwargs: Any
    ) -> httpcore.NetworkStream:
        self.tcp_hosts.append(host)
        return self.stream


class RecordingAsyncStream(httpcore.AsyncMockStream):
    def __init__(self, buffer: list[bytes]):
        super().__init__(buffer)
        self.sni_names: list[str | None] = []

    async def start_tls(
        self,
        ssl_context: ssl.SSLContext,
        server_hostname: str | None = None,
        timeout: float | None = None,
    ) -> httpcore.AsyncNetworkStream:
        self.sni_names.append(server_hostname)
        return self


class RecordingAsyncBackend(httpcore.AsyncMockBackend):
    def __init__(self, buffer: list[bytes]):
        super().__init__(buffer)
        self.stream = RecordingAsyncStream(buffer)
        self.tcp_hosts: list[str] = []

    async def connect_tcp(
        self, host: str, port: int, **kwargs: Any
    ) -> httpcore.AsyncNetworkStream:
        self.tcp_hosts.append(host)
        return self.stream


@pytest.mark.parametrize(
    ("hostname", "expected_sni"),
    [
        ("example.com.", "example.com"),
        ("example.com", "example.com"),
        ("xn--bcher-kva.example.", "xn--bcher-kva.example"),
        ("example.com..", "example.com."),
    ],
)
@pytest.mark.parametrize("proxy_type", ["direct", "http", "socks5"])
def test_absolute_dns_hostname_sync(
    hostname: str, expected_sni: str, proxy_type: str
) -> None:
    # Mock streams record the server_hostname passed to the TLS handshake.
    # TCP destination resolution and HTTP request authority remain unchanged.
    if proxy_type == "http":
        chunks = [b"HTTP/1.1 200 Connection Established\r\n\r\n", *RESPONSE]
    elif proxy_type == "socks5":
        chunks = [
            b"\x05\x00",
            b"\x05\x00\x00\x01\xff\x00\x00\x01\x00\x50",
            *RESPONSE,
        ]
    else:
        chunks = RESPONSE.copy()
    backend = RecordingSyncBackend(chunks)
    proxy = (
        None
        if proxy_type == "direct"
        else httpcore.Proxy(f"{proxy_type}://proxy.example:8080")
    )
    with httpcore.ConnectionPool(network_backend=backend, proxy=proxy) as pool:
        response = pool.request("GET", f"https://{hostname}/")
        assert response.status == 200
        assert backend.stream.sni_names == [expected_sni]
        assert backend.tcp_hosts == [hostname if proxy is None else "proxy.example"]


def test_sni_extension_sync() -> None:
    backend = RecordingSyncBackend(RESPONSE.copy())
    with httpcore.ConnectionPool(network_backend=backend) as pool:
        response = pool.request(
            "GET",
            "https://example.com./",
            extensions={"sni_hostname": "override.example."},
        )
        assert response.status == 200
        assert backend.stream.sni_names == ["override.example"]
        assert backend.tcp_hosts == ["example.com."]


@pytest.mark.anyio
async def test_sni_extension_async() -> None:
    backend = RecordingAsyncBackend(RESPONSE.copy())
    async with httpcore.AsyncConnectionPool(network_backend=backend) as pool:
        response = await pool.request(
            "GET",
            "https://example.com./",
            extensions={"sni_hostname": "override.example."},
        )
        assert response.status == 200
        assert backend.stream.sni_names == ["override.example"]
        assert backend.tcp_hosts == ["example.com."]


@pytest.mark.anyio
@pytest.mark.parametrize(
    ("hostname", "expected_sni"),
    [
        ("example.com.", "example.com"),
        ("example.com", "example.com"),
        ("xn--bcher-kva.example.", "xn--bcher-kva.example"),
        ("example.com..", "example.com."),
    ],
)
@pytest.mark.parametrize("proxy_type", ["direct", "http", "socks5"])
async def test_absolute_dns_hostname_async(
    hostname: str, expected_sni: str, proxy_type: str
) -> None:
    if proxy_type == "http":
        chunks = [b"HTTP/1.1 200 Connection Established\r\n\r\n", *RESPONSE]
    elif proxy_type == "socks5":
        chunks = [
            b"\x05\x00",
            b"\x05\x00\x00\x01\xff\x00\x00\x01\x00\x50",
            *RESPONSE,
        ]
    else:
        chunks = RESPONSE.copy()
    backend = RecordingAsyncBackend(chunks)
    proxy = (
        None
        if proxy_type == "direct"
        else httpcore.Proxy(f"{proxy_type}://proxy.example:8080")
    )
    async with httpcore.AsyncConnectionPool(
        network_backend=backend, proxy=proxy
    ) as pool:
        response = await pool.request("GET", f"https://{hostname}/")
        assert response.status == 200
        assert backend.stream.sni_names == [expected_sni]
        assert backend.tcp_hosts == [hostname if proxy is None else "proxy.example"]
