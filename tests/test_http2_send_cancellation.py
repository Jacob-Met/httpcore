"""Cancellation while sending HTTP/2 request headers must release resources."""

import anyio
import pytest

import httpcore
from httpcore._synchronization import AsyncSemaphore


@pytest.mark.anyio
async def test_http2_cancelled_header_write_releases_stream_capacity():
    origin = httpcore.Origin(b"https", b"example.com", 443)
    conn = httpcore.AsyncHTTP2Connection(
        origin=origin, stream=httpcore.AsyncMockStream([])
    )
    conn._h2_state.initiate_connection()
    conn._sent_connection_init = True
    conn._max_streams = 1
    conn._max_streams_semaphore = AsyncSemaphore(1)
    paused_on_write = anyio.Event()
    resume_write = anyio.Event()

    async def paused_write(request):
        paused_on_write.set()
        await resume_write.wait()

    conn._write_outgoing_data = paused_write
    request = httpcore.Request(
        "GET", "https://example.com/", headers=[(b"host", b"example.com")]
    )

    async def send():
        await conn.handle_async_request(request)

    async with anyio.create_task_group() as group:
        group.start_soon(send)
        with anyio.fail_after(1):
            await paused_on_write.wait()
        assert conn._events, "did not reach send after stream registration"
        group.cancel_scope.cancel()

    # A cancellation after stream allocation must not strand the permit.
    assert conn._events == {}
    # Counts lifetime streams, including the cancelled stream that was allocated.
    assert conn._request_count == 1
    with anyio.fail_after(1):
        await conn._max_streams_semaphore.acquire()
    await conn.aclose()


@pytest.mark.anyio
async def test_http2_cancel_before_stream_allocation_releases_capacity():
    origin = httpcore.Origin(b"https", b"example.com", 443)
    conn = httpcore.AsyncHTTP2Connection(
        origin=origin, stream=httpcore.AsyncMockStream([])
    )
    conn._h2_state.initiate_connection()
    conn._sent_connection_init = True
    conn._max_streams = 1
    conn._max_streams_semaphore = AsyncSemaphore(1)
    paused_before_allocate = anyio.Event()
    resume_allocate = anyio.Event()

    async def paused_headers(request):
        paused_before_allocate.set()
        await resume_allocate.wait()
        raise AssertionError("should have been cancelled")

    conn._send_request_headers = paused_headers
    request = httpcore.Request(
        "GET", "https://example.com/", headers=[(b"host", b"example.com")]
    )

    async def send():
        await conn.handle_async_request(request)

    async with anyio.create_task_group() as group:
        group.start_soon(send)
        with anyio.fail_after(1):
            await paused_before_allocate.wait()
        group.cancel_scope.cancel()

    assert conn._events == {}
    assert conn._request_count == 0
    with anyio.fail_after(1):
        await conn._max_streams_semaphore.acquire()
    await conn.aclose()
