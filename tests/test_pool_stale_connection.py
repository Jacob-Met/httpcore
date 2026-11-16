from unittest.mock import AsyncMock, Mock

import pytest

import httpcore


def make_connection(*, stale=False, asynchronous=False):
    connection = Mock()
    connection.is_closed.return_value = False
    connection.has_expired.return_value = False
    connection.is_idle.return_value = False
    connection.is_available.return_value = True
    connection.can_handle_request.return_value = True
    if asynchronous:
        connection.handle_async_request = AsyncMock(
            side_effect=(
                [
                    httpcore.ConnectionNotAvailable(),
                    AssertionError("unavailable connection was reused"),
                ]
                if stale
                else None
            ),
            return_value=None if stale else httpcore.Response(200, content=b"ok"),
        )
        connection.aclose = AsyncMock()
    else:
        connection.handle_request.side_effect = (
            [
                httpcore.ConnectionNotAvailable(),
                AssertionError("unavailable connection was reused"),
            ]
            if stale
            else None
        )
        if not stale:
            connection.handle_request.return_value = httpcore.Response(
                200, content=b"ok"
            )
    return connection


def test_sync_connection_not_available_is_not_reassigned(monkeypatch):
    stale = make_connection(stale=True)
    ready = make_connection()
    with httpcore.ConnectionPool(max_connections=1) as pool:
        pool._connections.append(stale)
        monkeypatch.setattr(pool, "create_connection", lambda origin: ready)
        response = pool.request("GET", "http://example.com/")
        assert response.content == b"ok"
        assert stale.handle_request.call_count == 1
        assert stale not in pool.connections
        stale.close.assert_called_once()


@pytest.mark.anyio
async def test_async_connection_not_available_is_not_reassigned(monkeypatch):
    stale = make_connection(stale=True, asynchronous=True)
    ready = make_connection(asynchronous=True)
    async with httpcore.AsyncConnectionPool(max_connections=1) as pool:
        pool._connections.append(stale)
        monkeypatch.setattr(pool, "create_connection", lambda origin: ready)
        response = await pool.request("GET", "http://example.com/")
        assert response.content == b"ok"
        assert stale.handle_async_request.await_count == 1
        assert stale not in pool.connections
        stale.aclose.assert_awaited_once()


def test_sync_unavailable_connection_does_not_block_pool_capacity(monkeypatch):
    stale = make_connection(stale=True)
    stale.is_available.return_value = False
    ready = make_connection()
    connections = iter([stale, ready])
    with httpcore.ConnectionPool(max_connections=1) as pool:
        monkeypatch.setattr(pool, "create_connection", lambda origin: next(connections))
        response = pool.request(
            "GET", "http://example.com/", extensions={"timeout": {"pool": 0.01}}
        )
        assert response.content == b"ok"
        assert stale not in pool.connections
        stale.close.assert_called_once()


@pytest.mark.anyio
async def test_async_unavailable_connection_does_not_block_pool_capacity(monkeypatch):
    stale = make_connection(stale=True, asynchronous=True)
    stale.is_available.return_value = False
    ready = make_connection(asynchronous=True)
    connections = iter([stale, ready])
    async with httpcore.AsyncConnectionPool(max_connections=1) as pool:
        monkeypatch.setattr(pool, "create_connection", lambda origin: next(connections))
        response = await pool.request(
            "GET", "http://example.com/", extensions={"timeout": {"pool": 0.01}}
        )
        assert response.content == b"ok"
        assert stale not in pool.connections
        stale.aclose.assert_awaited_once()


def test_sync_rejected_multiplexed_connection_closes_after_inflight_stream(monkeypatch):
    from concurrent.futures import ThreadPoolExecutor
    from threading import Event

    rejected = Event()
    stale = make_connection(stale=True)
    ready = make_connection()
    calls = 0

    def handle(request):
        nonlocal calls
        calls += 1
        if calls == 1:
            return httpcore.Response(200, content=b"first")
        rejected.set()
        raise httpcore.ConnectionNotAvailable()

    stale.handle_request.side_effect = handle
    connections = iter([stale, ready])
    with httpcore.ConnectionPool(max_connections=1) as pool:
        monkeypatch.setattr(pool, "create_connection", lambda origin: next(connections))
        first = pool.handle_request(httpcore.Request("GET", "http://example.com/"))
        with ThreadPoolExecutor(max_workers=1) as executor:
            future = executor.submit(
                pool.request,
                "GET",
                "http://example.com/",
                extensions={"timeout": {"pool": 1}},
            )
            assert rejected.wait(1)
            assert stale.close.call_count == 0
            first.close()
            assert future.result(timeout=2).content == b"ok"
        stale.close.assert_called_once()


@pytest.mark.anyio
async def test_async_rejected_multiplexed_connection_closes_after_inflight_stream(
    monkeypatch,
):
    import anyio

    rejected = anyio.Event()
    stale = make_connection(stale=True, asynchronous=True)
    ready = make_connection(asynchronous=True)
    calls = 0

    async def handle(request):
        nonlocal calls
        calls += 1
        if calls == 1:
            return httpcore.Response(200, content=b"first")
        rejected.set()
        raise httpcore.ConnectionNotAvailable()

    stale.handle_async_request.side_effect = handle
    connections = iter([stale, ready])
    results = []
    async with httpcore.AsyncConnectionPool(max_connections=1) as pool:
        monkeypatch.setattr(pool, "create_connection", lambda origin: next(connections))
        first = await pool.handle_async_request(
            httpcore.Request("GET", "http://example.com/")
        )

        async def send_second():
            response = await pool.request(
                "GET", "http://example.com/", extensions={"timeout": {"pool": 1}}
            )
            results.append(response.content)

        async with anyio.create_task_group() as group:
            group.start_soon(send_second)
            with anyio.fail_after(1):
                await rejected.wait()
            assert stale.aclose.await_count == 0
            await first.aclose()

    assert results == [b"ok"]
    stale.aclose.assert_awaited_once()
