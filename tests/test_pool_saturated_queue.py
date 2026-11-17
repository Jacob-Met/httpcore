"""Saturated pools must not repeatedly search every waiting request."""

from unittest.mock import patch

import pytest

from httpcore._async.connection_pool import AsyncConnectionPool, AsyncPoolRequest
from httpcore._models import Request
from httpcore._sync.connection_pool import ConnectionPool, PoolRequest


@pytest.mark.anyio
async def test_async_saturated_pool_defers_queue_search():
    pool = AsyncConnectionPool(max_connections=1)
    first = AsyncPoolRequest(Request(b"GET", b"http://a.example/"))
    pool._requests.append(first)
    pool._assign_requests_to_connections()
    connection = first.connection
    assert connection is not None

    queued = [
        AsyncPoolRequest(Request(b"GET", f"http://{i}.example/".encode()))
        for i in range(100)
    ]
    pool._requests.extend(queued)
    with patch.object(connection, "can_handle_request", wraps=connection.can_handle_request) as can_handle:
        assert pool._assign_requests_to_connections() == []
        assert can_handle.call_count == 0
    assert all(request.is_queued() for request in queued)

    pool._requests.remove(first)
    pool._connections.remove(connection)
    closing = pool._assign_requests_to_connections()
    assert queued[0].connection is not None
    await pool._close_connections(closing)
    await pool.aclose()


def test_sync_saturated_pool_defers_queue_search():
    pool = ConnectionPool(max_connections=1)
    first = PoolRequest(Request(b"GET", b"http://a.example/"))
    pool._requests.append(first)
    pool._assign_requests_to_connections()
    connection = first.connection
    assert connection is not None

    queued = [
        PoolRequest(Request(b"GET", f"http://{i}.example/".encode()))
        for i in range(100)
    ]
    pool._requests.extend(queued)
    with patch.object(connection, "can_handle_request", wraps=connection.can_handle_request) as can_handle:
        assert pool._assign_requests_to_connections() == []
        assert can_handle.call_count == 0
    assert all(request.is_queued() for request in queued)

    pool._requests.remove(first)
    pool._connections.remove(connection)
    closing = pool._assign_requests_to_connections()
    assert queued[0].connection is not None
    pool._close_connections(closing)
    pool.close()
