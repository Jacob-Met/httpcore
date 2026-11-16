import asyncio
import typing

import anyio
import hpack
import hyperframe
import pytest

import httpcore


class SlowWriteStream(httpcore.AsyncNetworkStream):
    """
    A stream that we can use to test cancellations during
    the request writing.
    """

    async def write(
        self, buffer: bytes, timeout: typing.Optional[float] = None
    ) -> None:
        await anyio.sleep(999)

    async def aclose(self) -> None:
        await anyio.sleep(0)


class HandshakeThenSlowWriteStream(httpcore.AsyncNetworkStream):
    """
    A stream that we can use to test cancellations during
    the HTTP/2 request writing, after allowing the initial
    handshake to complete.
    """

    def __init__(self) -> None:
        self._handshake_complete = False

    async def write(
        self, buffer: bytes, timeout: typing.Optional[float] = None
    ) -> None:
        if not self._handshake_complete:
            self._handshake_complete = True
        else:
            await anyio.sleep(999)

    async def aclose(self) -> None:
        await anyio.sleep(0)


class SlowReadStream(httpcore.AsyncNetworkStream):
    """
    A stream that we can use to test cancellations during
    the response reading.
    """

    def __init__(self, buffer: typing.List[bytes]):
        self._buffer = buffer

    async def write(self, buffer, timeout=None):
        pass

    async def read(
        self, max_bytes: int, timeout: typing.Optional[float] = None
    ) -> bytes:
        if not self._buffer:
            await anyio.sleep(999)
        return self._buffer.pop(0)

    async def aclose(self):
        await anyio.sleep(0)


class SlowWriteBackend(httpcore.AsyncNetworkBackend):
    async def connect_tcp(
        self,
        host: str,
        port: int,
        timeout: typing.Optional[float] = None,
        local_address: typing.Optional[str] = None,
        socket_options: typing.Optional[typing.Iterable[httpcore.SOCKET_OPTION]] = None,
    ) -> httpcore.AsyncNetworkStream:
        return SlowWriteStream()


class SlowReadBackend(httpcore.AsyncNetworkBackend):
    def __init__(self, buffer: typing.List[bytes]):
        self._buffer = buffer

    async def connect_tcp(
        self,
        host: str,
        port: int,
        timeout: typing.Optional[float] = None,
        local_address: typing.Optional[str] = None,
        socket_options: typing.Optional[typing.Iterable[httpcore.SOCKET_OPTION]] = None,
    ) -> httpcore.AsyncNetworkStream:
        return SlowReadStream(self._buffer)


@pytest.mark.anyio
async def test_connection_pool_timeout_during_request():
    """
    An async timeout when writing an HTTP/1.1 response on the connection pool
    should leave the pool in a consistent state.

    In this case, that means the connection will become closed, and no
    longer remain in the pool.
    """
    network_backend = SlowWriteBackend()
    async with httpcore.AsyncConnectionPool(network_backend=network_backend) as pool:
        with anyio.move_on_after(0.01):
            await pool.request("GET", "http://example.com")
        assert not pool.connections


@pytest.mark.anyio
async def test_connection_pool_timeout_during_response():
    """
    An async timeout when reading an HTTP/1.1 response on the connection pool
    should leave the pool in a consistent state.

    In this case, that means the connection will become closed, and no
    longer remain in the pool.
    """
    network_backend = SlowReadBackend(
        [
            b"HTTP/1.1 200 OK\r\n",
            b"Content-Type: plain/text\r\n",
            b"Content-Length: 1000\r\n",
            b"\r\n",
            b"Hello, world!...",
        ]
    )
    async with httpcore.AsyncConnectionPool(network_backend=network_backend) as pool:
        with anyio.move_on_after(0.01):
            await pool.request("GET", "http://example.com")
        assert not pool.connections


@pytest.mark.anyio
async def test_h11_timeout_during_request():
    """
    An async timeout on an HTTP/1.1 during the request writing
    should leave the connection in a neatly closed state.
    """
    origin = httpcore.Origin(b"http", b"example.com", 80)
    stream = SlowWriteStream()
    async with httpcore.AsyncHTTP11Connection(origin, stream) as conn:
        with anyio.move_on_after(0.01):
            await conn.request("GET", "http://example.com")
        assert conn.is_closed()


@pytest.mark.anyio
async def test_h11_timeout_during_response():
    """
    An async timeout on an HTTP/1.1 during the response reading
    should leave the connection in a neatly closed state.
    """
    origin = httpcore.Origin(b"http", b"example.com", 80)
    stream = SlowReadStream(
        [
            b"HTTP/1.1 200 OK\r\n",
            b"Content-Type: plain/text\r\n",
            b"Content-Length: 1000\r\n",
            b"\r\n",
            b"Hello, world!...",
        ]
    )
    async with httpcore.AsyncHTTP11Connection(origin, stream) as conn:
        with anyio.move_on_after(0.01):
            await conn.request("GET", "http://example.com")
        assert conn.is_closed()


@pytest.mark.xfail
@pytest.mark.anyio
async def test_h2_timeout_during_handshake():
    """
    An async timeout on an HTTP/2 during the initial handshake
    should leave the connection in a neatly closed state.
    """
    origin = httpcore.Origin(b"http", b"example.com", 80)
    stream = SlowWriteStream()
    async with httpcore.AsyncHTTP2Connection(origin, stream) as conn:
        with anyio.move_on_after(0.01):
            await conn.request("GET", "http://example.com")
        assert conn.is_closed()


@pytest.mark.xfail
@pytest.mark.anyio
async def test_h2_timeout_during_request():
    """
    An async timeout on an HTTP/2 during a request
    should leave the connection in a neatly idle state.

    The connection is not closed because it is multiplexed,
    and a timeout on one request does not require the entire
    connection be closed.
    """
    origin = httpcore.Origin(b"http", b"example.com", 80)
    stream = HandshakeThenSlowWriteStream()
    async with httpcore.AsyncHTTP2Connection(origin, stream) as conn:
        with anyio.move_on_after(0.01):
            await conn.request("GET", "http://example.com")

        assert not conn.is_closed()
        assert conn.is_idle()


@pytest.mark.xfail
@pytest.mark.anyio
async def test_h2_timeout_during_response():
    """
    An async timeout on an HTTP/2 during the response reading
    should leave the connection in a neatly idle state.

    The connection is not closed because it is multiplexed,
    and a timeout on one request does not require the entire
    connection be closed.
    """
    origin = httpcore.Origin(b"http", b"example.com", 80)
    stream = SlowReadStream(
        [
            hyperframe.frame.SettingsFrame().serialize(),
            hyperframe.frame.HeadersFrame(
                stream_id=1,
                data=hpack.Encoder().encode(
                    [
                        (b":status", b"200"),
                        (b"content-type", b"plain/text"),
                    ]
                ),
                flags=["END_HEADERS"],
            ).serialize(),
            hyperframe.frame.DataFrame(
                stream_id=1, data=b"Hello, world!...", flags=[]
            ).serialize(),
        ]
    )
    async with httpcore.AsyncHTTP2Connection(origin, stream) as conn:
        with anyio.move_on_after(0.01):
            await conn.request("GET", "http://example.com")

        assert not conn.is_closed()
        assert conn.is_idle()


def test_connection_pool_does_not_leak_slot_on_cancelled_request():
    """
    A request which is cancelled after the pool has assigned it a newly
    created connection, but before that connection has been established,
    must not leave the connection behind holding a pool slot.

    Such a connection can never be reaped, because it is neither closed,
    expired, nor idle, and can never be reused, because it is not available.
    Every occurrence therefore consumes a slot permanently, until the pool
    fails every subsequent request with `PoolTimeout`, even though the server
    is perfectly healthy.

    This drives `asyncio` directly rather than going through `anyio`, because
    it relies on a task being cancelled while it is suspended waiting for a
    connection to be assigned to it. Scope based cancellation, as used by
    `anyio.move_on_after` and by trio, is only delivered at the next
    checkpoint, which falls inside the connection's own error handling where
    it is already dealt with correctly.

    See https://github.com/encode/httpcore/issues/1093
    """

    async def scenario() -> None:
        network_backend = httpcore.AsyncMockBackend(
            [
                b"HTTP/1.1 200 OK\r\n",
                b"Content-Type: plain/text\r\n",
                b"Content-Length: 13\r\n",
                b"\r\n",
                b"Hello, world!",
            ]
        )

        async with httpcore.AsyncConnectionPool(
            max_connections=1, network_backend=network_backend
        ) as pool:
            # Occupy the pool's only slot, leaving the response body unread so
            # that the connection remains checked out.
            response = await pool.handle_async_request(
                httpcore.Request(
                    "GET", "http://a.example/", headers={"Host": "a.example"}
                )
            )

            # A request to a different origin queues up behind it.
            task = asyncio.ensure_future(
                pool.handle_async_request(
                    httpcore.Request(
                        "GET", "http://b.example/", headers={"Host": "b.example"}
                    )
                )
            )
            for _ in range(10):
                await asyncio.sleep(0)
            assert not task.done()

            # Closing the first response frees the slot. The pool evicts the
            # now idle connection and assigns a freshly created one to the
            # queued request. Cancelling that request before it is scheduled
            # to resume leaves nothing to establish the new connection.
            await response.aclose()
            task.cancel()
            with pytest.raises(asyncio.CancelledError):
                await task

            # The abandoned connection must not be left occupying the pool.
            assert pool.connections == []

            # ...and so the pool must still be usable.
            response = await pool.handle_async_request(
                httpcore.Request(
                    "GET",
                    "http://c.example/",
                    headers={"Host": "c.example"},
                    extensions={"timeout": {"pool": 1.0}},
                )
            )
            await response.aread()
            assert response.status == 200

    asyncio.run(scenario())


class InterruptibleCloseStream(SlowReadStream):
    def __init__(self, buffer: typing.List[bytes]):
        super().__init__(buffer)
        self.close_calls = 0

    async def aclose(self) -> None:
        self.close_calls += 1
        if self.close_calls == 1:
            raise asyncio.CancelledError


class InterruptibleCloseBackend(httpcore.AsyncNetworkBackend):
    def __init__(self, buffer: typing.List[bytes]):
        self.stream = InterruptibleCloseStream(buffer)

    async def connect_tcp(
        self,
        host: str,
        port: int,
        timeout: typing.Optional[float] = None,
        local_address: typing.Optional[str] = None,
        socket_options: typing.Optional[typing.Iterable[httpcore.SOCKET_OPTION]] = None,
    ) -> httpcore.AsyncNetworkStream:
        return self.stream


@pytest.mark.parametrize("read_body", [False, True])
def test_connection_pool_retries_interrupted_response_close(read_body):
    async def run_test() -> None:
        network_backend = InterruptibleCloseBackend(
            [
                b"HTTP/1.1 200 OK\r\n",
                b"Content-Length: 13\r\n",
                b"\r\n",
                b"Hello, world!",
            ]
        )
        async with httpcore.AsyncConnectionPool(
            network_backend=network_backend, max_keepalive_connections=0
        ) as pool:
            response = await pool.handle_async_request(
                httpcore.Request(
                    "GET", "http://example.com", headers={"Host": "example.com"}
                )
            )

            if read_body:
                await response.aread()

            with pytest.raises(asyncio.CancelledError):
                await response.aclose()

            assert bool(pool.connections) is not read_body
            await response.aclose()
            assert not pool.connections
            assert network_backend.stream.close_calls == 2
            await response.aclose()
            assert network_backend.stream.close_calls == 2

    asyncio.run(run_test())

class HandshakeThenBlockedWriteStream(httpcore.AsyncNetworkStream):
    """
    A stream that allows the HTTP/2 handshake to complete, then blocks on
    every subsequent write, signalling when it has done so.

    This lets a request be cancelled at a precise point, once its headers have
    been handed to `h2` but before they have reached the network.
    """

    def __init__(self) -> None:
        self._handshake_complete = False
        self.blocked = anyio.Event()

    async def write(
        self, buffer: bytes, timeout: typing.Optional[float] = None
    ) -> None:
        if not self._handshake_complete:
            self._handshake_complete = True
            return
        self.blocked.set()
        await anyio.sleep(999)

    async def read(
        self, max_bytes: int, timeout: typing.Optional[float] = None
    ) -> bytes:
        await anyio.sleep(999)  # pragma: nocover
        return b""  # pragma: nocover

    async def aclose(self) -> None:
        await anyio.sleep(0)


@pytest.mark.anyio
async def test_h2_stream_count_stays_in_sync_after_cancellation():
    """
    Cancelling an HTTP/2 request releases our own stream semaphore, so `h2`
    must be told that the stream is finished as well.

    If the two drift apart then the connection eventually rejects new streams
    with "Max outbound streams is 100, 100 open", while we still believe there
    is spare capacity available.

    See https://github.com/encode/httpcore/issues/1022
    """
    origin = httpcore.Origin(b"http", b"example.com", 80)
    stream = HandshakeThenBlockedWriteStream()
    async with httpcore.AsyncHTTP2Connection(origin, stream) as conn:
        # An overall ceiling, so that a regression here fails rather than
        # hanging. The cancellation itself is triggered deterministically,
        # once the request headers are known to have reached the stream.
        with anyio.move_on_after(5.0):
            async with anyio.create_task_group() as task_group:

                async def send_request() -> None:
                    await conn.request("GET", "http://example.com")

                task_group.start_soon(send_request)
                await stream.blocked.wait()
                task_group.cancel_scope.cancel()

        # We have released our own stream slot, so h2's count of open streams
        # has to agree, or the two drift apart by one on every cancellation
        # until no further streams can be opened at all.
        assert conn._h2_state.open_outbound_streams == 0
