import hpack
import hyperframe.frame
import pytest
from tests import concurrency

import httpcore



def test_http2_connection():
    origin = httpcore.Origin(b"https", b"example.com", 443)
    stream = httpcore.MockStream(
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
                stream_id=1, data=b"Hello, world!", flags=["END_STREAM"]
            ).serialize(),
        ]
    )
    with httpcore.HTTP2Connection(
        origin=origin, stream=stream, keepalive_expiry=5.0
    ) as conn:
        response = conn.request("GET", "https://example.com/")
        assert response.status == 200
        assert response.content == b"Hello, world!"

        assert conn.is_idle()
        assert conn.is_available()
        assert not conn.is_closed()
        assert not conn.has_expired()
        assert (
            conn.info() == "'https://example.com:443', HTTP/2, IDLE, Request Count: 1"
        )
        assert (
            repr(conn)
            == "<HTTP2Connection ['https://example.com:443', IDLE, Request Count: 1]>"
        )



def test_http2_connection_closed():
    origin = httpcore.Origin(b"https", b"example.com", 443)
    stream = httpcore.MockStream(
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
                stream_id=1, data=b"Hello, world!", flags=["END_STREAM"]
            ).serialize(),
            # Connection is closed after the first response
            hyperframe.frame.GoAwayFrame(
                stream_id=0, error_code=0, last_stream_id=1
            ).serialize(),
        ]
    )
    with httpcore.HTTP2Connection(
        origin=origin, stream=stream, keepalive_expiry=5.0
    ) as conn:
        conn.request("GET", "https://example.com/")

        with pytest.raises(httpcore.ConnectionNotAvailable):
            conn.request("GET", "https://example.com/")

        assert not conn.is_available()



def test_http2_connection_post_request():
    origin = httpcore.Origin(b"https", b"example.com", 443)
    stream = httpcore.MockStream(
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
                stream_id=1, data=b"Hello, world!", flags=["END_STREAM"]
            ).serialize(),
        ]
    )
    with httpcore.HTTP2Connection(origin=origin, stream=stream) as conn:
        response = conn.request(
            "POST",
            "https://example.com/",
            headers={b"content-length": b"17"},
            content=b'{"data": "upload"}',
        )
        assert response.status == 200
        assert response.content == b"Hello, world!"



def test_http2_connection_with_remote_protocol_error():
    """
    If a remote protocol error occurs, then no response will be returned,
    and the connection will not be reusable.
    """
    origin = httpcore.Origin(b"https", b"example.com", 443)
    stream = httpcore.MockStream([b"Wait, this isn't valid HTTP!", b""])
    with httpcore.HTTP2Connection(origin=origin, stream=stream) as conn:
        with pytest.raises(httpcore.RemoteProtocolError):
            conn.request("GET", "https://example.com/")



def test_http2_connection_with_rst_stream():
    """
    If a stream reset occurs, then no response will be returned,
    but the connection will remain reusable for other requests.
    """
    origin = httpcore.Origin(b"https", b"example.com", 443)
    stream = httpcore.MockStream(
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
            # Stream is closed midway through the first response...
            hyperframe.frame.RstStreamFrame(stream_id=1, error_code=8).serialize(),
            # ...Which doesn't prevent the second response.
            hyperframe.frame.HeadersFrame(
                stream_id=3,
                data=hpack.Encoder().encode(
                    [
                        (b":status", b"200"),
                        (b"content-type", b"plain/text"),
                    ]
                ),
                flags=["END_HEADERS"],
            ).serialize(),
            hyperframe.frame.DataFrame(
                stream_id=3, data=b"Hello, world!", flags=["END_STREAM"]
            ).serialize(),
            b"",
        ]
    )
    with httpcore.HTTP2Connection(origin=origin, stream=stream) as conn:
        with pytest.raises(httpcore.RemoteProtocolError):
            conn.request("GET", "https://example.com/")
        response = conn.request("GET", "https://example.com/")
        assert response.status == 200



def test_http2_connection_with_goaway():
    """
    If a GoAway frame occurs, then no response will be returned,
    and the connection will not be reusable for other requests.
    """
    origin = httpcore.Origin(b"https", b"example.com", 443)
    stream = httpcore.MockStream(
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
            # Connection is closed midway through the first response...
            hyperframe.frame.GoAwayFrame(stream_id=0, error_code=0).serialize(),
            # ...We'll never get to this second response.
            hyperframe.frame.HeadersFrame(
                stream_id=3,
                data=hpack.Encoder().encode(
                    [
                        (b":status", b"200"),
                        (b"content-type", b"plain/text"),
                    ]
                ),
                flags=["END_HEADERS"],
            ).serialize(),
            hyperframe.frame.DataFrame(
                stream_id=3, data=b"Hello, world!", flags=["END_STREAM"]
            ).serialize(),
            b"",
        ]
    )
    with httpcore.HTTP2Connection(origin=origin, stream=stream) as conn:
        # The initial request has been closed midway, with an unrecoverable error.
        with pytest.raises(httpcore.RemoteProtocolError):
            conn.request("GET", "https://example.com/")

        # The second request can receive a graceful `ConnectionNotAvailable`,
        # and may be retried on a new connection.
        with pytest.raises(httpcore.ConnectionNotAvailable):
            conn.request("GET", "https://example.com/")



def test_http2_connection_with_flow_control():
    origin = httpcore.Origin(b"https", b"example.com", 443)
    stream = httpcore.MockStream(
        [
            hyperframe.frame.SettingsFrame().serialize(),
            # Available flow: 65,535
            hyperframe.frame.WindowUpdateFrame(
                stream_id=0, window_increment=10_000
            ).serialize(),
            hyperframe.frame.WindowUpdateFrame(
                stream_id=1, window_increment=10_000
            ).serialize(),
            # Available flow: 75,535
            hyperframe.frame.WindowUpdateFrame(
                stream_id=0, window_increment=10_000
            ).serialize(),
            hyperframe.frame.WindowUpdateFrame(
                stream_id=1, window_increment=10_000
            ).serialize(),
            # Available flow: 85,535
            hyperframe.frame.WindowUpdateFrame(
                stream_id=0, window_increment=10_000
            ).serialize(),
            hyperframe.frame.WindowUpdateFrame(
                stream_id=1, window_increment=10_000
            ).serialize(),
            # Available flow: 95,535
            hyperframe.frame.WindowUpdateFrame(
                stream_id=0, window_increment=10_000
            ).serialize(),
            hyperframe.frame.WindowUpdateFrame(
                stream_id=1, window_increment=10_000
            ).serialize(),
            # Available flow: 105,535
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
                stream_id=1, data=b"100,000 bytes received", flags=["END_STREAM"]
            ).serialize(),
        ]
    )
    with httpcore.HTTP2Connection(origin=origin, stream=stream) as conn:
        response = conn.request(
            "POST",
            "https://example.com/",
            content=b"x" * 100_000,
        )
        assert response.status == 200
        assert response.content == b"100,000 bytes received"



def test_http2_connection_attempt_close():
    """
    A connection can only be closed when it is idle.
    """
    origin = httpcore.Origin(b"https", b"example.com", 443)
    stream = httpcore.MockStream(
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
                stream_id=1, data=b"Hello, world!", flags=["END_STREAM"]
            ).serialize(),
        ]
    )
    with httpcore.HTTP2Connection(origin=origin, stream=stream) as conn:
        with conn.stream("GET", "https://example.com/") as response:
            response.read()
            assert response.status == 200
            assert response.content == b"Hello, world!"

        conn.close()
        with pytest.raises(httpcore.ConnectionNotAvailable):
            conn.request("GET", "https://example.com/")



def test_http2_request_to_incorrect_origin():
    """
    A connection can only send requests to whichever origin it is connected to.
    """
    origin = httpcore.Origin(b"https", b"example.com", 443)
    stream = httpcore.MockStream([])
    with httpcore.HTTP2Connection(origin=origin, stream=stream) as conn:
        with pytest.raises(RuntimeError):
            conn.request("GET", "https://other.com/")



def test_http2_remote_max_streams_update():
    """
    If the remote server updates the maximum concurrent streams value, we should
    be adjusting how many streams we will allow.
    """
    origin = httpcore.Origin(b"https", b"example.com", 443)
    stream = httpcore.MockStream(
        [
            hyperframe.frame.SettingsFrame(
                settings={hyperframe.frame.SettingsFrame.MAX_CONCURRENT_STREAMS: 1000}
            ).serialize(),
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
            hyperframe.frame.DataFrame(stream_id=1, data=b"Hello, world!").serialize(),
            hyperframe.frame.SettingsFrame(
                settings={hyperframe.frame.SettingsFrame.MAX_CONCURRENT_STREAMS: 50}
            ).serialize(),
            hyperframe.frame.DataFrame(
                stream_id=1, data=b"Hello, world...again!", flags=["END_STREAM"]
            ).serialize(),
        ]
    )
    with httpcore.HTTP2Connection(origin=origin, stream=stream) as conn:
        with conn.stream("GET", "https://example.com/") as response:
            i = 0
            for chunk in response.iter_stream():
                if i == 0:
                    assert chunk == b"Hello, world!"
                    assert conn._h2_state.remote_settings.max_concurrent_streams == 1000
                    assert conn._max_streams == min(
                        conn._h2_state.remote_settings.max_concurrent_streams,
                        conn._h2_state.local_settings.max_concurrent_streams,
                    )
                elif i == 1:
                    assert chunk == b"Hello, world...again!"
                    assert conn._h2_state.remote_settings.max_concurrent_streams == 50
                    assert conn._max_streams == min(
                        conn._h2_state.remote_settings.max_concurrent_streams,
                        conn._h2_state.local_settings.max_concurrent_streams,
                    )
                i += 1



def test_http2_concurrent_send_request_headers() -> None:
    origin = httpcore.Origin(b"https", b"example.com", 443)
    stream = httpcore.MockStream([])
    conn = httpcore.HTTP2Connection(origin=origin, stream=stream)
    conn._sent_connection_init = True

    def send_headers(worker_id: int) -> None:
        request = httpcore.Request(
            method=b"GET",
            url=f"https://example.com/item/{worker_id}",
            headers=[
                (b"host", b"example.com"),
                (b"user-agent", b"httpcore-test"),
            ]
            + [
                (f"x-custom-{k}".encode(), f"val-{k}-{worker_id}".encode())
                for k in range(15)
            ],
        )
        stream_id = conn._send_request_headers(request=request)
        assert stream_id > 0

    with concurrency.open_nursery() as nursery:
        for i in range(10):
            nursery.start_soon(send_headers, i)


def test_http2_concurrent_body_flow_control_credit():
    """Two uploads must not both spend the same connection flow-control credit."""
    from concurrent.futures import ThreadPoolExecutor
    from threading import Barrier
    from unittest import mock

    origin = httpcore.Origin(b"https", b"example.com", 443)
    stream = httpcore.MockStream(
        [
            hyperframe.frame.SettingsFrame().serialize(),
            hyperframe.frame.WindowUpdateFrame(
                stream_id=0, window_increment=512
            ).serialize(),
        ]
    )
    conn = httpcore.HTTP2Connection(origin=origin, stream=stream)
    conn._h2_state.initiate_connection()
    requests = [
        httpcore.Request(
            "POST",
            f"https://example.com/{i}",
            headers={b"host": b"example.com", b"content-length": b"200"},
            content=b"x" * 200,
        )
        for i in range(3)
    ]
    stream_ids = [conn._send_request_headers(request) for request in requests]

    remaining = 65535 - 256
    while remaining:
        amount = min(16384, remaining)
        conn._h2_state.send_data(stream_ids[0], b"x" * amount)
        remaining -= amount

    original_wait = conn._wait_for_outgoing_flow
    race = Barrier(2, timeout=5)

    def synchronize_credit_read(request, stream_id):
        flow = original_wait(request, stream_id)
        if flow == 256:
            race.wait()
        return flow

    sent = {stream_id: 0 for stream_id in stream_ids[1:]}
    original_send_data = conn._h2_state.send_data

    def track_send(stream_id, data, *args, **kwargs):
        original_send_data(stream_id, data, *args, **kwargs)
        if stream_id in sent:
            sent[stream_id] += len(data)

    def upload(index):
        conn._send_stream_data(requests[index], stream_ids[index], b"x" * 200)

    with mock.patch.object(conn, "_wait_for_outgoing_flow", synchronize_credit_read):
        with mock.patch.object(conn._h2_state, "send_data", track_send):
            with ThreadPoolExecutor(max_workers=2) as workers:
                list(workers.map(upload, (1, 2)))
    assert sent == {stream_ids[1]: 200, stream_ids[2]: 200}


def test_http2_wait_for_negative_body_flow_control_credit():
    import h2.settings

    settings = hyperframe.frame.SettingsFrame()
    settings.settings[h2.settings.SettingCodes.INITIAL_WINDOW_SIZE] = 200
    update = hyperframe.frame.WindowUpdateFrame(stream_id=1, window_increment=1000)
    origin = httpcore.Origin(b"https", b"example.com", 443)
    stream = httpcore.MockStream([settings.serialize(), update.serialize()])
    conn = httpcore.HTTP2Connection(origin=origin, stream=stream)
    conn._h2_state.initiate_connection()
    request = httpcore.Request(
        "POST",
        "https://example.com/",
        headers={b"host": b"example.com", b"content-length": b"700"},
        content=b"x" * 700,
    )
    stream_id = conn._send_request_headers(request)
    conn._h2_state.send_data(stream_id, b"x" * 500)
    conn._receive_events(request)
    assert conn._h2_state.local_flow_control_window(stream_id) == -300

    assert conn._wait_for_outgoing_flow(request, stream_id) == 700
    conn._send_stream_data(request, stream_id, b"x" * 200)
    assert conn._h2_state.local_flow_control_window(stream_id) == 500
