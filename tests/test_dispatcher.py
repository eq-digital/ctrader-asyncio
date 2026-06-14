"""Tests for ctrader_asyncio.dispatcher."""

from __future__ import annotations

import asyncio
import struct
import sys
from pathlib import Path
from unittest.mock import AsyncMock, MagicMock

import pytest

_PROTO_DIR = str(Path(__file__).parent.parent / "src" / "ctrader_asyncio" / "proto")
if _PROTO_DIR not in sys.path:
    sys.path.insert(0, _PROTO_DIR)

from ctrader_asyncio.dispatcher import Dispatcher, REQUEST_TIMEOUT_SECONDS
from ctrader_asyncio.exceptions import (
    ConnectionLostError,
    RequestTimeoutError,
    ServerError,
)
from ctrader_asyncio.proto import OpenApiCommonMessages_pb2 as _common


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def _make_envelope(payload_type: int, client_msg_id: str = "") -> _common.ProtoMessage:
    """Build a minimal ProtoMessage envelope."""
    kwargs: dict = {"payloadType": payload_type}
    if client_msg_id:
        kwargs["clientMsgId"] = client_msg_id
    return _common.ProtoMessage(**kwargs)


def _make_error_envelope(client_msg_id: str = "") -> _common.ProtoMessage:
    """Build a ProtoErrorRes envelope."""
    inner = _common.ProtoErrorRes(errorCode="NOT_AUTHENTICATED", description="Bad token")
    kwargs: dict = {
        "payloadType": 50,
        "payload": inner.SerializeToString(),
    }
    if client_msg_id:
        kwargs["clientMsgId"] = client_msg_id
    return _common.ProtoMessage(**kwargs)


def _fake_connection(send_side_effect=None) -> MagicMock:
    conn = MagicMock()
    if send_side_effect:
        conn.send = AsyncMock(side_effect=send_side_effect)
    else:
        conn.send = AsyncMock()
    return conn


# ---------------------------------------------------------------------------
# Registration
# ---------------------------------------------------------------------------

def test_register_adds_callback():
    d = Dispatcher()
    cb = AsyncMock()
    d.register(51, cb)
    assert cb in d._callbacks[51]


def test_register_multiple_callbacks_same_type():
    d = Dispatcher()
    cb1, cb2 = AsyncMock(), AsyncMock()
    d.register(51, cb1)
    d.register(51, cb2)
    assert d._callbacks[51] == [cb1, cb2]


def test_unregister_removes_callback():
    d = Dispatcher()
    cb = AsyncMock()
    d.register(51, cb)
    d.unregister(51, cb)
    assert cb not in d._callbacks.get(51, [])


def test_unregister_silently_ignores_missing():
    d = Dispatcher()
    cb = AsyncMock()
    d.unregister(51, cb)  # Should not raise


# ---------------------------------------------------------------------------
# route() — event callbacks
# ---------------------------------------------------------------------------

@pytest.mark.asyncio
async def test_route_fires_callback_for_unsolicited_event():
    d = Dispatcher()
    cb = AsyncMock()
    d.register(51, cb)

    envelope = _make_envelope(51)
    await d.route(envelope)

    cb.assert_awaited_once_with(envelope)


@pytest.mark.asyncio
async def test_route_fires_multiple_callbacks_in_order():
    d = Dispatcher()
    order = []
    async def cb1(e): order.append(1)
    async def cb2(e): order.append(2)
    d.register(51, cb1)
    d.register(51, cb2)

    await d.route(_make_envelope(51))
    assert order == [1, 2]


@pytest.mark.asyncio
async def test_route_ignores_unknown_payload_type():
    d = Dispatcher()
    # No callback registered — should not raise
    await d.route(_make_envelope(9999))


@pytest.mark.asyncio
async def test_route_callback_exception_does_not_crash_loop():
    d = Dispatcher()
    async def bad_cb(e): raise RuntimeError("boom")
    good_cb = AsyncMock()
    d.register(51, bad_cb)
    d.register(51, good_cb)

    await d.route(_make_envelope(51))
    good_cb.assert_awaited_once()


# ---------------------------------------------------------------------------
# route() — response correlation
# ---------------------------------------------------------------------------

@pytest.mark.asyncio
async def test_route_resolves_pending_future():
    d = Dispatcher()
    loop = asyncio.get_running_loop()
    future = loop.create_future()
    d._pending["req-1"] = future

    envelope = _make_envelope(51, client_msg_id="req-1")
    await d.route(envelope)

    assert future.done()
    assert future.result() is envelope


@pytest.mark.asyncio
async def test_route_error_envelope_sets_exception_on_future():
    d = Dispatcher()
    loop = asyncio.get_running_loop()
    future = loop.create_future()
    d._pending["req-1"] = future

    envelope = _make_error_envelope(client_msg_id="req-1")
    await d.route(envelope)

    assert future.done()
    with pytest.raises(ServerError) as exc_info:
        future.result()
    assert exc_info.value.error_code == "NOT_AUTHENTICATED"


@pytest.mark.asyncio
async def test_route_removes_future_from_pending_after_resolve():
    d = Dispatcher()
    loop = asyncio.get_running_loop()
    future = loop.create_future()
    d._pending["req-1"] = future

    await d.route(_make_envelope(51, client_msg_id="req-1"))
    assert "req-1" not in d._pending


@pytest.mark.asyncio
async def test_route_does_not_call_callbacks_for_correlated_response():
    d = Dispatcher()
    cb = AsyncMock()
    d.register(51, cb)

    loop = asyncio.get_running_loop()
    future = loop.create_future()
    d._pending["req-1"] = future

    await d.route(_make_envelope(51, client_msg_id="req-1"))
    cb.assert_not_awaited()


# ---------------------------------------------------------------------------
# send_request()
# ---------------------------------------------------------------------------

@pytest.mark.asyncio
async def test_send_request_returns_response():
    d = Dispatcher()
    conn = _fake_connection()

    request = _common.ProtoHeartbeatEvent()

    async def fake_send(msg):
        # Simulate server responding immediately
        client_msg_id = msg.clientMsgId
        envelope = _make_envelope(51, client_msg_id=client_msg_id)
        await d.route(envelope)

    conn.send = fake_send

    response = await d.send_request(conn, request)
    assert response.payloadType == 51


@pytest.mark.asyncio
async def test_send_request_assigns_client_msg_id():
    d = Dispatcher()
    conn = _fake_connection()
    assigned_ids = []

    async def fake_send(msg):
        assigned_ids.append(msg.clientMsgId)
        envelope = _make_envelope(51, client_msg_id=msg.clientMsgId)
        await d.route(envelope)

    conn.send = fake_send

    await d.send_request(conn, _common.ProtoHeartbeatEvent())
    assert len(assigned_ids) == 1
    assert len(assigned_ids[0]) == 36  # UUID format


@pytest.mark.asyncio
async def test_send_request_timeout_raises_request_timeout_error():
    d = Dispatcher()
    conn = _fake_connection()  # send is AsyncMock that does nothing — no response arrives

    with pytest.raises(RequestTimeoutError):
        await d.send_request(conn, _common.ProtoHeartbeatEvent(), timeout=0.05)


@pytest.mark.asyncio
async def test_send_request_timeout_clears_pending():
    d = Dispatcher()
    conn = _fake_connection()

    with pytest.raises(RequestTimeoutError):
        await d.send_request(conn, _common.ProtoHeartbeatEvent(), timeout=0.05)

    assert d.pending_count == 0


@pytest.mark.asyncio
async def test_send_request_send_failure_clears_pending():
    d = Dispatcher()
    conn = _fake_connection(send_side_effect=ConnectionLostError("gone"))

    with pytest.raises(ConnectionLostError):
        await d.send_request(conn, _common.ProtoHeartbeatEvent())

    assert d.pending_count == 0


@pytest.mark.asyncio
async def test_send_request_propagates_server_error():
    d = Dispatcher()
    conn = _fake_connection()

    async def fake_send(msg):
        envelope = _make_error_envelope(client_msg_id=msg.clientMsgId)
        await d.route(envelope)

    conn.send = fake_send

    with pytest.raises(ServerError) as exc_info:
        await d.send_request(conn, _common.ProtoHeartbeatEvent())

    assert exc_info.value.error_code == "NOT_AUTHENTICATED"


# ---------------------------------------------------------------------------
# cancel_all()
# ---------------------------------------------------------------------------

@pytest.mark.asyncio
async def test_cancel_all_rejects_all_pending_futures():
    d = Dispatcher()
    loop = asyncio.get_running_loop()

    futures = []
    for i in range(3):
        f = loop.create_future()
        d._pending[f"req-{i}"] = f
        futures.append(f)

    d.cancel_all()

    for f in futures:
        assert f.done()
        with pytest.raises(ConnectionLostError):
            f.result()


def test_cancel_all_clears_pending():
    d = Dispatcher()
    loop = asyncio.new_event_loop()
    try:
        for i in range(3):
            d._pending[f"req-{i}"] = loop.create_future()
        d.cancel_all()
        assert d.pending_count == 0
    finally:
        loop.close()


def test_cancel_all_on_empty_dispatcher_does_not_raise():
    d = Dispatcher()
    d.cancel_all()  # Should not raise


# ---------------------------------------------------------------------------
# pending_count
# ---------------------------------------------------------------------------

def test_pending_count_is_zero_initially():
    d = Dispatcher()
    assert d.pending_count == 0
