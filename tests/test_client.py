"""Tests for ctrader_asyncio.client.

All tests mock the network layer — no real TCP connections are made.
"""

from __future__ import annotations

import asyncio
import sys
from pathlib import Path
from unittest.mock import AsyncMock, MagicMock, patch

import pytest

_PROTO_DIR = str(Path(__file__).parent.parent / "src" / "ctrader_asyncio" / "proto")
if _PROTO_DIR not in sys.path:
    sys.path.insert(0, _PROTO_DIR)

from ctrader_asyncio.client import CTraderClient
from ctrader_asyncio.exceptions import ConnectionLostError
from ctrader_asyncio.proto import OpenApiCommonMessages_pb2 as _common


async def _dummy_token(account_id: int) -> str:
    return f"token-{account_id}"


def _make_client(**kwargs) -> CTraderClient:
    defaults = dict(
        host="demo.ctraderapi.com",
        client_id="cid",
        client_secret="csecret",
        account_ids=[111],
        token_provider=_dummy_token,
        reconnect=False,
    )
    defaults.update(kwargs)
    return CTraderClient(**defaults)


def _mock_connection(receive_side_effect=None):
    conn = MagicMock()
    conn.open = AsyncMock()
    conn.close = AsyncMock()
    conn.send = AsyncMock()
    conn.is_open = True
    if receive_side_effect:
        conn.receive = AsyncMock(side_effect=receive_side_effect)
    else:
        conn.receive = AsyncMock(side_effect=asyncio.CancelledError)
    return conn


def test_client_stores_constructor_args():
    async def tok(aid): return "t"
    client = CTraderClient(
        host="live.ctraderapi.com",
        client_id="cid",
        client_secret="sec",
        account_ids=[1, 2, 3],
        token_provider=tok,
        port=9999,
        reconnect=False,
    )
    assert client.host == "live.ctraderapi.com"
    assert client.client_id == "cid"
    assert client.client_secret == "sec"
    assert client.account_ids == [1, 2, 3]
    assert client.port == 9999
    assert client.reconnect is False


def test_client_default_port_and_reconnect():
    client = _make_client()
    assert client.port == 5035
    assert client.reconnect is False


def test_register_stores_callback():
    client = _make_client()
    cb = AsyncMock()
    client.register(2126, cb)
    assert cb in client._event_callbacks[2126]


def test_unregister_removes_callback():
    client = _make_client()
    cb = AsyncMock()
    client.register(2126, cb)
    client.unregister(2126, cb)
    assert cb not in client._event_callbacks.get(2126, [])


def test_unregister_silently_ignores_missing():
    client = _make_client()
    client.unregister(9999, AsyncMock())


def test_register_propagates_to_live_dispatcher():
    client = _make_client()
    dispatcher = MagicMock()
    client._dispatcher = dispatcher
    cb = AsyncMock()
    client.register(51, cb)
    dispatcher.register.assert_called_once_with(51, cb)


@pytest.mark.asyncio
async def test_send_request_raises_when_not_connected():
    client = _make_client()
    with pytest.raises(ConnectionLostError):
        await client.send_request(_common.ProtoHeartbeatEvent())


@pytest.mark.asyncio
async def test_wait_until_ready_times_out_when_not_connected():
    client = _make_client()
    with pytest.raises(asyncio.TimeoutError):
        await client.wait_until_ready(timeout=0.05)


@pytest.mark.asyncio
async def test_client_connects_auths_and_becomes_ready():
    """Happy path: wait_until_ready() resolves after auth completes."""
    conn = _mock_connection()
    ready_was_set = False

    async def patched_auth_app(dispatcher, connection, client_id, client_secret, app_authed):
        app_authed.set()

    async def patched_auth_accounts(dispatcher, connection, account_ids, token_provider):
        pass

    async def fake_receive_loop(self, self_conn, self_disp):
        nonlocal ready_was_set
        ready_was_set = self._ready.is_set()
        raise asyncio.CancelledError

    with patch("ctrader_asyncio.client.Connection", return_value=conn), \
         patch("ctrader_asyncio.client.authenticate_app", side_effect=patched_auth_app), \
         patch("ctrader_asyncio.client.authenticate_accounts", side_effect=patched_auth_accounts), \
         patch("ctrader_asyncio.client.run_heartbeat", new=AsyncMock(side_effect=asyncio.CancelledError)), \
         patch.object(CTraderClient, "_receive_loop", fake_receive_loop):

        client = _make_client()
        async with client:
            try:
                await client.wait_until_ready(timeout=2.0)
            except asyncio.TimeoutError:
                pass

    assert ready_was_set


@pytest.mark.asyncio
async def test_client_close_is_idempotent():
    client = _make_client()
    await client.close()
    await client.close()


@pytest.mark.asyncio
async def test_client_context_manager_cleans_up():
    conn = _mock_connection()

    with patch("ctrader_asyncio.client.Connection", return_value=conn), \
         patch("ctrader_asyncio.client.authenticate_app", new=AsyncMock()), \
         patch("ctrader_asyncio.client.authenticate_accounts", new=AsyncMock()), \
         patch("ctrader_asyncio.client.run_heartbeat", new=AsyncMock(side_effect=asyncio.CancelledError)):

        client = _make_client()
        async with client:
            pass

        assert client._lifecycle_task is None or client._lifecycle_task.done()


@pytest.mark.asyncio
async def test_reconnect_false_does_not_retry():
    conn = _mock_connection()
    call_count = 0

    async def failing_open():
        nonlocal call_count
        call_count += 1
        raise OSError("refused")

    conn.open = failing_open

    with patch("ctrader_asyncio.client.Connection", return_value=conn):
        client = _make_client(reconnect=False)
        async with client:
            await asyncio.sleep(0.1)

    assert call_count == 1


@pytest.mark.asyncio
async def test_reconnect_true_retries_on_failure():
    """With reconnect=True, _lifecycle_loop retries _connect_and_run on failure."""
    call_count = 0

    async def fake_connect_and_run(self):
        nonlocal call_count
        call_count += 1
        if call_count >= 3:
            raise asyncio.CancelledError
        raise OSError("refused")

    client = _make_client(reconnect=True)

    async def instant_sleep(_delay):
        pass

    with patch.object(CTraderClient, "_connect_and_run", fake_connect_and_run), \
         patch("ctrader_asyncio.client.asyncio.sleep", instant_sleep):
        try:
            await client._lifecycle_loop()
        except asyncio.CancelledError:
            pass

    assert call_count == 3


@pytest.mark.asyncio
async def test_receive_loop_handles_server_disconnect_event():
    disconnect = _common.ProtoMessage(payloadType=2148)
    conn = _mock_connection(receive_side_effect=[disconnect])
    dispatcher = MagicMock()
    dispatcher.cancel_all = MagicMock()

    client = _make_client()
    client._connection = conn
    client._dispatcher = dispatcher

    with pytest.raises(ConnectionLostError, match="disconnect"):
        await client._receive_loop(conn, dispatcher)

    dispatcher.cancel_all.assert_called_once()


@pytest.mark.asyncio
async def test_receive_loop_routes_normal_messages():
    heartbeat = _common.ProtoMessage(payloadType=51)
    conn = _mock_connection(
        receive_side_effect=[heartbeat, asyncio.CancelledError()]
    )
    dispatcher = MagicMock()
    dispatcher.route = AsyncMock()
    dispatcher.cancel_all = MagicMock()

    client = _make_client()

    with pytest.raises(asyncio.CancelledError):
        await client._receive_loop(conn, dispatcher)

    dispatcher.route.assert_awaited_once_with(heartbeat)


@pytest.mark.asyncio
async def test_receive_loop_cancels_all_on_connection_lost():
    conn = _mock_connection(
        receive_side_effect=ConnectionLostError("dropped")
    )
    dispatcher = MagicMock()
    dispatcher.cancel_all = MagicMock()

    client = _make_client()

    with pytest.raises(ConnectionLostError):
        await client._receive_loop(conn, dispatcher)

    dispatcher.cancel_all.assert_called_once()
