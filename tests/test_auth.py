"""Tests for ctrader_asyncio.auth."""

from __future__ import annotations

import asyncio
import sys
from pathlib import Path
from unittest.mock import AsyncMock, MagicMock, patch

import pytest

_PROTO_DIR = str(Path(__file__).parent.parent / "src" / "ctrader_asyncio" / "proto")
if _PROTO_DIR not in sys.path:
    sys.path.insert(0, _PROTO_DIR)

from ctrader_asyncio.auth import authenticate_app, authenticate_accounts
from ctrader_asyncio.exceptions import AuthenticationError, ServerError
from ctrader_asyncio.proto import OpenApiCommonMessages_pb2 as _common


def _fake_dispatcher(response_payload_type: int = 2101, raises=None):
    """Return a dispatcher mock that resolves send_request immediately."""
    d = MagicMock()
    if raises:
        d.send_request = AsyncMock(side_effect=raises)
    else:
        envelope = _common.ProtoMessage(payloadType=response_payload_type)
        d.send_request = AsyncMock(return_value=envelope)
    return d


def _fake_connection():
    conn = MagicMock()
    conn.send = AsyncMock()
    return conn


# ---------------------------------------------------------------------------
# authenticate_app
# ---------------------------------------------------------------------------

@pytest.mark.asyncio
async def test_authenticate_app_sets_event_on_success():
    dispatcher = _fake_dispatcher(response_payload_type=2101)
    conn = _fake_connection()
    event = asyncio.Event()

    await authenticate_app(dispatcher, conn, "cid", "csecret", event)

    assert event.is_set()


@pytest.mark.asyncio
async def test_authenticate_app_sends_correct_credentials():
    sent_messages = []

    async def capture_send(connection, message, **kwargs):
        sent_messages.append(message)
        return _common.ProtoMessage(payloadType=2101)

    dispatcher = MagicMock()
    dispatcher.send_request = capture_send
    conn = _fake_connection()
    event = asyncio.Event()

    await authenticate_app(dispatcher, conn, "my-client-id", "my-secret", event)

    assert len(sent_messages) == 1
    req = sent_messages[0]
    assert req.clientId == "my-client-id"
    assert req.clientSecret == "my-secret"


@pytest.mark.asyncio
async def test_authenticate_app_raises_on_server_error():
    dispatcher = _fake_dispatcher(raises=ServerError("INVALID_CLIENT_ID"))
    conn = _fake_connection()
    event = asyncio.Event()

    with pytest.raises(AuthenticationError, match="INVALID_CLIENT_ID"):
        await authenticate_app(dispatcher, conn, "bad-id", "bad-secret", event)

    assert not event.is_set()


@pytest.mark.asyncio
async def test_authenticate_app_does_not_set_event_on_failure():
    dispatcher = _fake_dispatcher(raises=Exception("timeout"))
    conn = _fake_connection()
    event = asyncio.Event()

    with pytest.raises(AuthenticationError):
        await authenticate_app(dispatcher, conn, "cid", "csecret", event)

    assert not event.is_set()


# ---------------------------------------------------------------------------
# authenticate_accounts
# ---------------------------------------------------------------------------

@pytest.mark.asyncio
async def test_authenticate_accounts_sends_one_request_per_account():
    sent_messages = []

    async def capture_send(connection, message, **kwargs):
        sent_messages.append(message)
        return _common.ProtoMessage(payloadType=2103)

    dispatcher = MagicMock()
    dispatcher.send_request = capture_send
    conn = _fake_connection()

    async def token_provider(account_id: int) -> str:
        return f"token-{account_id}"

    await authenticate_accounts(dispatcher, conn, [111, 222, 333], token_provider)

    assert len(sent_messages) == 3
    account_ids_sent = {m.ctidTraderAccountId for m in sent_messages}
    assert account_ids_sent == {111, 222, 333}


@pytest.mark.asyncio
async def test_authenticate_accounts_fetches_token_per_account():
    fetched = []

    async def token_provider(account_id: int) -> str:
        fetched.append(account_id)
        return f"tok-{account_id}"

    async def capture_send(connection, message, **kwargs):
        return _common.ProtoMessage(payloadType=2103)

    dispatcher = MagicMock()
    dispatcher.send_request = capture_send
    conn = _fake_connection()

    await authenticate_accounts(dispatcher, conn, [10, 20], token_provider)

    assert sorted(fetched) == [10, 20]


@pytest.mark.asyncio
async def test_authenticate_accounts_uses_token_from_provider():
    used_tokens = []

    async def capture_send(connection, message, **kwargs):
        used_tokens.append((message.ctidTraderAccountId, message.accessToken))
        return _common.ProtoMessage(payloadType=2103)

    dispatcher = MagicMock()
    dispatcher.send_request = capture_send
    conn = _fake_connection()

    async def token_provider(account_id: int) -> str:
        return f"fresh-token-{account_id}"

    await authenticate_accounts(dispatcher, conn, [42], token_provider)

    assert used_tokens == [(42, "fresh-token-42")]


@pytest.mark.asyncio
async def test_authenticate_accounts_raises_auth_error_on_server_error():
    dispatcher = _fake_dispatcher(raises=ServerError("ACCESS_TOKEN_INVALID"))
    conn = _fake_connection()

    async def token_provider(account_id: int) -> str:
        return "bad-token"

    with pytest.raises(AuthenticationError, match="ACCESS_TOKEN_INVALID"):
        await authenticate_accounts(dispatcher, conn, [99], token_provider)


@pytest.mark.asyncio
async def test_authenticate_accounts_empty_list_does_nothing():
    dispatcher = _fake_dispatcher()
    conn = _fake_connection()

    async def token_provider(account_id: int) -> str:
        return "token"

    # Should complete without sending anything
    await authenticate_accounts(dispatcher, conn, [], token_provider)
    dispatcher.send_request.assert_not_awaited()
