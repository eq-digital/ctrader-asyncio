"""Tests for ctrader_asyncio.exceptions."""

import pytest
from ctrader_asyncio.exceptions import (
    CTraderError,
    ConnectionLostError,
    AuthenticationError,
    RequestTimeoutError,
    ServerError,
)


def test_all_errors_inherit_from_ctrader_error():
    assert issubclass(ConnectionLostError, CTraderError)
    assert issubclass(AuthenticationError, CTraderError)
    assert issubclass(RequestTimeoutError, CTraderError)
    assert issubclass(ServerError, CTraderError)


def test_connection_lost_default_message():
    exc = ConnectionLostError()
    assert "Connection" in str(exc)


def test_connection_lost_custom_message():
    exc = ConnectionLostError("EOF on socket")
    assert str(exc) == "EOF on socket"


def test_authentication_error_message():
    exc = AuthenticationError("INVALID_CLIENT_ID")
    assert "INVALID_CLIENT_ID" in str(exc)


def test_request_timeout_stores_client_msg_id():
    exc = RequestTimeoutError("abc-123")
    assert exc.client_msg_id == "abc-123"
    assert "abc-123" in str(exc)


def test_server_error_stores_error_code():
    exc = ServerError("NOT_AUTHENTICATED")
    assert exc.error_code == "NOT_AUTHENTICATED"
    assert exc.description is None
    assert "NOT_AUTHENTICATED" in str(exc)


def test_server_error_with_description():
    exc = ServerError("BLOCKED", "Rate limit exceeded")
    assert exc.error_code == "BLOCKED"
    assert exc.description == "Rate limit exceeded"
    assert "Rate limit exceeded" in str(exc)


def test_catchable_as_base():
    with pytest.raises(CTraderError):
        raise ConnectionLostError()

    with pytest.raises(CTraderError):
        raise ServerError("UNKNOWN_ERROR")
