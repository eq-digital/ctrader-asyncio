"""Custom exceptions for ctrader-asyncio.

All library-specific errors inherit from :class:`CTraderError` so callers
can catch the whole family with a single ``except CTraderError`` clause.
"""


class CTraderError(Exception):
    """Base class for all ctrader-asyncio errors."""


class ConnectionLostError(CTraderError):
    """Raised when the TCP connection to the cTrader server drops unexpectedly.

    When this exception is raised, all pending request futures are cancelled
    with this error before the reconnect cycle begins.

    Args:
        message: Human-readable description of why the connection was lost.
    """

    def __init__(self, message: str = "Connection to cTrader server lost") -> None:
        super().__init__(message)


class AuthenticationError(CTraderError):
    """Raised when application or account authentication fails.

    Args:
        message: Human-readable error from the server (e.g. ``INVALID_CLIENT_ID``).
    """

    def __init__(self, message: str) -> None:
        super().__init__(message)


class RequestTimeoutError(CTraderError):
    """Raised when a request does not receive a response within 30 seconds.

    Args:
        client_msg_id: The ``clientMsgId`` of the request that timed out.
    """

    def __init__(self, client_msg_id: str) -> None:
        self.client_msg_id = client_msg_id
        super().__init__(f"Request timed out: clientMsgId={client_msg_id}")


class ServerError(CTraderError):
    """Raised when the server returns a ``ProtoOAErrorRes`` or ``ProtoErrorRes``.

    Args:
        error_code: The ``errorCode`` string from the server message.
        description: Optional human-readable description from the server.
    """

    def __init__(self, error_code: str, description: str | None = None) -> None:
        self.error_code = error_code
        self.description = description
        detail = f" — {description}" if description else ""
        super().__init__(f"Server error: {error_code}{detail}")
