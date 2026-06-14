"""Request/response routing for the cTrader Open API.

The :class:`Dispatcher` sits between :class:`~ctrader_asyncio.connection.Connection`
and the rest of the library.  It owns two responsibilities:

1. **Request tracking** — assigns a ``clientMsgId`` UUID to every outgoing
   request, stores an :class:`asyncio.Future` keyed to that ID, and resolves
   the future when the matching response arrives.

2. **Event routing** — dispatches unsolicited server messages (price ticks,
   execution reports, disconnect events, heartbeats) to registered async
   callback functions keyed by ``payloadType``.

The dispatcher does **not** own the connection and does **not** reconnect.
On connection loss, call :meth:`Dispatcher.cancel_all` to reject every
pending future with :class:`~ctrader_asyncio.exceptions.ConnectionLostError`
before the reconnect cycle begins.
"""

from __future__ import annotations

import asyncio
import sys
import uuid
from pathlib import Path
from typing import Awaitable, Callable

_PROTO_DIR = str(Path(__file__).parent / "proto")
if _PROTO_DIR not in sys.path:
    sys.path.insert(0, _PROTO_DIR)

from ctrader_asyncio.proto import OpenApiCommonMessages_pb2 as _common
from ctrader_asyncio.exceptions import ConnectionLostError, RequestTimeoutError, ServerError
from ctrader_asyncio.connection import _get_payload_type

# Type alias for async event callbacks.
# Receives the raw ProtoMessage envelope; the callback decodes payload itself.
EventCallback = Callable[[_common.ProtoMessage], Awaitable[None]]

# Timeout applied to every request future via asyncio.wait_for.
REQUEST_TIMEOUT_SECONDS: float = 30.0

# payloadType values that indicate an error response from the server.
# Both the common error (50) and the OA-specific error are handled.
_ERROR_PAYLOAD_TYPES: frozenset[int] = frozenset({
    50,    # ERROR_RES       (ProtoErrorRes)
    2142,  # PROTO_OA_ERROR_RES (ProtoOAErrorRes)
})


class Dispatcher:
    """Routes outgoing requests and incoming responses/events.

    One :class:`Dispatcher` instance is paired with one
    :class:`~ctrader_asyncio.connection.Connection`.  Create a new
    :class:`Dispatcher` after every reconnect — do not reuse across
    connection lifecycles.

    Example::

        dispatcher = Dispatcher()

        # Register a callback for unsolicited spot price events
        async def on_spot(envelope):
            ...
        dispatcher.register(2126, on_spot)  # PROTO_OA_SPOT_EVENT

        # Route an incoming envelope (called from the receive loop)
        await dispatcher.route(envelope)

        # Send a tracked request and await its response envelope
        response_envelope = await dispatcher.send_request(connection, request_msg)
    """

    def __init__(self) -> None:
        # clientMsgId → Future[ProtoMessage]
        self._pending: dict[str, asyncio.Future[_common.ProtoMessage]] = {}
        # payloadType → list of async callbacks
        self._callbacks: dict[int, list[EventCallback]] = {}

    # ------------------------------------------------------------------
    # Event callback registration
    # ------------------------------------------------------------------

    def register(self, payload_type: int, callback: EventCallback) -> None:
        """Register an async callback for a specific ``payloadType``.

        Multiple callbacks can be registered for the same type — they are
        called in registration order.

        Args:
            payload_type: The integer ``payloadType`` to listen for.
            callback: An async callable that accepts a single
                :class:`~ctrader_asyncio.proto.OpenApiCommonMessages_pb2.ProtoMessage`
                argument.
        """
        self._callbacks.setdefault(payload_type, []).append(callback)

    def unregister(self, payload_type: int, callback: EventCallback) -> None:
        """Remove a previously registered callback.

        Silently does nothing if the callback was not registered.

        Args:
            payload_type: The ``payloadType`` the callback was registered for.
            callback: The exact callback object that was passed to
                :meth:`register`.
        """
        callbacks = self._callbacks.get(payload_type, [])
        try:
            callbacks.remove(callback)
        except ValueError:
            pass

    # ------------------------------------------------------------------
    # Incoming message routing
    # ------------------------------------------------------------------

    async def route(self, envelope: _common.ProtoMessage) -> None:
        """Route one incoming :class:`ProtoMessage` envelope.

        Called by the receive loop for every message that arrives from the
        server.  The routing logic is:

        1. If ``envelope.clientMsgId`` matches a pending future, resolve or
           reject that future and return.
        2. Otherwise, fire all registered callbacks for
           ``envelope.payloadType``.

        Args:
            envelope: The decoded envelope from
                :meth:`~ctrader_asyncio.connection.Connection.receive`.
        """
        client_msg_id = envelope.clientMsgId if envelope.HasField("clientMsgId") else ""

        if client_msg_id and client_msg_id in self._pending:
            future = self._pending.pop(client_msg_id)
            if not future.done():
                if envelope.payloadType in _ERROR_PAYLOAD_TYPES:
                    error_code, description = _extract_error(envelope)
                    future.set_exception(ServerError(error_code, description))
                else:
                    future.set_result(envelope)
            return

        # Unsolicited event — fire callbacks
        callbacks = self._callbacks.get(envelope.payloadType, [])
        for cb in callbacks:
            try:
                await cb(envelope)
            except Exception:
                # Individual callback failures must not crash the receive loop.
                pass

    # ------------------------------------------------------------------
    # Outgoing request tracking
    # ------------------------------------------------------------------

    async def send_request(
        self,
        connection: object,
        message: object,
        timeout: float = REQUEST_TIMEOUT_SECONDS,
    ) -> _common.ProtoMessage:
        """Send *message* and wait for the correlated response.

        Assigns a UUID ``clientMsgId`` to the message, registers a
        :class:`asyncio.Future` for it, sends the message via *connection*,
        and waits up to *timeout* seconds for :meth:`route` to resolve the
        future.

        Args:
            connection: A
                :class:`~ctrader_asyncio.connection.Connection` instance.
            message: Any compiled protobuf message with a ``clientMsgId``
                field (all cTrader request messages have one).
            timeout: Seconds to wait before raising
                :class:`~ctrader_asyncio.exceptions.RequestTimeoutError`.
                Defaults to :data:`REQUEST_TIMEOUT_SECONDS` (30 s).

        Returns:
            The response :class:`ProtoMessage` envelope from the server.

        Raises:
            RequestTimeoutError: If no response arrives within *timeout*
                seconds.
            ServerError: If the server responds with an error payload type.
            ConnectionLostError: If the connection drops while waiting.
        """
        client_msg_id = str(uuid.uuid4())
        envelope = _common.ProtoMessage(
            payloadType=_get_payload_type(message),
            payload=message.SerializeToString(),
            clientMsgId=client_msg_id,
        )

        loop = asyncio.get_running_loop()
        future: asyncio.Future[_common.ProtoMessage] = loop.create_future()
        self._pending[client_msg_id] = future

        try:
            await connection.send(envelope)
        except Exception:
            self._pending.pop(client_msg_id, None)
            future.cancel()
            raise

        try:
            return await asyncio.wait_for(future, timeout=timeout)
        except asyncio.TimeoutError:
            self._pending.pop(client_msg_id, None)
            raise RequestTimeoutError(client_msg_id)
        except asyncio.CancelledError:
            self._pending.pop(client_msg_id, None)
            raise ConnectionLostError("Request cancelled — connection lost")

    # ------------------------------------------------------------------
    # Connection loss handling
    # ------------------------------------------------------------------

    def cancel_all(self) -> None:
        """Cancel every pending future with :class:`ConnectionLostError`.

        Call this before starting a reconnect cycle.  After this call,
        :attr:`pending_count` will be zero.

        All callers awaiting :meth:`send_request` will receive a
        :class:`~ctrader_asyncio.exceptions.ConnectionLostError`.
        """
        error = ConnectionLostError("Connection lost — request cancelled")
        for future in self._pending.values():
            if not future.done():
                future.set_exception(error)
        self._pending.clear()

    # ------------------------------------------------------------------
    # Introspection
    # ------------------------------------------------------------------

    @property
    def pending_count(self) -> int:
        """Number of requests currently awaiting a response."""
        return len(self._pending)


# ------------------------------------------------------------------
# Private helpers
# ------------------------------------------------------------------

def _extract_error(envelope: _common.ProtoMessage) -> tuple[str, str | None]:
    """Extract ``errorCode`` and ``description`` from an error envelope.

    Handles both ``ProtoErrorRes`` (payloadType 50) and
    ``ProtoOAErrorRes`` (payloadType 2142).

    Args:
        envelope: An envelope whose ``payloadType`` is in
            :data:`_ERROR_PAYLOAD_TYPES`.

    Returns:
        A ``(error_code, description)`` tuple.  ``description`` may be
        ``None`` if the server did not include one.
    """
    try:
        if envelope.payloadType == 50:
            from ctrader_asyncio.proto import OpenApiCommonMessages_pb2 as common
            msg = common.ProtoErrorRes()
            msg.ParseFromString(envelope.payload)
            description = msg.description if msg.HasField("description") else None
            return msg.errorCode, description
        else:
            from ctrader_asyncio.proto import OpenApiMessages_pb2 as oa
            msg = oa.ProtoOAErrorRes()
            msg.ParseFromString(envelope.payload)
            description = msg.description if msg.HasField("description") else None
            return msg.errorCode, description
    except Exception:
        return "UNKNOWN_ERROR", None
