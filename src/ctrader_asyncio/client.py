"""High-level cTrader Open API client.

:class:`CTraderClient` is the single entry point for library users.  It owns
the full connection lifecycle:

* Opens a TLS TCP connection via :class:`~ctrader_asyncio.connection.Connection`.
* Runs the strict auth sequence (app auth → account auth) via
  :mod:`ctrader_asyncio.auth`.
* Starts the heartbeat loop via :mod:`ctrader_asyncio.heartbeat`.
* Runs a receive loop that feeds every incoming message to the
  :class:`~ctrader_asyncio.dispatcher.Dispatcher`.
* On connection loss, cancels pending requests, tears down tasks, and
  retries with exponential backoff (1 s → 2 s → 4 s … capped at 60 s).

Tokens are never cached — ``token_provider`` is called fresh on every
connect and reconnect.

Example::

    async def get_token(account_id: int) -> str:
        return await vault.fetch(account_id)

    client = CTraderClient(
        host="demo.ctraderapi.com",
        client_id="your-client-id",
        client_secret="your-secret",
        account_ids=[12345678],
        token_provider=get_token,
    )

    async def on_spot(envelope):
        print("spot event", envelope.payloadType)

    client.register(2126, on_spot)   # PROTO_OA_SPOT_EVENT

    async with client:
        await client.wait_until_ready()
        response = await client.send_request(some_proto_message)
"""

from __future__ import annotations

import asyncio
import logging
import sys
from pathlib import Path
from typing import Awaitable, Callable

_PROTO_DIR = str(Path(__file__).parent / "proto")
if _PROTO_DIR not in sys.path:
    sys.path.insert(0, _PROTO_DIR)

from ctrader_asyncio.connection import Connection
from ctrader_asyncio.dispatcher import Dispatcher
from ctrader_asyncio.auth import authenticate_app, authenticate_accounts
from ctrader_asyncio.heartbeat import run_heartbeat
from ctrader_asyncio.exceptions import ConnectionLostError
from ctrader_asyncio.proto import OpenApiCommonMessages_pb2 as _common

log = logging.getLogger(__name__)

# Exponential backoff: start at 1 s, double each attempt, cap at 60 s.
_BACKOFF_BASE: float = 1.0
_BACKOFF_CAP: float = 60.0

# payloadType for the server-initiated disconnect event.
_DISCONNECT_EVENT: int = 2148  # PROTO_OA_CLIENT_DISCONNECT_EVENT


class CTraderClient:
    """Manages one TLS connection to a cTrader Open API host.

    One instance = one TCP connection = one ``client_id``.  Do **not** open
    two ``CTraderClient`` instances for the same ``client_id`` — the server
    will silently drop ``ProtoOAApplicationAuthReq`` on the second one.

    Args:
        host: cTrader server hostname, e.g. ``"demo.ctraderapi.com"`` or
            ``"live.ctraderapi.com"``.
        client_id: The cTrader Open API client ID.
        client_secret: The cTrader Open API client secret.
        account_ids: List of ``ctidTraderAccountId`` values to authenticate
            after app auth succeeds.
        token_provider: An async callable ``(account_id: int) -> str`` that
            returns a fresh access token for the given account.  Called once
            per account on every connect and reconnect — tokens are never
            cached.
        port: TCP port.  Defaults to 5035.
        reconnect: Whether to reconnect automatically on connection loss.
            Defaults to ``True``.

    Example::

        client = CTraderClient(
            host="demo.ctraderapi.com",
            client_id="abc",
            client_secret="xyz",
            account_ids=[12345678],
            token_provider=my_token_fn,
        )
        async with client:
            await client.wait_until_ready()
            response = await client.send_request(my_request_msg)
    """

    def __init__(
        self,
        host: str,
        client_id: str,
        client_secret: str,
        account_ids: list[int],
        token_provider: Callable[[int], Awaitable[str]],
        port: int = 5035,
        reconnect: bool = True,
    ) -> None:
        self.host = host
        self.client_id = client_id
        self.client_secret = client_secret
        self.account_ids = list(account_ids)
        self.token_provider = token_provider
        self.port = port
        self.reconnect = reconnect

        self._connection: Connection | None = None
        self._dispatcher: Dispatcher | None = None
        self._app_authed: asyncio.Event = asyncio.Event()
        self._ready: asyncio.Event = asyncio.Event()

        # Background tasks: receive loop + heartbeat
        self._receive_task: asyncio.Task | None = None
        self._heartbeat_task: asyncio.Task | None = None
        # The top-level lifecycle task (run loop + reconnect)
        self._lifecycle_task: asyncio.Task | None = None

        # Pending event callback registrations (payload_type → [callbacks])
        # stored here so they survive reconnects and are re-registered on each
        # new Dispatcher.
        self._event_callbacks: dict[int, list] = {}

    # ------------------------------------------------------------------
    # Public API
    # ------------------------------------------------------------------

    def register(self, payload_type: int, callback) -> None:
        """Register an async callback for an unsolicited server event.

        Callbacks survive reconnects — they are re-registered on every new
        :class:`~ctrader_asyncio.dispatcher.Dispatcher`.

        Args:
            payload_type: Integer ``payloadType`` to listen for.
            callback: Async callable ``(envelope: ProtoMessage) -> None``.
        """
        self._event_callbacks.setdefault(payload_type, []).append(callback)
        if self._dispatcher is not None:
            self._dispatcher.register(payload_type, callback)

    def unregister(self, payload_type: int, callback) -> None:
        """Remove a previously registered callback.

        Args:
            payload_type: The ``payloadType`` the callback was registered for.
            callback: The exact callback object passed to :meth:`register`.
        """
        callbacks = self._event_callbacks.get(payload_type, [])
        try:
            callbacks.remove(callback)
        except ValueError:
            pass
        if self._dispatcher is not None:
            self._dispatcher.unregister(payload_type, callback)

    async def send_request(self, message, timeout: float = 30.0):
        """Send a request message and wait for the correlated response.

        Args:
            message: Any compiled protobuf request message.
            timeout: Seconds to wait for a response.  Defaults to 30 s.

        Returns:
            The response :class:`ProtoMessage` envelope.

        Raises:
            ConnectionLostError: If not connected.
            RequestTimeoutError: If no response arrives within *timeout*.
            ServerError: If the server returns an error payload.
        """
        if self._dispatcher is None or self._connection is None:
            raise ConnectionLostError("Client is not connected")
        return await self._dispatcher.send_request(
            self._connection, message, timeout=timeout
        )

    async def wait_until_ready(self, timeout: float = 60.0) -> None:
        """Block until app auth and all account auths have completed.

        Args:
            timeout: Maximum seconds to wait.  Defaults to 60 s.

        Raises:
            asyncio.TimeoutError: If the client is not ready within *timeout*.
        """
        await asyncio.wait_for(self._ready.wait(), timeout=timeout)

    # ------------------------------------------------------------------
    # Async context manager
    # ------------------------------------------------------------------

    async def __aenter__(self) -> "CTraderClient":
        """Start the connection lifecycle in a background task."""
        self._lifecycle_task = asyncio.create_task(
            self._lifecycle_loop(), name="ctrader-lifecycle"
        )
        return self

    async def __aexit__(self, exc_type, exc, tb) -> None:
        """Gracefully shut down the connection and all background tasks."""
        await self.close()

    async def close(self) -> None:
        """Shut down the client cleanly.

        Cancels all background tasks, closes the connection, and cancels any
        pending requests.  Safe to call multiple times.
        """
        if self._lifecycle_task and not self._lifecycle_task.done():
            self._lifecycle_task.cancel()
            await asyncio.gather(self._lifecycle_task, return_exceptions=True)
        await self._teardown()

    # ------------------------------------------------------------------
    # Internal lifecycle
    # ------------------------------------------------------------------

    async def _lifecycle_loop(self) -> None:
        """Connect, auth, run, and reconnect with exponential backoff."""
        backoff = _BACKOFF_BASE
        attempt = 0

        while True:
            attempt += 1
            try:
                await self._connect_and_run()
                backoff = _BACKOFF_BASE
            except asyncio.CancelledError:
                return
            except Exception as exc:
                log.warning(
                    "ctrader connection lost (attempt %d): %s — "
                    "reconnecting in %.0f s",
                    attempt, exc, backoff,
                )

            if not self.reconnect:
                return

            try:
                await asyncio.sleep(backoff)
            except asyncio.CancelledError:
                return

            backoff = min(backoff * 2, _BACKOFF_CAP)

    async def _connect_and_run(self) -> None:
        """One full connection lifecycle: connect → auth → receive loop."""
        self._ready.clear()
        self._app_authed.clear()

        conn = Connection(self.host, self.port)
        await conn.open()
        self._connection = conn
        log.debug("Connected to %s:%s", self.host, self.port)

        dispatcher = Dispatcher()
        # Re-register all stored callbacks onto the fresh dispatcher.
        for payload_type, callbacks in self._event_callbacks.items():
            for cb in callbacks:
                dispatcher.register(payload_type, cb)
        self._dispatcher = dispatcher

        try:
            # Step 1: app auth
            await authenticate_app(
                dispatcher, conn,
                self.client_id, self.client_secret,
                self._app_authed,
            )
            log.debug("App auth complete")

            # Step 2: account auth (concurrent)
            await authenticate_accounts(
                dispatcher, conn,
                self.account_ids, self.token_provider,
            )
            log.debug("Account auth complete for %s", self.account_ids)

            # Signal ready — callers blocking on wait_until_ready() unblock here.
            self._ready.set()

            # Step 3: start heartbeat
            self._heartbeat_task = asyncio.create_task(
                run_heartbeat(conn), name="ctrader-heartbeat"
            )

            # Step 4: receive loop (runs until connection loss)
            await self._receive_loop(conn, dispatcher)

        except asyncio.CancelledError:
            raise
        finally:
            self._ready.clear()
            await self._teardown()

    async def _receive_loop(
        self, conn: Connection, dispatcher: Dispatcher
    ) -> None:
        """Read messages from the wire and feed them to the dispatcher.

        Runs until :class:`ConnectionLostError` or cancellation.

        Args:
            conn: The active connection.
            dispatcher: The active dispatcher.
        """
        while True:
            try:
                envelope = await conn.receive()
            except ConnectionLostError as exc:
                log.debug("Receive loop ended: %s", exc)
                dispatcher.cancel_all()
                raise
            except asyncio.CancelledError:
                dispatcher.cancel_all()
                raise

            # Server-initiated disconnect — treat as connection loss.
            if envelope.payloadType == _DISCONNECT_EVENT:
                log.warning("Server sent disconnect event")
                dispatcher.cancel_all()
                raise ConnectionLostError("Server disconnected the client")

            await dispatcher.route(envelope)

    async def _teardown(self) -> None:
        """Cancel heartbeat task and close the connection.  Idempotent."""
        if self._heartbeat_task and not self._heartbeat_task.done():
            self._heartbeat_task.cancel()
            await asyncio.gather(self._heartbeat_task, return_exceptions=True)
            self._heartbeat_task = None

        if self._connection is not None:
            await self._connection.close()
            self._connection = None

        self._dispatcher = None
