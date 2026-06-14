"""Low-level TLS TCP connection to the cTrader Open API.

Responsibilities
----------------
* Open and close a TLS TCP socket to ``demo.ctraderapi.com`` or
  ``live.ctraderapi.com`` on port **5035**.
* Send and receive raw protobuf frames using the 4-byte big-endian
  length-prefix framing that the cTrader wire protocol requires.
* Expose ``send()`` and ``receive()`` coroutines that work in terms of
  :class:`~google.protobuf.message.Message` objects — callers never touch
  raw bytes.

What this module does NOT do
-----------------------------
* Authentication — that is :mod:`ctrader_asyncio.auth`.
* Request routing / response matching — that is
  :mod:`ctrader_asyncio.dispatcher`.
* Reconnection — the caller (the future ``CTraderClient``) owns that loop.
"""

from __future__ import annotations

import asyncio
import ssl
import struct
import sys
from pathlib import Path
from typing import TYPE_CHECKING

# The compiled _pb2 files use bare module imports (e.g.
# ``import OpenApiCommonModelMessages_pb2``), so the proto package directory
# must be on sys.path before they are imported.
_PROTO_DIR = str(Path(__file__).parent / "proto")
if _PROTO_DIR not in sys.path:
    sys.path.insert(0, _PROTO_DIR)

from ctrader_asyncio.proto import OpenApiCommonMessages_pb2 as _common  # noqa: E402

from ctrader_asyncio.exceptions import ConnectionLostError  # noqa: E402

if TYPE_CHECKING:
    from google.protobuf.message import Message

# cTrader Open API always uses port 5035 for TLS.
CTRADER_PORT: int = 5035

# Maximum wire frame the server will accept (from ProtoErrorCode.FRAME_TOO_LONG).
# We enforce this on send to fail early rather than get disconnected.
_MAX_FRAME_BYTES: int = 1_000_000  # 1 MB — well above any realistic message

# The outer envelope for every cTrader message, regardless of direction.
_ProtoMessage = _common.ProtoMessage


class Connection:
    """A single TLS TCP connection to one cTrader Open API host.

    One :class:`Connection` instance maps to exactly one TCP socket.
    Do **not** open two connections for the same ``client_id`` — the server
    will silently drop ``ProtoOAApplicationAuthReq`` on the second one.

    Args:
        host: Hostname of the cTrader server, e.g.
            ``"demo.ctraderapi.com"`` or ``"live.ctraderapi.com"``.
        port: TCP port. Defaults to :data:`CTRADER_PORT` (5035).
        ssl_context: Optional custom :class:`ssl.SSLContext`. When ``None``
            (the default) a secure context is built automatically with the
            system CA bundle and hostname verification enabled.

    Example::

        conn = Connection("demo.ctraderapi.com")
        await conn.open()
        try:
            raw_msg = await conn.receive()
        finally:
            await conn.close()
    """

    def __init__(
        self,
        host: str,
        port: int = CTRADER_PORT,
        ssl_context: ssl.SSLContext | None = None,
    ) -> None:
        self.host = host
        self.port = port
        self._ssl_context = ssl_context or _make_ssl_context()
        self._reader: asyncio.StreamReader | None = None
        self._writer: asyncio.StreamWriter | None = None

    # ------------------------------------------------------------------
    # Public lifecycle
    # ------------------------------------------------------------------

    async def open(self) -> None:
        """Open the TLS TCP connection.

        Performs the TCP + TLS handshake.  Must be called before
        :meth:`send` or :meth:`receive`.

        Raises:
            OSError: If the TCP connection or TLS handshake fails.
        """
        self._reader, self._writer = await asyncio.open_connection(
            self.host,
            self.port,
            ssl=self._ssl_context,
        )

    async def close(self) -> None:
        """Close the connection gracefully.

        Safe to call even if the connection was never opened or has already
        been closed — it is idempotent.
        """
        if self._writer is not None:
            try:
                self._writer.close()
                await self._writer.wait_closed()
            except Exception:
                pass
            finally:
                self._writer = None
                self._reader = None

    @property
    def is_open(self) -> bool:
        """``True`` if the underlying socket appears to be connected."""
        return self._writer is not None and not self._writer.is_closing()

    # ------------------------------------------------------------------
    # Public send / receive
    # ------------------------------------------------------------------

    async def send(self, message: Message) -> None:
        """Wrap *message* in a :class:`ProtoMessage` envelope and send it.

        The caller passes any concrete protobuf message (e.g.
        ``ProtoOAApplicationAuthReq``).  This method:

        1. Serialises *message* to bytes.
        2. Builds a ``ProtoMessage`` wrapper with the correct ``payloadType``
           and ``payload``.
        3. Writes a 4-byte big-endian length prefix followed by the
           serialised ``ProtoMessage``.

        Args:
            message: Any compiled protobuf message instance.

        Raises:
            ConnectionLostError: If the writer is not available.
            ValueError: If the serialised frame exceeds ``_MAX_FRAME_BYTES``.
        """
        if self._writer is None:
            raise ConnectionLostError("Cannot send — connection is not open")

        payload_type = _get_payload_type(message)
        envelope = _ProtoMessage(
            payloadType=payload_type,
            payload=message.SerializeToString(),
        )
        frame = envelope.SerializeToString()

        if len(frame) > _MAX_FRAME_BYTES:
            raise ValueError(
                f"Frame too large: {len(frame)} bytes (max {_MAX_FRAME_BYTES})"
            )

        header = struct.pack(">I", len(frame))
        try:
            self._writer.write(header + frame)
            await self._writer.drain()
        except (OSError, asyncio.CancelledError) as exc:
            raise ConnectionLostError(str(exc)) from exc

    async def receive(self) -> _ProtoMessage:
        """Read the next :class:`ProtoMessage` from the wire.

        Reads the 4-byte length prefix, then reads exactly that many bytes
        and deserialises them as a ``ProtoMessage``.

        Returns:
            The decoded :class:`ProtoMessage` envelope.  Callers inspect
            ``envelope.payloadType`` to determine the inner message type,
            then deserialise ``envelope.payload`` accordingly.

        Raises:
            ConnectionLostError: If the connection drops mid-read or the
                reader returns EOF.
        """
        if self._reader is None:
            raise ConnectionLostError("Cannot receive — connection is not open")

        try:
            header = await self._reader.readexactly(4)
        except asyncio.IncompleteReadError as exc:
            raise ConnectionLostError("Connection closed while reading frame header") from exc
        except (OSError, asyncio.CancelledError) as exc:
            raise ConnectionLostError(str(exc)) from exc

        (frame_length,) = struct.unpack(">I", header)

        try:
            frame = await self._reader.readexactly(frame_length)
        except asyncio.IncompleteReadError as exc:
            raise ConnectionLostError(
                f"Connection closed while reading frame body ({frame_length} bytes expected)"
            ) from exc
        except (OSError, asyncio.CancelledError) as exc:
            raise ConnectionLostError(str(exc)) from exc

        envelope = _ProtoMessage()
        envelope.ParseFromString(frame)
        return envelope


# ------------------------------------------------------------------
# Private helpers
# ------------------------------------------------------------------

def _make_ssl_context() -> ssl.SSLContext:
    """Return a secure :class:`ssl.SSLContext` using the system CA bundle.

    Hostname verification and certificate validation are both enabled.
    """
    ctx = ssl.create_default_context()
    return ctx


def _get_payload_type(message: Message) -> int:
    """Extract the numeric ``payloadType`` from a protobuf message instance.

    cTrader protobuf messages carry their own payload-type enum value in
    field 1.  This helper reads it so :meth:`Connection.send` can populate
    the ``ProtoMessage`` envelope correctly.

    Args:
        message: Any compiled protobuf message with a ``payloadType`` field.

    Returns:
        The integer payload type value.

    Raises:
        AttributeError: If *message* has no ``payloadType`` field (i.e. it
            is not a valid cTrader API message).
    """
    descriptor = message.DESCRIPTOR
    field = descriptor.fields_by_name.get("payloadType")
    if field is None:
        raise AttributeError(
            f"{descriptor.name} has no payloadType field — "
            "is this a valid cTrader Open API message?"
        )
    raw = getattr(message, "payloadType")
    return int(raw)
