"""Tests for ctrader_asyncio.connection.

These tests cover framing logic and error handling without opening a real
network connection.  The TLS TCP layer is mocked using asyncio.StreamReader
and asyncio.StreamWriter fakes.
"""

from __future__ import annotations

import asyncio
import struct
import sys
from pathlib import Path
from unittest.mock import AsyncMock, MagicMock, patch

import pytest

_PROTO_DIR = str(Path(__file__).parent.parent / "src" / "ctrader_asyncio" / "proto")
if _PROTO_DIR not in sys.path:
    sys.path.insert(0, _PROTO_DIR)

from ctrader_asyncio.connection import Connection, _get_payload_type, CTRADER_PORT
from ctrader_asyncio.exceptions import ConnectionLostError
from ctrader_asyncio.proto import OpenApiCommonMessages_pb2 as _common
from ctrader_asyncio.proto import OpenApiMessages_pb2 as _oa


def _fake_writer() -> MagicMock:
    w = MagicMock()
    w.is_closing.return_value = False
    w.write = MagicMock()
    w.drain = AsyncMock()
    w.close = MagicMock()
    w.wait_closed = AsyncMock()
    return w


def _fake_reader_from_bytes(data: bytes) -> asyncio.StreamReader:
    reader = asyncio.StreamReader()
    reader.feed_data(data)
    reader.feed_eof()
    return reader


def test_default_port():
    assert CTRADER_PORT == 5035


def test_constructor_stores_host_and_port():
    conn = Connection("demo.ctraderapi.com")
    assert conn.host == "demo.ctraderapi.com"
    assert conn.port == CTRADER_PORT


def test_constructor_custom_port():
    conn = Connection("demo.ctraderapi.com", port=9999)
    assert conn.port == 9999


def test_is_open_before_connect():
    conn = Connection("demo.ctraderapi.com")
    assert conn.is_open is False


@pytest.mark.asyncio
async def test_open_sets_is_open():
    conn = Connection("demo.ctraderapi.com")
    reader = asyncio.StreamReader()
    writer = _fake_writer()
    with patch("asyncio.open_connection", new=AsyncMock(return_value=(reader, writer))):
        await conn.open()
    assert conn.is_open is True


@pytest.mark.asyncio
async def test_close_is_idempotent():
    conn = Connection("demo.ctraderapi.com")
    await conn.close()
    assert conn.is_open is False


@pytest.mark.asyncio
async def test_close_after_open():
    conn = Connection("demo.ctraderapi.com")
    reader = asyncio.StreamReader()
    writer = _fake_writer()
    with patch("asyncio.open_connection", new=AsyncMock(return_value=(reader, writer))):
        await conn.open()
        await conn.close()
    assert conn.is_open is False
    writer.close.assert_called_once()


@pytest.mark.asyncio
async def test_send_writes_length_prefixed_frame():
    conn = Connection("demo.ctraderapi.com")
    reader = asyncio.StreamReader()
    writer = _fake_writer()

    with patch("asyncio.open_connection", new=AsyncMock(return_value=(reader, writer))):
        await conn.open()

    heartbeat = _common.ProtoHeartbeatEvent()
    await conn.send(heartbeat)

    assert writer.write.called
    raw = writer.write.call_args[0][0]
    assert len(raw) > 4

    (frame_len,) = struct.unpack(">I", raw[:4])
    assert frame_len == len(raw) - 4

    envelope = _common.ProtoMessage()
    envelope.ParseFromString(raw[4:])
    assert envelope.payloadType == 51


@pytest.mark.asyncio
async def test_send_raises_without_open():
    conn = Connection("demo.ctraderapi.com")
    heartbeat = _common.ProtoHeartbeatEvent()
    with pytest.raises(ConnectionLostError):
        await conn.send(heartbeat)


@pytest.mark.asyncio
async def test_send_raises_on_oserror():
    conn = Connection("demo.ctraderapi.com")
    reader = asyncio.StreamReader()
    writer = _fake_writer()
    writer.drain = AsyncMock(side_effect=OSError("broken pipe"))

    with patch("asyncio.open_connection", new=AsyncMock(return_value=(reader, writer))):
        await conn.open()

    with pytest.raises(ConnectionLostError, match="broken pipe"):
        await conn.send(_common.ProtoHeartbeatEvent())


@pytest.mark.asyncio
async def test_receive_decodes_frame():
    heartbeat = _common.ProtoHeartbeatEvent()
    envelope_out = _common.ProtoMessage(
        payloadType=51,
        payload=heartbeat.SerializeToString(),
    )
    frame = envelope_out.SerializeToString()
    wire = struct.pack(">I", len(frame)) + frame

    reader = _fake_reader_from_bytes(wire)
    writer = _fake_writer()
    conn = Connection("demo.ctraderapi.com")

    with patch("asyncio.open_connection", new=AsyncMock(return_value=(reader, writer))):
        await conn.open()

    envelope_in = await conn.receive()
    assert envelope_in.payloadType == 51


@pytest.mark.asyncio
async def test_receive_raises_on_eof_in_header():
    reader = _fake_reader_from_bytes(b"")
    writer = _fake_writer()
    conn = Connection("demo.ctraderapi.com")

    with patch("asyncio.open_connection", new=AsyncMock(return_value=(reader, writer))):
        await conn.open()

    with pytest.raises(ConnectionLostError, match="header"):
        await conn.receive()


@pytest.mark.asyncio
async def test_receive_raises_on_eof_in_body():
    wire = struct.pack(">I", 100) + b"\x00" * 10
    reader = _fake_reader_from_bytes(wire)
    writer = _fake_writer()
    conn = Connection("demo.ctraderapi.com")

    with patch("asyncio.open_connection", new=AsyncMock(return_value=(reader, writer))):
        await conn.open()

    with pytest.raises(ConnectionLostError, match="body"):
        await conn.receive()


@pytest.mark.asyncio
async def test_receive_raises_without_open():
    conn = Connection("demo.ctraderapi.com")
    with pytest.raises(ConnectionLostError):
        await conn.receive()


def test_get_payload_type_heartbeat():
    msg = _common.ProtoHeartbeatEvent()
    assert _get_payload_type(msg) == 51


def test_get_payload_type_error_res():
    msg = _common.ProtoErrorRes(errorCode="UNKNOWN_ERROR")
    assert _get_payload_type(msg) == 50


def test_get_payload_type_raises_for_non_ctrader_message():
    from google.protobuf import descriptor_pb2
    foreign = descriptor_pb2.FileDescriptorProto()
    with pytest.raises(AttributeError, match="payloadType"):
        _get_payload_type(foreign)
