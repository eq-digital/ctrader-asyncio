"""Tests for ctrader_asyncio.heartbeat."""

from __future__ import annotations

import asyncio
from unittest.mock import AsyncMock, MagicMock

import pytest

from ctrader_asyncio.heartbeat import run_heartbeat, HEARTBEAT_INTERVAL_SECONDS
from ctrader_asyncio.exceptions import ConnectionLostError


def _fake_connection(send_side_effect=None):
    conn = MagicMock()
    conn.send = AsyncMock(side_effect=send_side_effect)
    return conn


# ---------------------------------------------------------------------------
# Constants
# ---------------------------------------------------------------------------

def test_default_interval_is_under_30_seconds():
    assert HEARTBEAT_INTERVAL_SECONDS < 30.0


# ---------------------------------------------------------------------------
# Normal operation
# ---------------------------------------------------------------------------

@pytest.mark.asyncio
async def test_heartbeat_sends_after_interval():
    conn = _fake_connection()
    task = asyncio.create_task(run_heartbeat(conn, interval=0.05))
    await asyncio.sleep(0.12)
    task.cancel()
    await asyncio.gather(task, return_exceptions=True)
    assert conn.send.await_count >= 2


@pytest.mark.asyncio
async def test_heartbeat_cancelled_cleanly():
    conn = _fake_connection()
    task = asyncio.create_task(run_heartbeat(conn, interval=10.0))
    await asyncio.sleep(0.01)
    task.cancel()
    results = await asyncio.gather(task, return_exceptions=True)
    # CancelledError is the expected outcome
    assert isinstance(results[0], asyncio.CancelledError)


# ---------------------------------------------------------------------------
# Connection loss
# ---------------------------------------------------------------------------

@pytest.mark.asyncio
async def test_heartbeat_raises_connection_lost_on_send_failure():
    conn = _fake_connection(send_side_effect=OSError("broken pipe"))
    task = asyncio.create_task(run_heartbeat(conn, interval=0.01))
    results = await asyncio.gather(task, return_exceptions=True)
    assert isinstance(results[0], ConnectionLostError)


@pytest.mark.asyncio
async def test_heartbeat_propagates_connection_lost_error():
    conn = _fake_connection(send_side_effect=ConnectionLostError("gone"))
    task = asyncio.create_task(run_heartbeat(conn, interval=0.01))
    results = await asyncio.gather(task, return_exceptions=True)
    assert isinstance(results[0], ConnectionLostError)
