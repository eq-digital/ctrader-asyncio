"""Heartbeat loop for the cTrader Open API connection.

The cTrader server closes idle connections after ~30 seconds of silence.
:func:`run_heartbeat` sends a ``ProtoHeartbeatEvent`` every
:data:`HEARTBEAT_INTERVAL_SECONDS` seconds for as long as the connection
is alive.

Usage::

    task = asyncio.create_task(run_heartbeat(connection))
    # ... later, on disconnect:
    task.cancel()
    await asyncio.gather(task, return_exceptions=True)
"""

from __future__ import annotations

import asyncio
import sys
from pathlib import Path

_PROTO_DIR = str(Path(__file__).parent / "proto")
if _PROTO_DIR not in sys.path:
    sys.path.insert(0, _PROTO_DIR)

from ctrader_asyncio.proto import OpenApiCommonMessages_pb2 as _common
from ctrader_asyncio.exceptions import ConnectionLostError

# Send a heartbeat every 25 seconds — safely under the 30-second server limit.
HEARTBEAT_INTERVAL_SECONDS: float = 25.0


async def run_heartbeat(
    connection: object,
    interval: float = HEARTBEAT_INTERVAL_SECONDS,
) -> None:
    """Send ``ProtoHeartbeatEvent`` on a fixed interval until cancelled.

    This coroutine runs forever until it is cancelled or the connection
    raises :class:`~ctrader_asyncio.exceptions.ConnectionLostError`.  The
    caller should run it as an :class:`asyncio.Task` and cancel it when
    tearing down the connection.

    Args:
        connection: The active :class:`~ctrader_asyncio.connection.Connection`.
        interval: Seconds between heartbeats.  Defaults to
            :data:`HEARTBEAT_INTERVAL_SECONDS` (25 s).

    Raises:
        ConnectionLostError: If sending a heartbeat fails, propagated to the
            caller so the reconnect cycle can begin.
    """
    heartbeat = _common.ProtoHeartbeatEvent()
    while True:
        await asyncio.sleep(interval)
        try:
            await connection.send(heartbeat)
        except ConnectionLostError:
            raise
        except asyncio.CancelledError:
            raise
        except Exception as exc:
            raise ConnectionLostError(f"Heartbeat send failed: {exc}") from exc
