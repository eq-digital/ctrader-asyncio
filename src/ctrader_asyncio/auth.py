"""Application and account authentication for the cTrader Open API.

Auth sequence (strict — do not reorder):

1. :func:`authenticate_app` — sends ``ProtoOAApplicationAuthReq``, waits for
   ``ProtoOAApplicationAuthRes``.  Sets ``app_authed`` event on success.
2. :func:`authenticate_accounts` — sends one ``ProtoOAAccountAuthReq`` per
   account ID **concurrently**, waits for all responses.  Tokens are fetched
   fresh from ``token_provider`` at call time — never cached.

Both functions raise on failure.  The caller (``CTraderClient``) is
responsible for clearing ``app_authed`` and retrying after a reconnect.
"""

from __future__ import annotations

import asyncio
import sys
from pathlib import Path
from typing import Callable, Awaitable

_PROTO_DIR = str(Path(__file__).parent / "proto")
if _PROTO_DIR not in sys.path:
    sys.path.insert(0, _PROTO_DIR)

from ctrader_asyncio.proto import OpenApiMessages_pb2 as _oa
from ctrader_asyncio.exceptions import AuthenticationError


async def authenticate_app(
    dispatcher: object,
    connection: object,
    client_id: str,
    client_secret: str,
    app_authed: asyncio.Event,
) -> None:
    """Send ``ProtoOAApplicationAuthReq`` and set *app_authed* on success.

    Args:
        dispatcher: The active :class:`~ctrader_asyncio.dispatcher.Dispatcher`.
        connection: The active :class:`~ctrader_asyncio.connection.Connection`.
        client_id: The cTrader Open API client ID.
        client_secret: The cTrader Open API client secret.
        app_authed: An :class:`asyncio.Event` that is set when app auth
            succeeds.  The caller should clear this before every reconnect.

    Raises:
        AuthenticationError: If the server responds with an error.
        RequestTimeoutError: If no response arrives within 30 seconds.
        ConnectionLostError: If the connection drops during the request.
    """
    req = _oa.ProtoOAApplicationAuthReq(
        clientId=client_id,
        clientSecret=client_secret,
    )
    try:
        await dispatcher.send_request(connection, req)
    except Exception as exc:
        raise AuthenticationError(f"Application auth failed: {exc}") from exc

    app_authed.set()


async def authenticate_accounts(
    dispatcher: object,
    connection: object,
    account_ids: list[int],
    token_provider: Callable[[int], Awaitable[str]],
) -> None:
    """Authenticate all accounts concurrently.

    Fetches an access token for each account by calling ``token_provider``
    and sends all ``ProtoOAAccountAuthReq`` messages concurrently via
    :func:`asyncio.gather`.

    Args:
        dispatcher: The active :class:`~ctrader_asyncio.dispatcher.Dispatcher`.
        connection: The active :class:`~ctrader_asyncio.connection.Connection`.
        account_ids: List of ``ctidTraderAccountId`` values to authenticate.
        token_provider: An async callable ``(account_id: int) -> str`` that
            returns a fresh access token.  Called once per account per auth
            cycle — tokens are never cached.

    Raises:
        AuthenticationError: If any account auth fails.
        RequestTimeoutError: If any request times out.
        ConnectionLostError: If the connection drops during any request.
    """
    async def _auth_one(account_id: int) -> None:
        token = await token_provider(account_id)
        req = _oa.ProtoOAAccountAuthReq(
            ctidTraderAccountId=account_id,
            accessToken=token,
        )
        try:
            await dispatcher.send_request(connection, req)
        except Exception as exc:
            raise AuthenticationError(
                f"Account auth failed for {account_id}: {exc}"
            ) from exc

    await asyncio.gather(*(_auth_one(aid) for aid in account_ids))
