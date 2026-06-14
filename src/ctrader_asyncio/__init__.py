"""ctrader-asyncio — asyncio-native Python client for the cTrader Open API.

Typical usage::

    import asyncio
    from ctrader_asyncio import CTraderClient

    async def get_token(account_id: int) -> str:
        return "your-access-token"

    async def main():
        client = CTraderClient(
            host="demo.ctraderapi.com",
            client_id="your-client-id",
            client_secret="your-client-secret",
            account_ids=[12345678],
            token_provider=get_token,
        )
        async with client:
            await client.wait_until_ready()
            # client is ready — send requests here

    asyncio.run(main())
"""

from ctrader_asyncio.client import CTraderClient
from ctrader_asyncio.exceptions import (
    CTraderError,
    ConnectionLostError,
    AuthenticationError,
    RequestTimeoutError,
    ServerError,
)

__all__ = [
    "CTraderClient",
    "CTraderError",
    "ConnectionLostError",
    "AuthenticationError",
    "RequestTimeoutError",
    "ServerError",
]
