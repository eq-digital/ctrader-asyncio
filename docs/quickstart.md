# Quickstart

## Installation

```bash
pip install ctrader-asyncio
```

## Prerequisites

You need a cTrader Open API application registered at
[openapi.ctrader.com](https://openapi.ctrader.com). You will be given a
**Client ID** and **Client Secret**. You also need at least one trader account
ID (`ctidTraderAccountId`) and a valid **access token** for it.

## Minimal example

```python
import asyncio
from ctrader_asyncio import CTraderClient

async def get_token(account_id: int) -> str:
    # Return a fresh access token for this account.
    # In production, fetch this from your database or secrets vault.
    return "your-access-token"

async def main():
    client = CTraderClient(
        host="demo.ctraderapi.com",       # or live.ctraderapi.com
        client_id="your-client-id",
        client_secret="your-client-secret",
        account_ids=[12345678],
        token_provider=get_token,
    )

    async with client:
        # Block until app auth + all account auths complete (up to 60 s).
        await client.wait_until_ready()
        print("Connected and authenticated.")

asyncio.run(main())
```

## Listening for events

Register an async callback before entering the context manager.
The callback receives the raw `ProtoMessage` envelope — decode the payload
yourself using the compiled proto classes.

```python
import asyncio
from ctrader_asyncio import CTraderClient
from ctrader_asyncio.proto import OpenApiMessages_pb2 as oa

PROTO_OA_SPOT_EVENT = 2126

async def get_token(account_id: int) -> str:
    return "your-access-token"

async def on_spot(envelope):
    event = oa.ProtoOASpotEvent()
    event.ParseFromString(envelope.payload)
    print(f"Symbol {event.symbolId}: bid={event.bid}  ask={event.ask}")

async def main():
    client = CTraderClient(
        host="demo.ctraderapi.com",
        client_id="your-client-id",
        client_secret="your-client-secret",
        account_ids=[12345678],
        token_provider=get_token,
    )

    client.register(PROTO_OA_SPOT_EVENT, on_spot)

    async with client:
        await client.wait_until_ready()
        # Subscribe to spot prices, send other requests, etc.
        await asyncio.sleep(60)

asyncio.run(main())
```

## Sending a request

Use `client.send_request()` with any compiled proto request message.
It returns the response `ProtoMessage` envelope.

```python
from ctrader_asyncio.proto import OpenApiMessages_pb2 as oa

async with client:
    await client.wait_until_ready()

    req = oa.ProtoOAVersionReq()
    response_envelope = await client.send_request(req)

    version_res = oa.ProtoOAVersionRes()
    version_res.ParseFromString(response_envelope.payload)
    print("Server version:", version_res.version)
```

## Error handling

All library errors inherit from `CTraderError`:

```python
from ctrader_asyncio import (
    CTraderError,
    ConnectionLostError,
    AuthenticationError,
    RequestTimeoutError,
    ServerError,
)

try:
    response = await client.send_request(req)
except ServerError as e:
    print(f"Server rejected the request: {e.error_code}")
except RequestTimeoutError as e:
    print(f"Request timed out: {e.client_msg_id}")
except ConnectionLostError:
    print("Connection dropped — client will reconnect automatically")
```

## Reconnection

By default (`reconnect=True`) the client reconnects automatically on connection
loss using exponential backoff (1 s → 2 s → 4 s … capped at 60 s). Tokens are
fetched fresh from `token_provider` on every reconnect — never cached.

To disable reconnection:

```python
client = CTraderClient(..., reconnect=False)
```
