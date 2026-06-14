# ctrader-asyncio

`ctrader-asyncio` is a modern, asyncio-native Python client for the
[cTrader Open API](https://help.ctrader.com/open-api/). It replaces the
abandoned official Spotware SDK (`ctrader-open-api`), which is hard-pinned to
the legacy Twisted framework and has not been meaningfully maintained since
mid-2024. This library uses pure `asyncio`, fresh November 2025 proto files,
and has reconnect logic, heartbeat handling, and multi-account multiplexing
built in from the ground up.

## Key constraints

!!! warning "One connection per host"
    cTrader allows at most **one TCP connection per host type per `client_id`**.
    Opening a second simultaneous connection to the same host with the same
    credentials will cause `ProtoOAApplicationAuthReq` to time out silently.
    Each `CTraderClient` instance manages one demo connection and one live
    connection. If you have multiple credential sets, create one `CTraderClient`
    per set — do not try to share a connection across credential sets.

## Status

This library is currently in **pre-alpha** (Phase 1 — Foundation). It is not
yet available on PyPI. The first public release (`v0.1.0`) is planned for
completion of Phase 4.

## License

MIT — see [LICENSE](https://github.com/eq-digital/ctrader-asyncio/blob/main/LICENSE).
