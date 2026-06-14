# Protobuf Notes

Known quirks and pitfalls when working with the cTrader Open API proto files.
These were learned in production and must be respected throughout the library.

## Singular field names on repeated fields

Protobuf repeated fields in cTrader responses use **singular** names, not plural.

```python
# WRONG
positions = response.positions

# CORRECT
positions = response.position
```

Always check the compiled `_pb2.py` file for the exact field name.

## HasField guards on optional fields

Always use `HasField` before accessing optional protobuf fields. Accessing an
unset optional field does not raise an exception — it returns a default value
silently, which can cause subtle bugs.

```python
# WRONG — silent default if field is unset
error_code = response.errorCode

# CORRECT
if response.HasField("errorCode"):
    error_code = response.errorCode
```

## Schema drift

The official Spotware SDK (`ctrader-open-api` 0.9.2) ships proto files that
are out of date. For example, the `returnProtectionOrders` field on
`ProtoOAReconcileReq` does not exist in the SDK's proto files but is present
in the November 2025 `openapi-proto-messages` repo.

This library always uses the fresh proto files from the forked
`eq-digital/openapi-proto-messages` repository. Never copy proto definitions
from the official SDK.

## Protobuf extraction before isinstance checks

Always extract the inner message from a wrapper before performing `isinstance`
checks. Checking the wrapper type directly will always pass regardless of the
actual message type inside.

```python
# WRONG
if isinstance(response, ProtoOAOrderErrorEvent):
    ...

# CORRECT — extract first, then check
message = extract(response)  # library utility
if isinstance(message, ProtoOAOrderErrorEvent):
    ...
```

## Token rotation

Spotware revokes old access tokens **immediately** on rotation. Never cache a
token across reconnects. The library always fetches the current token from the
caller at auth time, on every connection attempt.
