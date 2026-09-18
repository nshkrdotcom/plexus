# TypeSafe integration

Plexus is explicitly designed to use `typesafe_sdk` 0.4.0 **to the fullest**.

## Direct usage points

- `TypeSafeSDK.prepare!/1`
- `TypeSafeSDK.Prepared.fingerprint/1`
- `TypeSafeSDK.evaluate/4`
- `TypeSafeSDK.evaluate_stream/4`
- `TypeSafeSDK.evaluate_many/4`
- `TypeSafeSDK.OTP.Server`
- `TypeSafeSDK.Response.fetch/2`
- `TypeSafeSDK.Telemetry`
- `TypeSafeSDK.Test` in tests

## Contract reuse

`Plexus.Contract` centralizes prepared question sets so many actors can share the same semantic contract and stable fingerprint.

## Batched work

`Plexus.Strategy.Fanout` demonstrates how one root actor or coordinator can run many states through a single prepared contract using `evaluate_stream/4` or `evaluate_many/4` instead of issuing one-off calls.
