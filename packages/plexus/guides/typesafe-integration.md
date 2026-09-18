# TypeSafe integration

Plexus targets `typesafe_sdk ~> 0.4.0` and uses its public semantic/runtime surface rather than recreating it.

## Prepared contracts

`TypeSafeSDK.prepare!/1` and `TypeSafeSDK.Prepared.fingerprint/1` back `Plexus.Contract.Registry`. Named/versioned contracts make the same semantic questions reusable across many actors and experiments.

## Batch-first measurement

The framework path resolves a contract fingerprint, derives `Contract.memo_key/2`, checks replay/cache, and submits misses to a coalescer. Coalescers close on batch size or delay and call:

```elixir
TypeSafeSDK.evaluate_many(client, states, prepared,
  ordered: true,
  on_error: :collect,
  max_concurrency: max_concurrency,
  cancellation: token
)
```

A shared `Pristine.Cancellation` token belongs to each physical batch. Pruning removes the actor's waiters; if no waiters remain, the batch token is cancelled.

`evaluate_stream/4` remains exposed through `Plexus.Strategy.Fanout` for coordinator-held sweeps that benefit from lazy early results.

## OTP server

`Plexus.Actor` uses `TypeSafeSDK.OTP.Server`. The SDK callback order is `handle_evaluation(result, tag, state)`. The README/examples use that order for direct evaluations.

## Telemetry

Each run attaches to TypeSafeSDK evaluate/answer/batch-cancel telemetry and filters on explicit `telemetry_metadata.plexus_run_id`. Plexus does not log semantic state, questions, API keys, bodies or transport errors. Token measurements from successful TypeSafe calls update the run's observed token ledger.

## Test fixtures

`TypeSafeSDK.Test` remains the deterministic semantic oracle. The Plexus test suite uses the real serialization/decoding path while replacing only the transport, making scheduler/coalescer tests meaningful without live API calls.

## Live transport evidence

The standalone dataset runtime attaches a privacy-safe listener to TypeSafeSDK evaluation telemetry.
It distinguishes Plexus logical measurement accounting from confirmed provider responses. A provider
response is confirmed only when telemetry includes an HTTP status and non-empty request ID.

The summary exposes endpoint/model selection, returned model, status distribution, request IDs, retries,
and token usage without storing semantic state, question text, request/response bodies, credentials, or
authorization headers.
