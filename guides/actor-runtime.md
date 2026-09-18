# Actor runtime

Plexus actors remain ordinary `TypeSafeSDK.OTP.Server` modules.

## Two evaluation paths

The direct TypeSafe path is still valid:

```elixir
{:evaluate, {tag, input, prepared}, state}
```

TypeSafeSDK later calls:

```elixir
handle_evaluation({:ok, response}, tag, state)
# or
handle_evaluation({:error, error}, tag, state)
```

Use this path only when intentionally bypassing Plexus policy.

The normal Plexus path is a command:

```elixir
Plexus.Actor.dispatch(context,
  {:measure, tag, input, contract_or_name, opts}
)
```

The actor receives:

```elixir
{:plexus, :measurement, tag, {:ok, response}}
```

through `handle_cast/2`.

## Why commands matter

Actor-to-framework effects go through one interpreter so Plexus can interpose:

- depth and population admission
- budget checks
- scheduler barriers
- cache/replay
- coalescing
- typed topology
- cancellation/pruning
- run recording and telemetry

An actor that directly starts children or directly evaluates TypeSafe has explicitly stepped outside those policies.

## Shared bounded concurrency

Measurement coalescers launch whole batches under the run's measurement task supervisor; TypeSafeSDK then bounds the evaluations within that batch. Expansion has a completely separate task supervisor and queue.
