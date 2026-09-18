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

## Shared concurrency control

Measurement coalescers launch batch-wrapper tasks under the run's measurement task supervisor; TypeSafeSDK controls concurrency among the distinct evaluations in each group. Expansion has a separate task supervisor and queue. Distinct uncached TypeSafeSDK 0.4 inputs remain distinct provider requests.

## Completion, resident actors, and managed activity

The default `activity_mode: :work` means an actor counts as active work until `{:complete, result}` or termination retires its lifecycle reference. `{:complete, result}` publishes the result while leaving the process addressable for queries; repeated completion and later termination retire it only once.

`activity_mode: :resident` is for long-lived entities such as system-twin services or hypotheses. A resident actor's mere existence does not keep quiescence false. Its managed messages, command envelopes, timers, measurements, and expansion work still participate in the same accounting. This allows tens of thousands of idle addressable entities without pretending they are completed jobs.

```elixir
{:ok, pid} =
  Plexus.Run.start_actor(run,
    module: MyServiceActor,
    actor_id: {:service, "checkout"},
    class: :service,
    activity_mode: :resident,
    init_arg: %{service: "checkout"}
  )
```

Use `Plexus.Run.cast/3` and `call/4` to include queued messages in quiescence accounting; callback wrappers release each activity ticket once. Raw `send`, direct GenServer calls and direct TypeSafe OTP evaluation are escape hatches outside managed message accounting and should not be used when run settlement depends on quiescence.
