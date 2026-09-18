# Getting started

## Dependency

```elixir
{:plexus, "~> 0.1.0"}
```

Plexus already depends on `typesafe_sdk ~> 0.4.0`.

## Start a run

```elixir
client = TypeSafeSDK.new_client(api_key: System.fetch_env!("TYPESAFE_API_KEY"))

{:ok, run} =
  Plexus.start_run(
    id: :demo,
    client: client,
    max_population: 10_000,
    budgets: [measure: 100_000, expand: 100],
    batch: [max: 64, delay_ms: 10],
    schedule: :async
  )
```

A run owns its own ETS graph/cache/contracts/record/replay tables, atomic budget ledger, quiescence counters, partitioned actor supervisors, measurement-coalescer supervisor and a separate expansion queue.

## Register reusable semantic contracts

```elixir
prepared = TypeSafeSDK.prepare!(urgent: TypeSafeSDK.noul("Is this urgent?"))
:ok = Plexus.register_contract(run, :triage, prepared, version: 1)
```

## Start an actor

```elixir
{:ok, pid} =
  Plexus.start_actor(run,
    module: MyApp.Worker,
    actor_id: {:ticket, 1},
    class: :ticket,
    init_arg: %{text: "Checkout is broken."}
  )
```

The actor receives `run`, `run_id`, `actor_id`, `parent_id`, `class`, and `metadata` in its init argument.

## Request framework-managed measurement

Inside the actor:

```elixir
Plexus.Actor.dispatch(state.context,
  {:measure, :triage, %{ticket: state.text}, :triage, []}
)
```

The result arrives later as:

```elixir
{:plexus, :measurement, :triage, {:ok, response}}
```

This path can cache, replay, dedupe, coalesce, charge budgets and obey the run scheduling regime before TypeSafe work starts.
