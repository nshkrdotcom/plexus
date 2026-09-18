# Getting started

## Dependency

```elixir
{:plexus, "~> 0.1.0"}
```

## Start a run

```elixir
client = TypeSafeSDK.new_client(api_key: System.fetch_env!("TYPESAFE_API_KEY"))
{:ok, run} = Plexus.start_run(id: :demo, client: client)
```

A run owns:

- one shared `TypeSafeSDK.Client`
- one shared `Task.Supervisor`
- one `DynamicSupervisor` for semantic actors
- references into the graph store

## Start an actor

```elixir
{:ok, pid} =
  Plexus.start_actor(run,
    module: Plexus.Examples.IntakeCoordinator,
    actor_id: {:ticket, 1},
    init_arg: %{run: run, text: "Customer says checkout is broken."}
  )
```

## Trigger semantic work

```elixir
Plexus.cast(pid, :classify)
```

The actor returns `{:evaluate, ...}` and the TypeSafe SDK performs the semantic call asynchronously.
