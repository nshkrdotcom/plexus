# Architecture

Plexus is a thin coordination kernel over TypeSafeSDK, not a replacement semantic client.

## Global layer

Only three resources are global:

- a partitioned `Registry`
- `Run.Directory`, a tiny ETS directory from `run_id` to the run's config table
- the `DynamicSupervisor` that owns run supervisors

There is no global graph GenServer and no run GenServer on the actor-birth hot path.

## Per-run resource island

Each `Run.Supervisor` owns:

- `Run.Owner`, which owns all per-run ETS tables and lock-free counters
- a task supervisor for measurement-batch wrappers
- a distinct task supervisor for expensive expansion
- a `PartitionSupervisor` whose partitions are `DynamicSupervisor`s for actors
- a dynamic supervisor for per-contract measurement coalescers
- a scheduling policy server
- an expansion priority queue

`Run.Config.fetch!/1` reads the resource map directly from ETS. Actor birth then routes straight to a supervisor partition selected by `actor_id`; graph metadata writes are ETS operations.

## Effect path

Actors declare cross-cutting work with `Actor.Command`. `Actor.Interpreter` is the single policy seam for spawning, graph edges, measurement, expansion, belief publication, budget operations, pruning, sleep/wake, and completion.

This is what makes depth/population limits, replay, scheduler barriers and future governance interposable. Strategy modules should not call low-level spawn/evaluation APIs unless intentionally opting out of framework policy.

## Measure and expand are different tiers

Measurement uses TypeSafe prepared contracts, large concurrency, cache/dedupe and batching. Expansion is rarer and expensive, uses a separate bounded queue, and is represented by a provider-neutral adapter seam. A slow expansion can therefore never consume the measurement task slots.

## Failure domains

Run-owned ETS tables die with `Run.Owner`. The run supervisor uses `:rest_for_one`, so losing the owner reconstructs downstream run resources rather than leaving workers attached to stale table ids. A run crash is isolated from other runs.
