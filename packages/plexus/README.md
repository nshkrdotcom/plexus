<p align="center">
  <img src="assets/plexus.svg" width="200" alt="Plexus logo" />
</p>

# Plexus

**High-concurrency actor runtime for large-scale semantic graphs and search.**

Plexus is an Elixir actor runtime built on [TypeSafeSDK](https://github.com/nshkrdotcom/typesafe_sdk) for workloads that require large populations of concurrent actors—such as hypothesis swarms, Monte Carlo Tree Search, belief graphs, and spatiotemporal clustering.

Instead of each actor firing independent, uncoordinated API calls, Plexus acts as the coordination layer over [TypeSafeSDK](https://github.com/nshkrdotcom/typesafe_sdk):
- **Fast actor lifecycle** — Spawn and supervise thousands of concurrent actors across partitioned supervisors without coordinator mailbox bottlenecks.
- **Typed graph topology** — Maintain parent/child, dependency, and neighbor relationships directly in fast, run-isolated ETS tables.
- **Request coalescing** — Intercept evaluation requests across actors, automatically deduplicating identical calls and grouping them into short configurable windows.
- **Budget & scheduling control** — Enforce atomic token and cost limits, with support for both asynchronous execution and step-by-step barrier synchronization (BSP).

Rather than locking you into rigid agent patterns, Plexus provides composable primitives so you can assemble search trees, belief graphs, particle filters, hypothesis swarms, or recursive map/reduce workflows from the same core runtime.

> **Status**: Plexus is currently in early development (`0.1.0`). See [HANDOFF.md](HANDOFF.md).

## How it works

Actors declare what they want to evaluate. Plexus intercepts these effects, coalesces matching requests, and runs them as batched evaluations through [TypeSafe](https://github.com/nshkrdotcom/typesafe_sdk), returning the results directly to the requesting actors.

```text
Actor local state
    │
    ├─ commands ────────────────┐
    │                           ▼
    │                 Actor.Interpreter
    │                  │   │   │   │
    │                  │   │   │   └── expansion queue
    │                  │   │   └────── budgets / admission
    │                  │   └────────── typed graph / pruning
    │                  └────────────── scheduler / replay / record
    │
    └─ measure ──> cache/replay ──> per-contract coalescer
                                      │
                                      └─ TypeSafeSDK.evaluate_many/4
```

Independent actors do not need to manage individual HTTP requests or rate limits. Requests sharing a prepared contract and evaluation options are deduplicated and grouped by size or a short configurable delay window before hitting the measurement backend.

## Core primitives

- **Population runtime** — Per-run metadata, secondary indexes, top-k/Pareto tracking, and seeded sampling.
- **Typed topology** — Fast in-memory edges in ETS (such as `:child`, `:supports`, `:contradicts`, `:depends_on`, `:neighbor`, or custom edge types).
- **Measurement coalescing** — Named prepared contracts with automatic deduplication, memoization, and batching through [`TypeSafeSDK.evaluate_many/4`](https://github.com/nshkrdotcom/typesafe_sdk).
- **Scheduling regimes** — Support for asynchronous execution, Bulk Synchronous Parallel (BSP) barriers, and prioritized command queues.
- **Belief state** — Track confidence distributions (Bernoulli, categorical, ordinal) alongside raw evaluation values.
- **Quiescence counters** — Run-scoped lock-free counters for termination and quiescence detection.
- **Budgets & admission** — Atomic token and cost meters with hierarchical credit accounts.
- **Selection & pruning** — Coordinated actor termination and graph cleanup.
- **Generative expansion** — Dedicated priority queue and supervision for LLM proposals, isolated from measurement traffic.
- **Provenance & invalidation** — Dependency tracking with epoch-based cache invalidation.
- **Run recording & replay** — Append-only event logs and deterministic replay fixtures.
- **Stop & reduce** — Composable termination predicates and population reducers.

## Architecture

Plexus uses partitioned dynamic supervisors and per-run ETS tables to avoid bottlenecking on a single coordinator process:

```text
Plexus.Supervisor
├── partitioned Registry
├── Run.Directory                 # run_id -> per-run config-table pointer
└── RunSupervisor
    └── Plexus.Run.Supervisor (per run)
        ├── Run.Owner             # owns all per-run ETS tables/counters
        ├── Task.Supervisor       # measurement batch wrappers
        ├── Expand Task.Supervisor
        ├── PartitionSupervisor
        │   └── N × DynamicSupervisor  # actor population, hashed by actor_id
        ├── Measure DynamicSupervisor   # per-contract coalescers
        ├── Schedule.Server
        └── Expand.Queue
```

Spawning an actor only requires starting a worker in the partitioned supervisor and writing edge metadata to ETS. Each run manages its own ETS tables, allowing clean and immediate table destruction when the run finishes.

## Installation

```elixir
def deps do
  [
    {:plexus, "~> 0.1.0"}
  ]
end
```

Plexus is built on [`typesafe_sdk`](https://github.com/nshkrdotcom/typesafe_sdk) (which is included automatically), and integrates with `pristine` (for cancellation tokens) and `telemetry` for event dispatching.

## Start a run

Configure a [TypeSafeSDK client](https://github.com/nshkrdotcom/typesafe_sdk) and pass it when starting your run:

```elixir
client = TypeSafeSDK.new_client(api_key: System.fetch_env!("TYPESAFE_API_KEY"))

{:ok, run} =
  Plexus.start_run(
    id: :experiment_42,
    client: client,
    max_population: 100_000,
    max_depth: 64,
    budgets: [measure: 1_000_000, expand: 500, tokens: 5_000_000],
    batch: [max: 64, delay_ms: 10, max_concurrency: 16],
    cache: [ttl_ms: 60_000, max_entries: 100_000],
    schedule: :async,
    replay: :record
  )
```

## Register a contract once

```elixir
Plexus.register_contract(run, :bounds,
  TypeSafeSDK.prepare!(
    contains_useful: TypeSafeSDK.noul("Could this region contain a strong candidate?"),
    density: TypeSafeSDK.score("How dense are good candidates here?", ~w(none low med high))
  ),
  version: 1,
  batch: [max: 64, delay_ms: 10]
)
```

The prepared fingerprint serves as the cache key, coalescer partition key, and comparability handle for experiments where you hold semantics fixed while changing topology or scheduling.

## Actors declare effects

`Plexus.Actor` builds on [`TypeSafeSDK.OTP.Server`](https://github.com/nshkrdotcom/typesafe_sdk), so direct `{:evaluate, ...}` remains available as a lower-level escape hatch when needed. The standard framework path is `Plexus.Actor.dispatch/2`:

```elixir
defmodule MyApp.Region do
  use Plexus.Actor

  alias Plexus.Actor

  @impl true
  def init(args) do
    {:ok, %{context: Actor.context(args), region: args.region}}
  end

  @impl true
  def handle_cast(:measure, state) do
    Actor.dispatch(state.context,
      {:measure, :bounds, %{region: state.region}, :bounds, []}
    )

    {:noreply, state}
  end

  def handle_cast({:plexus, :measurement, :bounds, {:ok, response}}, state) do
    belief = Plexus.Belief.from(response, :contains_useful)

    commands =
      if Plexus.Belief.probability(belief) < 0.1 do
        [{:prune, state.context.actor_id}]
      else
        [{:belief, belief}]
      end

    Actor.dispatch(state.context, commands)
    {:noreply, state}
  end

  # Required by TypeSafeSDK.OTP.Server; used only by direct {:evaluate, ...} calls.
  @impl true
  def handle_evaluation(result, tag, state) do
    {:noreply, Map.put(state, :last_direct_evaluation, {tag, result})}
  end
end
```

Direct evaluations via TypeSafe trigger `handle_evaluation(result, tag, state)`. In contrast, Plexus-managed measurements return as `{:plexus, :measurement, tag, result}` so they pass through caching, request coalescing, and budget accounting first.

## Example applications

The `examples/` directory contains six runnable dataset-backed workloads. `02_incident_commander` is the actor-model reference application; the other five are integration/acceptance workloads and are not presented as evidence that actor execution is inherently better than a centralized async pipeline:

- **SWE-bench Verified issue swarm** — Semantic issue routing evaluated against real patch-shape labels.
- **NYC 311 city signal tracker** — Spatiotemporal incident clustering over service request feeds, where only dense clusters allocate semantic budget.
- **GAIA living incident commander** — Raw historical telemetry is replayed while persistent service/hypothesis actors are alive. Hypotheses revise repeatedly, message/challenge peers, react to provenance invalidation, spend local branch credit, and can have live subtrees pruned. Fault-injection labels are withheld until post-run scoring.
- **deps.dev dependency upgrade search** — Resolves dependency trees, evaluates changed-node risk, and runs a pruned beam search over migration order.
- **SciFact research evidence graph** — Support/contradiction graph evaluated against scientific claims.
- **NOAA Storm Events alert swarm** — Processes storm event feeds by waking daily worker populations and aggregating state-level impacts.

Each example includes a script to fetch the upstream dataset into `.plexus-data/` (ignored by Git). Minimal test fixtures are included in the repository for offline testing.

See the [example catalog](examples/README.md), [dataset notes](examples/DATASETS.md), and [example applications guide](guides/examples.md).

## Controlled scheduling experiments

The same actor code can run asynchronously or under Bulk Synchronous Parallel (BSP) barriers:

```elixir
Plexus.schedule(run, :async)

Plexus.schedule(run, {:bsp, []})
{:ok, round, released_commands} = Plexus.barrier(run)
```

Replay mode allows holding model responses fixed while testing different scheduling regimes or actor topologies. Record a run once, export the `{memo_key, result}` entries with `Plexus.replay_entries/1`, load them into a fresh run with `replay: :replay`, and compare behavior under different execution orders. Use `Plexus.Record.File.write/2` and `load/2` for versioned files with matching contract manifests.

## Calibration and belief state

Raw model scores often require calibration before they can be treated as reliable probabilities. Plexus provides reliability diagrams and isotonic calibration out of the box:

```elixir
samples = [{0.1, false}, {0.3, false}, {0.7, true}, {0.9, true}]
rows = Plexus.Belief.Calibration.reliability(samples, 10)
model = Plexus.Belief.Calibration.fit_isotonic(samples)
calibrated_p = Plexus.Belief.Calibration.apply(model, 0.63)
```

The raw TypeSafe response remains accessible on `Plexus.Belief`; calibration is an explicit step when needed by your application.

## Generative expansion

Generative tasks (such as synthesizing new proposals or hypotheses) run on a dedicated queue separate from fast measurement contracts. `Plexus.Expand.Queue` provides its own task supervision, concurrency controls, priority ordering, and strict proposal schema validation:

```elixir
Plexus.Expand.Schema.proposals()
```

Provide an `inference_client` to use `Plexus.Expand.InferenceAdapter`. See the [expansion guide](guides/expansion.md) for details on capability checks, stream monitoring, and provider cancellation handling.

## Observability

Plexus emits `[:plexus, ...]` telemetry events across actor lifecycles, measurements, and expansion queues. It also attaches a run-scoped handler to TypeSafe telemetry to update token budget ledgers in real time. Event logs track operational metadata and counters without logging sensitive payload bodies or credentials.

```elixir
Plexus.budget(run)
Plexus.events(run)
```

## Guides

- [Getting started](guides/getting-started.md)
- [Architecture](guides/architecture.md)
- [Kernel primitives](guides/kernel-primitives.md)
- [Actor runtime](guides/actor-runtime.md)
- [TypeSafe integration](guides/typesafe-integration.md)
- [Graph and subtrees](guides/graph-and-subtrees.md)
- [Calibration and replay](guides/calibration-and-replay.md)
- [Expansion tier](guides/expansion.md)
- [Testing and release](guides/testing-and-release.md)
- [Example applications](guides/examples.md)
- [Experiments and measured results](guides/experiments.md)

## Non-goals

Plexus is focused on single-node execution. It does not handle distributed multi-node clusters, durable mailbox persistence across node crashes, or high-level prompt engineering and chat loops. Transport retries and provider APIs remain the responsibility of [TypeSafe](https://github.com/nshkrdotcom/typesafe_sdk) and the inference layer.

## License

MIT © 2026 nshkrdotcom. See [LICENSE](LICENSE).
