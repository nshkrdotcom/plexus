<p align="center">
  <img src="assets/plexus.svg" width="200" alt="Plexus logo" />
</p>

# Plexus

**A single-node BEAM kernel for massively asynchronous semantic computation.**

Plexus turns `typesafe_sdk` 0.4.x into a coordination substrate for large semantic populations. TypeSafe remains the measurement engine; Plexus owns the things a population needs around it: cheap births, typed topology, declarative actor effects, coalesced measurement, budgets, scheduling regimes, replay, calibration, pruning, and a separate expansion queue.

The design rule is simple: **ship primitives, not a catalog of hard-coded patterns.** Particle filters, asynchronous belief graphs, MCTS, branch-and-bound, semantic cellular automata, hypothesis populations, and recursive map/reduce should be strategies assembled from the same small kernel.

> Status: this repository is still `0.1.0` and the current kernel pass was authored without an Elixir runtime in the authoring environment. The source is intentionally explicit about what is implemented versus what requires BEAM-side validation. See [HANDOFF.md](HANDOFF.md).

## Why Plexus exists

`typesafe_sdk` already provides prepared semantic contracts, typed noul/choice/score answers, batching, cancellation, telemetry, deterministic test fixtures, and an OTP facade. Plexus does **not** rebuild those capabilities.

Plexus adds a population runtime around them:

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

Independent actors no longer need to issue independent HTTP requests. Requests sharing a prepared-contract fingerprint and evaluation options are deduplicated and closed into bounded batches by size or delay.

## Kernel primitives

The kernel implements the following primitives, with precise semantics and limits in the guides:

- **Population** — per-run metadata, secondary indexes, top-k/Pareto, seeded sampling and population operators.
- **Typed topology** — per-run ETS ordered-set edges such as `:child`, `:supports`, `:contradicts`, `:depends_on`, `:neighbor`, or application-defined edge types.
- **Local measurement** — named prepared contracts, memoization, replay, dedupe, coalescing, and `evaluate_many/4` as the normal framework path.
- **Scheduling regime** — `:async`, BSP buffering/barriers, bounded command progress and queued priority ordering.
- **Belief state** — explicit Bernoulli/categorical/ordinal projections with raw values retained.
- **Quiescence counters** — run-scoped lock-free counters exposed to stop logic.
- **Budget/admission** — atomic meters plus explicit hierarchical credit accounts.
- **Selection/pruning** — process cancellation/termination before topology deletion.
- **Expansion** — Hex `inference` 0.4.1, a separate priority queue, fail-closed capabilities, neutral streams/monitoring, accounting and proposal materialization. Physical cancellation depends on provider support.
- **Provenance/invalidation** — `:depends_on` edges plus epoch/stale repair helpers.
- **Run record/replay** — append-only events and versioned, checksummed fixed-response files.
- **Stop/reduce** — composable predicates and population reducers.

## Hot-path architecture

The original scaffold serialized node birth through a run GenServer and a global graph GenServer. The current architecture removes those hot mailboxes:

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

Node birth is now one partitioned supervisor start plus lock-free ETS inserts for node/topology metadata. Each run owns its tables, so teardown destroys the run's data without walking a global store.

## Installation

```elixir
def deps do
  [
    {:plexus, "~> 0.1.0"}
  ]
end
```

Plexus depends on `typesafe_sdk ~> 0.4.0`, `pristine ~> 0.4.0` for physical batch cancellation tokens, and `telemetry ~> 1.3` for run/SDK event integration.

## Start a run

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

The prepared fingerprint becomes the cache key component, coalescer partition key, and comparability handle for experiments that hold semantics fixed while changing topology or scheduling.

## Actors declare effects

`Plexus.Actor` still uses `TypeSafeSDK.OTP.Server`, so direct `{:evaluate, ...}` remains available as a low-level escape hatch. The framework path is `Plexus.Actor.dispatch/2`:

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

TypeSafeSDK 0.4.x calls `handle_evaluation(result, tag, state)` for its direct OTP path. Plexus-managed measurement results instead arrive as `{:plexus, :measurement, tag, result}` so they can pass through cache/coalescing/budget policy first.

## Controlled scheduling experiments

The same actor code can be run asynchronously or under BSP:

```elixir
Plexus.schedule(run, :async)

Plexus.schedule(run, {:bsp, []})
{:ok, round, released_commands} = Plexus.barrier(run)
```

Replay mode is intended to hold semantic responses fixed while execution order changes. Record once, export the run's `{memo_key, result}` entries with `Plexus.replay_entries/1`, load them into a fresh `replay: :replay` run with `Plexus.load_replay/2`, and then change only the scheduling regime. Use `Plexus.Record.File.write/2` and `load/2` for versioned files with matching contract/config manifests.

## Calibration is part of the kernel, not an afterthought

Do not treat raw noul values as calibrated Bayesian likelihoods without evidence. Plexus includes a dependency-free reliability-diagram data path and isotonic calibrator:

```elixir
samples = [{0.1, false}, {0.3, false}, {0.7, true}, {0.9, true}]
rows = Plexus.Belief.Calibration.reliability(samples, 10)
model = Plexus.Belief.Calibration.fit_isotonic(samples)
calibrated_p = Plexus.Belief.Calibration.apply(model, 0.63)
```

The raw TypeSafe answer remains attached to `Plexus.Belief`; calibration is an explicit application/experiment choice.

## Expansion is a separate tier

Expensive generative work never shares the measurement queue. `Plexus.Expand.Queue` has its own task supervisor, concurrency limit, priority ordering, capability preflight seam, and standard strict proposal schema:

```elixir
Plexus.Expand.Schema.proposals()
```

Supply an `inference_client` to use `Plexus.Expand.InferenceAdapter`, backed by the published Hex dependency. See the expansion guide for stream monitoring, capability checks and provider-specific cancellation limits.

## Observability

Plexus emits `[:plexus, ...]` events for run/actor/measurement/expansion lifecycle and attaches a run-filtered handler to TypeSafeSDK semantic telemetry. TypeSafe token measurements feed the run budget ledger. The event log intentionally records bounded metadata rather than prompts, states, credentials, or raw transport bodies.

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
- [Experiments and measured results](guides/experiments.md)

## Non-goals

Plexus is intentionally single-node. It does not provide multi-node distribution, durable process/mailbox persistence, provider abstraction, a general workflow DSL, prompt management, or tool-calling agent loops. Provider concerns belong in the inference layer; transport/retries remain TypeSafe/Pristine concerns.

## License

MIT © 2026 nshkrdotcom. See [LICENSE](LICENSE).
