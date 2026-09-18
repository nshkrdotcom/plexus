<p align="center">
  <img src="assets/plexus.svg" alt="Plexus Logo" width="200" height="200">
</p>

# Plexus

<p align="center">
  <a href="https://github.com/nshkrdotcom/plexus"><img src="https://img.shields.io/badge/GitHub-nshkrdotcom%2Fplexus-24292e?logo=github" alt="GitHub"/></a>
  <a href="https://hex.pm/packages/plexus"><img src="https://img.shields.io/hexpm/v/plexus.svg" alt="Hex.pm"/></a>
  <a href="https://hexdocs.pm/plexus"><img src="https://img.shields.io/badge/hex-docs-blue.svg" alt="HexDocs"/></a>
  <a href="LICENSE"><img src="https://img.shields.io/badge/license-MIT-blue.svg" alt="MIT License"/></a>
</p>

<p align="center">
  <b>A single-node BEAM kernel for massively asynchronous semantic computation.</b><br>
  Plexus turns <code>typesafe_sdk</code> 0.4.x into a coordination substrate for large semantic populations.
</p>

---

TypeSafe remains the measurement engine; Plexus owns the things a population needs around it: cheap births, typed topology, declarative actor effects, coalesced measurement, budgets, scheduling regimes, replay, calibration, pruning, and a separate expansion queue.

The design rule is simple: **ship primitives, not a catalog of hard-coded patterns.** Particle filters, asynchronous belief graphs, MCTS, branch-and-bound, semantic cellular automata, hypothesis populations, and recursive map/reduce should be strategies assembled from the same small kernel.

## Packages

| Package | Description |
| :--- | :--- |
| [`plexus`](packages/plexus/) | Core kernel — population runtime, actor interpreter, typed topology, measurement coalescing, budgets, scheduling, replay, calibration, and expansion queue. |

## Quick start

```elixir
def deps do
  [
    {:plexus, "~> 0.1.0"}
  ]
end
```

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

See the [package README](packages/plexus/README.md) for the full API walkthrough, actor examples, calibration, expansion, and guides.

## Kernel primitives

- **Population** — per-run metadata, secondary indexes, top-k/Pareto, seeded sampling and population operators.
- **Typed topology** — per-run ETS ordered-set edges such as `:child`, `:supports`, `:contradicts`, `:depends_on`, `:neighbor`, or application-defined edge types.
- **Local measurement** — named prepared contracts, memoization, replay, dedupe, coalescing, and `evaluate_many/4` as the normal framework path.
- **Scheduling regime** — `:async`, BSP buffering/barriers, bounded command progress and queued priority ordering.
- **Belief state** — explicit Bernoulli/categorical/ordinal projections with raw values retained.
- **Budget/admission** — atomic meters plus explicit hierarchical credit accounts.
- **Selection/pruning** — process cancellation/termination before topology deletion.
- **Expansion** — a separate priority queue, fail-closed capabilities, neutral streams/monitoring, accounting and proposal materialization.
- **Provenance/invalidation** — `:depends_on` edges plus epoch/stale repair helpers.
- **Run record/replay** — append-only events and versioned, checksummed fixed-response files.

## Architecture

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

## Repository layout

```
plexus/
├── packages/
│   └── plexus/          # core kernel (Elixir, Mix)
├── assets/              # repo-level SVG assets
├── LICENSE              # MIT
└── README.md            # this file
```

## Non-goals

Plexus is intentionally single-node. It does not provide multi-node distribution, durable process/mailbox persistence, provider abstraction, a general workflow DSL, prompt management, or tool-calling agent loops. Provider concerns belong in the inference layer; transport/retries remain TypeSafe/Pristine concerns.

## License

MIT © 2026 nshkrdotcom. See [LICENSE](LICENSE).
