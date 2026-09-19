<p align="center">
  <img src="assets/plexus.svg" alt="Plexus Logo" width="200" height="200">
</p>

# Plexus

<p align="center">
  <a href="https://github.com/nshkrdotcom/plexus"><img src="https://img.shields.io/badge/GitHub-nshkrdotcom%2Fplexus-24292e?logo=github" alt="GitHub"/></a>
  <a href="https://github.com/nshkrdotcom/typesafe_sdk"><img src="https://img.shields.io/badge/built%20on-TypeSafeSDK-blueviolet?logo=github" alt="TypeSafeSDK"/></a>
  <a href="https://hex.pm/packages/plexus"><img src="https://img.shields.io/hexpm/v/plexus.svg" alt="Hex.pm"/></a>
  <a href="https://hexdocs.pm/plexus"><img src="https://img.shields.io/badge/hex-docs-blue.svg" alt="HexDocs"/></a>
  <a href="LICENSE"><img src="https://img.shields.io/badge/license-MIT-blue.svg" alt="MIT License"/></a>
</p>

<p align="center">
  <b>High-concurrency actor runtime for large-scale semantic graphs and search.</b><br>
  Coordinate thousands of evaluating actors with automatic request coalescing, graph topology, and strict token budgets.
</p>

---

Plexus is an Elixir actor runtime built on [TypeSafeSDK](https://github.com/nshkrdotcom/typesafe_sdk) for workloads that require large populations of concurrent actors—such as hypothesis swarms, Monte Carlo Tree Search, belief graphs, and spatiotemporal clustering.

Instead of each actor firing independent, uncoordinated API calls, Plexus acts as the coordination layer over [TypeSafeSDK](https://github.com/nshkrdotcom/typesafe_sdk):
- **Fast actor lifecycle** — Spawn and supervise thousands of concurrent actors across partitioned supervisors without coordinator mailbox bottlenecks.
- **Typed graph topology** — Maintain parent/child, dependency, and neighbor relationships directly in fast, run-isolated ETS tables.
- **Request coalescing** — Intercept evaluation requests across actors, automatically deduplicating identical calls and batching them into bounded windows.
- **Budget & scheduling control** — Enforce atomic token and cost limits, with support for both asynchronous execution and step-by-step barrier synchronization (BSP).

Rather than locking you into rigid agent patterns, Plexus provides composable primitives so you can assemble search trees, belief graphs, particle filters, hypothesis swarms, or recursive map/reduce workflows from the same core runtime.

## Packages

| Package | Description |
| :--- | :--- |
| [`plexus`](packages/plexus/) | Core runtime — actor supervision, typed topology, measurement coalescing, budgets, scheduling, replay, calibration, and expansion queue. |

## Quick start

Add `plexus` to your `mix.exs` dependencies. It automatically includes [`typesafe_sdk`](https://github.com/nshkrdotcom/typesafe_sdk) as its core semantic evaluation engine:

```elixir
def deps do
  [
    {:plexus, "~> 0.1.0"}
  ]
end
```

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

See the [package README](packages/plexus/README.md) for the full API walkthrough, actor examples, calibration, expansion, and guides.

## Core primitives

- **Population runtime** — Per-run metadata, secondary indexes, top-k/Pareto tracking, and seeded sampling.
- **Typed topology** — Fast in-memory edges in ETS (e.g., `:child`, `:supports`, `:contradicts`, `:depends_on`, `:neighbor`, or custom edge types).
- **Measurement coalescing** — Named prepared contracts with automatic deduplication, memoization, and batching via [`TypeSafeSDK.evaluate_many/4`](https://github.com/nshkrdotcom/typesafe_sdk).
- **Scheduling regimes** — Support for asynchronous dispatch, Bulk Synchronous Parallel (BSP) barriers, and prioritized command queues.
- **Belief state** — Track confidence distributions (Bernoulli, categorical, ordinal) alongside raw evaluation values.
- **Budgets & admission** — Atomic token and cost meters with hierarchical credit accounts.
- **Selection & pruning** — Coordinated actor termination and graph cleanup.
- **Generative expansion** — Dedicated priority queue and supervision for LLM proposals, isolated from measurement traffic.
- **Provenance & invalidation** — Dependency tracking with epoch-based cache invalidation.
- **Run recording & replay** — Append-only event logs and deterministic replay fixtures.

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

Plexus is focused on single-node execution. It does not handle distributed multi-node clusters, durable mailbox persistence across node crashes, or high-level prompt engineering and chat loops. Transport retries and provider APIs remain the responsibility of [TypeSafe](https://github.com/nshkrdotcom/typesafe_sdk) and the inference layer.

## License

MIT © 2026 nshkrdotcom. See [LICENSE](LICENSE).
