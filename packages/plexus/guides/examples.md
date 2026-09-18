# Example applications

The `examples/` directory demonstrates what Plexus can *be used to build*, rather than mirroring the kernel API one primitive at a time.

The examples span six computational shapes: semantic routing, spatiotemporal clustering, root-cause hypothesis populations, resolved dependency-graph search, scientific evidence graphs, and a large event-driven swarm. TypeSafe/Jev usage ranges from dense to sparse, but semantic work always enters through Plexus's managed measurement path so budget, cache/replay, coalescing, cancellation and run telemetry remain interposable.

## Dataset-first design

Each application targets a recognized external dataset and includes a fetch/preparation step. The datasets are cached locally under `.plexus-data/` and are not part of the Hex package's source repository history. Workload knobs bound *a run*, not the conceptual dataset.

See [`examples/README.md`](../examples/README.md) for the catalog and exact commands, and [`examples/DATASETS.md`](../examples/DATASETS.md) for source/provenance notes.

## Why these are not benchmarks

The examples print useful counts and application outcomes, but they do not create new throughput claims. Runtime characterization remains in `experiments/` with retained artifacts and methodology. In particular, the NOAA event swarm is an application workload; the existing node-birth experiment remains the source for raw 100,000-actor birth measurements.

## Live semantic calls

Example run scripts require a real TypeSafe client. Offline deterministic coverage belongs to the test suite. There is no hidden mock mode in `examples/`.

For expensive runs, control the number of *semantic decision points* separately from the number of actors. This is often the point of Plexus: a large stateful population can contain comparatively sparse semantic measurements.

## Verifying that semantic work reached TypeSafe

Standalone dataset examples expose two independent facts:

1. the application-level `semantic measurements` count; and
2. the `TYPESAFE LIVE TRANSPORT SUMMARY` emitted from TypeSafeSDK telemetry.

For live acceptance, use the transport summary as the network proof. A confirmed response has an HTTP
status and provider request ID and reports the returned model plus token usage. This prevents a Plexus
measurement/accounting event from being mistaken for evidence that a remote provider call actually occurred.
