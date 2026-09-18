# Example applications

The `examples/` directory contains two categories that must not be conflated. `02_incident_commander` is the actor-model reference application; the other five are real-dataset integration workloads that validate ingestion, managed semantic calls, graph/population mechanics, and packaging. They are not evidence that actors improve a workload whose essential control flow is still ordinary centralized async fan-out.

Real data and real provider calls are necessary integration evidence, but they are not sufficient actor-model evidence.

## Flagship acceptance criterion

A flagship example fails if deleting the actors and replacing them with a central `Enum.map` / `Task.async_stream` / `asyncio.gather` preserves essentially the same state model and control flow.

The GAIA living system twin instead requires these properties simultaneously:

- service and hypothesis identities persist across many evidence epochs;
- raw evidence arrives while the actor graph is already running;
- actors receive direct managed messages from independent producers and peers;
- topology can change after hypotheses already exist;
- provenance invalidation changes future work in already-live actors;
- the same hypothesis can perform many semantic revisions;
- hypotheses can challenge peers without reporting to a global arbiter;
- hierarchical branch credit changes which child investigations can exist;
- runtime pruning can cancel a live hypothesis subtree while unrelated actors continue;
- the final hypothesis population is not known at startup;
- termination emerges from telemetry EOF plus zero managed activity, not an expected completion count.

If a conventional baseline can preserve those semantics with a simple queue/gather loop, the example has failed its purpose.

## GAIA chronological system twin

`02_incident_commander` reads real GAIA trace and business CSV streams lazily. A replay actor merges source heads in historical event-time order and dispatches raw events to resident service actors. No finished semantic service summaries are created before execution.

Service actors maintain rolling local telemetry state and construct `:calls` topology as trace parentage becomes known. Relevant anomaly events trigger focused Jev measurements. Hypotheses remain resident, subscribe to service-change events, request snapshots actor-to-actor, revise repeatedly, challenge competing hypotheses, and respond to provenance invalidation.

The run/fault-injection rows are not visible to the live actors or semantic contracts. They are read only after quiescence for post-run scoring.

See [`examples/02_incident_commander/README.md`](../examples/02_incident_commander/README.md) for commands, workload controls, and the required live acceptance evidence.

## Dataset-first design

Each application targets a recognized external dataset and includes a fetch/preparation step. Downloaded data is cached under `.plexus-data/` and is not committed to the package repository.

See [`examples/README.md`](../examples/README.md) for the catalog and [`examples/DATASETS.md`](../examples/DATASETS.md) for source/provenance notes.

## Why these are not benchmarks

The examples print useful counts and application outcomes, but they do not create new throughput claims. Runtime characterization remains in `experiments/` with retained artifacts and methodology. The GAIA live-progress counters are observability for one application run, not a general provider or BEAM performance claim.

## Live semantic calls

Runnable example scripts require a real TypeSafe client. Deterministic fixtures belong to the test suite; there is no fixture fallback in `examples/`.

TypeSafeSDK 0.4 treats each distinct uncached semantic input as a distinct provider request. Plexus coalescing deduplicates identical work and controls how independent evaluations are launched; it does not collapse many different evaluations into one provider request.

## Verifying provider work

Standalone dataset examples expose two independent facts:

1. application-level Plexus measurement/accounting; and
2. the `TYPESAFE LIVE TRANSPORT SUMMARY` emitted from TypeSafeSDK telemetry.

For live acceptance, use the transport summary as network evidence. A confirmed response has an HTTP status and provider request ID and reports the returned model plus token usage. The GAIA progress display additionally separates replay progress from TypeSafe progress so preprocessing/replay time cannot be mistaken for provider latency.
