# Example applications

The `examples/` directory contains two categories that must not be conflated. `02_incident_commander` is the actor-native reference application; the other five are real-dataset integration workloads that validate ingestion, managed semantic calls, graph/population mechanics and packaging, but are still flattenable into conventional centralized async programs.

That distinction is deliberate. Real data and real provider calls are necessary integration evidence, but they are not by themselves evidence for the actor execution model.


## Actor-native acceptance criterion

A flagship example fails the architectural bar if deleting the actors and replacing them with a central `Enum.map`/`Task.async_stream`/`asyncio.gather` leaves essentially the same control flow. An actor-native example should require several of these properties simultaneously:

- population cardinality is unknown at startup;
- an actor's semantic result spawns/prunes/wakes other actors before any global layer barrier;
- one actor's output changes another actor's future execution;
- finite shared credits affect local lifecycle decisions;
- obsolete work can be cancelled rather than merely ignored after a round;
- failures can remain local to a subpopulation;
- termination emerges from quiescence, convergence, or budget rather than an expected completion count;
- schedule/order can change the trajectory while explicit invariants remain testable.

`02_incident_commander` now clears the minimal version of this bar: a seed hypothesis requests evidence actor-to-actor; its TypeSafe `next_action` can reserve a shared hypothesis-population credit and spawn a child along the trace topology; children begin independently; the final population is not known to the orchestrator; and the only terminal wait is quiescence.

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
