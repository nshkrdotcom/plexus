# Kernel primitives

Plexus treats higher-level semantic patterns as recombinations of a small kernel.

## P1 Population

`Plexus.Population` queries the per-run node table by class or predicate and provides top-k and Pareto selection. Birth/death is managed by `Plexus.Run`.

## P2 Typed topology

`Plexus.Graph` stores typed weighted edges in a per-run ETS `:bag`, indexed in both directions. `:child` is only one possible edge type.

## P3 Local measurement

`Plexus.Measure` resolves prepared contracts, checks replay/cache, and routes misses to `Measure.Coalescer`. Coalescers dedupe identical memo keys and use `TypeSafeSDK.evaluate_many/4`.

## P4 Scheduling regime

`:async` executes commands immediately. BSP buffers commands until `Plexus.barrier/1`. Bounded-async and priority are policy seams in the first pass and need characterization before they should be used for research claims.

## P5 Belief

`Plexus.Belief` projects noul → Bernoulli, choice → categorical, score → ordinal. Raw answers are preserved. `Belief.Calibration` supplies explicit calibration rather than assuming raw probabilities are calibrated.

## P6 Quiescence

Run-scoped `:counters` track active-actor, measurement, expansion and timer activity. The current counters are intentionally low-level; the next BEAM pass should validate termination semantics against actor lifecycle and BSP experiments.

## P7 Budget/admission

`Plexus.Budget` uses `:atomics` for measure, expand, token and population meters. Work is reserved before admission where cost is known. Observed TypeSafe token use is recorded from telemetry after completion.

## P8 Selection/pruning

`Plexus.Run.prune/2` first asks measurement/expansion queues to cancel actor work, then terminates processes from leaves upward, then removes topology metadata.

## P9 Expansion

`Plexus.Expand.Queue`, `Expand.Schema`, and `Expand.Materializer` define the tier without binding Plexus to a provider library. The actual inference-library adapter is a required handoff item.

## P10 Provenance/invalidation

`Plexus.Provenance` uses `:depends_on` edges and staleness/epoch metadata to invalidate dependent nodes.

## P11 Run record/replay

`Plexus.Record` keeps an append-only per-run event log and fixed-response table. Durable import/export is not claimed yet.

## P12 Stop/reduce

`Plexus.Stop` and `Plexus.Reduce` provide composable stop predicates and reducers over the surviving population.
