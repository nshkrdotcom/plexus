# Changelog

All notable changes to this project will be documented in this file.

The format is based on Keep a Changelog and this project adheres to Semantic Versioning.

## [Unreleased]

### Changed

- Replaced the GAIA incident commander search tree with a chronological living system twin: raw telemetry is replayed while resident services/hypotheses are active; hypotheses revise repeatedly, challenge peers, react to provenance invalidation, spend local branch credit, and can prune live subtrees before quiescence.
- Added resident actor lifecycle semantics so persistent entities remain addressable without their idle existence preventing quiescence; managed messages, timers, measurements, and expansions still count as work.
- Added optional managed provenance invalidation notifications and repair-queue cleanup after successful epoch-checked repair.
- Changed live TypeSafe example summaries to retain aggregate transport evidence while printing only a small request-id sample.
- Fixed concurrent population admission, completion during init, partition/owner cleanup, task saturation and cancellation teardown
- Made graph metadata updates atomic and replaced high-degree bag indexes with ordered indexes
- Tracked managed actor messages/commands/timers and retired active actors exactly once across completion/termination races
- Included evaluation options in memo keys and allowed cache/replay hits with exhausted measurement credit

- Replaced the global graph/run hot-path GenServer calls with per-run ETS resources and partitioned actor supervisors
- Made `Plexus.Actor.Command` executable through a central interpreter for spawn, topology, measurement, expansion, budget and pruning effects
- Routed framework-managed measurements through per-contract cache/replay/dedupe coalescers backed by `TypeSafeSDK.evaluate_many/4`
- Added physical TypeSafe batch cancellation tokens for subtree pruning and separated expensive expansion work onto its own capacity-limited queue

### Added

- Added six dataset-backed runnable example applications covering SWE-bench Verified, NYC 311, GAIA/MicroSS, deps.dev, SciFact and NOAA Storm Events, with ignored local-data fetch/cache paths and offline parser coverage
- Published Hex inference 0.4.1 integration with streams, TypeSafe monitoring, capability preflight and independent expansion accounting
- Versioned safe durable replay files with contract/config manifests and checksums
- Finite-ahead command scheduling, queued priorities, hierarchical credit accounts, secondary indexes, seeded resampling/tournaments and leaf population operators
- Prioritized epoch-checked repair, confidence/deadline/stability/oscillation stop predicates
- Runtime acceptance suites on Elixir 1.18/OTP 27 and Elixir 1.20/OTP 29, plus measured benchmark/calibration/replay artifacts

- Typed weighted graph edges, population selection helpers and provenance invalidation
- Atomic run budgets, quiescence counters, append-only event recording and fixed-response replay storage
- Named/versioned contract registry
- Async/BSP scheduling regime layer with explicit barriers
- Bernoulli/categorical/ordinal belief projections plus reliability diagnostics and isotonic calibration
- Provider-neutral expansion adapter seam, strict proposal schema and proposal-to-command materializer
- Focused kernel tests and expanded architecture/calibration/expansion documentation

## [0.1.0] - 2026-09-17

- Added privacy-safe live TypeSafe transport evidence to dataset examples, including HTTP status, provider request IDs, returned models, token totals, and a finite direct metering probe.

### Added

- Initial release of `plexus`
- Single-project semantic actor runtime centered on `typesafe_sdk` `~> 0.4.0`
- Run supervisor, actor registry, subtree graph, and prepared-contract helper modules
- Example coordinator/worker actors demonstrating recursive TypeSafe workflows
- Guides, package metadata, HexDocs extras/menu wiring, SVG brand asset, tests, and handoff documentation
