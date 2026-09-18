# Changelog

All notable changes to this project will be documented in this file.

The format is based on Keep a Changelog and this project adheres to Semantic Versioning.

## [Unreleased]

### Changed

- Replaced the global graph/run hot-path GenServer calls with per-run ETS resources and partitioned actor supervisors
- Made `Plexus.Actor.Command` executable through a central interpreter for spawn, topology, measurement, expansion, budget and pruning effects
- Routed framework-managed measurements through per-contract cache/replay/dedupe coalescers backed by `TypeSafeSDK.evaluate_many/4`
- Added physical TypeSafe batch cancellation tokens for subtree pruning and separated expensive expansion work onto its own bounded queue

### Added

- Typed weighted graph edges, population selection helpers and provenance invalidation
- Atomic run budgets, quiescence counters, append-only event recording and fixed-response replay storage
- Named/versioned contract registry
- Async/BSP scheduling regime layer with explicit barriers
- Bernoulli/categorical/ordinal belief projections plus reliability diagnostics and isotonic calibration
- Provider-neutral expansion adapter seam, strict proposal schema and proposal-to-command materializer
- Focused kernel tests and expanded architecture/calibration/expansion documentation

## [0.1.0] - 2026-09-17

### Added

- Initial release of `plexus`
- Single-project semantic actor runtime centered on `typesafe_sdk` `~> 0.4.0`
- Run supervisor, actor registry, subtree graph, and prepared-contract helper modules
- Example coordinator/worker actors demonstrating recursive TypeSafe workflows
- Guides, package metadata, HexDocs extras/menu wiring, SVG brand asset, tests, and handoff documentation