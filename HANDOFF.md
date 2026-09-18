# Plexus kernel completion — 2026-09-17

The original static-only handoff has been implemented and exercised on BEAM. Release coordinates remain 0.1.0 with changes under Unreleased. The cleanup in `868b64c` passed its baseline gates; later behavioral tests identified and corrected runtime issues without undoing the kernel architecture.

## Delivered

- Run-owned ETS, partitioned actor admission and lifecycle cleanup, atomic graph updates and indexed typed topology.
- Command-mediated measurement/expansion/population effects; managed message/timer accounting and exactly-once actor retirement.
- TypeSafe coalescing, option-aware cache/replay keys, saturation retry, shared physical cancellation and fail-closed replay.
- Hex inference 0.4.1 integration, neutral streams, TypeSafe monitor example/test, independent expansion capacity and usage/cost/duration accounting.
- Bounded command progress, queued priority ordering, BSP barriers, hierarchical credit APIs, seeded selection, leaf split/merge/migration, prioritized epoch-checked repair and extended stop predicates.
- Versioned durable replay with manifests, checksums, safe terms and incompatibility errors.
- Reproducible node-birth/measurement sweeps, live labeled calibration artifacts and controlled async/BSP replay.

## Validation

The current suite has 57 passing tests on both Elixir 1.18.4 / OTP 27 and Elixir 1.20.3 / OTP 29. Formatter, warnings-as-errors compilation, strict Credo, Dialyzer, ExDoc, Hex build and publication dry-run all passed, including the publication dry-run; reproduction commands are in [Testing and release](guides/testing-and-release.md).

See [Experiments](guides/experiments.md) and the checked-in `artifacts/` for measured results, methodology and limits. No real Hex publication or version bump is part of this change.

## Operational limits

- Physical inference stopping depends on a backend that explicitly supports and honors cancellation. The library has no universal physical cancellation operation.
- Quiescence covers managed framework activity; raw process messages/direct SDK escape hatches are outside that accounting.
- Bounded async bounds command-envelope progress, not arbitrary semantic staleness. Priority orders currently queued arrivals. Neither has a research performance claim.
- Hierarchical accounts are explicit application credit APIs; ordinary framework work uses the root meter. Population migration requires an application snapshot and supports leaves, not live mailbox transfer.
- Repair work is application-driven. Secondary indexes retain historical values until teardown. Cache eviction remains arbitrary, not LRU.
- Durable replay stores semantic responses, not actor/process checkpoints. Owner failure discards the run's in-memory state; partition failure loses affected actors and releases their resources.
- The primality calibration sample is small and claim-specific. The replay strategy is an independent-node control, not evidence of iterative convergence superiority. Measurements reach 100,000 actors; no million-actor or provider throughput claim is made.
