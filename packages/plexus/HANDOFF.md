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

## Dataset-backed examples overlay — 2026-09-17

This overlay adds six real-data example applications under `examples/` plus package/docs/ignore/test plumbing. The examples target SWE-bench Verified, NYC 311, GAIA/MicroSS, deps.dev, SciFact and NOAA Storm Events. External corpora are fetched into `.plexus-data/` and are intentionally excluded from Git.

The authoring environment for this overlay does **not** contain Elixir/OTP, so the existing 57-test BEAM validation above remains historical baseline evidence and was not re-run after the examples change. Direct dataset acquisition was also not executed from the container because its DNS/network path is unavailable; the fetch URLs and source schemas were reviewed against the current upstream documentation instead. The next BEAM-capable, networked agent should run, in order:

```bash
mix deps.get
mix format --check-formatted
mix compile --warnings-as-errors
mix test
mix credo --strict
mix dialyzer
mix docs
mix hex.build
```

Then perform at least one live dataset + TypeSafe pass for every example using its README command. Start with low semantic-call controls, verify expected application output and `Plexus.budget(run).measure.used`, then run larger actor-population cases. Do not replace live example calls with `TypeSafeSDK.Test`; fixtures under `test/fixtures/examples/` are only for parser/release coverage.

Particular runtime checks:

- SWE-bench: verify dataset-server paging still returns 500 Verified rows and that patch content is not included in semantic input.
- NYC 311: verify Socrata legacy resource endpoint remains accepted; if the city requires SODA 3 POST for large queries, update only the fetcher while preserving cached JSONL shape.
- GAIA: verify the `release-v1.0` Git LFS acquisition plus `business_split.zip`, `run.zip`, and `trace_split.zip` extraction with 7z/7zz; confirm the extracted July CSV layout before the live pass.
- deps.dev: verify scoped-package path encoding, exact version-key diffing when graphs contain duplicate package nodes, provenance topology, and migration-order beam pruning on a source/target graph with changed nodes.
- SciFact: verify tar extraction layout and public dev labels.
- NOAA: verify current bulk-index filename discovery, CSV parsing (including multiline quoted narratives), subscription-counter gating after real `wake_on` installation, selective day fan-out, and a full-year run with `--limit-events 0` on a suitably sized host.
