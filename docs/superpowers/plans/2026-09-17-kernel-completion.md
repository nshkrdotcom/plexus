# Kernel completion implementation plan

**Goal:** Validate the quality cleanup and complete the supplied kernel handoff with executable evidence.
**Architecture:** Preserve run-owned ETS, partitioned actor supervision, command effects, TypeSafe measurement, independent expansion, and fail-closed replay. Keep 0.1.0 release coordinates.
**Spec:** User-supplied Plexus kernel implementation handoff, 2026-09-17; HANDOFF.md.

## Work sequence

- [x] Audit 868b64c and run dependency, format, compile, ExUnit, Credo, Dialyzer, docs and Hex dry-run gates. All passed on Elixir 1.20.3 / OTP 29; baseline has 11 tests.
- [ ] Runtime lifecycle: add concurrent population admission, startup completion, partition/isolation, termination and owner restart tests; fix demonstrated failures in run/owner/actor/graph code. Instrument managed message and command lifetimes.
- [ ] Measurement: cover batching, option separation, caching, replay misses, ordered failures, shared cancellation, saturation and teardown using TypeSafe fixtures; fix queue lifecycle failures.
- [ ] Expansion: use latest Hex inference 0.4.1, implement public-API adapter, capability translation, stream/cancellation/accounting and materialization tests. No local path dependencies.
- [ ] Complete scheduling bounds, budget grants, selection operators, repair queue, stop predicates and atomic graph updates with focused tests and explicit semantics.
- [ ] Durable replay: versioned safe format, manifest, integrity checks, compatibility failures and round-trip tests.
- [ ] Experiments: reproducible labeled claim dataset, phrasing-specific held-out calibration metrics/plots; fixed-store async/BSP experiment; node birth and measurement parameter sweeps with measured artifacts.
- [ ] Final documentation and all quality/package gates; commit and push stable verified stages.

Each implementation stage uses a failing behavioral test before the fix and focused tests afterward. Final claims distinguish deterministic fixture evidence, measured BEAM evidence, and live-provider evidence. No subagents. No release bump.
