# Plexus handoff

This checkout contains the pre-release `0.1.0` kernel implementation pass based on the published Plexus primitive plan and `typesafe_sdk` 0.4.0 source. It was authored in an environment without Elixir/OTP, so source work is complete enough for handoff but **runtime validation is not complete**.

The separately delivered `PLEXUS_KERNEL_HANDOFF_2026-09-17.md` is the detailed agent handoff. Use that document for the exact verification/fix sequence.

## Implemented in this pass

- per-run ETS resource ownership and lock-free config reads
- partitioned Registry + partitioned actor DynamicSupervisors
- typed `:bag` topology instead of children-list read/modify/write
- command interpreter as the cross-cutting policy seam
- named/versioned prepared contract registry
- cache + replay + per-contract measurement coalescing
- `TypeSafeSDK.evaluate_many/4` as the default framework measurement backend
- Pristine cancellation tokens owned per physical TypeSafe batch
- atomic measure/expand/token/population budgets
- TypeSafe telemetry forwarding into per-run event records and observed token accounting
- BSP buffering/barriers and run-level schedule swapping
- belief projection, reliability diagnostics and isotonic calibration
- population/provenance/stop/reduce helpers
- separate expansion priority queue + adapter/schema/materializer seams
- examples converted from direct `Run.start_actor/2` recursion to interpreter-mediated commands

## Deliberately not claimed complete

- no Elixir compile/test/docs/Hex gate has run here
- the concrete external `inference` adapter was not implemented because its source/API was not supplied
- expansion streaming, physical provider cancellation, and trace cost accounting require that real inference integration
- replay export/import exists in memory; durable serialization/versioning still needs a BEAM-validated format
- bounded-async/priority scheduling needs runtime semantics/characterization; BSP and async are the primary implemented comparison paths
- million-actor performance is not claimed without benchmarks

## First commands on a real Elixir host

```bash
mix deps.get
mix format
mix format --check-formatted
mix compile --warnings-as-errors
mix test --warnings-as-errors
mix docs --warnings-as-errors
mix hex.build
mix hex.publish --dry-run --yes
```

Do not weaken tests or delete kernel seams merely to make the first compile green. Fix exact API/syntax mismatches and then run the focused tests listed in `guides/testing-and-release.md`.
