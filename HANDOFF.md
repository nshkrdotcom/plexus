# Plexus handoff

This repository was authored in a constrained environment without a full Elixir runtime, Hex, or an installed dependency graph. The codebase is intended to be **structurally complete** and **ready for the next agent** to take through compile, test, and publish.

## Intended identity

- **Project name:** Plexus
- **Hex package:** `plexus`
- **GitHub repo:** `github.com/nshkrdotcom/plexus`
- **Version:** `0.1.0`
- **Core dependency:** `{:typesafe_sdk, "~> 0.4.0"}`

## Design decision

Plexus intentionally does **not** rebuild TypeSafe execution.

Instead it treats `TypeSafeSDK.OTP.Server`, `TypeSafeSDK.prepare!/1`, `TypeSafeSDK.evaluate/4`, `TypeSafeSDK.evaluate_stream/4`, and `TypeSafeSDK.evaluate_many/4` as the core semantic engine and builds only the coordination layer around them.

## What the next agent must verify

1. `mix deps.get`
2. `mix compile`
3. `mix test`
4. `mix docs`
5. `mix hex.build`
6. `mix hex.publish --dry-run`

## Most likely compile-touch areas

Because this was not compiled here, the next agent should expect to check and possibly adjust:

- exact `TypeSafeSDK.Client` and `TypeSafeSDK.Test` APIs used in tests/examples
- `TypeSafeSDK.OTP.Server` callback shapes and option names
- `DynamicSupervisor.child_spec/1` shapes in `Plexus.Run`
- any minor map/struct field names in telemetry and response handling
- `ExDoc` extra grouping behavior against the chosen Elixir/ExDoc version

## Architectural intent of each module

- `Plexus` — public convenience API
- `Plexus.Application` — root app supervisor
- `Plexus.Run` — bounded per-run coordinator that owns one shared task supervisor, one actor supervisor, and graph references
- `Plexus.Registry` — named actor lookup
- `Plexus.Graph` — parent/child/subtree metadata
- `Plexus.Contract` — prepared contracts + fingerprinted semantic batch helpers
- `Plexus.Actor` — thin ergonomic wrapper for semantic actors
- `Plexus.Actor.Command` / `Plexus.Actor.Context` — lightweight coordination types
- `Plexus.Strategy.Fanout` — batch-oriented TypeSafe sweep helper
- `Plexus.Examples.*` — reference implementation demonstrating recursive semantic work

## Expected follow-up work

- tighten compile details after the first real `mix test`
- add richer subtree pruning and actor lifecycle instrumentation
- optionally add memoization/caching around `Plexus.Contract.batch_*`
- optionally add structured telemetry attachment helpers
- optionally add a second example strategy (triage swarm, review forest, or recursive research fan-out)

## Suggested verification order

1. compile core runtime modules
2. compile example actors
3. fix test helper assumptions against `typesafe_sdk` real test APIs
4. generate docs and confirm HexDocs menu/logo/extras
5. perform package dry-run and check asset/file inclusion

## Release checklist

- confirm `assets/plexus.svg` renders in README and HexDocs
- confirm package name `plexus` is available on Hex; rename only if required
- tag `v0.1.0`
- publish `typesafe_sdk` dependency prerequisites first if needed in the target environment
