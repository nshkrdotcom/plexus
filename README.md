<p align="center">
  <img src="assets/plexus.svg" alt="Plexus" width="200" height="200"/>
</p>

<p align="center">
  <a href="https://hex.pm/packages/plexus"><img src="https://img.shields.io/hexpm/v/plexus.svg" alt="Hex.pm"/></a>
  <a href="https://hexdocs.pm/plexus"><img src="https://img.shields.io/badge/hex-docs-blue.svg" alt="HexDocs"/></a>
  <a href="https://github.com/nshkrdotcom/plexus"><img src="https://img.shields.io/badge/GitHub-repo-black?logo=github" alt="GitHub"/></a>
  <a href="https://hex.pm/packages/plexus"><img src="https://img.shields.io/hexpm/l/plexus.svg" alt="License"/></a>
</p>

# Plexus

**Plexus** is a BEAM-native semantic actor substrate built to **show off `typesafe_sdk` 0.4.0 directly**, not hide it behind a second runtime.

It is deliberately small:

- **semantic actors** are ordinary `TypeSafeSDK.OTP.Server` processes
- **wide semantic sweeps** use `TypeSafeSDK.evaluate_stream/4` and `TypeSafeSDK.evaluate_many/4`
- **prepared question contracts** are reused through `TypeSafeSDK.prepare!/1`
- **recursive decisions** happen inside `handle_evaluation/3`
- **subtree metadata** is tracked in a lightweight BEAM graph service
- **one shared `Task.Supervisor`** provides bounded concurrency across many actors

The design goal is simple:

> Build rich, dynamic, stateful semantic systems around TypeSafe without rebuilding TypeSafe itself.

## What Plexus is

Plexus is a **single Mix project** that provides:

- a small runtime for starting and supervising semantic actor runs
- a graph store for parent/child/subtree relationships
- a thin contract layer for prepared TypeSafe question sets
- example actors that recursively fan out semantic work
- tests, docs, guides, and packaging scaffolding ready for a follow-on agent

## What Plexus is not

Plexus is **not**:

- a new HTTP client
- a new batching engine
- a replacement for `typesafe_sdk`
- a distributed cluster runtime
- a sandboxing or security boundary

Those concerns belong elsewhere. Plexus is the orchestration layer that coordinates **many semantic actor decisions** on one node.

## Why this exists

The main architectural insight is that `typesafe_sdk` 0.4.0 already contains most of the hard semantic machinery:

- prepared contracts + fingerprints
- synchronous and asynchronous evaluation
- bounded batching streams
- recursive OTP server flows
- cancellation propagation
- telemetry hooks
- response decoding and answer structures

Plexus builds only the missing layer: **dynamic semantic populations**.

## Features

- **Thin actor wrapper** around `TypeSafeSDK.OTP.Server`
- **Run coordinator** with shared client/task supervisor wiring
- **Parent/child/subtree graph** with pruning helpers
- **Prepared contracts** with stable fingerprints and memo keys
- **Batch helper** that drives `evaluate_stream/4` / `evaluate_many/4`
- **Reference example**: intake coordinator + evidence workers
- **Release scaffolding**: README, guides, CHANGELOG, LICENSE, package metadata, HexDocs extras, CI skeleton, and handoff notes

## Installation

```elixir
{:plexus, "~> 0.1.0"}
```

## Quick start

```elixir
client = TypeSafeSDK.new_client(api_key: System.fetch_env!("TYPESAFE_API_KEY"))

{:ok, run} =
  Plexus.start_run(
    id: :demo,
    client: client,
    actor_task_supervisor_opts: [max_children: 64]
  )

{:ok, actor} =
  Plexus.start_actor(run,
    module: Plexus.Examples.IntakeCoordinator,
    actor_id: {:ticket, 1},
    init_arg: %{
      text: "Customer says login is broken and billing is wrong.",
      run: run
    }
  )

Plexus.cast(actor, :classify)
```

See the guides for the complete flow.

## Architecture in one screen

```text
Plexus.Run
├── shared TypeSafeSDK.Client
├── shared Task.Supervisor   (bounded semantic concurrency)
├── DynamicSupervisor        (semantic actor population)
├── Registry                 (actor addressing)
└── Graph                    (parent/child/subtree metadata)

Actors
└── use TypeSafeSDK.OTP.Server
    ├── return {:evaluate, ...} without blocking
    ├── handle_evaluation/3 receives semantic results
    └── may recursively spawn more actors or trigger batched work
```

## Guide map

- [Guide index](guides/index.md)
- [Getting started](guides/getting-started.md)
- [Architecture](guides/architecture.md)
- [Actor runtime](guides/actor-runtime.md)
- [TypeSafe integration](guides/typesafe-integration.md)
- [Graph and subtrees](guides/graph-and-subtrees.md)
- [Testing and release](guides/testing-and-release.md)

## Development expectations

This repository was prepared in an environment **without a working Elixir toolchain**, so it is designed as a strong handoff base:

- package metadata is filled in
- docs menu is wired for HexDocs
- module structure and tests are laid out
- TypeSafe integration points are explicit
- remaining compile/runtime verification steps are documented in `HANDOFF.md`

## License

MIT © 2026 nshkrdotcom
