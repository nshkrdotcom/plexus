# Plexus handoff — GAIA living system twin — 2026-09-18

This tree contains the Plexus kernel plus an Unreleased replacement for the GAIA incident-commander reference application. The new reference is designed to make persistent actor identity, direct messaging, temporal evidence, provenance invalidation, hierarchical branch credit, pruning, and quiescence consequential to the computation rather than wrapping a batch search tree in GenServers.

The detailed design and agent handoff are:

- `docs/superpowers/specs/2026-09-18-gaia-living-system-twin-design.md`
- `docs/superpowers/plans/2026-09-18-gaia-living-system-twin.md`
- `GAIA_LIVING_TWIN_HANDOFF.md`

## Kernel state carried forward

Plexus remains a single-node Elixir/OTP coordination kernel over TypeSafeSDK. Existing capabilities include run-owned ETS, partitioned actor supervisors, managed actor effects, TypeSafe coalescing/cache/replay, cancellation, typed graph topology, quiescence accounting, hierarchical credit accounts, population operators, provenance, scheduling regimes, expansion, recording, and replay.

This change adds two kernel seams needed by a real persistent system twin:

- `activity_mode: :resident` on `Plexus.Run.start_actor/2`: idle resident existence does not count as unfinished work, while managed messages/timers/measurements/expansions still do.
- `Plexus.Provenance.invalidate/3` with `notify: true`: stale dependents receive a managed `{:plexus, :invalidated, upstream_id, epoch}` message; successful `repair/3` removes that actor's queued repair entry.

## GAIA reference replacement

The old GAIA reference precomputed service evidence and then executed a recursive hypothesis tree. The replacement does not.

- Raw MicroSS trace and business CSV streams are opened lazily and merged by source-head event time.
- A replay actor stays ordinary active work until EOF, preventing premature quiescence.
- Service actors are resident and maintain rolling local state over many raw events.
- Trace parentage mutates the service `:calls` graph while the replay is running, including late parent spans.
- Real anomaly events trigger focused TypeSafe signal measurements.
- Hypothesis actors are resident and can perform many revisions across later evidence epochs.
- One-shot service wake subscriptions are re-armed after every wake.
- Hypotheses discover indexed peers, create `:contradicts` edges, and send direct challenge messages.
- Topology changes invalidate existing provenance dependencies and notify live hypotheses to reconsider.
- `Budget.Accounts` `:expand` grants are used as local investigation capital for child branches.
- Refuted/conceded hypotheses use the normal Plexus pruning path so queued/in-flight managed work and child processes are cancelled with the subtree.
- Fault-injection/run rows are withheld until after quiescence and used only for scoring.
- There is no expected hypothesis count, layer barrier, or pre-run semantic evidence summary.

## Verification status

The artifact-authoring container does not provide Elixir, Erlang/OTP, or Mix. Source/test/docs work was therefore prepared here, but BEAM compilation, formatter output, ExUnit, Credo, Dialyzer, ExDoc, and Hex build must be run by the next agent. Do not interpret static archive checks as runtime certification.

A prior repository state (before this living-twin overlay) was exercised successfully on the user's BEAM environment. That historical evidence does not certify the new files.

## Required BEAM gate

From the Plexus package root, run:

```bash
mix deps.get
mix format
mix format --check-formatted
mix compile --warnings-as-errors
MIX_ENV=test mix compile --warnings-as-errors
mix test test/plexus/runtime_acceptance_test.exs --trace
mix test test/plexus/provenance_test.exs --trace
mix test test/plexus/incident_commander_example_test.exs --trace
mix test test/plexus/examples_typesafe_metrics_test.exs --trace
mix test
mix credo --strict
mix dialyzer
rm -rf doc
mix docs
rm -f plexus-*.tar
mix hex.build
rm -f plexus-*.tar
```

Fix every warning or failure before a live provider run. Do not weaken the architectural assertions to make the suite pass.

## Live acceptance

Fetch/reuse real GAIA first:

```bash
mix run examples/02_incident_commander/fetch.exs
```

A high-throughput profile is documented in `GAIA_LIVING_TWIN_HANDOFF.md`. Before running it, note the requested `--max-measurements`; with TypeSafeSDK 0.4, every distinct uncached evaluation is a distinct provider request.

A successful process exit is not sufficient. The live run must demonstrate persistent service identity, repeated revision of the same hypothesis actor, direct peer challenges, live invalidation/reconsideration, changing topology, subtree pruning while unrelated actors continue, local branch-credit effects, TypeSafe transport evidence, and quiescence only after telemetry EOF.

## Publication

Do not publish to Hex from this handoff. The package version remains unchanged and these changes belong under `Unreleased` until the BEAM/runtime gate and real-data acceptance pass are recorded.
