# Expansion tier

Expansion is intentionally separate from TypeSafe measurement.

`Plexus.Expand.Queue` owns:

- a dedicated task supervisor
- a small concurrency limit (default 4)
- priority ordering
- fail-closed required-capability checks
- actor-scoped cancellation of queued/local wrapper tasks

Implement `Plexus.Expand.Adapter` against the chosen inference dependency:

```elixir
@callback capabilities(client) :: map()
@callback expand(client, spec, opts) :: {:ok, response} | {:error, reason}
```

The adapter is where provider-neutral inference response/stream/cancellation APIs are translated. Plexus does not guess those APIs.

## Proposal materialization

`Plexus.Expand.Schema.proposals/0` returns the strict proposal schema. After the inference layer has validated structured output, `Plexus.Expand.Materializer.commands/3` converts proposals to the same `{:spawn, ...}` / `{:edge, ...}` commands used by actors, preserving admission and topology policy.

The materializer requires an application-supplied module resolver and uses existing atoms only for remote class/edge names to avoid atom-table growth.

## Next integration work

The BEAM/inference-enabled agent should add:

- concrete capability preflight using the actual inference library
- provider stream-event adaptation
- stream-delta TypeSafe monitor + physical generation cancellation
- trace usage/cost/duration accounting into the budget/event record
- an integration test proving expansion never consumes measurement queue capacity
