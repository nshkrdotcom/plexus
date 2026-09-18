# Testing and release

The current kernel pass was produced without Elixir/OTP installed. No compile/test claim should be inferred from static source work.

## Required first gate

```bash
mix deps.get
mix format
mix format --check-formatted
mix compile --warnings-as-errors
mix test --warnings-as-errors
```

Fix source/API mismatches before adding features.

## Focused kernel tests

```bash
mix test test/plexus/budget_test.exs
mix test test/plexus/calibration_test.exs
mix test test/plexus/graph_test.exs
mix test test/plexus/measure_coalescer_test.exs
mix test test/plexus/schedule_test.exs
mix test test/plexus/intake_example_test.exs
```

The coalescer test is particularly important: two independent actors submit the same state/contract and the TypeSafe test transport must observe one request.

## Static/package gates

```bash
mix credo --strict
mix dialyzer
mix docs --warnings-as-errors
mix hex.build
mix hex.publish --dry-run --yes
```

Inspect the Hex inventory and generated docs. Confirm all public modules/types render and all packaged guides/logo/license files are present.

## Runtime characterization before claims

Add benchmark evidence for:

- node births/second, with and without graph edges
- actors per partition and scheduler utilization
- measurement logical items/second vs physical HTTP requests/second
- requests saved by dedupe/cache/coalescing
- cancellation latency and whether physical transport cancellation occurs
- per-node memory at 10k/100k+ actors
- BSP barrier cost and deterministic replay equivalence

Provider rate limits may dominate actor spawn cost; report both separately.
