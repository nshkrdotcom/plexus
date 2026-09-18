# Testing and release

The kernel has now been compiled and tested on Elixir 1.18.4 / OTP 27 and Elixir 1.20.3 / OTP 29. The original cleanup commit passed the baseline gates; subsequent runtime tests exposed and fixed admission, initialization, cancellation, saturation and accounting bugs. See `HANDOFF.md` for the final gate inventory.

## Quality and package gates

```bash
mix deps.get
mix format --check-formatted
mix compile --warnings-as-errors
mix test --warnings-as-errors
mix credo --strict
mix dialyzer
mix docs --warnings-as-errors
mix hex.build
mix hex.publish --dry-run --yes
```

`mix.exs` selects the docs environment for `hex.publish`, so the package dry-run also builds documentation. The version remains 0.1.0; package dry-run is not publication.

## Focused evidence

The acceptance suites cover concurrent population bounds, partition distribution/loss, isolation, owner replacement, managed message/timer lifetimes, completion races, atomic metadata updates, budget grants, secondary indexes, selection effects, provenance repair and stop conditions.

Coalescer tests use `TypeSafeSDK.Test` through real SDK serialization/validation. They cover deduplication, ordered scatter, options, contract batch overrides, cache/replay, exhausted-credit reuse, per-item failures, shared cancellation, all-waiter cancellation, task saturation and teardown. Pruning tests assert token cancellation before actor termination and topology removal while unrelated work survives.

Inference tests use the published library with its mock adapter and a conforming cancellation fixture. They cover streams, TypeSafe delta monitoring, capability preflight, structured objects, independent task capacity and exactly-once accounting. They do not establish every provider's physical cancellation behavior.

## Characterization

See [Experiments](experiments.md) for reproducible artifacts. Node-birth measurements reach 100,000 actors. Measurement sweeps use an isolated transport and distinguish logical throughput, physical requests and batch items. Live calibration/replay observations are reported separately. Neither fixture throughput nor BEAM spawn rate establishes provider rate limits or million-actor scale.

## Live TypeSafe request proof

Dataset examples install `Plexus.Examples.Support.TypeSafeMetrics` through the shared runtime helper.
A successful live run ends with output shaped like:

```text
===== TYPESAFE LIVE TRANSPORT SUMMARY =====
client endpoint               https://api.typesafe.ai
client requested model        jev-latest
client transport              Pristine.Adapters.Transport.Finch
TypeSafe evaluate stops        1
confirmed HTTP responses      1
confirmed HTTP 2xx            1
TypeSafe input tokens          857
TypeSafe output tokens         70
request IDs captured           1/1
  req_... status=200 model=jev-... input=857 output=70 retries=0
===== END TYPESAFE LIVE TRANSPORT SUMMARY =====
```

For an independent provider-metering experiment that bypasses Plexus orchestration:

```bash
mix run experiments/typesafe_metering.exs --count 10 --timeout-ms 30000
```

The metering probe sends unique nonce-bearing states with retries disabled, requires one successful HTTP
response and one unique provider request ID per requested call, sums returned token usage, and writes a JSON
report under the operating-system temporary directory.
