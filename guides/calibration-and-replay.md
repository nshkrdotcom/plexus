# Calibration and replay

## Calibration before belief math

Raw noul probabilities are model outputs, not automatically calibrated likelihoods. `Plexus.Belief.Calibration` supplies reliability bins, expected calibration error and dependency-free isotonic fitting/application. Keep calibration and held-out data separate and test the actual question phrasings used by the strategy.

`experiments/calibration.exs` records three equivalent primality questions over 60 labeled integers. Labels come from an exact arithmetic predicate, with balanced, disjoint calibration and held-out sets. The report includes raw observations, split membership, fitted maps, reliability bins, ECE, Brier score and log loss, by phrasing and pooled. `experiments/plot_calibration.py` exports CSV, SVG and PNG reliability artifacts.

The experiment chooses rank/monotone aggregation for later graph math. Strong results on this small synthetic claim do not establish transfer to other domains or justify multiplying raw noul values as Bayesian confidence. See [Experiments](experiments.md) for measured results and reproduction commands.

## Fixed-response replay

`replay: :record` stores completed measurements. `replay: :replay` serves only that store and returns `{:error, {:replay_miss, key}}` without transport access. `Contract.memo_key/3` includes state, prepared-contract fingerprint and evaluation options; default options retain the `/2` key.

Use `Plexus.replay_entries/1` and `Plexus.load_replay/2` for exact in-memory transfer. For durable files:

```elixir
run_id = Plexus.Run.run_id(run)
:ok = Plexus.Record.File.write(run_id, "responses.plexus")
# In a fresh run with matching contracts and identity:
:ok = Plexus.Record.File.load(fresh_run_id, "responses.plexus")
```

Register named/versioned contracts before loading. Set `replay_identity` at run startup to identify the strategy, dataset and evaluation configuration. The manifest freezes these, kernel/SDK versions, contracts and admission limits; schedule/replay modes are excluded so controlled comparisons can change them.

Version 1 uses a JSON envelope, SHA-256 and uncompressed ETF decoded with `:safe`. It rejects executable/process terms, new atoms, excessive depth, oversized files and incompatible manifests. Application atoms must already be loaded. Checksums detect damage, not malicious replacement. Exported responses may contain application data; store them accordingly. No process/mailbox snapshot is implied.

`experiments/replay.exs` records eight live responses, then compares fresh async/BSP runs with an unstubbed test transport. It checks identical final values and zero requests, and records event order, round curves, useful work, elapsed and CPU time. Its independent-node topology establishes replay control, not an iterative convergence advantage.
