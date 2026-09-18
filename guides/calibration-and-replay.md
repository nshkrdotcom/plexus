# Calibration and replay

## Calibration before belief math

Raw noul probabilities are semantic model outputs, not automatically calibrated likelihoods. Before multiplying or propagating them through a graph, collect labeled data across the actual question phrasings used by the strategy.

`Plexus.Belief.Calibration` provides:

- binned reliability data
- expected calibration error
- a dependency-free isotonic fit
- runtime application of that monotone map

Keep the raw value and fitted map in experiment artifacts. If calibration is poor or unstable across phrasings, prefer rank/monotone aggregation to probabilistic interpretation.

## Replay

Run option `replay: :record` stores each completed measurement result by `Contract.memo_key/2`. `replay: :replay` serves only that store and returns an explicit replay miss rather than touching the network.

The live store is per-run ETS, but `Plexus.replay_entries/1` and `Plexus.load_replay/2` let a fresh run consume the exact same in-memory `{memo_key, result}` set. Use that path for controlled BSP-vs-async tests. Durable file serialization/versioning still needs BEAM-side design and validation.
