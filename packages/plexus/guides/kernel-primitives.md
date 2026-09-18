# Kernel primitives

Plexus keeps policy in actor commands and run resources in ETS. The implementation is single-node; the APIs below are kernel building blocks for application strategies.

## P1 Population

`Plexus.Population` provides class/predicate queries, top-k, Pareto, seeded weighted resampling and tournament selection. `index/2` and `lookup/3` index top-level attributes. Secondary indexes validate candidates against current node metadata and retain historical values until run teardown; use low-cardinality fields. Concurrent queries are not snapshots.

## P2 Typed topology

Nodes are an ETS set; class and bidirectional typed-edge indexes are ordered sets with encoded keys. `:child` is one edge type among arbitrary application atoms. Weights and provenance are retained. `Graph.update/3` uses compare-and-swap retries; its callback must be pure because it can run more than once.

## P3 Local measurement

`Plexus.Measure` resolves contracts, checks replay/cache, then reserves credit and submits misses to a coalescer. Memo keys include effective evaluation options. Coalescers deduplicate open-window requests, share results among waiters, retry task saturation, and call `TypeSafeSDK.evaluate_many/4`. In SDK 0.4.0 each distinct item still uses a transport request: batching controls concurrency; cache/deduplication reduce request counts. Cache eviction is bounded arbitrary eviction, not LRU.

## P4 Scheduling

`:async` executes commands immediately. BSP buffers until `Plexus.barrier/1`. `{:bounded_async, k}` allows each actor at most `k` command envelopes ahead of the slowest active actor. An active idle participant can hold progress back; completed actors leave the cohort. This bounds command updates, not semantic data age. `{:priority, fun}` orders currently queued envelopes, with FIFO ties; it cannot order future arrivals. Switching regimes flushes buffered commands and records the transition. Only async/BSP have a checked-in controlled experiment.

## P5 Belief

`Plexus.Belief` projects noul to Bernoulli, choice to categorical, and score to ordinal while retaining raw answers. Calibration is explicit. Combining raw scores does not manufacture a calibrated probability. The primality experiment supplies a small held-out calibration example, not evidence for unrelated claims.

## P6 Quiescence

Counters cover active actors, managed messages, commands, measurements, expansions and timers. `Run.cast/3` and `Run.call/4` track queued messages through callback completion. Completion retires an actor once; later termination cannot retire it again. Raw `send`, direct GenServer calls and the low-level TypeSafe OTP evaluation escape hatch are outside this accounting. Use managed APIs for quiescence-based termination.

## P7 Budget/admission

Atomic meters cover measure, expand, tokens and population. Known costs are reserved before work; observed tokens are consumed after completion. `max_population` also clamps an explicitly larger population budget. Cache/replay hits need no measurement credit.

`Budget.Accounts` implements finite parent grants, child reservations, sibling unused-credit transfers and leaf close/refund. Grants reserve parent credit up front. Applications explicitly operate scoped accounts through the `{:credits, action, args}` command; ordinary framework work uses the root atomic meter and is not automatically assigned to child accounts. Ledger state survives ledger-process restart; an interrupted close can conservatively retain credit, but cannot refund twice.

## P8 Selection and pruning

Prune synchronously cancels queued/active work, terminates leaves first, then removes topology. Population effects pass through the interpreter. Replication copies initial input unless supplied a snapshot. Split/merge replace compatible leaves using application inputs; failed admission rolls back new births. Migration moves a leaf between runs with an explicit snapshot and does not migrate mailbox contents. Resampling returns selected nodes; applications choose which replication/pruning effects to issue.

## P9 Expansion

`Expand.InferenceAdapter` uses Hex `inference` 0.4.1, with a separate queue/task supervisor, fail-closed capability checks, neutral streams, monitoring and response accounting. Physical cancellation requires explicit provider support. Schema-validated proposals pass through an application resolver and the normal command interpreter; remote strings never create atoms.

## P10 Provenance and repair

Dependency edges retain schema version, upstream epoch and evidence. Recursive invalidation marks nodes stale and enqueues prioritized repair work. Further upstream changes continue advancing a stale node's epoch, while a repair already pending for that node is not queued or notified again until the pending repair clears. Applications submit epoch-checked repairs, so older semantic results cannot overwrite newer evidence. The kernel does not prescribe the semantic repair algorithm.

## P11 Record/replay

Events and responses are run-owned ETS. `Record.File` exports versioned, checksummed fixed-response files with contract/config manifests and safe term validation. Replay misses fail closed. This is response replay, not process/mailbox checkpointing.

## P12 Stop/reduce

Stop predicates include quiescence, budget, population, explicit confidence projections, monotonic deadlines, no-improvement, epsilon stability and oscillation. Call `Stop.observe/2` once per comparable completed round for history-based predicates. `all/1` and `any/1` compose them; reducers provide top-k, best-one and collect.
