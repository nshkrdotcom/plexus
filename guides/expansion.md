# Expansion tier

Plexus uses the published Hex `inference` dependency (`~> 0.4.1`, locked to 0.4.1), independently of TypeSafe measurement. Supply an `inference_client` at run startup to select `Plexus.Expand.InferenceAdapter` automatically, or implement the adapter behaviour for another backend.

```elixir
inference = Inference.client!(adapter: Inference.Adapters.Mock)
{:ok, run} = Plexus.start_run(client: semantic_client, inference_client: inference)
```

The queue has a separate task supervisor, concurrency cap, priority ordering, expansion budget and actor-scoped cancellation. Required capabilities must be exactly `:supported`; missing, unknown and partial support fail startup. Completion preserves `Inference.Response.object`, usage, cost and trace duration; successful queue accounting happens once.

## Streams and cancellation

Use `stream: true`, an optional `on_event` observer, and a `monitor` callback. Neutral delta events accumulate text, while final response events preserve the provider response. A monitor returning `:halt` rejects output and sets the cancellation token. It can evaluate deltas with a prepared TypeSafe contract; `inference_adapter_test.exs` exercises this through the real SDK test transport. Monitoring frequency, contract, fail-closed decision and its separate measurement budget are application policy.

Inference 0.4.1 has no universal physical-cancellation API. Plexus always cancels local queued/active wrapper work. It forwards its Pristine token in request options only when the client declares `:cancellation` supported; the backend must honor that contract. Require the capability at run startup when physical generation cancellation is necessary. The tests prove the token handoff with a conforming fixture, not cancellation at every external provider.

## Proposal materialization

`Plexus.Expand.Schema.proposals/0` supplies the proposal schema. After structured-output validation, `Plexus.Expand.Materializer.commands/3` converts objects to spawn/edge commands. Dispatch those commands through `Plexus.Actor.dispatch/2` so normal depth, population, budget and scheduling policy applies.

An application-owned module resolver maps known class names to actor modules. Class/edge conversion uses existing atoms only; unknown remote strings raise rather than creating atoms. Inference backend dependencies and provider configuration remain application choices.
