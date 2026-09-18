# Architecture

Plexus is a **thin substrate** over `typesafe_sdk`.

## Layers

```text
Plexus public API
├── Run coordinator
├── Actor registry
├── Graph store
├── Contract helpers
└── Example strategies

TypeSafeSDK
├── Prepared contracts
├── OTP semantic server
├── Unary evaluation
├── Batched stream/many evaluation
├── Response decoding
├── Telemetry
└── Cancellation
```

## Non-goals

Plexus does not attempt to solve:

- multi-node scheduling
- sandboxing
- provider abstraction beyond what TypeSafe already owns
- persistence layers beyond lightweight in-memory graph metadata

Those can come later if the single-node substrate proves useful.
