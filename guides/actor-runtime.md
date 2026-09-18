# Actor runtime

Plexus actors are ordinary `TypeSafeSDK.OTP.Server` modules.

A typical actor:

- stores semantic state locally
- receives domain messages
- returns `{:evaluate, ...}` for non-blocking TypeSafe work
- handles semantic results in `handle_evaluation/3`
- may spawn children or send more messages based on the result

Use `Plexus.Actor` for convenience and `Plexus.Run` for orchestration.

## Shared bounded concurrency

Each run owns one `Task.Supervisor`. Every actor evaluation uses that shared supervisor, so one run can bound total concurrent semantic work without inventing another worker pool.
