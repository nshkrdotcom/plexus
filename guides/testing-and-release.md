# Testing and release

## Primary checks

```bash
mix deps.get
mix compile
mix test
mix docs
mix hex.build
mix hex.publish --dry-run
```

## Notes

This repository was scaffolded without a working Elixir runtime, so `HANDOFF.md` explains the first-pass compile and package checks the next agent should perform.
