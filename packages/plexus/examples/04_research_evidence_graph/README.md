# 04 — SciFact research evidence graph

Builds an asynchronous claim/evidence graph from the standard **SciFact** scientific fact-verification dataset. Claim actors own aggregate state; claim/document pair actors ask TypeSafe/Jev whether an abstract supports, contradicts, or fails to resolve a claim, then create typed evidence edges and report back to the claim actor.

The dev split supplies gold evidence labels, so the example reports relation accuracy instead of merely printing model output.

```bash
mix run examples/04_research_evidence_graph/fetch.exs
TYPESAFE_API_KEY=... mix run examples/04_research_evidence_graph/run.exs -- --claims 50
```

Use `--claims 450` for the full public dev claim split. Data is downloaded from SciFact's official S3 release into `.plexus-data/scifact/` and ignored by Git.

Upstream licensing: SciFact claims/evidence annotations are CC BY 4.0; corpus abstracts derive from S2ORC under ODC-By 1.0. The downloaded dataset is not redistributed by Plexus.

All run scripts also accept `--token-budget N` to replace the workload-scaled token ledger with an explicit hard cap.
