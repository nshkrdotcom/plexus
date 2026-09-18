Code.require_file("../support/runtime.exs", __DIR__)
Code.require_file("../support/data.exs", __DIR__)
Code.require_file("../support/metrics.exs", __DIR__)
Code.require_file("application.exs", __DIR__)

alias Plexus.Examples.Support.Runtime

{opts, _, invalid} =
  OptionParser.parse(Runtime.cli_args(),
    strict: [
      data_dir: :string,
      source_dir: :string,
      day: :string,
      max_rows: :integer,
      max_services: :integer,
      seed_services: :integer,
      max_hypotheses: :integer,
      max_depth: :integer,
      branch_width: :integer,
      token_budget: :integer,
      timeout_ms: :integer
    ]
  )

if invalid != [], do: raise(ArgumentError, "invalid options: #{inspect(invalid)}")

Plexus.Examples.IncidentCommander.run(opts)
