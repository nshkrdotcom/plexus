Code.require_file("../support/runtime.exs", __DIR__)
Code.require_file("../support/data.exs", __DIR__)
Code.require_file("../support/metrics.exs", __DIR__)
Code.require_file("chronology.exs", __DIR__)
Code.require_file("topology.exs", __DIR__)
Code.require_file("progress.exs", __DIR__)
Code.require_file("application.exs", __DIR__)

alias Plexus.Examples.IncidentCommander
alias Plexus.Examples.Support.Runtime

{opts, _argv, invalid} =
  OptionParser.parse(Runtime.cli_args(),
    strict: [
      data_dir: :string,
      source_dir: :string,
      day: :string,
      max_events: :integer,
      speed: :string,
      max_hypotheses: :integer,
      max_depth: :integer,
      branch_width: :integer,
      branch_credits: :integer,
      peer_challenges: :integer,
      signal_every: :integer,
      hypothesis_update_every: :integer,
      incident_window_seconds: :integer,
      max_measurements: :integer,
      token_budget: :integer,
      batch_size: :integer,
      batch_delay_ms: :integer,
      max_in_flight_batches: :integer,
      max_concurrency: :integer,
      actor_partitions: :integer,
      timeout_ms: :integer,
      chunk_size: :integer,
      progress_every: :integer,
      progress_heartbeat_ms: :integer,
      record_path: :string,
      trigger_probability: :float
    ]
  )

if invalid != [], do: raise(ArgumentError, "invalid GAIA options: #{inspect(invalid)}")

speed =
  case Keyword.get(opts, :speed, "max") do
    "max" ->
      :max

    value ->
      case Float.parse(value) do
        {number, ""} when number > 0 -> number
        _ -> raise ArgumentError, "--speed must be max or a positive numeric replay multiplier"
      end
  end

opts = Keyword.put(opts, :speed, speed)

max_measurements = Keyword.get(opts, :max_measurements, 50_000)
max_hypotheses = Keyword.get(opts, :max_hypotheses, 20_000)
max_events = Keyword.get(opts, :max_events, 0)

if max_measurements < 1, do: raise(ArgumentError, "--max-measurements must be at least 1")
if max_hypotheses < 1, do: raise(ArgumentError, "--max-hypotheses must be at least 1")
if max_events < 0, do: raise(ArgumentError, "--max-events must be 0 or greater")

IO.puts("============================================================")
IO.puts("GAIA LIVING SYSTEM TWIN")
IO.puts("============================================================")
IO.puts("day                         #{Keyword.get(opts, :day, "2021-07-01")}")

IO.puts(
  "raw-event limit             #{if(max_events == 0, do: "all selected-day events", else: max_events)}"
)

IO.puts("replay speed                #{speed}")
IO.puts("max live hypotheses         #{max_hypotheses}")
IO.puts("max semantic evaluations    #{max_measurements}")
IO.puts("TypeSafe concurrency        #{Keyword.get(opts, :max_concurrency, 32)}")

IO.puts(
  "actor partitions            #{Keyword.get(opts, :actor_partitions, System.schedulers_online())}"
)

IO.puts("event record                #{Keyword.get(opts, :record_path, "<disabled>")}")
IO.puts("")
IO.puts("Distinct uncached semantic evaluations are distinct TypeSafe provider requests.")
IO.puts("Run/fault-injection rows remain hidden until post-run scoring.")
IO.puts("============================================================")

IncidentCommander.run(opts)
