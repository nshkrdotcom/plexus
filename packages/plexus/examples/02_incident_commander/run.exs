Code.require_file("../support/runtime.exs", __DIR__)
Code.require_file("../support/data.exs", __DIR__)
Code.require_file("../support/metrics.exs", __DIR__)

alias Plexus.Examples.Support.{Data, Metrics, Runtime}


defmodule Plexus.Examples.IncidentCommander.Evidence do
  use Plexus.Actor
  alias Plexus.Actor

  @impl true
  def init(args), do: {:ok, %{context: Actor.context(args), summary: args.summary}}

  @impl true
  def handle_cast(:publish, state) do
    Actor.dispatch(state.context, {:complete, state.summary})
    {:noreply, state}
  end

  @impl true
  def handle_evaluation(_, _, state), do: {:noreply, state}
end


defmodule Plexus.Examples.IncidentCommander.Hypothesis do
  use Plexus.Actor
  alias Plexus.Actor

  @impl true
  def init(args) do
    {:ok, %{context: Actor.context(args), service: args.service, evidence: args.evidence, contract: args.contract}}
  end

  @impl true
  def handle_cast(:evaluate, state) do
    semantic_state = %{
      candidate_root_service: state.service,
      observed_trace_failures: state.evidence.trace_failures,
      observed_log_signals: state.evidence.log_signals,
      trace_examples: state.evidence.trace_examples,
      log_examples: state.evidence.log_examples,
      comparison: state.evidence.comparison,
      calls: state.evidence.calls,
      called_by: state.evidence.called_by
    }

    Actor.dispatch(state.context, {:measure, :root_cause, semantic_state, state.contract, []})
    {:noreply, state}
  end

  def handle_cast({:plexus, :measurement, :root_cause, {:ok, response}}, state) do
    root = Plexus.Belief.from(response, :root_cause)
    mode = Plexus.Belief.from(response, :failure_mode)
    strength = Plexus.Belief.from(response, :evidence_strength)

    Actor.dispatch(state.context, [
      {:belief, root},
      {:complete,
       %{
         service: state.service,
         root_probability: root.value,
         failure_mode: mode.value,
         evidence_strength: strength.value,
         evidence_count: state.evidence.trace_failures + state.evidence.log_signals
       }}
    ])

    {:noreply, state}
  end

  def handle_cast({:plexus, :measurement, :root_cause, {:error, error}}, state) do
    Actor.dispatch(state.context, {:complete, %{service: state.service, error: inspect(error)}})
    {:noreply, state}
  end

  @impl true
  def handle_evaluation(_, _, state), do: {:noreply, state}
end


defmodule Plexus.Examples.IncidentCommander do
  alias Plexus.Examples.IncidentCommander.{Evidence, Hypothesis}
  alias Plexus.Examples.Support.{Data, Metrics, Runtime}
  alias Plexus.{Graph, Provenance, Run}

  def run(opts) do
    data_dir = opts[:data_dir] || Runtime.data_dir("gaia")
    source = opts[:source_dir] || Path.join([data_dir, "GAIA-DataSet", "MicroSS"])
    unless File.dir?(source), do: raise("missing GAIA MicroSS directory #{source}; run fetch.exs first or pass --source-dir")

    day = opts[:day] || "2021-07-01"
    max_rows = opts[:max_rows] || 100_000
    max_services = opts[:max_services] || 16
    if max_rows < 1, do: raise(ArgumentError, "--max-rows must be at least 1")
    if max_services < 1, do: raise(ArgumentError, "--max-services must be at least 1")

    files = Path.wildcard(Path.join(source, "**/*.csv"))
    run_files = Enum.filter(files, &path_kind?(&1, "run"))
    trace_files = Enum.filter(files, &path_kind?(&1, "trace"))
    business_files = Enum.filter(files, &path_kind?(&1, "business"))

    truth = anomaly_truth(run_files, day, max_rows)
    trace_rows = rows_for_day(trace_files, day, max_rows)
    business_rows = rows_for_day(business_files, day, max_rows)
    trace_edges = trace_dependencies(trace_rows)
    evidence = build_evidence(trace_rows, business_rows, trace_edges)

    services =
      evidence
      |> Enum.sort_by(fn {_service, row} -> -(row.trace_failures + row.log_signals) end)
      |> Enum.take(max_services)

    if services == [], do: raise("no GAIA evidence rows found for #{day}; choose another --day or inspect the downloaded dataset")

    comparison = Enum.map(services, fn {service, row} -> %{service: service, evidence_count: row.trace_failures + row.log_signals} end)

    run = Runtime.start_run!(
      max_population: max_services * 2 + 100,
      budgets: [
        measure: max_services,
        population: max_services * 2 + 100,
        tokens: Runtime.token_budget(opts, length(services), per_call: 12_000, floor: 250_000)
      ]
    )

    try do
      contract = TypeSafeSDK.prepare!(
        root_cause: TypeSafeSDK.noul("Is the named candidate service plausibly the root cause, rather than merely a downstream victim, of the observed incident evidence?"),
        failure_mode:
          TypeSafeSDK.choice(
            "Which failure mode best explains the evidence for this service?",
            resource_exhaustion: "CPU, memory, connection, or other resource exhaustion",
            dependency_failure: "Failure caused by an upstream or downstream dependency",
            request_path_error: "Application/request path or RPC error",
            configuration: "Configuration or deployment fault",
            unknown: "Evidence is insufficient to distinguish a mode"
          ),
        evidence_strength:
          TypeSafeSDK.score("How diagnostically strong is the evidence for this service as root cause?", ["weak", "limited", "useful", "strong"])
      )
      :ok = Plexus.register_contract(run, :gaia_root_cause, contract, version: 1)
      run_id = Run.run_id(run)

      selected_services = services |> Enum.map(&elem(&1, 0)) |> MapSet.new()

      Enum.each(services, fn {service, row} ->
        row = Map.put(row, :comparison, comparison)
        evidence_id = {:evidence, service}
        hypothesis_id = {:hypothesis, service}

        {:ok, _} =
          Plexus.start_actor(run,
            module: Evidence,
            actor_id: evidence_id,
            class: :evidence,
            init_arg: %{summary: row}
          )

        {:ok, _} =
          Plexus.start_actor(run,
            module: Hypothesis,
            actor_id: hypothesis_id,
            class: :hypothesis,
            init_arg: %{service: service, evidence: row, contract: :gaia_root_cause}
          )

        :ok = Provenance.depend(run_id, hypothesis_id, evidence_id, %{dataset: :gaia, day: day})
        Plexus.cast({run, evidence_id}, :publish)
        Plexus.cast({run, hypothesis_id}, :evaluate)
      end)

      Enum.each(trace_edges, fn {{caller, callee}, count} ->
        if MapSet.member?(selected_services, caller) and MapSet.member?(selected_services, callee) do
          Graph.add_edge(
            run_id,
            :calls,
            {:evidence, caller},
            {:evidence, callee},
            count * 1.0,
            %{dataset: :gaia, day: day, spans: count}
          )
        end
      end)

      Runtime.await_class_complete!(run, :hypothesis, length(services), opts[:timeout_ms] || 180_000)
      Runtime.await_quiescent!(run, 30_000)
      report(run, day, truth)
    after
      Runtime.stop(run)
    end
  end

  defp build_evidence(trace_rows, business_rows, trace_edges) do
    traces =
      trace_rows
      |> Enum.filter(fn row -> row["status_code"] not in [nil, "", "200"] end)
      |> Enum.group_by(&(&1["service_name"] || &1["service"] || "unknown"))

    logs =
      business_rows
      |> Enum.filter(fn row -> Regex.match?(~r/(error|warning|fail|timeout|exception)/i, row["message"] || "") end)
      |> Enum.group_by(&(&1["service"] || "unknown"))

    relationships = relationship_summaries(trace_edges)

    (Map.keys(traces) ++ Map.keys(logs) ++ Map.keys(relationships))
    |> Enum.uniq()
    |> Map.new(fn service ->
      service_traces = Map.get(traces, service, [])
      service_logs = Map.get(logs, service, [])
      relation = Map.get(relationships, service, %{calls: [], called_by: []})

      {service,
       %{
         trace_failures: length(service_traces),
         log_signals: length(service_logs),
         trace_examples:
           Enum.take(Enum.map(service_traces, &Map.take(&1, ["url", "status_code", "message"])), 5),
         log_examples: Enum.take(Enum.map(service_logs, & &1["message"]), 5),
         calls: relation.calls,
         called_by: relation.called_by
       }}
    end)
  end

  defp trace_dependencies(rows) do
    spans =
      rows
      |> Enum.reduce(%{}, fn row, acc ->
        trace_id = row["trace_id"]
        span_id = row["span_id"]
        service = row["service_name"] || row["service"]

        if trace_id in [nil, ""] or span_id in [nil, ""] or service in [nil, ""] do
          acc
        else
          Map.put(acc, {trace_id, span_id}, service)
        end
      end)

    Enum.reduce(rows, %{}, fn row, acc ->
      trace_id = row["trace_id"]
      parent_id = row["parent_id"]
      callee = row["service_name"] || row["service"]
      caller = Map.get(spans, {trace_id, parent_id})

      if caller in [nil, ""] or callee in [nil, ""] or caller == callee do
        acc
      else
        Map.update(acc, {caller, callee}, 1, &(&1 + 1))
      end
    end)
  end

  defp relationship_summaries(trace_edges) do
    Enum.reduce(trace_edges, %{}, fn {{caller, callee}, count}, acc ->
      acc
      |> Map.update(caller, %{calls: [{callee, count}], called_by: []}, fn summary ->
        %{summary | calls: [{callee, count} | summary.calls]}
      end)
      |> Map.update(callee, %{calls: [], called_by: [{caller, count}]}, fn summary ->
        %{summary | called_by: [{caller, count} | summary.called_by]}
      end)
    end)
    |> Map.new(fn {service, summary} ->
      {service,
       %{
         calls: Enum.sort_by(summary.calls, fn {_name, count} -> -count end) |> Enum.take(8),
         called_by: Enum.sort_by(summary.called_by, fn {_name, count} -> -count end) |> Enum.take(8)
       }}
    end)
  end

  defp anomaly_truth(files, day, limit) do
    files
    |> rows_for_day(day, limit)
    |> Enum.filter(fn row -> Regex.match?(~r/anomal/i, row["message"] || "") end)
    |> Enum.map(fn row -> %{service: row["service"], message: row["message"]} end)
    |> Enum.take(50)
  end

  defp rows_for_day(files, day, limit) do
    files
    |> Stream.flat_map(&Data.csv_maps!/1)
    |> Stream.filter(fn row -> String.starts_with?(row["timestamp"] || row["datetime"] || "", day) end)
    |> Enum.take(limit)
  end

  defp path_kind?(path, kind), do: path |> String.downcase() |> String.contains?("/#{kind}/")

  defp report(run, day, truth) do
    id = Run.run_id(run)
    top =
      Runtime.top_complete(run, :hypothesis, 10, fn result ->
        (result[:root_probability] || 0.0) * (1.0 + (result[:evidence_strength] || 0.0)) * :math.log(1.0 + (result[:evidence_count] || 0))
      end)

    truth_services = truth |> Enum.map(& &1.service) |> Enum.reject(&is_nil/1) |> Enum.uniq()
    predicted =
      case List.first(top) do
        nil -> nil
        {_id, attrs} -> attrs.result[:service]
      end

    IO.puts("\nGAIA incident commander — #{day}")
    Metrics.print_table([
      {"hypotheses", length(Graph.by_class(id, :hypothesis))},
      {"semantic measurements", Plexus.budget(run).measure.used},
      {"known injected services", Enum.join(truth_services, ", ")},
      {"top predicted service", predicted || "none"},
      {"top hits injected service", predicted in truth_services}
    ])

    IO.puts("\nRanked root-cause hypotheses:")
    Enum.each(top, fn {_id, attrs} -> IO.inspect(attrs.result) end)
  end
end

{opts, _, _} =
  OptionParser.parse(System.argv(),
    strict: [data_dir: :string, source_dir: :string, day: :string, max_rows: :integer, max_services: :integer, token_budget: :integer, timeout_ms: :integer]
  )

Plexus.Examples.IncidentCommander.run(opts)
