defmodule Plexus.Examples.IncidentCommander.Policy do
  @moduledoc false

  @spec next_targets(term(), map(), MapSet.t(), pos_integer(), MapSet.t()) ::
          [map()]
  def next_targets(action, evidence, visited, branch_width, available_services)
      when is_integer(branch_width) and branch_width > 0 do
    action
    |> normalize_action()
    |> candidate_edges(evidence)
    |> Enum.reject(fn %{service: service} ->
      MapSet.member?(visited, service) or not MapSet.member?(available_services, service)
    end)
    |> Enum.uniq_by(& &1.service)
    |> Enum.sort_by(fn row -> {-row.count, row.service} end)
    |> Enum.take(branch_width)
  end

  def normalize_action(action) when is_atom(action), do: Atom.to_string(action)
  def normalize_action(action) when is_binary(action), do: action
  def normalize_action(_), do: "stop"

  defp candidate_edges("investigate_callers", evidence),
    do: tagged(Map.get(evidence, :called_by, []), :caller)

  defp candidate_edges("investigate_callees", evidence),
    do: tagged(Map.get(evidence, :calls, []), :callee)

  defp candidate_edges("investigate_both", evidence),
    do:
      tagged(Map.get(evidence, :called_by, []), :caller) ++
        tagged(Map.get(evidence, :calls, []), :callee)

  defp candidate_edges(_, _evidence), do: []

  defp tagged(edges, direction) do
    Enum.flat_map(edges, fn
      %{service: service, count: count} when is_binary(service) and is_number(count) ->
        [%{service: service, count: count, direction: direction}]

      %{"service" => service, "count" => count} when is_binary(service) and is_number(count) ->
        [%{service: service, count: count, direction: direction}]

      _ ->
        []
    end)
  end
end

defmodule Plexus.Examples.IncidentCommander.Evidence do
  use Plexus.Actor

  alias Plexus.Actor

  @impl true
  def init(args) do
    {:ok,
     %{
       context: Actor.context(args),
       service: args.service,
       summary: args.summary
     }}
  end

  @impl true
  def handle_cast(:publish, state) do
    Actor.dispatch(state.context, {:complete, state.summary})
    {:noreply, state}
  end

  def handle_cast({:request_evidence, requester}, state) do
    Actor.dispatch(
      state.context,
      {:send, requester, {:gaia_evidence, state.service, state.summary}}
    )

    {:noreply, state}
  end

  @impl true
  def handle_evaluation(_, _, state), do: {:noreply, state}
end

defmodule Plexus.Examples.IncidentCommander.Hypothesis do
  use Plexus.Actor

  alias Plexus.Actor
  alias Plexus.Budget.Accounts
  alias Plexus.Examples.IncidentCommander.Policy

  @impl true
  def init(args) do
    {:ok,
     %{
       context: Actor.context(args),
       service: args.service,
       evidence_id: args.evidence_id,
       contract: args.contract,
       pool: args.pool,
       path: args.path,
       origin: args.origin,
       depth: args.depth,
       max_depth: args.max_depth,
       branch_width: args.branch_width,
       available_services: args.available_services,
       parent_assessment: Map.get(args, :parent_assessment),
       evidence: nil
     }}
  end

  @impl true
  def handle_cast(:evaluate, state) do
    Actor.dispatch(state.context, [
      {:edge, :consults, state.context.actor_id, state.evidence_id, 1.0,
       %{dataset: :gaia, path: state.path}},
      {:send, state.evidence_id, {:request_evidence, state.context.actor_id}}
    ])

    {:noreply, state}
  end

  def handle_cast({:gaia_evidence, service, evidence}, %{service: service} = state) do
    semantic_state = %{
      candidate_root_service: state.service,
      investigation_origin: state.origin,
      investigation_path: state.path,
      investigation_depth: state.depth,
      parent_assessment: semantic_parent_assessment(state.parent_assessment),
      observed_trace_failures: evidence.trace_failures,
      observed_log_signals: evidence.log_signals,
      trace_examples: evidence.trace_examples,
      log_examples: evidence.log_examples,
      calls: evidence.calls,
      called_by: evidence.called_by
    }

    Actor.dispatch(
      state.context,
      {:measure, :root_cause, semantic_state, state.contract, []}
    )

    {:noreply, %{state | evidence: evidence}}
  end

  def handle_cast({:gaia_evidence, _other_service, _evidence}, state), do: {:noreply, state}

  def handle_cast({:plexus, :measurement, :root_cause, {:ok, response}}, state) do
    root = Plexus.Belief.from(response, :root_cause)
    mode = Plexus.Belief.from(response, :failure_mode)
    strength = Plexus.Belief.from(response, :evidence_strength)
    action = Plexus.Belief.from(response, :next_action)

    targets =
      if state.depth < state.max_depth do
        Policy.next_targets(
          action.value,
          state.evidence,
          MapSet.new(state.path),
          state.branch_width,
          state.available_services
        )
      else
        []
      end

    assessment = %{
      service: state.service,
      root_probability: root.value,
      failure_mode: mode.value,
      evidence_strength: strength.value,
      next_action: action.value,
      path: state.path,
      depth: state.depth
    }

    {spawned_targets, spawn_commands} = reserve_children(state, targets, assessment)

    result = %{
      service: state.service,
      root_probability: root.value,
      failure_mode: mode.value,
      evidence_strength: strength.value,
      next_action: action.value,
      evidence_count: state.evidence.trace_failures + state.evidence.log_signals,
      path: state.path,
      depth: state.depth,
      spawned_targets: spawned_targets
    }

    commands = [{:belief, root}] ++ spawn_commands ++ [{:complete, result}]
    Actor.dispatch(state.context, commands)

    {:noreply, state}
  end

  def handle_cast({:plexus, :measurement, :root_cause, {:error, error}}, state) do
    Actor.dispatch(
      state.context,
      {:complete,
       %{
         service: state.service,
         path: state.path,
         depth: state.depth,
         error: inspect(error),
         spawned_targets: []
       }}
    )

    {:noreply, state}
  end

  def handle_cast({:plexus, :command_error, {:spawn, _child_id}, _reason}, state) do
    :ok = Accounts.refund(state.context.run_id, state.pool, :population, 1)
    {:noreply, state}
  end

  @impl true
  def handle_evaluation(_, _, state), do: {:noreply, state}

  defp semantic_parent_assessment(nil), do: nil

  defp semantic_parent_assessment(assessment) when is_map(assessment) do
    Map.new(assessment, fn {key, value} -> {key, semantic_json_value(value)} end)
  end

  defp semantic_json_value(nil), do: nil
  defp semantic_json_value(value) when is_boolean(value), do: value
  defp semantic_json_value(value) when is_number(value), do: value
  defp semantic_json_value(value) when is_binary(value), do: value
  defp semantic_json_value(value) when is_atom(value), do: Atom.to_string(value)

  defp semantic_json_value(value) when is_list(value),
    do: Enum.map(value, &semantic_json_value/1)

  defp semantic_json_value(value) when is_map(value) do
    Map.new(value, fn {key, nested} -> {key, semantic_json_value(nested)} end)
  end

  defp reserve_children(state, targets, assessment) do
    Enum.reduce(targets, {[], []}, fn target, {spawned, commands} ->
      case Accounts.reserve(state.context.run_id, state.pool, :population, 1) do
        :ok ->
          child_path = state.path ++ [target.service]
          child_id = {:hypothesis, state.origin, child_path}

          init_arg = %{
            service: target.service,
            evidence_id: {:evidence, target.service},
            contract: state.contract,
            pool: state.pool,
            path: child_path,
            origin: state.origin,
            depth: state.depth + 1,
            max_depth: state.max_depth,
            branch_width: state.branch_width,
            available_services: state.available_services,
            parent_assessment:
              Map.merge(assessment, %{
                transition: target.direction,
                transition_span_count: target.count
              })
          }

          child_commands = [
            {:spawn, :hypothesis, __MODULE__, init_arg,
             [
               actor_id: child_id,
               metadata: %{
                 gaia_origin: state.origin,
                 gaia_depth: state.depth + 1,
                 gaia_transition: target.direction
               }
             ]},
            {:edge, :investigates, state.context.actor_id, child_id, target.count * 1.0,
             %{direction: target.direction}},
            {:send, child_id, :evaluate}
          ]

          {[target.service | spawned], commands ++ child_commands}

        {:error, :budget_exhausted} ->
          {spawned, commands}
      end
    end)
    |> then(fn {spawned, commands} -> {Enum.reverse(spawned), commands} end)
  end
end

defmodule Plexus.Examples.IncidentCommander do
  alias Plexus.Budget.Accounts
  alias Plexus.Examples.IncidentCommander.{Evidence, Hypothesis}
  alias Plexus.Examples.Support.{Data, Metrics, Runtime}
  alias Plexus.{Graph, Run}

  @hypothesis_pool :gaia_hypothesis_population

  def hypothesis_pool, do: @hypothesis_pool

  def run(opts) do
    data_dir = opts[:data_dir] || Runtime.data_dir("gaia")
    source = opts[:source_dir] || Path.join([data_dir, "GAIA-DataSet", "MicroSS"])

    unless File.dir?(source),
      do:
        raise(
          "missing GAIA MicroSS directory #{source}; run fetch.exs first or pass --source-dir"
        )

    day = opts[:day] || "2021-07-01"
    max_rows = opts[:max_rows] || 100_000
    max_services = opts[:max_services] || 32
    seed_services = opts[:seed_services] || 3
    max_hypotheses = opts[:max_hypotheses] || 24
    max_depth = opts[:max_depth] || 3
    branch_width = opts[:branch_width] || 2

    validate_positive!(:max_rows, max_rows)
    validate_positive!(:max_services, max_services)
    validate_positive!(:seed_services, seed_services)
    validate_positive!(:max_hypotheses, max_hypotheses)
    validate_non_negative!(:max_depth, max_depth)
    validate_positive!(:branch_width, branch_width)

    files = Path.wildcard(Path.join(source, "**/*.csv"))
    run_files = Enum.filter(files, &path_kind?(&1, "run"))
    trace_files = Enum.filter(files, &path_kind?(&1, "trace"))
    business_files = Enum.filter(files, &path_kind?(&1, "business"))

    truth = anomaly_truth(run_files, day, max_rows)
    trace_rows = rows_for_day(trace_files, day, max_rows)
    business_rows = rows_for_day(business_files, day, max_rows)
    all_trace_edges = trace_dependencies(trace_rows)
    all_evidence = build_evidence(trace_rows, business_rows, all_trace_edges)

    evidence =
      all_evidence
      |> Enum.sort_by(fn {_service, row} -> -(row.trace_failures + row.log_signals) end)
      |> Enum.take(max_services)
      |> Map.new()

    if map_size(evidence) == 0,
      do:
        raise(
          "no GAIA evidence rows found for #{day}; choose another --day or inspect the downloaded dataset"
        )

    selected = evidence |> Map.keys() |> MapSet.new()

    trace_edges =
      Map.filter(all_trace_edges, fn {{caller, callee}, _count} ->
        MapSet.member?(selected, caller) and MapSet.member?(selected, callee)
      end)

    run =
      Runtime.start_run!(
        max_population: map_size(evidence) + max_hypotheses + 100,
        max_depth: max_depth,
        budgets: [
          measure: max_hypotheses,
          population: map_size(evidence) + max_hypotheses * 2 + 100,
          tokens: Runtime.token_budget(opts, max_hypotheses, per_call: 12_000, floor: 250_000)
        ]
      )

    try do
      summary =
        investigate(run, evidence, trace_edges,
          day: day,
          seed_services: seed_services,
          max_hypotheses: max_hypotheses,
          max_depth: max_depth,
          branch_width: branch_width,
          timeout_ms: opts[:timeout_ms] || 180_000
        )

      report(run, day, truth, summary)
      summary
    after
      Runtime.stop(run)
    end
  end

  def investigate(run, evidence, trace_edges, opts \\ []) when is_map(evidence) do
    if map_size(evidence) == 0, do: raise(ArgumentError, "evidence universe cannot be empty")

    seed_services = Keyword.get(opts, :seed_services, 3)
    max_hypotheses = Keyword.get(opts, :max_hypotheses, 24)
    max_depth = Keyword.get(opts, :max_depth, 3)
    branch_width = Keyword.get(opts, :branch_width, 2)
    timeout_ms = Keyword.get(opts, :timeout_ms, 180_000)
    day = Keyword.get(opts, :day, "synthetic")

    validate_positive!(:seed_services, seed_services)
    validate_positive!(:max_hypotheses, max_hypotheses)
    validate_non_negative!(:max_depth, max_depth)
    validate_positive!(:branch_width, branch_width)

    contract = contract()
    :ok = Plexus.register_contract(run, :gaia_root_cause, contract, version: 2)
    run_id = Run.run_id(run)

    :ok =
      Accounts.grant(
        run_id,
        :root,
        @hypothesis_pool,
        population: max_hypotheses
      )

    available_services = evidence |> Map.keys() |> MapSet.new()

    Enum.each(evidence, fn {service, row} ->
      {:ok, _} =
        Plexus.start_actor(run,
          module: Evidence,
          actor_id: {:evidence, service},
          class: :evidence,
          init_arg: %{service: service, summary: row}
        )

      Plexus.cast({run, {:evidence, service}}, :publish)
    end)

    Enum.each(trace_edges, fn {{caller, callee}, count} ->
      Graph.add_edge(
        run_id,
        :calls,
        {:evidence, caller},
        {:evidence, callee},
        count * 1.0,
        %{dataset: :gaia, day: day, spans: count}
      )
    end)

    seeds =
      evidence
      |> Enum.sort_by(fn {service, row} ->
        {-(row.trace_failures + row.log_signals), service}
      end)
      |> Enum.take(min(seed_services, max_hypotheses))

    Enum.each(seeds, fn {service, _row} ->
      :ok = Accounts.reserve(run_id, @hypothesis_pool, :population, 1)

      actor_id = {:hypothesis, service, [service]}

      case Plexus.start_actor(run,
             module: Hypothesis,
             actor_id: actor_id,
             class: :hypothesis,
             init_arg: %{
               service: service,
               evidence_id: {:evidence, service},
               contract: :gaia_root_cause,
               pool: @hypothesis_pool,
               path: [service],
               origin: service,
               depth: 0,
               max_depth: max_depth,
               branch_width: branch_width,
               available_services: available_services,
               parent_assessment: nil
             },
             metadata: %{gaia_origin: service, gaia_depth: 0}
           ) do
        {:ok, _pid} ->
          Plexus.cast({run, actor_id}, :evaluate)

        {:error, reason} ->
          :ok = Accounts.refund(run_id, @hypothesis_pool, :population, 1)
          raise "could not start GAIA seed hypothesis #{inspect(service)}: #{inspect(reason)}"
      end
    end)

    Runtime.await_quiescent!(run, timeout_ms)
    summarize(run, length(seeds))
  end

  defp contract do
    TypeSafeSDK.prepare!(
      root_cause:
        TypeSafeSDK.noul(
          "Is the named candidate service plausibly the root cause, rather than merely a downstream victim, of the observed incident evidence?"
        ),
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
        TypeSafeSDK.score(
          "How diagnostically strong is the evidence for this service as root cause?",
          ["weak", "limited", "useful", "strong"]
        ),
      next_action:
        TypeSafeSDK.choice(
          "What should this investigation do next for the candidate service? Choose stop only when the current path is sufficiently resolved or has no useful continuation.",
          investigate_callers:
            "Follow services that call this service because the current service may be a downstream victim.",
          investigate_callees:
            "Follow services called by this service because a dependency may be the underlying source.",
          investigate_both:
            "Investigate both callers and callees because the direction of causality is unresolved.",
          stop: "Do not create another hypothesis from this path."
        )
    )
  end

  defp summarize(run, seed_count) do
    id = Run.run_id(run)
    hypotheses = Graph.by_class(id, :hypothesis)

    complete =
      hypotheses
      |> Enum.filter(fn {_actor_id, attrs} -> attrs[:status] == :complete end)

    successful =
      Enum.reject(complete, fn {_actor_id, attrs} ->
        Map.has_key?(attrs[:result] || %{}, :error)
      end)

    failed =
      Enum.filter(complete, fn {_actor_id, attrs} ->
        Map.has_key?(attrs[:result] || %{}, :error)
      end)

    ranked =
      successful
      |> Enum.sort_by(
        fn {_actor_id, attrs} -> hypothesis_score(attrs[:result] || %{}) end,
        :desc
      )

    pool = Accounts.snapshot(id)[@hypothesis_pool]

    %{
      seed_hypotheses: seed_count,
      hypotheses_spawned: length(hypotheses),
      dynamic_hypotheses: max(length(hypotheses) - seed_count, 0),
      completed_hypotheses: length(complete),
      successful_hypotheses: length(successful),
      failed_hypotheses: length(failed),
      hypothesis_credits_used: pool.meters.population.used,
      hypothesis_credits_remaining: pool.meters.population.remaining,
      max_investigation_depth: max_depth(complete),
      ranked: ranked
    }
  end

  defp hypothesis_score(result) do
    (result[:root_probability] || 0.0) *
      (1.0 + numeric(result[:evidence_strength])) *
      :math.log(1.0 + (result[:evidence_count] || 0))
  end

  defp max_depth([]), do: 0

  defp max_depth(rows) do
    rows
    |> Enum.map(fn {_actor_id, attrs} -> get_in(attrs, [:result, :depth]) || 0 end)
    |> Enum.max()
  end

  defp numeric(value) when is_number(value), do: value * 1.0
  defp numeric(_), do: 0.0

  defp build_evidence(trace_rows, business_rows, trace_edges) do
    traces =
      trace_rows
      |> Enum.filter(fn row -> row["status_code"] not in [nil, "", "200"] end)
      |> Enum.group_by(&(&1["service_name"] || &1["service"] || "unknown"))

    logs =
      business_rows
      |> Enum.filter(fn row ->
        Regex.match?(~r/(error|warning|fail|timeout|exception)/i, row["message"] || "")
      end)
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
           Enum.take(
             Enum.map(service_traces, &Map.take(&1, ["url", "status_code", "message"])),
             5
           ),
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
         calls:
           summary.calls
           |> Enum.sort_by(fn {_name, count} -> -count end)
           |> Enum.take(8)
           |> Enum.map(fn {name, count} -> %{service: name, count: count} end),
         called_by:
           summary.called_by
           |> Enum.sort_by(fn {_name, count} -> -count end)
           |> Enum.take(8)
           |> Enum.map(fn {name, count} -> %{service: name, count: count} end)
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
    Data.balanced_csv_maps!(files, ["timestamp", "datetime"], day, limit)
  end

  defp path_kind?(path, kind), do: path |> String.downcase() |> String.contains?("/#{kind}/")

  defp report(run, day, truth, summary) do
    truth_services = truth |> Enum.map(& &1.service) |> Enum.reject(&is_nil/1) |> Enum.uniq()

    predicted =
      case List.first(summary.ranked) do
        nil -> nil
        {_id, attrs} -> attrs.result[:service]
      end

    IO.puts("\nGAIA incident commander — #{day}")

    Metrics.print_table([
      {"seed hypotheses", summary.seed_hypotheses},
      {"hypotheses spawned", summary.hypotheses_spawned},
      {"dynamic descendants", summary.dynamic_hypotheses},
      {"completed hypotheses", summary.completed_hypotheses},
      {"successful hypotheses", summary.successful_hypotheses},
      {"failed hypotheses", summary.failed_hypotheses},
      {"max investigation depth", summary.max_investigation_depth},
      {"hypothesis credits used", summary.hypothesis_credits_used},
      {"semantic measurements", Plexus.budget(run).measure.used},
      {"known injected services", Enum.join(truth_services, ", ")},
      {"top predicted service", predicted || "none"},
      {"top hits injected service", predicted in truth_services}
    ])

    IO.puts("\nRanked root-cause investigation paths:")

    Enum.each(Enum.take(summary.ranked, 10), fn {_id, attrs} ->
      IO.inspect(attrs.result)
    end)
  end

  defp validate_positive!(name, value) do
    if not is_integer(value) or value < 1,
      do: raise(ArgumentError, "--#{option_name(name)} must be at least 1")
  end

  defp validate_non_negative!(name, value) do
    if not is_integer(value) or value < 0,
      do: raise(ArgumentError, "--#{option_name(name)} must be non-negative")
  end

  defp option_name(name), do: name |> Atom.to_string() |> String.replace("_", "-")
end
