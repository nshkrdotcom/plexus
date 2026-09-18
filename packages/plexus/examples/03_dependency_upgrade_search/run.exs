Code.require_file("../support/runtime.exs", __DIR__)
Code.require_file("../support/metrics.exs", __DIR__)

alias Plexus.Examples.Support.{Metrics, Runtime}

defmodule Plexus.Examples.DependencyUpgrade.Dependency do
  use Plexus.Actor

  alias Plexus.Actor

  @impl true
  def init(args) do
    {:ok,
     %{
       context: Actor.context(args),
       node: args.node,
       previous: args.previous,
       metadata: args.metadata,
       contract: args.contract,
       changed: args.changed
     }}
  end

  @impl true
  def handle_cast(:analyze, %{changed: false} = state) do
    Actor.dispatch(
      state.context,
      {:complete, %{changed: false, name: state.node["versionKey"]["name"]}}
    )

    {:noreply, state}
  end

  def handle_cast(:analyze, state) do
    key = state.node["versionKey"]

    input = %{
      package: key["name"],
      source_versions: state.previous,
      to_version: key["version"],
      dependency_relation: state.node["relation"],
      target_licenses: state.metadata["licenses"] || [],
      target_security_advisories: state.metadata["advisoryKeys"] || [],
      deprecated: state.metadata["isDeprecated"] || false,
      deprecation_reason: state.metadata["deprecatedReason"]
    }

    Actor.dispatch(state.context, {:measure, :upgrade_risk, input, state.contract, []})
    {:noreply, state}
  end

  def handle_cast({:plexus, :measurement, :upgrade_risk, {:ok, response}}, state) do
    attention = Plexus.Belief.from(response, :attention)
    risk = Plexus.Belief.from(response, :risk)
    reason = Plexus.Belief.from(response, :reason)
    key = state.node["versionKey"]

    Actor.dispatch(state.context, [
      {:belief, attention},
      {:complete,
       %{
         changed: true,
         name: key["name"],
         from: state.previous,
         to: key["version"],
         relation: state.node["relation"],
         attention: attention.value,
         risk: risk.value,
         reason: reason.value,
         advisories: length(state.metadata["advisoryKeys"] || [])
       }}
    ])

    {:noreply, state}
  end

  def handle_cast({:plexus, :measurement, :upgrade_risk, {:error, error}}, state) do
    Actor.dispatch(
      state.context,
      {:complete,
       %{
         changed: true,
         error: inspect(error),
         name: state.node["versionKey"]["name"]
       }}
    )

    {:noreply, state}
  end

  @impl true
  def handle_evaluation(_, _, state), do: {:noreply, state}
end

defmodule Plexus.Examples.DependencyUpgrade.Plan do
  use Plexus.Actor

  alias Plexus.Actor

  @impl true
  def init(args) do
    {:ok,
     %{
       context: Actor.context(args),
       order: args.order,
       remaining: args.remaining,
       score: args.score,
       added: args.added
     }}
  end

  @impl true
  def handle_cast(:score, state) do
    Actor.dispatch(
      state.context,
      {:complete,
       %{
         order: state.order,
         remaining: state.remaining,
         score: state.score,
         added: state.added
       }}
    )

    {:noreply, state}
  end

  @impl true
  def handle_evaluation(_, _, state), do: {:noreply, state}
end

defmodule Plexus.Examples.DependencyUpgrade do
  alias Plexus.Examples.DependencyUpgrade.{Dependency, Plan}
  alias Plexus.Examples.Support.{Metrics, Runtime}
  alias Plexus.{Graph, Population, Provenance, Run}

  def run(opts) do
    data_dir = resolve_data_dir(opts)
    from_graph = read_json!(Path.join(data_dir, "from_graph.json"))
    to_graph = read_json!(Path.join(data_dir, "to_graph.json"))
    metadata = read_json!(Path.join(data_dir, "metadata.json"))
    scenario = read_json!(Path.join(data_dir, "scenario.json"))

    max_nodes = opts[:max_nodes] || 2_000
    if max_nodes < 1, do: raise(ArgumentError, "--max-nodes must be at least 1")
    target_nodes = Enum.take(to_graph["nodes"], max_nodes)
    from_versions = versions_by_name(from_graph["nodes"])
    from_keys = version_keys(from_graph["nodes"])
    changed_count = Enum.count(target_nodes, &changed?(&1, from_keys))
    search_items = max(opts[:search_items] || 8, 1)
    beam_width = max(opts[:beam_width] || 6, 1)
    branch_width = max(opts[:branch_width] || 4, 1)
    search_allowance = search_items * beam_width * branch_width + 100

    run =
      Runtime.start_run!(
        max_population: length(target_nodes) + search_allowance,
        budgets: [
          measure: changed_count,
          population: length(target_nodes) + search_allowance,
          tokens: Runtime.token_budget(opts, changed_count, per_call: 6_000, floor: 250_000)
        ]
      )

    try do
      register_contract(run)
      run_id = Run.run_id(run)
      ids = start_dependency_population(run, target_nodes, from_versions, from_keys, metadata)
      connect_dependency_topology(run_id, to_graph["edges"], ids)

      Enum.each(Map.values(ids), &Plexus.cast({run, &1}, :analyze))
      Runtime.await_class_complete!(run, :dependency, map_size(ids), opts[:timeout_ms] || 180_000)
      Runtime.await_messages_drained!(run)

      plans =
        search_migration_plans(
          run,
          to_graph,
          target_nodes,
          search_items,
          beam_width,
          branch_width,
          opts[:timeout_ms] || 180_000
        )

      Runtime.await_quiescent!(run, 30_000)
      report(run, scenario, changed_count, plans)
    after
      Runtime.stop(run)
    end
  end

  defp register_contract(run) do
    contract =
      TypeSafeSDK.prepare!(
        attention:
          TypeSafeSDK.noul(
            "Does this dependency version transition deserve explicit human migration review?"
          ),
        risk:
          TypeSafeSDK.score(
            "How much migration risk does this dependency transition introduce given the supplied metadata?",
            ["negligible", "low", "moderate", "high"]
          ),
        reason:
          TypeSafeSDK.choice(
            "What is the dominant reason this transition deserves attention?",
            compatibility: "API or behavior compatibility risk",
            security: "Security advisory or security posture change",
            deprecation: "Deprecation or end-of-life signal",
            transitive_churn: "Indirect dependency churn or graph impact",
            routine: "Routine upgrade with no standout concern"
          )
      )

    :ok = Plexus.register_contract(run, :dependency_upgrade, contract, version: 1)
  end

  defp start_dependency_population(run, target_nodes, from_versions, from_keys, metadata) do
    target_nodes
    |> Enum.with_index()
    |> Map.new(fn {node, index} ->
      key = node["versionKey"]
      id = {:dependency, index, key["name"], key["version"]}
      previous = Map.get(from_versions, key["name"], [])
      changed = changed?(node, from_keys)

      {:ok, _} =
        Plexus.start_actor(run,
          module: Dependency,
          actor_id: id,
          class: :dependency,
          init_arg: %{
            node: node,
            previous: previous,
            metadata: Map.get(metadata, metadata_key(key), %{}),
            contract: :dependency_upgrade,
            changed: changed
          }
        )

      {index, id}
    end)
  end

  defp connect_dependency_topology(run_id, edges, ids) do
    Enum.each(edges, fn edge ->
      with from when not is_nil(from) <- Map.get(ids, edge["fromNode"]),
           to when not is_nil(to) <- Map.get(ids, edge["toNode"]) do
        Provenance.depend(run_id, from, to, %{
          requirement: edge["requirement"],
          dataset: :deps_dev
        })
      else
        _ -> :ok
      end
    end)
  end

  defp search_migration_plans(
         run,
         to_graph,
         target_nodes,
         search_items,
         beam_width,
         branch_width,
         timeout
       ) do
    run_id = Run.run_id(run)

    items =
      Graph.by_class(run_id, :dependency)
      |> Enum.flat_map(fn {_id, attrs} ->
        case attrs[:result] do
          %{changed: true, name: name} = result when not is_nil(name) -> [result]
          _ -> []
        end
      end)
      |> Enum.reject(&Map.has_key?(&1, :error))
      |> Enum.uniq_by(& &1.name)
      |> Enum.sort_by(&(-priority(&1)))
      |> Enum.take(search_items)

    names = Enum.map(items, & &1.name)

    if names == [] do
      []
    else
      priorities = Map.new(items, &{&1.name, priority(&1)})
      prerequisites = changed_prerequisites(to_graph, target_nodes, MapSet.new(names))
      root_id = {:migration_plan, 0, 0}

      {:ok, _} =
        Plexus.start_actor(run,
          module: Plan,
          actor_id: root_id,
          class: :plan,
          init_arg: %{order: [], remaining: names, score: 0.0, added: nil}
        )

      Plexus.cast({run, root_id}, :score)
      Runtime.await_actor_ids_complete!(run, [root_id], timeout)

      root = %{id: root_id, order: [], remaining: names, score: 0.0}

      1..length(names)
      |> Enum.reduce([root], fn depth, beam ->
        candidates =
          beam
          |> Enum.with_index()
          |> Enum.flat_map(fn {parent, parent_index} ->
            choices =
              ready_choices(parent, prerequisites)
              |> Enum.sort_by(&(-Map.get(priorities, &1, 0.0)))
              |> Enum.take(branch_width)

            choices
            |> Enum.with_index()
            |> Enum.map(fn {name, choice_index} ->
              id =
                {:migration_plan, depth, parent_index, choice_index,
                 :erlang.unique_integer([:positive])}

              order = parent.order ++ [name]
              remaining = List.delete(parent.remaining, name)
              score = parent.score + Map.get(priorities, name, 0.0) / depth

              {:ok, _} =
                Plexus.start_actor(run,
                  module: Plan,
                  actor_id: id,
                  parent_id: parent.id,
                  class: :plan,
                  init_arg: %{
                    order: order,
                    remaining: remaining,
                    score: score,
                    added: name
                  }
                )

              Plexus.cast({run, id}, :score)
              %{id: id, order: order, remaining: remaining, score: score}
            end)
          end)

        ids = Enum.map(candidates, & &1.id)
        Runtime.await_actor_ids_complete!(run, ids, timeout)

        kept = candidates |> Enum.sort_by(&(-&1.score)) |> Enum.take(beam_width)
        kept_ids = MapSet.new(Enum.map(kept, & &1.id))

        candidates
        |> Enum.reject(&MapSet.member?(kept_ids, &1.id))
        |> Enum.each(&Plexus.prune(run, &1.id))

        kept
      end)
      |> Enum.sort_by(&(-&1.score))
    end
  end

  defp ready_choices(plan, prerequisites) do
    completed = MapSet.new(plan.order)

    ready =
      Enum.filter(plan.remaining, fn name ->
        prerequisites
        |> Map.get(name, MapSet.new())
        |> MapSet.subset?(completed)
      end)

    # Real resolved package graphs can contain changed-only cycles. If a cycle
    # prevents progress, let the search break the tie rather than deadlocking.
    if ready == [], do: plan.remaining, else: ready
  end

  defp changed_prerequisites(to_graph, target_nodes, selected) do
    index_to_name =
      target_nodes
      |> Enum.with_index()
      |> Map.new(fn {node, index} -> {index, node["versionKey"]["name"]} end)

    Enum.reduce(to_graph["edges"], %{}, fn edge, acc ->
      from = Map.get(index_to_name, edge["fromNode"])
      to = Map.get(index_to_name, edge["toNode"])

      if MapSet.member?(selected, from) and MapSet.member?(selected, to) and from != to do
        Map.update(acc, from, MapSet.new([to]), &MapSet.put(&1, to))
      else
        acc
      end
    end)
  end

  defp priority(result) do
    attention = result[:attention] || 0.0
    risk = result[:risk] || 0.0
    advisories = result[:advisories] || 0
    attention * (1.0 + risk) + :math.log(1.0 + advisories)
  end

  defp report(run, scenario, changed_count, plans) do
    id = Run.run_id(run)

    changed =
      Graph.by_class(id, :dependency)
      |> Enum.filter(fn {_id, attrs} -> get_in(attrs, [:result, :changed]) == true end)
      |> Enum.sort_by(fn {_id, attrs} ->
        -(get_in(attrs, [:result, :attention]) || 0.0) *
          (1.0 + (get_in(attrs, [:result, :risk]) || 0.0))
      end)

    pareto =
      Population.pareto(id, :dependency, [
        fn attrs -> get_in(attrs, [:result, :attention]) || 0.0 end,
        fn attrs -> get_in(attrs, [:result, :risk]) || 0.0 end
      ])
      |> Enum.filter(fn {_id, attrs} -> get_in(attrs, [:result, :changed]) == true end)

    IO.puts("\ndeps.dev dependency upgrade search")

    Metrics.print_table([
      {"scenario", "#{scenario["package"]} #{scenario["from"]} -> #{scenario["to"]}"},
      {"graph nodes", Graph.count(id)},
      {"changed nodes", changed_count},
      {"semantic measurements", Plexus.budget(run).measure.used},
      {"Pareto attention/risk frontier", length(pareto)},
      {"surviving migration plans", length(plans)}
    ])

    IO.puts("\nHighest-attention changed dependencies:")
    changed |> Enum.take(15) |> Enum.each(fn {_actor_id, attrs} -> IO.inspect(attrs.result) end)

    IO.puts("\nBest dependency-aware migration orders:")

    plans
    |> Enum.take(5)
    |> Enum.each(fn plan -> IO.inspect(%{score: plan.score, order: plan.order}) end)
  end

  defp resolve_data_dir(opts) do
    case opts[:data_dir] do
      nil ->
        candidates = Path.wildcard(Path.join([Runtime.root(), ".plexus-data", "deps-dev-*"]))

        case Enum.sort(candidates) |> List.last() do
          nil -> raise "no deps.dev scenario found; run fetch.exs or pass --data-dir"
          path -> path
        end

      path ->
        path
    end
  end

  defp read_json!(path), do: path |> File.read!() |> Jason.decode!()

  defp versions_by_name(nodes) do
    nodes
    |> Enum.map(& &1["versionKey"])
    |> Enum.group_by(& &1["name"], & &1["version"])
    |> Map.new(fn {name, versions} -> {name, Enum.sort(Enum.uniq(versions))} end)
  end

  defp version_keys(nodes) do
    nodes
    |> Enum.map(&version_tuple(&1["versionKey"]))
    |> MapSet.new()
  end

  defp changed?(node, from_keys) do
    not MapSet.member?(from_keys, version_tuple(node["versionKey"]))
  end

  defp metadata_key(key) do
    Enum.join([key["system"], key["name"], key["version"]], "\u001F")
  end

  defp version_tuple(key), do: {key["system"], key["name"], key["version"]}
end

{opts, _, _} =
  OptionParser.parse(System.argv(),
    strict: [
      data_dir: :string,
      max_nodes: :integer,
      search_items: :integer,
      beam_width: :integer,
      branch_width: :integer,
      token_budget: :integer,
      timeout_ms: :integer
    ]
  )

Plexus.Examples.DependencyUpgrade.run(opts)
