Code.require_file("../support/runtime.exs", __DIR__)
Code.require_file("../support/data.exs", __DIR__)
Code.require_file("../support/metrics.exs", __DIR__)

alias Plexus.Examples.Support.{Data, Metrics, Runtime}

defmodule Plexus.Examples.CitySignal.Cluster do
  use Plexus.Actor
  alias Plexus.Actor

  @impl true
  def init(args) do
    {:ok,
     %{
       context: Actor.context(args),
       key: args.key,
       expected: args.expected,
       contract: args.contract,
       count: 0,
       complaints: %{},
       descriptors: %{},
       agencies: %{}
     }}
  end

  @impl true
  def handle_cast({:report, record}, state) do
    next = %{
      state
      | count: state.count + 1,
        complaints: bump(state.complaints, record["complaint_type"]),
        descriptors: bump(state.descriptors, record["descriptor"]),
        agencies: bump(state.agencies, record["agency"])
    }

    if next.count == next.expected do
      Actor.dispatch(next.context, {:measure, :cluster, semantic_state(next), next.contract, []})
    end

    {:noreply, next}
  end

  def handle_cast({:plexus, :measurement, :cluster, {:ok, response}}, state) do
    coherent = Plexus.Belief.from(response, :coherent)
    theme = Plexus.Belief.from(response, :theme)
    strength = Plexus.Belief.from(response, :strength)

    result = %{
      key: state.key,
      report_count: state.count,
      coherent: coherent.value,
      theme: theme.value,
      strength: strength.value,
      top_complaints: top(state.complaints, 5)
    }

    Actor.dispatch(state.context, [{:belief, coherent}, {:complete, result}])
    {:noreply, state}
  end

  def handle_cast({:plexus, :measurement, :cluster, {:error, error}}, state) do
    Actor.dispatch(
      state.context,
      {:complete, %{key: state.key, error: inspect(error), report_count: state.count}}
    )

    {:noreply, state}
  end

  @impl true
  def handle_evaluation(_, _, state), do: {:noreply, state}

  defp semantic_state(state) do
    %{
      time_and_grid_cell: inspect(state.key),
      report_count: state.count,
      complaint_mix: top(state.complaints, 8),
      problem_details: top(state.descriptors, 8),
      responding_agencies: top(state.agencies, 5)
    }
  end

  defp bump(map, nil), do: map
  defp bump(map, ""), do: map
  defp bump(map, key), do: Map.update(map, key, 1, &(&1 + 1))
  defp top(map, n), do: map |> Enum.sort_by(fn {_k, v} -> -v end) |> Enum.take(n)
end

defmodule Plexus.Examples.CitySignal.Report do
  use Plexus.Actor
  alias Plexus.Actor

  @impl true
  def init(args),
    do: {:ok, %{context: Actor.context(args), record: args.record, cluster: args.cluster}}

  @impl true
  def handle_cast(:route, state) do
    Actor.dispatch(state.context, [
      {:edge, :observed_in, state.context.actor_id, state.cluster, 1.0},
      {:send, state.cluster, {:report, state.record}},
      {:complete, :routed}
    ])

    {:noreply, state}
  end

  @impl true
  def handle_evaluation(_, _, state), do: {:noreply, state}
end

defmodule Plexus.Examples.CitySignal do
  alias Plexus.Examples.CitySignal.{Cluster, Report}
  alias Plexus.Examples.Support.{Data, Metrics, Runtime}
  alias Plexus.{Graph, Run}

  def run(opts) do
    data_dir = opts[:data_dir] || Runtime.data_dir("nyc-311")
    path = Path.join(data_dir, "requests.jsonl")
    unless File.exists?(path), do: raise("missing #{path}; run fetch.exs first")

    max_clusters = opts[:max_clusters] || 40
    min_cluster = opts[:min_cluster] || 4
    grid = opts[:grid] || 0.02
    if max_clusters < 1, do: raise(ArgumentError, "--max-clusters must be at least 1")
    if min_cluster < 1, do: raise(ArgumentError, "--min-cluster must be at least 1")
    if grid <= 0, do: raise(ArgumentError, "--grid must be greater than 0")

    {source_count, counts} = cluster_counts(path, grid)

    selected_keys =
      counts
      |> Enum.filter(fn {_key, count} -> count >= min_cluster end)
      |> Enum.sort_by(fn {_key, count} -> -count end)
      |> Enum.take(max_clusters)
      |> Map.new()

    selected = MapSet.new(Map.keys(selected_keys))

    grouped =
      Data.jsonl!(path)
      |> Stream.map(&{cluster_key(&1, grid), &1})
      |> Stream.filter(fn {key, _record} -> MapSet.member?(selected, key) end)
      |> Enum.group_by(fn {key, _record} -> key end, fn {_key, record} -> record end)

    selected_count = Enum.sum(Map.values(selected_keys))

    if selected_count == 0 do
      raise "no NYC 311 clusters met --min-cluster #{min_cluster}; widen the fetch window or lower the threshold"
    end

    run =
      Runtime.start_run!(
        max_population: selected_count + max_clusters + 100,
        budgets: [
          measure: max_clusters,
          population: selected_count + max_clusters + 100,
          tokens: Runtime.token_budget(opts, length(grouped), per_call: 6_000, floor: 250_000)
        ]
      )

    try do
      prepared =
        TypeSafeSDK.prepare!(
          coherent:
            TypeSafeSDK.noul(
              "Do these co-located reports plausibly represent one shared operational incident rather than ordinary background demand?"
            ),
          theme:
            TypeSafeSDK.choice(
              "What operational theme best describes this cluster?",
              infrastructure: "Utility, street, building, or physical infrastructure",
              sanitation: "Waste, cleanliness, pests, or environmental sanitation",
              safety: "Public safety or hazardous condition",
              noise: "Noise or quality-of-life disturbance",
              housing: "Housing or property condition",
              transportation: "Road, transit, vehicle, or traffic condition",
              other: "No single listed theme fits"
            ),
          strength:
            TypeSafeSDK.score(
              "How strong is the evidence that this cluster represents a distinct local incident?",
              ["background", "weak", "moderate", "strong"]
            )
        )

      :ok = Plexus.register_contract(run, :city_cluster, prepared, version: 1)

      Enum.each(grouped, fn {key, rows} ->
        cluster_id = {:cluster, key}

        {:ok, _} =
          Plexus.start_actor(run,
            module: Cluster,
            actor_id: cluster_id,
            class: :cluster,
            init_arg: %{key: key, expected: length(rows), contract: :city_cluster}
          )

        Enum.each(rows, fn record ->
          report_id = {:report, record["unique_key"]}

          {:ok, _} =
            Plexus.start_actor(run,
              module: Report,
              actor_id: report_id,
              class: :report,
              init_arg: %{record: record, cluster: cluster_id}
            )

          Plexus.cast({run, report_id}, :route)
        end)
      end)

      Runtime.await_class_complete!(run, :cluster, length(grouped), opts[:timeout_ms] || 180_000)
      Runtime.await_quiescent!(run, 30_000)
      report(run, source_count, selected_count)
    after
      Runtime.stop(run)
    end
  end

  defp cluster_counts(path, grid) do
    Data.jsonl!(path)
    |> Enum.reduce({0, %{}}, fn record, {count, counts} ->
      key = cluster_key(record, grid)
      next = if is_nil(key), do: counts, else: Map.update(counts, key, 1, &(&1 + 1))
      {count + 1, next}
    end)
  end

  defp cluster_key(record, grid) do
    with lat when is_number(lat) <- Data.parse_number(record["latitude"]),
         lon when is_number(lon) <- Data.parse_number(record["longitude"]),
         created when is_binary(created) <- record["created_date"] do
      {time_bucket(created), Float.floor(lat / grid) * grid, Float.floor(lon / grid) * grid}
    else
      _ -> nil
    end
  end

  defp time_bucket(<<date::binary-size(10), "T", hour::binary-size(2), _::binary>>) do
    h = String.to_integer(hour)
    date <> "T" <> String.pad_leading(Integer.to_string(div(h, 3) * 3), 2, "0")
  end

  defp time_bucket(other), do: String.slice(other, 0, 13)

  defp report(run, source_count, routed_count) do
    id = Run.run_id(run)
    clusters = Graph.by_class(id, :cluster)

    IO.puts("\nNYC 311 city signal tracker")

    Metrics.print_table([
      {"source records", source_count},
      {"routed report actors", routed_count},
      {"cluster actors", length(clusters)},
      {"semantic measurements", Plexus.budget(run).measure.used},
      {"graph nodes", Graph.count(id)}
    ])

    IO.puts("\nHighest-signal clusters:")

    Runtime.top_complete(run, :cluster, 12, fn result ->
      (result[:coherent] || 0.0) * (1.0 + (result[:strength] || 0.0)) *
        :math.log(1.0 + (result[:report_count] || 0))
    end)
    |> Enum.each(fn {_id, attrs} -> IO.inspect(attrs.result) end)
  end
end

{opts, _, _} =
  OptionParser.parse(Runtime.cli_args(),
    strict: [
      data_dir: :string,
      max_clusters: :integer,
      min_cluster: :integer,
      grid: :float,
      token_budget: :integer,
      timeout_ms: :integer
    ]
  )

Plexus.Examples.CitySignal.run(opts)
