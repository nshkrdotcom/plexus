Code.require_file("../support/runtime.exs", __DIR__)
Code.require_file("../support/data.exs", __DIR__)
Code.require_file("../support/metrics.exs", __DIR__)

alias Plexus.Examples.Support.{Data, Metrics, Runtime}

defmodule Plexus.Examples.AlertSwarm.Event do
  use Plexus.Actor
  alias Plexus.Actor

  @impl true
  def init(args) do
    {:ok,
     %{
       context: Actor.context(args),
       record: args.record,
       day: args.day,
       target: args.target,
       subscriber_counter: args.subscriber_counter
     }}
  end

  @impl true
  def handle_cast(:subscribe, state) do
    Actor.dispatch(state.context, [
      {:wake_on, {:storm_day, state.day}},
      {:send, state.subscriber_counter, :subscribed}
    ])

    {:noreply, state}
  end

  def handle_cast({:plexus, :event, {:storm_day, _day}, _payload}, state) do
    Actor.dispatch(state.context, [
      {:edge, :occurred_in, state.context.actor_id, state.target, 1.0},
      {:send, state.target, {:storm_event, state.record}},
      {:complete, :delivered}
    ])

    {:noreply, state}
  end

  @impl true
  def handle_evaluation(_, _, state), do: {:noreply, state}
end

defmodule Plexus.Examples.AlertSwarm.SubscriptionCounter do
  use Plexus.Actor
  alias Plexus.Actor

  @impl true
  def init(args), do: {:ok, %{context: Actor.context(args), expected: args.expected, count: 0}}

  @impl true
  def handle_cast(:subscribed, state) do
    count = state.count + 1

    if count == state.expected,
      do: Actor.dispatch(state.context, {:complete, %{subscriptions: count}})

    {:noreply, %{state | count: count}}
  end

  @impl true
  def handle_evaluation(_, _, state), do: {:noreply, state}
end

defmodule Plexus.Examples.AlertSwarm.RegionDay do
  use Plexus.Actor
  alias Plexus.Actor

  @impl true
  def init(args) do
    {:ok,
     %{
       context: Actor.context(args),
       key: args.key,
       expected: args.expected,
       semantic: args.semantic,
       contract: args.contract,
       count: 0,
       types: %{},
       deaths: 0,
       injuries: 0,
       property: 0.0,
       crop: 0.0,
       narratives: []
     }}
  end

  @impl true
  def handle_cast({:storm_event, record}, state) do
    next = %{
      state
      | count: state.count + 1,
        types: Map.update(state.types, record["EVENT_TYPE"] || "unknown", 1, &(&1 + 1)),
        deaths:
          state.deaths + integer(record["DEATHS_DIRECT"]) + integer(record["DEATHS_INDIRECT"]),
        injuries:
          state.injuries + integer(record["INJURIES_DIRECT"]) +
            integer(record["INJURIES_INDIRECT"]),
        property: state.property + damage(record["DAMAGE_PROPERTY"]),
        crop: state.crop + damage(record["DAMAGE_CROPS"]),
        narratives: take_narrative(state.narratives, record["EVENT_NARRATIVE"])
    }

    if next.count == next.expected, do: finalize(next)
    {:noreply, next}
  end

  def handle_cast({:plexus, :measurement, :compound, {:ok, response}}, state) do
    compound = Plexus.Belief.from(response, :compound)
    posture = Plexus.Belief.from(response, :posture)
    severity = Plexus.Belief.from(response, :severity)

    Actor.dispatch(state.context, [
      {:belief, compound},
      {:complete,
       base_result(state)
       |> Map.merge(%{
         compound: compound.value,
         posture: posture.value,
         semantic_severity: severity.value
       })}
    ])

    {:noreply, state}
  end

  def handle_cast({:plexus, :measurement, :compound, {:error, error}}, state) do
    Actor.dispatch(
      state.context,
      {:complete, base_result(state) |> Map.put(:error, inspect(error))}
    )

    {:noreply, state}
  end

  @impl true
  def handle_evaluation(_, _, state), do: {:noreply, state}

  defp finalize(%{semantic: true} = state) do
    input = base_result(state) |> Map.put(:narrative_examples, Enum.reverse(state.narratives))
    Actor.dispatch(state.context, {:measure, :compound, input, state.contract, []})
  end

  defp finalize(state), do: Actor.dispatch(state.context, {:complete, base_result(state)})

  defp base_result(state) do
    %{
      key: state.key,
      event_count: state.count,
      event_types: state.types |> Enum.sort_by(fn {_k, v} -> -v end) |> Enum.take(8),
      deaths: state.deaths,
      injuries: state.injuries,
      property_damage_usd: state.property,
      crop_damage_usd: state.crop,
      semantic: state.semantic
    }
  end

  defp integer(nil), do: 0
  defp integer(""), do: 0
  defp integer(value) when is_integer(value), do: value

  defp integer(value) do
    case Integer.parse(to_string(value)) do
      {number, _} -> number
      :error -> 0
    end
  end

  defp damage(nil), do: 0.0
  defp damage(""), do: 0.0

  defp damage(value) do
    string = value |> to_string() |> String.trim() |> String.upcase()

    {number, suffix} =
      case Float.parse(string) do
        {parsed, rest} -> {parsed, String.trim(rest)}
        :error -> {0.0, ""}
      end

    multiplier =
      case suffix do
        "K" -> 1.0e3
        "M" -> 1.0e6
        "B" -> 1.0e9
        _ -> 1.0
      end

    number * multiplier
  end

  defp take_narrative(list, nil), do: list
  defp take_narrative(list, ""), do: list
  defp take_narrative(list, text) when length(list) < 4, do: [String.slice(text, 0, 1_000) | list]
  defp take_narrative(list, _), do: list
end

defmodule Plexus.Examples.AlertSwarm do
  alias Plexus.Examples.AlertSwarm.{Event, RegionDay, SubscriptionCounter}
  alias Plexus.Examples.Support.{Data, Metrics, Runtime}
  alias Plexus.{Graph, Run}

  def run(opts) do
    data_dir = opts[:data_dir] || Runtime.data_dir("noaa-storm-events")
    year = opts[:year] || active_year!(data_dir)
    path = Path.join(data_dir, "storm_events_#{year}.csv")
    unless File.exists?(path), do: raise("missing #{path}; run fetch.exs first")

    limit = opts[:limit_events] || 10_000
    max_semantic = opts[:semantic_groups] || 25
    if limit < 0, do: raise(ArgumentError, "--limit-events must be 0 (full year) or positive")
    if max_semantic < 0, do: raise(ArgumentError, "--semantic-groups must be non-negative")

    events = Data.csv_maps!(path) |> maybe_take(limit) |> Enum.to_list()
    grouped = events |> Enum.group_by(&group_key/1) |> Enum.reject(fn {key, _} -> is_nil(key) end)
    actor_event_count = Enum.sum(Enum.map(grouped, fn {_key, rows} -> length(rows) end))

    if actor_event_count == 0 do
      raise "no NOAA Storm Events rows had both a usable day and STATE"
    end

    semantic_keys =
      grouped
      |> Enum.sort_by(fn {_key, rows} -> -impact_score(rows) end)
      |> Enum.take(max_semantic)
      |> Enum.map(&elem(&1, 0))
      |> MapSet.new()

    population_limit = actor_event_count + length(grouped) + 101

    run =
      Runtime.start_run!(
        max_population: population_limit,
        budgets: [
          measure: max_semantic,
          population: population_limit,
          tokens:
            Runtime.token_budget(opts, MapSet.size(semantic_keys),
              per_call: 12_000,
              floor: 250_000
            )
        ]
      )

    try do
      contract =
        TypeSafeSDK.prepare!(
          compound:
            TypeSafeSDK.noul(
              "Do the storm events and narratives in this state/day form a coherent compound operational incident rather than unrelated weather reports?"
            ),
          posture:
            TypeSafeSDK.choice(
              "What operational posture best matches the combined impact?",
              monitor: "Monitor; limited immediate operational consequences",
              prepare: "Prepare resources for material but contained consequences",
              respond: "Active response is warranted due to significant impacts",
              recover: "Primary concern is recovery from already-realized damage"
            ),
          severity:
            TypeSafeSDK.score("How severe is the combined operational impact?", [
              "minor",
              "moderate",
              "major",
              "extreme"
            ])
        )

      :ok = Plexus.register_contract(run, :storm_compound, contract, version: 1)

      counter_id = :storm_subscription_counter

      {:ok, _} =
        Plexus.start_actor(run,
          module: SubscriptionCounter,
          actor_id: counter_id,
          class: :subscription_counter,
          init_arg: %{expected: actor_event_count}
        )

      Enum.each(grouped, fn {key, rows} ->
        group_id = {:region_day, key}

        {:ok, _} =
          Plexus.start_actor(run,
            module: RegionDay,
            actor_id: group_id,
            class: :region_day,
            init_arg: %{
              key: key,
              expected: length(rows),
              semantic: MapSet.member?(semantic_keys, key),
              contract: :storm_compound
            }
          )

        Enum.each(rows, fn record ->
          event_id = {:storm_event, record["EVENT_ID"] || :erlang.unique_integer([:positive])}

          {:ok, _} =
            Plexus.start_actor(run,
              module: Event,
              actor_id: event_id,
              class: :storm_event,
              init_arg: %{
                record: record,
                day: elem(key, 0),
                target: group_id,
                subscriber_counter: counter_id
              }
            )

          Plexus.cast({run, event_id}, :subscribe)
        end)
      end)

      Runtime.await_class_complete!(run, :subscription_counter, 1, opts[:timeout_ms] || 120_000)

      grouped
      |> Enum.map(fn {{day, _state}, _rows} -> day end)
      |> Enum.uniq()
      |> Enum.sort()
      |> Enum.each(&Plexus.publish(run, {:storm_day, &1}, %{year: year}))

      Runtime.await_class_complete!(
        run,
        :region_day,
        length(grouped),
        opts[:timeout_ms] || 240_000
      )

      Runtime.await_quiescent!(run, 60_000)
      report(run, year, length(events), actor_event_count, length(grouped), max_semantic)
    after
      Runtime.stop(run)
    end
  end

  defp group_key(record) do
    day = event_day(record["BEGIN_DATE_TIME"])
    state = record["STATE"]
    if day && state, do: {day, state}, else: nil
  end

  defp event_day(nil), do: nil

  defp event_day(value) do
    case Regex.run(~r/^(\d{2}-[A-Z]{3}-\d{2})/i, value) do
      [_, day] -> String.upcase(day)
      _ -> String.slice(value, 0, 10)
    end
  end

  defp impact_score(rows) do
    Enum.reduce(rows, length(rows) * 1.0, fn row, acc ->
      acc + 50.0 * numeric(row["DEATHS_DIRECT"]) + 10.0 * numeric(row["INJURIES_DIRECT"])
    end)
  end

  defp numeric(nil), do: 0

  defp numeric(value) do
    case Float.parse(to_string(value)) do
      {number, _} -> number
      :error -> 0
    end
  end

  defp maybe_take(stream, 0), do: stream
  defp maybe_take(stream, n), do: Stream.take(stream, n)

  defp active_year!(dir),
    do:
      dir |> Path.join("active_year.txt") |> File.read!() |> String.trim() |> String.to_integer()

  defp report(run, year, source_event_count, actor_event_count, group_count, semantic_limit) do
    id = Run.run_id(run)
    actual_semantic = Plexus.budget(run).measure.used

    IO.puts("\nNOAA Storm Events alert swarm — #{year}")

    Metrics.print_table([
      {"source storm-event rows", source_event_count},
      {"storm-event actors", actor_event_count},
      {"state/day actors", group_count},
      {"semantic state/day groups", actual_semantic},
      {"semantic group cap", semantic_limit},
      {"typed graph nodes", Graph.count(id)}
    ])

    IO.puts("\nHighest-impact region/day results:")

    Runtime.top_complete(run, :region_day, 12, fn result ->
      (result[:deaths] || 0) * 1000 + (result[:injuries] || 0) * 100 +
        :math.log(1.0 + (result[:property_damage_usd] || 0)) + (result[:event_count] || 0)
    end)
    |> Enum.each(fn {_id, attrs} -> IO.inspect(attrs.result) end)
  end
end

{opts, _, _} =
  OptionParser.parse(Runtime.cli_args(),
    strict: [
      data_dir: :string,
      year: :integer,
      limit_events: :integer,
      semantic_groups: :integer,
      token_budget: :integer,
      timeout_ms: :integer
    ]
  )

Plexus.Examples.AlertSwarm.run(opts)
