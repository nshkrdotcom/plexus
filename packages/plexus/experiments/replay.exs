defmodule Plexus.Experiment.ReplayActor do
  use Plexus.Actor
  alias Plexus.{Actor, Belief}
  @impl true
  def init(args), do: {:ok, args}
  @impl true
  def handle_cast(:start, state) do
    Actor.dispatch(state, {:measure, :prime, %{integer: state.integer}, state.contract, []})
    {:noreply, state}
  end

  def handle_cast({:plexus, :measurement, :prime, {:ok, response}}, state) do
    belief = Belief.from(response, :prime)
    Actor.dispatch(state, [{:belief, belief}, {:complete, belief.value}])
    {:noreply, state}
  end

  def handle_cast({:plexus, :measurement, _, {:error, error}}, _state), do: raise(inspect(error))
  def handle_cast(_, state), do: {:noreply, state}
  @impl true
  def handle_evaluation(_, _, state), do: {:noreply, state}
end

defmodule Plexus.Experiment.Replay do
  alias Plexus.{Graph, Record, Run}
  alias Plexus.Record.File, as: ReplayFile
  alias Plexus.Schedule.Quiescence

  def execute(client, mode, regime, path) do
    prepared =
      TypeSafeSDK.prepare!(prime: TypeSafeSDK.noul("Is the integer in the state a prime number?"))

    integers = [101, 102, 107, 111, 127, 143, 149, 169]

    {:ok, run} =
      Plexus.start_run(
        client: client,
        replay: mode,
        schedule: regime,
        replay_identity: %{strategy: "primality-independent-v1", integers: integers},
        actor_partitions: 4
      )

    id = Run.run_id(run)
    Plexus.register_contract(run, :prime, prepared)
    if mode == :replay, do: :ok = ReplayFile.load(id, path)

    for n <- integers do
      {:ok, _} =
        Run.start_actor(run,
          module: Plexus.Experiment.ReplayActor,
          actor_id: n,
          init_arg: %{integer: n, contract: prepared}
        )
    end

    for {a, b} <- Enum.zip(integers, tl(integers)), do: Graph.add_edge(id, :neighbor, a, b)
    {runtime_before, _} = :erlang.statistics(:runtime)
    started = System.monotonic_time(:microsecond)
    for n <- integers, do: Run.cast(run, n, :start)
    curves = await(run, regime, started, [], 1_000)
    elapsed = System.monotonic_time(:microsecond) - started
    {runtime_after, _} = :erlang.statistics(:runtime)
    if mode == :record, do: :ok = ReplayFile.write(id, path)
    events = Record.events(id)

    summary = %{
      regime: inspect(regime),
      elapsed_us: elapsed,
      cpu_ms: runtime_after - runtime_before,
      curves: curves,
      results:
        Graph.nodes(id)
        |> Enum.sort()
        |> Enum.map(fn {id, node} -> %{id: id, value: node.result} end),
      event_order:
        Enum.map(events, fn event ->
          %{sequence: event.sequence, type: inspect(event.type), actor_id: event.data[:actor_id]}
        end),
      reused: Enum.count(events, &(&1.type == :measurement_reused)),
      physical_batches: Enum.count(events, &(&1.type == :measurement_batch_start)),
      quiescent: true
    }

    Plexus.stop_run(run)
    summary
  end

  defp await(_run, _regime, _started, _curves, 0), do: raise("experiment did not terminate")

  defp await(run, regime, started, curves, remaining) do
    # Synchronize actor mailbox dispatch before the BSP barrier.
    for {id, _} <- Graph.nodes(Run.run_id(run)), do: Run.call(run, id, :sync)
    round = if regime == :async, do: nil, else: elem(Plexus.barrier(run), 1)
    nodes = Graph.nodes(Run.run_id(run))

    values =
      Enum.flat_map(nodes, fn {_id, node} ->
        if node[:belief], do: [node.belief.value], else: []
      end)

    point = %{
      round: round,
      elapsed_us: System.monotonic_time(:microsecond) - started,
      completed: Enum.count(nodes, fn {_, node} -> node.status == :complete end),
      observed: length(values),
      mean_score: if(values == [], do: nil, else: Enum.sum(values) / length(values))
    }

    curves = [point | curves]

    if Quiescence.quiescent?(Run.config(run).quiescence) do
      Enum.reverse(curves)
    else
      Process.sleep(1)
      await(run, regime, started, curves, remaining - 1)
    end
  end
end

# Safe replay decoding does not create atoms. Load the response schema before
# importing a file into a fresh VM that has never made a live SDK request.
for app <- [:typesafe_sdk, :pristine],
    module <- Application.spec(app, :modules),
    do: Code.ensure_loaded!(module)

File.mkdir_p!("artifacts/replay")
path = "artifacts/replay/responses.plexus"

record =
  if "--replay-only" in System.argv() do
    "artifacts/replay/report.json" |> File.read!() |> Jason.decode!() |> Map.fetch!("record")
  else
    live = TypeSafeSDK.Client.new(api_key: System.fetch_env!("TYPESAFE_API_KEY"), retry: false)

    Plexus.Experiment.Replay.execute(live, :record, :async, path)
    |> Jason.encode!()
    |> Jason.decode!()
  end

replays =
  for regime <- [:async, {:bsp, []}] do
    client = TypeSafeSDK.Test.client()
    result = Plexus.Experiment.Replay.execute(client, :replay, regime, path)
    true = TypeSafeSDK.Test.requests(client) == []
    true = Jason.decode!(Jason.encode!(result.results)) == record["results"]
    TypeSafeSDK.Test.close(client)
    Map.put(result, :transport_requests, 0)
  end

File.write!(
  "artifacts/replay/report.json",
  Jason.encode!(
    %{
      record: record,
      replays: replays,
      interpretation:
        "Identical fixed semantic responses and final values. This independent-node strategy establishes a controlled replay baseline, not an iterative convergence advantage."
    },
    pretty: true
  )
)

IO.puts("Async and BSP replay matched the eight recorded live results with zero requests.")
