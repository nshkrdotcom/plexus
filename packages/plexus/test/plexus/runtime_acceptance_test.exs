defmodule Plexus.RuntimeAcceptanceTest.Actor do
  use Plexus.Actor

  @impl true
  def init(args) do
    if args[:complete], do: Plexus.Actor.dispatch(args, {:complete, :done})
    {:ok, args}
  end

  @impl true
  def handle_cast({:commands, commands}, state) do
    Plexus.Actor.dispatch(state, commands)
    {:noreply, state}
  end

  @impl true
  def handle_cast({:hold, owner}, state) do
    send(owner, {:resident_holding, self()})

    receive do
      :release_resident -> :ok
    end

    {:noreply, state}
  end

  @impl true
  def handle_call(:ping, _from, state), do: {:reply, :pong, state}

  @impl true
  def handle_evaluation(_result, _tag, state), do: {:noreply, state}
end

defmodule Plexus.RuntimeAcceptanceTest do
  use ExUnit.Case, async: true

  alias Plexus.{Budget, Graph, Run}
  alias Plexus.Run.Config
  alias Plexus.Schedule.Quiescence
  alias TypeSafeSDK.Test

  defp run(opts \\ []) do
    client = Test.client()
    {:ok, run} = Plexus.start_run(Keyword.merge([id: make_ref(), client: client], opts))

    on_exit(fn ->
      Plexus.stop_run(run)
      Test.close(client)
    end)

    run
  end

  defp birth(run, id, opts \\ []) do
    Run.start_actor(
      run,
      Keyword.merge([module: Plexus.RuntimeAcceptanceTest.Actor, actor_id: id], opts)
    )
  end

  test "concurrent admission obeys max_population even with a larger explicit budget" do
    run = run(max_population: 4, budgets: [population: 100], actor_partitions: 8)
    results = 1..80 |> Task.async_stream(&birth(run, &1), max_concurrency: 80) |> Enum.to_list()
    assert Enum.count(results, &match?({:ok, {:ok, _}}, &1)) == 4
    assert Graph.count(Run.run_id(run)) == 4
    assert Budget.used(Run.config(run).budget, :population) == 4
  end

  test "completion during init retires the actor exactly once" do
    run = run()
    {:ok, _} = birth(run, :early, init_arg: %{complete: true})
    config = Run.config(run)
    assert Graph.get(config.run_id, :early).status == :complete
    assert Quiescence.quiescent?(config.quiescence)
    assert :ok = Run.terminate_actor(run, :early)
    assert Quiescence.get(config.quiescence, :actors) == 0
    assert Budget.used(config.budget, :population) == 0
  end

  test "resident actors stay addressable without preventing idle quiescence" do
    run = run()
    {:ok, pid} = birth(run, :resident, activity_mode: :resident)
    config = Run.config(run)

    assert Process.alive?(pid)
    assert Graph.get(config.run_id, :resident).activity_mode == :resident
    assert Quiescence.get(config.quiescence, :actors) == 0
    assert Quiescence.quiescent?(config.quiescence)

    assert :ok = Run.cast(run, :resident, {:hold, self()})
    assert_receive {:resident_holding, ^pid}, 500
    refute Quiescence.quiescent?(config.quiescence)

    send(pid, :release_resident)
    eventually(fn -> Quiescence.quiescent?(config.quiescence) end)
  end

  test "actor activity mode rejects unsupported values" do
    run = run()

    assert {:error, {:invalid_activity_mode, :unknown}} =
             birth(run, :bad_mode, activity_mode: :unknown)

    assert Graph.get(Run.run_id(run), :bad_mode) == nil
  end

  test "actor ids are isolated and births use all configured partitions" do
    a = run(actor_partitions: 4)
    b = run(actor_partitions: 4)
    for id <- 1..32, do: assert({:ok, _} = birth(a, id))
    assert {:ok, other} = birth(b, 1)
    assert {:ok, first} = Run.actor_pid(a, 1)
    refute first == other
    partitions = Supervisor.which_children(Run.config(a).actor_supervisors)
    assert length(partitions) == 4

    assert Enum.all?(partitions, fn {_, pid, _, _} ->
             DynamicSupervisor.count_children(pid).active > 0
           end)
  end

  test "queued managed messages prevent quiescence after completion" do
    run = run()
    {:ok, pid} = birth(run, :done, init_arg: %{complete: true})
    :sys.suspend(pid)
    Run.cast(run, :done, {:commands, []})
    assert Quiescence.get(Run.config(run).quiescence, :messages) == 1
    refute Quiescence.quiescent?(Run.config(run).quiescence)
    :sys.resume(pid)
    assert :pong = Run.call(run, :done, :ping)
    assert Quiescence.quiescent?(Run.config(run).quiescence)
  end

  test "BSP command envelopes remain in flight until released" do
    run = run(schedule: {:bsp, []})
    Plexus.Actor.dispatch(%{run_id: Run.run_id(run), actor_id: :root}, {:edge, :child, :a, :b, 1})
    assert Quiescence.get(Run.config(run).quiescence, :messages) == 1
    Plexus.barrier(run)
    assert Quiescence.get(Run.config(run).quiescence, :messages) == 0
  end

  test "concurrent graph updates never lose increments" do
    run = run()
    id = Run.run_id(run)
    Graph.put(id, :counter, %{value: 0})

    1..500
    |> Task.async_stream(
      fn _ -> Graph.update(id, :counter, &Map.update!(&1, :value, fn v -> v + 1 end)) end,
      max_concurrency: 40
    )
    |> Stream.run()

    assert Graph.get(id, :counter).value == 500
  end

  @tag capture_log: true
  test "partition loss retires actors and credits without restarting orphan nodes" do
    run = run(actor_partitions: 1)
    {:ok, pid} = birth(run, :victim)
    ref = Process.monitor(pid)
    [{_, partition, _, _}] = Supervisor.which_children(Run.config(run).actor_supervisors)
    Process.exit(partition, :kill)
    assert_receive {:DOWN, ^ref, :process, ^pid, _}
    eventually(fn -> Graph.count(Run.run_id(run)) == 0 end)
    assert Budget.used(Run.config(run).budget, :population) == 0
    assert Quiescence.get(Run.config(run).quiescence, :actors) == 0
  end

  test "prune cancels timers and queued BSP effects before removing an actor" do
    run = run()
    id = Run.run_id(run)
    {:ok, _} = birth(run, :parent)
    Plexus.Actor.dispatch(%{run_id: id, actor_id: :parent}, {:sleep, 60_000})
    assert Quiescence.get(Run.config(run).quiescence, :timers) == 1
    Plexus.schedule(run, {:bsp, []})

    Plexus.Actor.dispatch(
      %{run_id: id, actor_id: :parent},
      {:spawn, :child, Plexus.RuntimeAcceptanceTest.Actor, %{}, actor_id: :late}
    )

    :ok = Run.prune(run, :parent)
    Plexus.barrier(run)
    assert Graph.count(id) == 0
    assert Quiescence.quiescent?(Run.config(run).quiescence)
  end

  test "concurrent completion retires exactly once" do
    run = run()
    {:ok, _} = birth(run, :done)
    context = %{run_id: Run.run_id(run), actor_id: :done}

    1..100
    |> Task.async_stream(fn _ -> Plexus.Actor.dispatch(context, {:complete, :ok}) end)
    |> Stream.run()

    assert Quiescence.get(Run.config(run).quiescence, :actors) == 0
    Run.terminate_actor(run, :done)
    assert Quiescence.get(Run.config(run).quiescence, :actors) == 0
  end

  test "completion racing termination retires each birth exactly once" do
    run = run()
    id = Run.run_id(run)

    for actor <- 1..100 do
      {:ok, _} = birth(run, actor)

      completion =
        Task.async(fn ->
          Plexus.Actor.dispatch(%{run_id: id, actor_id: actor}, {:complete, :ok})
        end)

      Run.terminate_actor(run, actor)
      Task.await(completion)
    end

    assert Graph.count(id) == 0
    assert Quiescence.quiescent?(Run.config(run).quiescence)
    assert Budget.used(Run.config(run).budget, :population) == 0
  end

  @tag capture_log: true
  test "owner death replaces ETS and restarts the resource island without stale directory pointers" do
    run = run()
    id = Run.run_id(run)
    old_table = Run.config(run).tables.nodes
    {:ok, actor} = birth(run, :victim)
    ref = Process.monitor(actor)
    Process.exit(run, :kill)
    assert_receive {:DOWN, ^ref, :process, ^actor, _}, 2_000

    eventually(fn ->
      case Config.fetch(id) do
        {:ok, config} -> config.tables.nodes != old_table
        _ -> false
      end
    end)

    assert :ets.info(old_table) == :undefined
    assert Graph.count(id) == 0
    assert :ok = Plexus.stop_run(id)
  end

  test "an actor can prune itself without waiting for its own callback shutdown" do
    run = run()
    {:ok, pid} = birth(run, :self)
    monitor = Process.monitor(pid)
    Run.cast(run, :self, {:commands, {:prune, :self}})
    assert_receive {:DOWN, ^monitor, :process, ^pid, _}, 500
    eventually(fn -> Graph.count(Run.run_id(run)) == 0 end)
    assert Quiescence.quiescent?(Run.config(run).quiescence)
  end

  defp eventually(fun, attempts \\ 100)
  defp eventually(fun, 0), do: assert(fun.())

  defp eventually(fun, attempts) do
    if fun.(),
      do: :ok,
      else:
        (
          Process.sleep(5)
          eventually(fun, attempts - 1)
        )
  end
end
