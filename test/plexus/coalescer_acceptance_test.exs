defmodule Plexus.CoalescerAcceptanceTest.Probe do
  use Plexus.Actor
  @impl true
  def init(args), do: {:ok, args}
  @impl true
  def handle_cast({:plexus, :measurement, tag, result}, state) do
    send(state.owner, {tag, result})
    {:noreply, state}
  end

  @impl true
  def handle_evaluation(_, _, state), do: {:noreply, state}
end

defmodule Plexus.CoalescerAcceptanceTest do
  use ExUnit.Case, async: true
  alias Plexus.{Actor, Budget, Measure, Record, Run}
  alias Plexus.Schedule.Quiescence
  alias TypeSafeSDK.Test

  defp setup_run(opts \\ [], callback \\ nil) do
    client = Test.client()

    client =
      if callback,
        do: Test.stub_callback(client, callback),
        else: Test.stub(client, flag: {:noul, 0.8})

    {:ok, run} = Plexus.start_run(Keyword.merge([client: client, batch: [delay_ms: 20]], opts))

    on_exit(fn ->
      Plexus.stop_run(run)
      Test.close(client)
    end)

    id = Run.run_id(run)

    for actor <- [:a, :b] do
      {:ok, _} =
        Run.start_actor(run,
          module: Plexus.CoalescerAcceptanceTest.Probe,
          actor_id: actor,
          init_arg: %{owner: self()}
        )
    end

    {run, id, client, TypeSafeSDK.prepare!(flag: TypeSafeSDK.noul("Flag?"))}
  end

  defp submit(id, actor, tag, input, contract, opts \\ []) do
    Actor.dispatch(%{run_id: id, actor_id: actor}, {:measure, tag, input, contract, opts})
  end

  test "task saturation retries without losing or double billing a waiter" do
    {run, id, client, contract} = setup_run(task_limit: 1)

    busy =
      Task.Supervisor.async_nolink(Run.config(run).task_supervisor, fn ->
        receive do
          :release -> :ok
        end
      end)

    submit(id, :a, :result, "state", contract, batch: [delay_ms: 1])
    Process.sleep(30)
    assert Quiescence.get(Run.config(run).quiescence, :measurements) == 1
    send(busy.pid, :release)
    Task.await(busy)
    assert_receive {:result, {:ok, _}}, 2_000
    assert length(Test.requests(client)) == 1
    assert Budget.used(Run.config(run).budget, :measure) == 1
  end

  test "distinct states scatter in one batch and a cache hit avoids transport" do
    {run, id, client, contract} = setup_run()
    submit(id, :a, :first, "one", contract)
    submit(id, :b, :second, "two", contract)
    assert_receive {:first, {:ok, _}}, 2_000
    assert_receive {:second, {:ok, _}}, 2_000

    assert [%{data: %{size: 2}}] =
             Enum.filter(Record.events(id), &(&1.type == :measurement_batch_start))

    submit(id, :a, :cached, "one", contract)
    assert_receive {:cached, {:ok, _}}
    assert length(Test.requests(client)) == 2
    assert Budget.used(Run.config(run).budget, :measure) == 2
  end

  test "replay misses fail closed" do
    {_run, id, client, contract} = setup_run(replay: :replay)
    submit(id, :a, :miss, "missing", contract)
    assert_receive {:miss, {:error, {:replay_miss, _}}}
    assert Test.requests(client) == []
  end

  test "one shared waiter survives cancellation, all-waiter cancellation sets physical token" do
    owner = self()

    callback = fn _ ->
      send(owner, {:transport, self()})

      receive do
        :release -> {:answers, [flag: {:noul, 0.8}]}
      end
    end

    {run, id, _client, contract} = setup_run([], callback)
    submit(id, :a, :a, "same", contract)
    submit(id, :b, :b, "same", contract)
    assert_receive {:transport, worker}, 2_000
    [{_, coalescer, _, _}] = DynamicSupervisor.which_children(Run.config(run).measure_supervisor)
    [batch] = :sys.get_state(coalescer).in_flight |> Map.values()
    Measure.cancel_actor(id, :a)
    refute Pristine.Cancellation.cancelled?(batch.cancellation)
    send(worker, :release)
    assert_receive {:b, {:ok, _}}, 2_000
    refute_receive {:a, _}, 20
    submit(id, :b, :cancel, "different", contract)
    assert_receive {:transport, worker2}, 2_000
    [batch2] = :sys.get_state(coalescer).in_flight |> Map.values()
    Measure.cancel_actor(id, :b)
    assert Pristine.Cancellation.cancelled?(batch2.cancellation)
    monitor = Process.monitor(worker2)
    assert_receive {:DOWN, ^monitor, :process, ^worker2, _}, 2_000
    assert Quiescence.get(Run.config(run).quiescence, :measurements) == 0
  end
end
