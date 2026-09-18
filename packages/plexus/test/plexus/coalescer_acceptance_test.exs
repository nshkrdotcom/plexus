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
  def handle_call({:watch_cancel, token}, _from, state),
    do: {:reply, :ok, Map.put(state, :token, token)}

  @impl true
  def terminate(_reason, %{token: token} = state) do
    send(
      state.owner,
      {:terminating, state.actor_id, Pristine.Cancellation.cancelled?(token),
       not is_nil(Plexus.Graph.get(state.run_id, state.actor_id))}
    )
  end

  def terminate(_reason, _state), do: :ok

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

  test "cache and replay remain usable with exhausted measurement credit" do
    {run, id, client, contract} = setup_run(budgets: [measure: 1])
    submit(id, :a, :original, "one", contract)
    assert_receive {:original, {:ok, result}}, 2_000
    submit(id, :a, :cached, "one", contract)
    assert_receive {:cached, {:ok, ^result}}
    submit(id, :a, :denied, "two", contract)
    assert_receive {:denied, {:error, {:budget_exhausted, :measure}}}
    assert length(Test.requests(client)) == 1
    assert Budget.used(Run.config(run).budget, :measure) == 1

    {_replay, replay_id, replay_client, _} = setup_run(replay: :replay, budgets: [measure: 0])
    Record.load_replay(replay_id, [{Plexus.Contract.memo_key("one", contract), {:ok, result}}])
    submit(replay_id, :a, :replayed, "one", contract)
    assert_receive {:replayed, {:ok, ^result}}
    submit(replay_id, :a, :missing, "missing", contract)
    assert_receive {:missing, {:error, {:replay_miss, _}}}
    assert Test.requests(replay_client) == []
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

  test "evaluation options separate batches and registered batch limits override run defaults" do
    {run, id, _client, contract} = setup_run(batch: [delay_ms: 1_000, max: 64])
    Plexus.register_contract(run, :small, contract, batch: [max: 1, delay_ms: 0])
    submit(id, :a, :one, "one", :small, model: "model-a")
    submit(id, :b, :two, "two", :small, model: "model-b")
    assert_receive {:one, {:ok, _}}, 2_000
    assert_receive {:two, {:ok, _}}, 2_000
    assert length(DynamicSupervisor.which_children(Run.config(run).measure_supervisor)) == 2

    assert Enum.all?(
             Enum.filter(Record.events(id), &(&1.type == :measurement_batch_start)),
             &(&1.data.size == 1)
           )
  end

  test "per-item failures scatter to the corresponding waiter" do
    callback = fn request ->
      if String.contains?(IO.iodata_to_binary(request.body), "bad-state"),
        do: {:transport_error, :closed},
        else: {:answers, [flag: {:noul, 0.8}]}
    end

    {_run, id, _client, contract} = setup_run([], callback)
    submit(id, :a, :bad, "bad-state", contract)
    submit(id, :b, :good, "good-state", contract)
    assert_receive {:bad, {:error, _}}, 2_000
    assert_receive {:good, {:ok, response}}, 2_000
    assert TypeSafeSDK.Response.values(response).flag == 0.8
  end

  test "run teardown cancels in-flight physical tokens and transport workers" do
    owner = self()

    callback = fn _ ->
      send(owner, {:blocked, self()})

      receive do
        :release -> {:answers, [flag: {:noul, 0.8}]}
      end
    end

    {run, id, _client, contract} = setup_run([], callback)
    submit(id, :a, :work, "one", contract)
    assert_receive {:blocked, worker}, 2_000
    [{_, coalescer, _, _}] = DynamicSupervisor.which_children(Run.config(run).measure_supervisor)
    [batch] = :sys.get_state(coalescer).in_flight |> Map.values()
    ref = Process.monitor(worker)
    Plexus.stop_run(run)
    assert Pristine.Cancellation.cancelled?(batch.cancellation)
    assert_receive {:DOWN, ^ref, :process, ^worker, _}, 2_000
  end

  test "cache reuse never crosses evaluation models" do
    {_run, id, client, contract} = setup_run()
    submit(id, :a, :first_model, "same", contract, model: "model-a")
    assert_receive {:first_model, {:ok, _}}, 2_000
    submit(id, :a, :second_model, "same", contract, model: "model-b")
    assert_receive {:second_model, {:ok, _}}, 2_000
    assert length(Test.requests(client)) == 2
  end

  test "subtree pruning cancels before termination and unlink while an unrelated batch survives" do
    owner = self()

    callback = fn request ->
      label =
        if String.contains?(IO.iodata_to_binary(request.body), "pruned"),
          do: :pruned,
          else: :survivor

      send(owner, {:blocked, label, self()})

      receive do
        :release -> {:answers, [flag: {:noul, 0.8}]}
      end
    end

    {run, id, _client, contract} = setup_run([], callback)

    {:ok, child} =
      Run.start_actor(run,
        module: Plexus.CoalescerAcceptanceTest.Probe,
        actor_id: :child,
        parent_id: :a,
        init_arg: %{owner: self()}
      )

    {:ok, parent} = Run.actor_pid(run, :a)
    submit(id, :a, :a, "pruned", contract)
    submit(id, :child, :child, "pruned", contract)
    submit(id, :b, :survivor, "survivor", contract, model: "other")
    assert_receive {:blocked, :pruned, _}, 2_000
    assert_receive {:blocked, :survivor, survivor_worker}, 2_000

    batches =
      for {_, pid, _, _} <- DynamicSupervisor.which_children(Run.config(run).measure_supervisor),
          batch <- Map.values(:sys.get_state(pid).in_flight),
          do: batch

    doomed = Enum.find(batches, &(hd(&1.entries).state == "pruned"))
    survivor = Enum.find(batches, &(hd(&1.entries).state == "survivor"))
    Run.call(run, :a, {:watch_cancel, doomed.cancellation})
    Run.call(run, :child, {:watch_cancel, doomed.cancellation})
    Run.prune(run, :a)
    assert_receive {:terminating, :child, true, true}
    assert_receive {:terminating, :a, true, true}
    refute Process.alive?(parent)
    refute Process.alive?(child)
    assert Plexus.Graph.count(id) == 1
    assert Plexus.Graph.incoming(id, :child) == []
    refute Pristine.Cancellation.cancelled?(survivor.cancellation)
    assert Process.alive?(survivor_worker)
    assert Budget.used(Run.config(run).budget, :population) == 1
    send(survivor_worker, :release)
    assert_receive {:survivor, {:ok, _}}, 2_000
    Actor.dispatch(%{run_id: id, actor_id: :b}, {:complete, :done})
    {:ok, survivor_pid} = Run.actor_pid(run, :b)
    :sys.get_state(survivor_pid)
    assert Quiescence.quiescent?(Run.config(run).quiescence)
  end
end
