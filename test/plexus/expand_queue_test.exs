defmodule Plexus.ExpandQueueTest.Probe do
  use Plexus.Actor
  @impl true
  def init(args), do: {:ok, args}
  @impl true
  def handle_cast({:plexus, :expansion, tag, result}, state) do
    send(state.owner, {tag, result})
    {:noreply, state}
  end

  @impl true
  def handle_evaluation(_, _, state), do: {:noreply, state}
end

defmodule Plexus.ExpandQueueTest.Provider do
  @behaviour Inference.Adapter
  @impl true
  def provider_kind, do: :model_endpoint
  @impl true
  def capabilities(_), do: [Inference.Capability.new(:cancellation, :supported)]
  @impl true
  def complete(client, request) do
    owner = Keyword.fetch!(client.adapter_opts, :owner)
    token = Keyword.fetch!(request.options, :cancellation)
    send(owner, {:started, self(), token})

    receive do
      :finish ->
        {:ok,
         Inference.Response.new(
           usage: %{total_tokens: 7},
           cost: 0.25,
           trace: Inference.Trace.new(duration_ms: 12)
         )}
    end
  end
end

defmodule Plexus.ExpandQueueTest do
  use ExUnit.Case, async: true
  alias Plexus.{Actor, Budget, Record, Run}
  alias Plexus.Expand.Queue
  alias Plexus.Schedule.Quiescence

  test "independent expansion concurrency, queued cancellation and exactly once accounting" do
    client = TypeSafeSDK.Test.client()

    inference =
      Inference.client!(adapter: Plexus.ExpandQueueTest.Provider, adapter_opts: [owner: self()])

    {:ok, run} =
      Plexus.start_run(
        id: make_ref(),
        client: client,
        inference_client: inference,
        task_limit: 1,
        expand_concurrency: 1,
        expand: [required_capabilities: [:cancellation]]
      )

    on_exit(fn ->
      Plexus.stop_run(run)
      TypeSafeSDK.Test.close(client)
    end)

    config = Run.config(run)
    # Exhaust measurement task capacity; expansion must still start.
    occupied =
      Task.Supervisor.async_nolink(config.task_supervisor, fn ->
        receive do
          :release -> :ok
        end
      end)

    for id <- [:a, :b] do
      {:ok, _} =
        Run.start_actor(run,
          module: Plexus.ExpandQueueTest.Probe,
          actor_id: id,
          init_arg: %{owner: self()}
        )

      Actor.dispatch(%{run_id: config.run_id, actor_id: id}, {:expand, id, "expand", []})
    end

    assert_receive {:started, worker, token}
    send(occupied.pid, :release)
    Task.await(occupied)
    refute Pristine.Cancellation.cancelled?(token)
    :ok = Queue.cancel_actor(config.run_id, :b)
    assert_receive {:b, {:error, :cancelled}}
    send(worker, :finish)
    assert_receive {:a, {:ok, %Inference.Response{}}}
    assert Budget.used(config.budget, :tokens) == 7
    assert Budget.used(config.budget, :expand) == 1
    assert Quiescence.get(config.quiescence, :expansions) == 0

    assert [%{data: %{accounting: %{cost: 0.25, duration_ms: 12}}}] =
             Enum.filter(Record.events(config.run_id), &(&1.type == :expansion_stop))
  end

  test "active cancellation sets provider token before returning" do
    client = TypeSafeSDK.Test.client()

    inference =
      Inference.client!(adapter: Plexus.ExpandQueueTest.Provider, adapter_opts: [owner: self()])

    {:ok, run} = Plexus.start_run(id: make_ref(), client: client, inference_client: inference)

    on_exit(fn ->
      Plexus.stop_run(run)
      TypeSafeSDK.Test.close(client)
    end)

    id = Run.run_id(run)

    {:ok, _} =
      Run.start_actor(run,
        module: Plexus.ExpandQueueTest.Probe,
        actor_id: :a,
        init_arg: %{owner: self()}
      )

    Actor.dispatch(%{run_id: id, actor_id: :a}, {:expand, :a, "expand", []})
    assert_receive {:started, worker, token}
    :ok = Queue.cancel_actor(id, :a)
    assert Pristine.Cancellation.cancelled?(token)
    refute Process.alive?(worker)
    assert Quiescence.get(Run.config(run).quiescence, :expansions) == 0
  end
end
