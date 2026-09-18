defmodule Plexus.MeasureCoalescerTest.Probe do
  use Plexus.Actor

  alias Plexus.Actor

  @impl true
  def init(%{owner: owner, prepared: prepared, input: input} = args) do
    {:ok, %{owner: owner, prepared: prepared, input: input, context: Actor.context(args)}}
  end

  @impl true
  def handle_cast(:measure, state) do
    Actor.dispatch(state.context, {:measure, :probe, state.input, state.prepared, batch: [delay_ms: 25, max: 64]})
    {:noreply, state}
  end

  def handle_cast({:plexus, :measurement, :probe, result}, state) do
    send(state.owner, {:measurement, state.context.actor_id, result})
    {:noreply, state}
  end

  @impl true
  def handle_evaluation(result, tag, state) do
    send(state.owner, {:direct_evaluation, tag, result})
    {:noreply, state}
  end
end

defmodule Plexus.MeasureCoalescerTest do
  use ExUnit.Case, async: true

  alias TypeSafeSDK.Test

  test "independent actors dedupe an identical state and contract into one request" do
    client = Test.client() |> Test.stub(flag: {:noul, 0.9})
    prepared = TypeSafeSDK.prepare!(flag: TypeSafeSDK.noul("Flag it?"))
    {:ok, run} = Plexus.start_run(id: make_ref(), client: client, batch: [delay_ms: 25, max: 64])

    on_exit(fn ->
      _ = Plexus.stop_run(run)
      Test.close(client)
    end)

    for actor_id <- [:a, :b] do
      {:ok, _pid} =
        Plexus.start_actor(run,
          module: Plexus.MeasureCoalescerTest.Probe,
          actor_id: actor_id,
          class: :probe,
          init_arg: %{owner: self(), prepared: prepared, input: %{same: "state"}}
        )

      :ok = Plexus.cast({run, actor_id}, :measure)
    end

    assert_receive {:measurement, :a, {:ok, _}}, 2_000
    assert_receive {:measurement, :b, {:ok, _}}, 2_000
    assert length(Test.requests(client)) == 1
  end
  test "recorded measurements can be loaded into a fresh replay run without transport calls" do
    prepared = TypeSafeSDK.prepare!(flag: TypeSafeSDK.noul("Flag it?"))
    record_client = Test.client() |> Test.stub(flag: {:noul, 0.9})
    {:ok, record_run} = Plexus.start_run(id: make_ref(), client: record_client, replay: :record, batch: [delay_ms: 1])

    on_exit(fn ->
      _ = Plexus.stop_run(record_run)
      Test.close(record_client)
    end)

    {:ok, _pid} =
      Plexus.start_actor(record_run,
        module: Plexus.MeasureCoalescerTest.Probe,
        actor_id: :record_probe,
        init_arg: %{owner: self(), prepared: prepared, input: %{same: "state"}}
      )

    Plexus.cast({record_run, :record_probe}, :measure)
    assert_receive {:measurement, :record_probe, {:ok, _}}, 2_000
    entries = Plexus.replay_entries(record_run)
    assert length(entries) == 1

    replay_client = Test.client()
    {:ok, replay_run} = Plexus.start_run(id: make_ref(), client: replay_client, replay: :replay)

    on_exit(fn ->
      _ = Plexus.stop_run(replay_run)
      Test.close(replay_client)
    end)

    :ok = Plexus.load_replay(replay_run, entries)

    {:ok, _pid} =
      Plexus.start_actor(replay_run,
        module: Plexus.MeasureCoalescerTest.Probe,
        actor_id: :replay_probe,
        init_arg: %{owner: self(), prepared: prepared, input: %{same: "state"}}
      )

    Plexus.cast({replay_run, :replay_probe}, :measure)
    assert_receive {:measurement, :replay_probe, {:ok, _}}, 2_000
    assert Test.requests(replay_client) == []

  end

end
