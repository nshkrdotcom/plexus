defmodule Plexus.ProvenanceTest.Probe do
  use Plexus.Actor

  @impl true
  def init(args), do: {:ok, args}

  @impl true
  def handle_cast({:plexus, :invalidated, upstream_id, epoch}, state) do
    send(state.owner, {:invalidated, state.actor_id, upstream_id, epoch})
    {:noreply, state}
  end

  @impl true
  def handle_evaluation(_, _, state), do: {:noreply, state}
end

defmodule Plexus.ProvenanceTest do
  use ExUnit.Case, async: true
  alias Plexus.{Graph, Provenance, Run}

  test "invalidation queues priority repairs and old epochs cannot clear newer invalidation" do
    client = TypeSafeSDK.Test.client()
    {:ok, run} = Plexus.start_run(client: client)

    on_exit(fn ->
      Plexus.stop_run(run)
      TypeSafeSDK.Test.close(client)
    end)

    id = Run.run_id(run)
    Graph.put(id, :source, %{})
    Graph.put(id, :low, %{repair_priority: 1})
    Graph.put(id, :high, %{repair_priority: 10})
    Provenance.depend(id, :low, :source, %{rule: "example"})
    Provenance.depend(id, :high, :source)
    Provenance.invalidate(id, :source)
    assert {:ok, %{actor_id: :high, epoch: epoch}} = Provenance.next_repair(id)
    assert {:ok, %{actor_id: :low}} = Provenance.next_repair(id)
    assert :empty = Provenance.next_repair(id)
    Provenance.invalidate(id, :source)
    assert {:error, :stale_epoch} = Provenance.repair(id, :high, epoch)
    assert Graph.get(id, :high).stale
    assert :ok = Provenance.repair(id, :high, Graph.get(id, :high).epoch)
    refute Graph.get(id, :high).stale
  end

  test "live invalidation notifies resident dependents and repair clears queued work" do
    client = TypeSafeSDK.Test.client()
    {:ok, run} = Plexus.start_run(client: client)

    on_exit(fn ->
      Plexus.stop_run(run)
      TypeSafeSDK.Test.close(client)
    end)

    id = Run.run_id(run)
    Graph.put(id, :source, %{})

    {:ok, _pid} =
      Run.start_actor(run,
        module: Plexus.ProvenanceTest.Probe,
        actor_id: :dependent,
        activity_mode: :resident,
        init_arg: %{owner: self(), actor_id: :dependent}
      )

    :ok = Provenance.depend(id, :dependent, :source, %{kind: :live_evidence})
    invalidated = Provenance.invalidate(id, :source, notify: true)

    assert :dependent in invalidated
    assert_receive {:invalidated, :dependent, :source, epoch}, 500
    assert Graph.get(id, :dependent).stale
    assert Graph.get(id, :dependent).epoch == epoch

    assert :ok = Provenance.repair(id, :dependent, epoch)
    refute Graph.get(id, :dependent).stale
    assert :empty = Provenance.next_repair(id)
  end

  test "repeated invalidation advances epoch without duplicating a pending repair" do
    client = TypeSafeSDK.Test.client()
    {:ok, run} = Plexus.start_run(client: client)

    on_exit(fn ->
      _ = Plexus.stop_run(run)
      TypeSafeSDK.Test.close(client)
    end)

    run_id = Plexus.Run.run_id(run)

    Plexus.Graph.put(run_id, :source, %{class: :source})
    Plexus.Graph.put(run_id, :derived, %{class: :derived, repair_priority: 10})
    :ok = Plexus.Provenance.depend(run_id, :derived, :source)

    assert [:derived, :source] |> Enum.sort() ==
             Plexus.Provenance.invalidate(run_id, :source) |> Enum.sort()

    first = Plexus.Graph.get(run_id, :derived)
    assert first.stale == true

    first_repairs =
      Plexus.Record.events(run_id)
      |> Enum.count(&(&1.type == :repair_queued and &1.data.actor_id == :derived))

    assert first_repairs == 1

    _ = Plexus.Provenance.invalidate(run_id, :source)

    second = Plexus.Graph.get(run_id, :derived)

    assert second.stale == true
    assert second.epoch == first.epoch + 1

    second_repairs =
      Plexus.Record.events(run_id)
      |> Enum.count(&(&1.type == :repair_queued and &1.data.actor_id == :derived))

    assert second_repairs == 1
  end
end
