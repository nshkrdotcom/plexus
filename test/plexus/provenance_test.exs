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
end
