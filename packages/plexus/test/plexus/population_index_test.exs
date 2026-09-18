defmodule Plexus.PopulationIndexTest do
  use ExUnit.Case, async: true
  alias Plexus.{Graph, Population, Run}

  test "secondary indexes track updates and filter stale/deleted entries" do
    client = TypeSafeSDK.Test.client()
    {:ok, run} = Plexus.start_run(client: client)

    on_exit(fn ->
      Plexus.stop_run(run)
      TypeSafeSDK.Test.close(client)
    end)

    id = Run.run_id(run)
    Graph.put(id, :a, %{stage: :ready})
    Graph.put(id, :b, %{stage: :waiting})
    Population.index(id, :stage)
    assert [{:a, _}] = Population.lookup(id, :stage, :ready)
    Graph.update(id, :a, &Map.put(&1, :stage, :waiting))
    assert [] == Population.lookup(id, :stage, :ready)
    assert length(Population.lookup(id, :stage, :waiting)) == 2
    Graph.delete_node(id, :a)
    assert [{:b, _}] = Population.lookup(id, :stage, :waiting)
  end
end
