defmodule Plexus.GraphTest do
  use ExUnit.Case, async: true

  alias Plexus.{Graph, Run}
  alias TypeSafeSDK.Test

  setup do
    client = Test.client()
    {:ok, run} = Plexus.start_run(id: make_ref(), client: client)

    on_exit(fn ->
      _ = Plexus.stop_run(run)
      Test.close(client)
    end)

    {:ok, run: run, run_id: Run.run_id(run)}
  end

  test "stores nodes and tracks typed subtree relationships", %{run_id: run_id} do
    :ok = Graph.put(run_id, :root, module: RootActor, class: :root)
    :ok = Graph.put(run_id, :child_a, module: ChildActor, class: :child)
    :ok = Graph.put(run_id, :child_b, module: ChildActor, class: :child)
    :ok = Graph.attach_child(run_id, :root, :child_a)
    :ok = Graph.attach_child(run_id, :root, :child_b)
    :ok = Graph.add_edge(run_id, :supports, :child_a, :child_b, 0.8, %{source: :test})

    assert Enum.sort(Graph.children(run_id, :root)) == [:child_a, :child_b]
    assert Enum.sort(Graph.subtree(run_id, :root)) == [:child_a, :child_b, :root]
    assert Enum.sort(Enum.map(Plexus.Population.by_class(run_id, :child), &elem(&1, 0))) == [:child_a, :child_b]
    assert [%{node: :child_b, weight: 0.8}] = Enum.map(Graph.outgoing(run_id, :child_a, :supports), &Map.take(&1, [:node, :weight]))
  end
end
