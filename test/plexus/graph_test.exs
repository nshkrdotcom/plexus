defmodule Plexus.GraphTest do
  use ExUnit.Case, async: false

  alias Plexus.Graph

  test "stores nodes and tracks subtree relationships" do
    run_id = make_ref()
    :ok = Graph.put(run_id, :root, module: RootActor)
    :ok = Graph.put(run_id, :child_a, module: ChildActor)
    :ok = Graph.put(run_id, :child_b, module: ChildActor)
    :ok = Graph.attach_child(run_id, :root, :child_a)
    :ok = Graph.attach_child(run_id, :root, :child_b)

    assert Enum.sort(Graph.children(run_id, :root)) == [:child_a, :child_b]
    assert Enum.sort(Graph.subtree(run_id, :root)) == [:child_a, :child_b, :root]
  end
end
