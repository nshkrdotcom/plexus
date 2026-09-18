defmodule Plexus.ScheduleTest do
  use ExUnit.Case, async: true

  alias Plexus.{Actor, Graph, Run}
  alias TypeSafeSDK.Test

  test "BSP buffers the same interpreter commands until a barrier" do
    client = Test.client()
    {:ok, run} = Plexus.start_run(id: make_ref(), client: client, schedule: {:bsp, []})
    run_id = Run.run_id(run)

    on_exit(fn ->
      _ = Plexus.stop_run(run)
      Test.close(client)
    end)

    context = %{
      run: run_id,
      run_id: run_id,
      actor_id: :root,
      parent_id: nil,
      class: :test,
      metadata: %{}
    }

    :ok = Actor.dispatch(context, {:edge, :supports, :root, :candidate, 1.0})

    assert Graph.outgoing(run_id, :root, :supports) == []
    assert {:ok, 1, 1} = Plexus.barrier(run)

    assert [%{node: :candidate}] =
             Enum.map(Graph.outgoing(run_id, :root, :supports), &Map.take(&1, [:node]))
  end

  test "bounded async holds a fast actor at k updates ahead of its slow peer" do
    client = Test.client()
    {:ok, run} = Plexus.start_run(client: client, schedule: {:bounded_async, 1})

    on_exit(fn ->
      Plexus.stop_run(run)
      Test.close(client)
    end)

    id = Run.run_id(run)
    for actor <- [:a, :b], do: Graph.put(id, actor, %{status: :active})

    for target <- [:one, :two],
        do: Actor.dispatch(%{run_id: id, actor_id: :a}, {:edge, :supports, :a, target, 1})

    Plexus.barrier(run)
    assert length(Graph.outgoing(id, :a)) == 1
    Actor.dispatch(%{run_id: id, actor_id: :b}, {:edge, :supports, :b, :one, 1})
    Plexus.barrier(run)
    assert length(Graph.outgoing(id, :a)) == 2
  end
end
