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
end
