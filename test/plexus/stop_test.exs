defmodule Plexus.StopTest do
  use ExUnit.Case, async: true
  alias Plexus.{Graph, Run, Stop}

  test "history predicates use consecutive observations and calibrated confidence is explicit" do
    client = TypeSafeSDK.Test.client()
    {:ok, run} = Plexus.start_run(client: client)

    on_exit(fn ->
      Plexus.stop_run(run)
      TypeSafeSDK.Test.close(client)
    end)

    id = Run.run_id(run)
    refute Stop.no_improvement(2).(id)
    for value <- [0.8, 0.8, 0.8], do: Stop.observe(id, value)
    assert Stop.no_improvement(2).(id)
    assert Stop.stable(0.001, 2).(id)
    Stop.observe(id, 0.9)
    refute Stop.no_improvement(2).(id)
    refute Stop.stable(0.001, 2).(id)
    Graph.put(id, :a, %{calibrated: 0.95})
    assert Stop.confidence(0.9, & &1.calibrated).(id)
    assert Stop.deadline(System.monotonic_time(:millisecond) - 1).(id)
  end

  test "oscillation requires repeated periods" do
    client = TypeSafeSDK.Test.client()
    {:ok, run} = Plexus.start_run(client: client)

    on_exit(fn ->
      Plexus.stop_run(run)
      TypeSafeSDK.Test.close(client)
    end)

    id = Run.run_id(run)
    for value <- [0.2, 0.8, 0.2, 0.8], do: Stop.observe(id, value)
    assert Stop.oscillation(2, 2).(id)
    Stop.observe(id, 0.5)
    refute Stop.oscillation(2, 2).(id)
  end
end
