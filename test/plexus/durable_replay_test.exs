defmodule Plexus.DurableReplayTest do
  use ExUnit.Case, async: true
  alias Plexus.{Record, Run}
  alias Plexus.Record.File, as: ReplayFile

  defp run(opts \\ []) do
    client = TypeSafeSDK.Test.client()
    {:ok, run} = Plexus.start_run(Keyword.merge([client: client], opts))

    on_exit(fn ->
      Plexus.stop_run(run)
      TypeSafeSDK.Test.close(client)
    end)

    Run.run_id(run)
  end

  @tag :tmp_dir
  test "durable replay round-trips exact typed results across schedules", %{tmp_dir: dir} do
    source = run()
    target = run(schedule: {:bsp, []}, replay: :replay)
    result = {:ok, %{answer: %TypeSafeSDK.NoulAnswer{noul: 0.75}}}
    Record.replay_put(source, "memo", result)
    path = Path.join(dir, "responses.plexus")
    assert :ok = ReplayFile.write(source, path)
    assert :ok = ReplayFile.load(target, path)
    assert Record.replay_entries(target) == [{"memo", result}]
    envelope = path |> File.read!() |> Jason.decode!()
    File.write!(path, Jason.encode!(Map.put(envelope, "sha256", String.duplicate("0", 64))))
    assert {:error, :checksum_mismatch} = ReplayFile.load(target, path)
  end

  @tag :tmp_dir
  test "incompatible config and executable terms fail without importing entries", %{tmp_dir: dir} do
    source = run()
    target = run(max_depth: 2)
    path = Path.join(dir, "responses.plexus")
    Record.replay_put(source, "memo", {:ok, 1})
    assert :ok = ReplayFile.write(source, path)
    assert {:error, :incompatible_manifest} = ReplayFile.load(target, path)
    assert Record.replay_entries(target) == []
    Record.replay_put(source, "unsafe", self())
    assert {:error, :unsafe_replay_term} = ReplayFile.write(source, path)
  end
end
