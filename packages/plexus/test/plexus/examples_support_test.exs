Code.require_file("../../examples/support/runtime.exs", __DIR__)

defmodule Plexus.ExamplesSupportTest do
  use ExUnit.Case, async: true

  alias Plexus.Examples.Support.{Data, Runtime}

  Code.require_file("../../examples/support/data.exs", __DIR__)
  Code.require_file("../../examples/support/http.exs", __DIR__)

  alias Plexus.Examples.Support.{Data, HTTP}

  test "JSONL helper streams complete objects" do
    rows = Data.jsonl!("test/fixtures/examples/sample.jsonl") |> Enum.to_list()
    assert Enum.map(rows, & &1["name"]) == ["alpha", "beta"]
  end

  test "CSV helper handles commas and escaped quotes" do
    rows = Data.csv_maps!("test/fixtures/examples/sample.csv") |> Enum.to_list()
    assert Enum.at(rows, 0)["message"] == "quoted, field"
    assert Enum.at(rows, 1)["message"] == "escaped \"quote\""
    assert Enum.at(rows, 2)["message"] == "multi\nline narrative"
  end

  test "path encoder protects package separators for deps.dev" do
    assert HTTP.encode_path("@scope/pkg") == "%40scope%2Fpkg"
  end

  test "JSONL writer preserves UTF-8 dataset text" do
    path =
      Path.join(
        System.tmp_dir!(),
        "plexus-jsonl-unicode-#{System.unique_integer([:positive])}.jsonl"
      )

    on_exit(fn -> File.rm(path) end)

    row = %{
      "text" => "café — naïve Δ",
      "emoji" => "⚡",
      "quoted" => "日本語"
    }

    Data.write_jsonl!(path, [row])

    assert [^row] =
             path
             |> Data.jsonl!()
             |> Enum.to_list()
  end

  test "CLI arguments tolerate a legacy separator" do
    assert Runtime.normalize_cli_args(["--", "--limit", "3"]) ==
             ["--limit", "3"]

    assert Runtime.normalize_cli_args(["--limit", "3"]) ==
             ["--limit", "3"]
  end

  test "balanced CSV sampling does not exhaust the first file" do
    dir =
      Path.join(
        System.tmp_dir!(),
        "plexus-balanced-csv-#{System.unique_integer([:positive])}"
      )

    File.mkdir_p!(dir)
    on_exit(fn -> File.rm_rf!(dir) end)

    first = Path.join(dir, "first.csv")
    second = Path.join(dir, "second.csv")

    File.write!(
      first,
      """
      timestamp,service
      2021-07-01T00:00:01,a
      2021-07-01T00:00:02,a
      2021-07-01T00:00:03,a
      """
    )

    File.write!(
      second,
      """
      timestamp,service
      2021-07-01T00:00:01,b
      2021-07-01T00:00:02,b
      2021-07-01T00:00:03,b
      """
    )

    rows =
      Data.balanced_csv_maps!(
        [first, second],
        ["timestamp"],
        "2021-07-01",
        4
      )

    assert length(rows) == 4
    assert Enum.count(rows, &(&1["service"] == "a")) == 2
    assert Enum.count(rows, &(&1["service"] == "b")) == 2
  end
end
