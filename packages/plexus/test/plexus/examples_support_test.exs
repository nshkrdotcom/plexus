defmodule Plexus.ExamplesSupportTest do
  use ExUnit.Case, async: true

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
end
