defmodule Plexus.StrategyFanoutTest do
  use ExUnit.Case, async: true

  alias Plexus.Strategy.Fanout
  alias TypeSafeSDK.Test

  test "delegates wide work to TypeSafe batch operations" do
    client = Test.client() |> Test.stub(flag: {:noul, 0.9})
    prepared = TypeSafeSDK.prepare!(flag: TypeSafeSDK.noul("Flag the item?"))

    results = Fanout.many(client, ["one", "two"], prepared, max_concurrency: 2)

    assert length(results) == 2
    assert Enum.all?(results, &match?({:ok, _}, &1))
  end
end
