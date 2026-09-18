defmodule Plexus.ContractTest do
  use ExUnit.Case, async: true

  alias Plexus.Contract

  test "prepared contracts expose stable fingerprints and memo keys" do
    prepared =
      Contract.new!(
        ticket_type:
          TypeSafeSDK.choice(
            "Classify the ticket.",
            billing: "Billing problem",
            technical: "Technical problem"
          )
      )

    assert "typesafe-prepared-v1:" <> _ = Contract.fingerprint(prepared)
    assert is_binary(Contract.memo_key(%{ticket: "hello"}, prepared))
  end
end
