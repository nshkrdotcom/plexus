defmodule Plexus.BeliefTest do
  use ExUnit.Case, async: true
  alias Plexus.Belief

  test "mixing raw nouls does not label their aggregate calibrated" do
    a = Belief.from_answer(%TypeSafeSDK.NoulAnswer{noul: 0.2})
    b = Belief.from_answer(%TypeSafeSDK.NoulAnswer{noul: 0.8})
    assert Belief.combine(a, b).value == 0.5
    assert Belief.combine(a, b).calibrated == nil
  end
end
