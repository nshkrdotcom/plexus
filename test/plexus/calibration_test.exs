defmodule Plexus.CalibrationTest do
  use ExUnit.Case, async: true

  alias Plexus.Belief.Calibration

  test "isotonic fit is monotone and reliability data exposes calibration gaps" do
    samples = [
      {0.1, false},
      {0.2, true},
      {0.3, false},
      {0.7, true},
      {0.8, true},
      {0.9, true}
    ]

    model = Calibration.fit_isotonic(samples)
    calibrated = Enum.map(0..10, &Calibration.apply(model, &1 / 10))
    assert calibrated == Enum.sort(calibrated)

    rows = Calibration.reliability(samples, 5)
    assert Enum.sum(Enum.map(rows, & &1.count)) == length(samples)
    assert Calibration.expected_calibration_error(samples, 5) >= 0.0
  end
end
