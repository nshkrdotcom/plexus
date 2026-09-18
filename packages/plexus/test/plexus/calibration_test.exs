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

  test "equal probabilities are pooled before isotonic fitting" do
    model = Calibration.fit_isotonic([{0.5, false}, {0.5, true}])
    assert Calibration.apply(model, 0.5) == 0.5
  end

  test "proper scoring rules report exact Brier and clipped log loss" do
    samples = [{0.25, false}, {0.75, true}]
    assert_in_delta Calibration.brier_score(samples), 0.0625, 1.0e-12
    assert_in_delta Calibration.log_loss(samples), -:math.log(0.75), 1.0e-12
    assert Calibration.log_loss([{0.0, true}]) > 20
  end
end
