alias Plexus.Belief.Calibration

# Labels come from integer arithmetic; no external dataset/license/dependency.
prime? = fn n -> Enum.all?(2..trunc(:math.sqrt(n)), &(rem(n, &1) != 0)) end
rank = fn n -> :crypto.hash(:sha256, "plexus-primality-v1:#{n}") end

rows =
  101..999
  |> Enum.group_by(prime?)
  |> Enum.flat_map(fn {label, numbers} ->
    numbers
    |> Enum.sort_by(rank)
    |> Enum.take(30)
    |> Enum.with_index()
    |> Enum.map(fn {n, index} ->
      %{id: n, label: label, split: if(index < 15, do: "calibration", else: "held_out")}
    end)
  end)
  |> Enum.sort_by(& &1.id)

questions = [
  prime: "Is the integer in the state a prime number?",
  divisors: "Does this integer have exactly two positive divisors, one and itself?",
  composite: "Is this integer greater than one and not composite?"
]

prepared =
  TypeSafeSDK.prepare!(Enum.map(questions, fn {key, text} -> {key, TypeSafeSDK.noul(text)} end))

client =
  TypeSafeSDK.Client.new(
    api_key: System.fetch_env!("TYPESAFE_API_KEY"),
    retry: false,
    timeout_ms: 30_000
  )

started = System.monotonic_time(:millisecond)

results =
  TypeSafeSDK.evaluate_many(client, Enum.map(rows, &%{integer: &1.id}), prepared,
    max_concurrency: 4,
    ordered: true,
    on_error: :collect
  )

observations =
  Enum.zip(rows, results)
  |> Enum.map(fn
    {row, {:ok, response}} -> Map.put(row, :probabilities, TypeSafeSDK.Response.values(response))
    {row, {:error, error}} -> Map.put(row, :error, inspect(error))
  end)

samples = fn data, split, phrasing ->
  data
  |> Enum.filter(&(&1.split == split and Map.has_key?(&1, :probabilities)))
  |> Enum.flat_map(fn row ->
    keys = if phrasing == :pooled, do: Keyword.keys(questions), else: [phrasing]
    Enum.map(keys, &{Map.fetch!(row.probabilities, &1), row.label})
  end)
end

metrics = fn values ->
  %{
    n: length(values),
    ece: Calibration.expected_calibration_error(values, 5),
    brier: Calibration.brier_score(values),
    log_loss: Calibration.log_loss(values),
    reliability: Calibration.reliability(values, 5)
  }
end

reports =
  Map.new(Keyword.keys(questions) ++ [:pooled], fn phrasing ->
    fit = samples.(observations, "calibration", phrasing)
    held_out = samples.(observations, "held_out", phrasing)
    model = Calibration.fit_isotonic(fit)
    calibrated = Enum.map(held_out, fn {p, y} -> {Calibration.apply(model, p), y} end)

    {phrasing,
     %{
       fit_n: length(fit),
       model: model,
       raw: metrics.(held_out),
       calibrated: metrics.(calibrated)
     }}
  end)

report = %{
  experiment: "primality-v1",
  source: "live TypeSafeSDK",
  questions: Map.new(questions),
  fingerprint: Plexus.Contract.fingerprint(prepared),
  dataset: rows,
  observations: observations,
  reports: reports,
  elapsed_ms: System.monotonic_time(:millisecond) - started,
  decision:
    "Use rank/monotone aggregation; this small arithmetic experiment does not justify Bayesian graph multiplication or calibration transfer to other claims.",
  limitations:
    "Balanced case-control sample; 30 fit and 30 held-out integers. Pooled phrasing observations are correlated, not independent samples. No claims of population-wide calibration."
}

File.mkdir_p!("artifacts/calibration")
File.write!("artifacts/calibration/report.json", Jason.encode!(report, pretty: true))

IO.puts(
  Jason.encode!(
    %{
      elapsed_ms: report.elapsed_ms,
      successful: Enum.count(observations, &Map.has_key?(&1, :probabilities)),
      reports: reports
    },
    pretty: true
  )
)
