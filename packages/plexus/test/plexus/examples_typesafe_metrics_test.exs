Code.require_file("../../examples/support/typesafe_metrics.exs", __DIR__)

defmodule Plexus.Examples.TypeSafeMetricsTest do
  use ExUnit.Case, async: false

  alias Plexus.Examples.Support.TypeSafeMetrics

  setup do
    TypeSafeMetrics.ensure_started(print_at_exit: false)
    TypeSafeMetrics.reset!()

    on_exit(fn ->
      TypeSafeMetrics.reset!()
    end)

    :ok
  end

  test "collects privacy-safe live response evidence from TypeSafe telemetry" do
    TypeSafeMetrics.register_client(%{
      base_url: "https://api.typesafe.ai",
      default_model: "jev-latest",
      transport: Pristine.Adapters.Transport.Finch
    })

    caller = %{
      plexus_run_id: :example_run,
      plexus_contract_fingerprint: "typesafe-prepared-v1:test"
    }

    :telemetry.execute(
      [:typesafe_sdk, :evaluate, :start],
      %{system_time: 1},
      %{
        caller: caller,
        requested_model: "jev-latest",
        question_count: 2
      }
    )

    :telemetry.execute(
      [:typesafe_sdk, :evaluate, :stop],
      %{
        duration: 5,
        input_tokens: 857,
        output_tokens: 70
      },
      %{
        caller: caller,
        outcome: :ok,
        status: 200,
        request_id: "req_test_live",
        model: "jev-1.13.0",
        retries: 0
      }
    )

    summary = TypeSafeMetrics.snapshot()

    assert summary.starts == 1
    assert summary.stops == 1
    assert summary.exceptions == 0
    assert summary.confirmed_http_responses == 1
    assert summary.http_successes == 1
    assert summary.http_errors == 0
    assert summary.input_tokens == 857
    assert summary.output_tokens == 70
    assert summary.statuses == %{200 => 1}
    assert summary.models == %{"jev-1.13.0" => 1}

    assert [%{request_id: "req_test_live", status: 200}] =
             summary.requests

    assert length(summary.runs) == 1
    assert length(summary.contracts) == 1

    inspected = inspect(summary)

    refute inspected =~ "authorization"
    refute inspected =~ "api_key"
    refute inspected =~ "semantic state"
  end

  test "tracks exceptions without inventing an HTTP response" do
    :telemetry.execute(
      [:typesafe_sdk, :evaluate, :exception],
      %{duration: 10},
      %{
        caller: %{plexus_run_id: :failed_run},
        outcome: :exception,
        kind: :error
      }
    )

    summary = TypeSafeMetrics.snapshot()

    assert summary.exceptions == 1
    assert summary.confirmed_http_responses == 0
    assert summary.requests == []
  end
end
