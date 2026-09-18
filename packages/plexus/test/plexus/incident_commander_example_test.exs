Code.require_file("../../examples/support/runtime.exs", __DIR__)
Code.require_file("../../examples/02_incident_commander/application.exs", __DIR__)

defmodule Plexus.IncidentCommanderExampleTest do
  use ExUnit.Case, async: true

  alias Plexus.Budget.Accounts
  alias Plexus.Examples.IncidentCommander
  alias Plexus.Examples.IncidentCommander.Policy
  alias Plexus.{Graph, Run}
  alias TypeSafeSDK.Test

  test "semantic decisions grow the hypothesis population without a depth barrier" do
    client =
      Test.client()
      |> Test.stub_callback(fn request ->
        state = request.body |> Jason.decode!() |> Map.fetch!("state")

        case state["candidate_root_service"] do
          "frontend" ->
            {:answers,
             [
               root_cause: {:noul, 0.22},
               failure_mode: {:choice, "request_path_error", 0.95},
               evidence_strength: {:score, 2, 0.91},
               next_action: {:choice, "investigate_callers", 0.97}
             ]}

          "gateway" ->
            {:answers,
             [
               root_cause: {:noul, 0.93},
               failure_mode: {:choice, "resource_exhaustion", 0.96},
               evidence_strength: {:score, 3, 0.94},
               next_action: {:choice, "stop", 0.99}
             ]}
        end
      end)

    {:ok, run} =
      Plexus.start_run(
        id: make_ref(),
        client: client,
        max_population: 20,
        max_depth: 3,
        budgets: [measure: 4, population: 20, tokens: 100_000],
        batch: [delay_ms: 1, max: 16]
      )

    on_exit(fn ->
      _ = Plexus.stop_run(run)
      Test.close(client)
    end)

    evidence = %{
      "frontend" => evidence_row(30, 10, [], [%{service: "gateway", count: 21}]),
      "gateway" => evidence_row(12, 7, [%{service: "frontend", count: 21}], [])
    }

    summary =
      IncidentCommander.investigate(
        run,
        evidence,
        %{{"gateway", "frontend"} => 21},
        seed_services: 1,
        max_hypotheses: 4,
        max_depth: 2,
        branch_width: 1,
        timeout_ms: 3_000
      )

    run_id = Run.run_id(run)
    seed = {:hypothesis, "frontend", ["frontend"]}
    child = {:hypothesis, "frontend", ["frontend", "gateway"]}

    assert summary.seed_hypotheses == 1
    assert summary.hypotheses_spawned == 2
    assert summary.dynamic_hypotheses == 1
    assert summary.completed_hypotheses == 2
    assert summary.max_investigation_depth == 1
    assert summary.hypothesis_credits_used == 2
    assert summary.hypothesis_credits_remaining == 2
    assert length(Test.requests(client)) == 2

    assert Graph.get(run_id, child).parent == seed
    assert Graph.get(run_id, seed).result.spawned_targets == ["gateway"]
    assert to_string(Graph.get(run_id, child).result.next_action) == "stop"

    assert [%{node: {:evidence, "frontend"}}] =
             Graph.outgoing(run_id, seed, :consults)

    assert Accounts.snapshot(run_id)[IncidentCommander.hypothesis_pool()].meters.population.used ==
             2
  end

  test "shared hypothesis credits stop actor-driven expansion without a known completion count" do
    client =
      Test.client()
      |> Test.stub_callback(fn _request ->
        {:answers,
         [
           root_cause: {:noul, 0.2},
           failure_mode: {:choice, "dependency_failure", 0.9},
           evidence_strength: {:score, 1, 0.9},
           next_action: {:choice, "investigate_callers", 0.99}
         ]}
      end)

    {:ok, run} =
      Plexus.start_run(
        id: make_ref(),
        client: client,
        max_population: 20,
        max_depth: 3,
        budgets: [measure: 2, population: 20, tokens: 100_000],
        batch: [delay_ms: 1, max: 16]
      )

    on_exit(fn ->
      _ = Plexus.stop_run(run)
      Test.close(client)
    end)

    evidence = %{
      "frontend" =>
        evidence_row(
          10,
          2,
          [],
          [%{service: "gateway", count: 20}, %{service: "auth", count: 18}]
        ),
      "gateway" => evidence_row(4, 1, [], []),
      "auth" => evidence_row(3, 1, [], [])
    }

    summary =
      IncidentCommander.investigate(run, evidence, %{},
        seed_services: 1,
        max_hypotheses: 1,
        max_depth: 3,
        branch_width: 2,
        timeout_ms: 3_000
      )

    assert summary.hypotheses_spawned == 1
    assert summary.dynamic_hypotheses == 0
    assert summary.hypothesis_credits_used == 1
    assert summary.hypothesis_credits_remaining == 0
    assert length(Test.requests(client)) == 1
  end

  test "investigation policy respects direction, visited services and the available universe" do
    evidence = %{
      called_by: [
        %{service: "gateway", count: 12},
        %{service: "frontend", count: 9},
        %{service: "outside", count: 20}
      ],
      calls: [%{service: "db", count: 7}]
    }

    assert Policy.next_targets(
             "investigate_both",
             evidence,
             MapSet.new(["frontend"]),
             2,
             MapSet.new(["frontend", "gateway", "db"])
           ) == [
             %{service: "gateway", count: 12, direction: :caller},
             %{service: "db", count: 7, direction: :callee}
           ]
  end

  test "the GAIA reference application has no orchestrator completion barrier" do
    run_source = File.read!("examples/02_incident_commander/run.exs")
    app_source = File.read!("examples/02_incident_commander/application.exs")

    refute run_source =~ "await_class_complete!"
    refute run_source =~ "await_actor_ids_complete!"
    refute app_source =~ "await_class_complete!"
    refute app_source =~ "await_actor_ids_complete!"
    assert app_source =~ "Runtime.await_quiescent!"
    assert app_source =~ "Accounts.reserve"
    assert app_source =~ "{:spawn, :hypothesis"
  end

  defp evidence_row(trace_failures, log_signals, calls, called_by) do
    %{
      trace_failures: trace_failures,
      log_signals: log_signals,
      trace_examples: [],
      log_examples: [],
      calls: calls,
      called_by: called_by
    }
  end
end
