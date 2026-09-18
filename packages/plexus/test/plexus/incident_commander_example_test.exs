Code.require_file("../../examples/support/runtime.exs", __DIR__)
Code.require_file("../../examples/support/data.exs", __DIR__)
Code.require_file("../../examples/support/metrics.exs", __DIR__)
Code.require_file("../../examples/02_incident_commander/chronology.exs", __DIR__)
Code.require_file("../../examples/02_incident_commander/topology.exs", __DIR__)
Code.require_file("../../examples/02_incident_commander/progress.exs", __DIR__)
Code.require_file("../../examples/02_incident_commander/application.exs", __DIR__)

defmodule Plexus.IncidentCommanderExampleTest do
  use ExUnit.Case, async: true

  alias Plexus.Budget.Accounts
  alias Plexus.Examples.IncidentCommander
  alias Plexus.Examples.IncidentCommander.{Chronology, Hypothesis, Replay, Topology}
  alias Plexus.Examples.Support.Runtime
  alias Plexus.{Graph, Run}
  alias Plexus.Schedule.Quiescence
  alias TypeSafeSDK.Test

  @fixture_dir Path.expand("../fixtures/examples/gaia_living", __DIR__)

  test "chronology lazily merges raw trace and business rows by event time" do
    cursor =
      Chronology.open!(
        [Path.join(@fixture_dir, "trace.csv")],
        [Path.join(@fixture_dir, "business.csv")],
        day: "2021-07-01"
      )

    {events, cursor} = take_events(cursor, 4, [])

    assert Enum.map(events, & &1.source) == [:trace, :business, :trace, :business]

    assert Enum.map(events, & &1.service) == [
             "frontend",
             "redisservice1",
             "gateway",
             "frontend"
           ]

    assert :eof = Chronology.next(cursor)
  end

  @tag :date_only_business_timestamp
  test "business day column uses the log message timestamp for event chronology" do
    cursor =
      Chronology.open!(
        [],
        [Path.join(@fixture_dir, "business_date_only.csv")],
        day: "2021-07-01"
      )

    assert {:ok, first, cursor} = Chronology.next(cursor)
    assert first.service == "redisservice1"
    assert first.timestamp == "2021-07-01 00:00:02.123"
    assert first.event_time_us == 1_625_097_602_123_000

    assert {:ok, second, cursor} = Chronology.next(cursor)
    assert second.service == "frontend"
    assert second.timestamp == "2021-07-01 00:00:04.456"
    assert second.event_time_us == 1_625_097_604_456_000

    assert :eof = Chronology.next(cursor)
  end

  @tag :replay_failure_propagation
  test "replay process death before a terminal result is fatal" do
    parent = self()

    replay =
      spawn(fn ->
        send(parent, {:replay_ready, self()})

        receive do
          :crash -> exit(:fixture_replay_crash)
        end
      end)

    assert_receive {:replay_ready, ^replay}

    spawn(fn ->
      Process.sleep(20)
      send(replay, :crash)
    end)

    assert_raise RuntimeError,
                 ~r/replay actor terminated before completion.*fixture_replay_crash/,
                 fn ->
                   IncidentCommander.await_replay_terminal!(replay, 1_000)
                 end
  end

  @tag :semantic_invalidation_source
  test "semantic service-state changes invalidate already-live dependent hypotheses" do
    signal_count = :atomics.new(1, signed: false)

    client =
      Test.client()
      |> Test.stub_callback(fn request ->
        state = request.body |> Jason.decode!() |> Map.fetch!("state")

        cond do
          Map.has_key?(state, "challenger") ->
            {:answers,
             [
               survives_challenge: {:noul, 0.9},
               disposition: {:choice, "stand", 0.98}
             ]}

          Map.has_key?(state, "hypothesis_service") ->
            {:answers,
             [
               root_cause: {:noul, 0.72},
               next_action: {:choice, "observe", 0.96},
               evidence_strength: {:score, 3, 0.94}
             ]}

          true ->
            n = :atomics.add_get(signal_count, 1, 1)
            mode = if n == 1, do: "request_path_error", else: "network"

            {:answers,
             [
               anomaly_relevance: {:noul, 0.95},
               failure_mode: {:choice, mode, 0.97},
               diagnostic_strength: {:score, if(n == 1, do: 2, else: 3), 0.95}
             ]}
        end
      end)

    {:ok, run} =
      Plexus.start_run(
        id: make_ref(),
        client: client,
        max_population: 100,
        max_depth: 6,
        budgets: [measure: 100, expand: 100, population: 100, tokens: 1_000_000],
        batch: [delay_ms: 1, max: 16, max_concurrency: 4]
      )

    topology = Topology.new()

    on_exit(fn ->
      Topology.close(topology)
      _ = Plexus.stop_run(run)
      Test.close(client)
    end)

    IncidentCommander.register_contracts!(run)
    IncidentCommander.prepare_indexes!(run)

    opts = [
      topology: topology,
      signal_every: 1,
      hypothesis_update_every: 1,
      trigger_probability: 0.5,
      branch_width: 1,
      branch_credits: 0,
      peer_challenges: 0,
      max_hypotheses: 50
    ]

    {:ok, _} = IncidentCommander.ensure_service(run, "frontend", opts)

    assert :ok =
             Run.cast(run, {:service, "frontend"}, {:telemetry, event(:trace, 1, "frontend")})

    eventually(fn -> hypothesis_nodes(run) != [] end)

    assert :ok =
             Run.cast(run, {:service, "frontend"}, {:telemetry, event(:trace, 2, "frontend")})

    eventually(fn ->
      Enum.any?(Plexus.Record.events(Run.run_id(run)), fn event ->
        event.type == :gaia_hypothesis_invalidated
      end)
    end)

    assert IncidentCommander.acceptance_evidence(run).invalidations >= 1
  end

  test "child hypothesis identity is owned by the parent branch" do
    left =
      IncidentCommander.child_hypothesis_id(
        {:hypothesis, "incident-a", "frontend", "request_path_error"},
        "incident-a",
        "redisservice1",
        "dependency_failure"
      )

    same_left =
      IncidentCommander.child_hypothesis_id(
        {:hypothesis, "incident-a", "frontend", "request_path_error"},
        "incident-a",
        "redisservice1",
        "dependency_failure"
      )

    right =
      IncidentCommander.child_hypothesis_id(
        {:hypothesis, "incident-a", "gateway", "request_path_error"},
        "incident-a",
        "redisservice1",
        "dependency_failure"
      )

    assert left == same_left
    refute left == right
  end

  test "late parent spans create live service-call topology when the parent arrives" do
    client = Test.client()
    {:ok, run} = Plexus.start_run(client: client)
    topology = Topology.new()

    on_exit(fn ->
      Topology.close(topology)
      Plexus.stop_run(run)
      Test.close(client)
    end)

    run_id = Run.run_id(run)
    Graph.put(run_id, {:service, "frontend"}, %{class: :service})
    Graph.put(run_id, {:service, "gateway"}, %{class: :service})

    child = %{
      source: :trace,
      event_id: "child",
      event_time_us: 1,
      service: "frontend",
      raw: %{
        "trace_id" => "t1",
        "span_id" => "child-1",
        "parent_id" => "parent-1"
      }
    }

    parent = %{
      source: :trace,
      event_id: "parent",
      event_time_us: 2,
      service: "gateway",
      raw: %{
        "trace_id" => "t1",
        "span_id" => "parent-1",
        "parent_id" => ""
      }
    }

    assert [] = Topology.observe_trace(run_id, topology, "frontend", child)
    assert [{"gateway", "frontend"}] = Topology.observe_trace(run_id, topology, "gateway", parent)

    assert [%{node: {:service, "frontend"}}] =
             Graph.outgoing(run_id, {:service, "gateway"}, :calls)
  end

  test "replay reaches quiescence after EOF while resident services and hypotheses remain alive" do
    client = semantic_client()

    {:ok, run} =
      Plexus.start_run(
        id: make_ref(),
        client: client,
        max_population: 200,
        max_depth: 8,
        budgets: [measure: 200, expand: 100, population: 200, tokens: 1_000_000],
        batch: [delay_ms: 1, max: 16, max_concurrency: 8]
      )

    topology = Topology.new()

    on_exit(fn ->
      Topology.close(topology)
      _ = Plexus.stop_run(run)
      Test.close(client)
    end)

    IncidentCommander.register_contracts!(run)
    IncidentCommander.prepare_indexes!(run)

    service_opts = [
      topology: topology,
      signal_every: 1,
      hypothesis_update_every: 1,
      trigger_probability: 0.5,
      branch_width: 1,
      branch_credits: 0,
      peer_challenges: 0,
      max_hypotheses: 50,
      incident_window_us: 30_000_000
    ]

    {:ok, _} =
      Run.start_actor(run,
        module: Replay,
        actor_id: :gaia_replay,
        class: :replay,
        init_arg: %{
          trace_files: [Path.join(@fixture_dir, "trace.csv")],
          business_files: [Path.join(@fixture_dir, "business.csv")],
          day: "2021-07-01",
          service_opts: service_opts,
          speed: 10.0,
          max_events: 0,
          chunk_size: 2
        }
      )

    Runtime.await_quiescent!(run, 5_000)

    run_id = Run.run_id(run)
    replay = Graph.get(run_id, :gaia_replay)
    assert replay.status == :complete
    assert replay.result == %{reason: :eof, events: 4}
    assert Graph.by_class(run_id, :service) != []
    assert Graph.by_class(run_id, :hypothesis) != []
    assert Quiescence.quiescent?(Run.config(run).quiescence)

    evidence = IncidentCommander.acceptance_evidence(run)
    assert evidence.semantic_before_eof
  end

  test "one resident hypothesis revises repeatedly as new raw evidence arrives" do
    client = semantic_client()

    {:ok, run} =
      Plexus.start_run(
        id: make_ref(),
        client: client,
        max_population: 100,
        max_depth: 6,
        budgets: [measure: 100, expand: 100, population: 100, tokens: 1_000_000],
        batch: [delay_ms: 1, max: 16, max_concurrency: 8]
      )

    topology = Topology.new()

    on_exit(fn ->
      Topology.close(topology)
      _ = Plexus.stop_run(run)
      Test.close(client)
    end)

    IncidentCommander.register_contracts!(run)
    IncidentCommander.prepare_indexes!(run)

    opts = [
      topology: topology,
      signal_every: 1,
      hypothesis_update_every: 1,
      trigger_probability: 0.5,
      branch_width: 1,
      branch_credits: 0,
      peer_challenges: 0
    ]

    {:ok, service_pid} = IncidentCommander.ensure_service(run, "frontend", opts)

    assert :ok =
             Run.cast(run, {:service, "frontend"}, {:telemetry, event(:trace, 1, "frontend")})

    eventually(fn -> hypothesis_nodes(run) != [] end)
    [{hypothesis_id, _}] = hypothesis_nodes(run)
    {:ok, hypothesis_pid} = Run.actor_pid(run, hypothesis_id)

    assert :ok =
             Run.cast(run, {:service, "frontend"}, {:telemetry, event(:business, 2, "frontend")})

    eventually(fn ->
      case Graph.get(Run.run_id(run), hypothesis_id) do
        %{revisions: revisions} when revisions >= 2 -> true
        _ -> false
      end
    end)

    assert {:ok, ^service_pid} = Run.actor_pid(run, {:service, "frontend"})
    assert {:ok, ^hypothesis_pid} = Run.actor_pid(run, hypothesis_id)
    assert Graph.get(Run.run_id(run), hypothesis_id).revisions >= 2
    assert length(Test.requests(client)) >= 4
  end

  test "a hypothesis discards a semantic result superseded by live invalidation" do
    owner = self()
    call_count = :atomics.new(1, signed: false)

    client =
      Test.client()
      |> Test.stub_callback(fn request ->
        state = request.body |> Jason.decode!() |> Map.fetch!("state")

        if Map.has_key?(state, "hypothesis_service") do
          n = :atomics.add_get(call_count, 1, 1)

          if n == 1 do
            send(owner, {:first_hypothesis_measurement_started, self()})

            receive do
              :release_first_hypothesis_measurement -> :ok
            after
              2_000 -> raise "test did not release first hypothesis measurement"
            end
          end

          {:answers,
           [
             root_cause: {:noul, if(n == 1, do: 0.15, else: 0.82)},
             next_action: {:choice, "observe", 0.96},
             evidence_strength: {:score, 3, 0.94}
           ]}
        else
          {:answers,
           [
             anomaly_relevance: {:noul, 0.95},
             failure_mode: {:choice, "request_path_error", 0.97},
             diagnostic_strength: {:score, 3, 0.95}
           ]}
        end
      end)

    {:ok, run} =
      Plexus.start_run(
        id: make_ref(),
        client: client,
        max_population: 100,
        max_depth: 6,
        budgets: [measure: 100, expand: 100, population: 100, tokens: 1_000_000],
        batch: [delay_ms: 1, max: 16, max_concurrency: 8]
      )

    topology = Topology.new()

    on_exit(fn ->
      Topology.close(topology)
      _ = Plexus.stop_run(run)
      Test.close(client)
    end)

    IncidentCommander.register_contracts!(run)
    IncidentCommander.prepare_indexes!(run)
    {:ok, _} = IncidentCommander.ensure_service(run, "frontend", topology: topology)

    hypothesis = {:hypothesis, "incident-a", "frontend", "request_path_error", :epoch_test}

    {:ok, _} =
      IncidentCommander.start_hypothesis(run, hypothesis,
        service: "frontend",
        incident: "incident-a",
        failure_mode: "request_path_error",
        topology: topology,
        branch_credits: 0,
        peer_challenges: 0
      )

    assert :ok = Run.cast(run, hypothesis, {:evidence, event(:trace, 1, "frontend")})
    assert_receive {:first_hypothesis_measurement_started, worker}, 1_000

    run_id = Run.run_id(run)

    assert hypothesis in Plexus.Provenance.invalidate(run_id, {:service, "frontend"},
             notify: true
           )

    eventually(fn -> Graph.get(run_id, hypothesis).epoch >= 1 end)
    send(worker, :release_first_hypothesis_measurement)

    eventually(fn ->
      events = Plexus.Record.events(run_id)

      Enum.any?(events, &(&1.type == :gaia_hypothesis_superseded_result)) and
        Graph.get(run_id, hypothesis).revisions >= 1 and
        not Graph.get(run_id, hypothesis).stale
    end)

    attrs = Graph.get(run_id, hypothesis)
    assert attrs.root_probability == 0.82
    assert attrs.revisions == 1
    assert length(Test.requests(client)) >= 2
  end

  test "peer challenge can prune a refuted branch while unrelated resident actors survive" do
    client = semantic_client(challenge_survival: 0.05)

    {:ok, run} =
      Plexus.start_run(
        id: make_ref(),
        client: client,
        max_population: 100,
        max_depth: 6,
        budgets: [measure: 100, expand: 100, population: 100, tokens: 1_000_000],
        batch: [delay_ms: 1, max: 16]
      )

    topology = Topology.new()

    on_exit(fn ->
      Topology.close(topology)
      _ = Plexus.stop_run(run)
      Test.close(client)
    end)

    IncidentCommander.register_contracts!(run)
    IncidentCommander.prepare_indexes!(run)
    {:ok, _} = IncidentCommander.ensure_service(run, "frontend", topology: topology)

    a = {:hypothesis, "incident-a", "frontend", "dependency_failure", 1}
    b = {:hypothesis, "incident-a", "frontend", "request_path_error", 1}

    {:ok, _} =
      IncidentCommander.start_hypothesis(run, a,
        service: "frontend",
        incident: "incident-a",
        failure_mode: "dependency_failure",
        topology: topology,
        branch_credits: 0,
        peer_challenges: 0
      )

    {:ok, _} =
      IncidentCommander.start_hypothesis(run, b,
        service: "frontend",
        incident: "incident-a",
        failure_mode: "request_path_error",
        topology: topology,
        branch_credits: 0,
        peer_challenges: 0
      )

    packet = %{
      "challenge_id" => "challenge-1",
      "service" => "frontend",
      "failure_mode" => "dependency_failure",
      "root_probability" => 0.9,
      "evidence" => %{"message" => "dependency recovered before symptom"}
    }

    assert :ok = Run.cast(run, b, {:challenge, a, packet})
    eventually(fn -> Graph.get(Run.run_id(run), b) == nil end)

    assert Graph.get(Run.run_id(run), a) != nil
    assert Graph.get(Run.run_id(run), {:service, "frontend"}) != nil

    evidence = IncidentCommander.acceptance_evidence(run)
    assert evidence.challenge_results >= 1
    assert evidence.subtree_prunes >= 1

    path =
      Path.join(
        System.tmp_dir!(),
        "plexus-gaia-events-#{System.unique_integer([:positive])}.jsonl"
      )

    assert :ok = IncidentCommander.write_event_log!(run, path)
    assert File.read!(path) =~ "gaia_challenge_result"
    File.rm!(path)
  end

  test "zero local branch credit prevents child hypothesis birth" do
    client = semantic_client(next_action: "investigate_callers")

    {:ok, run} =
      Plexus.start_run(
        id: make_ref(),
        client: client,
        max_population: 100,
        max_depth: 6,
        budgets: [measure: 100, expand: 100, population: 100, tokens: 1_000_000],
        batch: [delay_ms: 1, max: 16]
      )

    topology = Topology.new()

    on_exit(fn ->
      Topology.close(topology)
      _ = Plexus.stop_run(run)
      Test.close(client)
    end)

    IncidentCommander.register_contracts!(run)
    IncidentCommander.prepare_indexes!(run)
    {:ok, _} = IncidentCommander.ensure_service(run, "frontend", topology: topology)
    {:ok, _} = IncidentCommander.ensure_service(run, "gateway", topology: topology)
    Graph.add_edge(Run.run_id(run), :calls, {:service, "gateway"}, {:service, "frontend"})

    root = {:hypothesis, "incident-a", "frontend", "request_path_error", 1}

    {:ok, _} =
      IncidentCommander.start_hypothesis(run, root,
        service: "frontend",
        incident: "incident-a",
        failure_mode: "request_path_error",
        topology: topology,
        branch_credits: 0,
        branch_width: 2,
        peer_challenges: 0
      )

    assert :ok = Run.cast(run, root, {:evidence, event(:trace, 1, "frontend")})

    eventually(fn ->
      case Graph.get(Run.run_id(run), root) do
        %{revisions: revisions} when revisions >= 1 -> true
        _ -> false
      end
    end)

    assert Graph.children(Run.run_id(run), root) == []
    account = Hypothesis.account_id(root)
    assert Accounts.snapshot(Run.run_id(run))[account].meters.expand.remaining == 0

    eventually(fn ->
      Enum.any?(Plexus.Record.events(Run.run_id(run)), fn event ->
        event.type == :gaia_branch_credit_denied
      end)
    end)
  end

  test "the living twin run path has no known-population completion barrier or precomputed evidence summary" do
    run_source = File.read!("examples/02_incident_commander/run.exs")
    app_source = File.read!("examples/02_incident_commander/application.exs")

    refute run_source =~ "await_class_complete!"
    refute run_source =~ "await_actor_ids_complete!"
    refute app_source =~ "await_class_complete!"
    refute app_source =~ "await_actor_ids_complete!"
    refute app_source =~ "build_evidence"
    refute app_source =~ "trace_rows = rows_for_day"
    assert app_source =~ "activity_mode: :resident"
    assert app_source =~ "{:wake_on,"
    assert app_source =~ "{:challenge,"
    assert app_source =~ "Provenance.invalidate"
    assert app_source =~ "{:prune,"
    assert app_source =~ "Runtime.await_quiescent!"

    quiescence_at = byte_offset!(app_source, "Runtime.await_quiescent!")
    truth_at = byte_offset!(app_source, "truth = truth_services(run_files, day)")
    assert quiescence_at < truth_at
  end

  defp byte_offset!(source, needle) do
    case :binary.match(source, needle) do
      {offset, _length} -> offset
      :nomatch -> flunk("expected source to contain #{inspect(needle)}")
    end
  end

  defp semantic_client(opts \\ []) do
    next_action = Keyword.get(opts, :next_action, "observe")
    challenge_survival = Keyword.get(opts, :challenge_survival, 0.9)

    Test.client()
    |> Test.stub_callback(fn request ->
      state = request.body |> Jason.decode!() |> Map.fetch!("state")

      cond do
        Map.has_key?(state, "challenger") ->
          {:answers,
           [
             survives_challenge: {:noul, challenge_survival},
             disposition:
               {:choice, if(challenge_survival < 0.35, do: "concede", else: "stand"), 0.98}
           ]}

        Map.has_key?(state, "hypothesis_service") ->
          {:answers,
           [
             root_cause: {:noul, 0.72},
             next_action: {:choice, next_action, 0.96},
             evidence_strength: {:score, 3, 0.94}
           ]}

        true ->
          {:answers,
           [
             anomaly_relevance: {:noul, 0.95},
             failure_mode: {:choice, "request_path_error", 0.97},
             diagnostic_strength: {:score, 3, 0.95}
           ]}
      end
    end)
  end

  defp event(source, time, service) do
    %{
      source: source,
      event_id: "#{source}-#{time}-#{service}",
      event_time_us: time,
      service: service,
      raw: %{
        "timestamp" => "2021-07-01T00:00:0#{time}.000Z",
        "trace_id" => "trace-#{time}",
        "span_id" => "span-#{time}",
        "parent_id" => "",
        "service_name" => service,
        "service" => service,
        "status_code" => "500",
        "url" => "/checkout",
        "message" => "timeout failure #{time}"
      }
    }
  end

  defp hypothesis_nodes(run) do
    Graph.by_class(Run.run_id(run), :hypothesis)
  end

  defp take_events(cursor, 0, acc), do: {Enum.reverse(acc), cursor}

  defp take_events(cursor, count, acc) do
    case Chronology.next(cursor) do
      {:ok, event, cursor} -> take_events(cursor, count - 1, [event | acc])
      :eof -> {Enum.reverse(acc), cursor}
    end
  end

  defp eventually(fun, attempts \\ 200)
  defp eventually(fun, 0), do: assert(fun.())

  defp eventually(fun, attempts) do
    if fun.() do
      :ok
    else
      Process.sleep(10)
      eventually(fun, attempts - 1)
    end
  end
end
