defmodule Plexus.PopulationOperatorsTest.Actor do
  use Plexus.Actor
  @impl true
  def init(args), do: {:ok, args}
  @impl true
  def handle_evaluation(_, _, state), do: {:noreply, state}
end

defmodule Plexus.PopulationOperatorsTest do
  use ExUnit.Case, async: true
  alias Plexus.{Graph, Population, Run}

  test "seeded selection and replicate/split/merge/migrate obey interpreter admission" do
    client = TypeSafeSDK.Test.client()
    {:ok, run} = Plexus.start_run(client: client, max_population: 10)
    {:ok, destination} = Plexus.start_run(client: client)

    on_exit(fn ->
      Plexus.stop_run(run)
      Plexus.stop_run(destination)
      TypeSafeSDK.Test.close(client)
    end)

    id = Run.run_id(run)

    {:ok, _} =
      Run.start_actor(run,
        module: Plexus.PopulationOperatorsTest.Actor,
        actor_id: :source,
        class: :candidate,
        init_arg: %{content: "seed"}
      )

    context = %{run_id: id, actor_id: :source}
    assert :ok = Population.replicate(context, :source, :copy)
    assert {:ok, _} = Run.actor_pid(run, :copy)
    assert :ok = Population.split(context, :copy, left: %{content: "L"}, right: %{content: "R"})
    assert {:error, :not_found} = Run.actor_pid(run, :copy)
    assert :ok = Population.merge(context, [:left, :right], :joined, %{content: "L+R"})
    assert {:ok, joined_pid} = Run.actor_pid(run, :joined)
    assert {:error, :not_found} = Run.actor_pid(run, :left)
    assert :ok = Population.migrate(context, :joined, destination, %{content: "snapshot"})
    refute Process.alive?(joined_pid)
    assert {:error, :not_found} = Run.actor_pid(run, :joined)
    assert {:ok, _} = Run.actor_pid(destination, :joined)
    Graph.update(id, :source, &Map.put(&1, :score, 3))

    assert [{:source, _}, {:source, _}] =
             Population.resample(id, :candidate, 2, fn _ -> 1 end, 42)

    assert {:source, _} = Population.tournament(id, :candidate, 3, & &1.score, 42)
  end

  test "failed split rolls back admitted children and preserves its source" do
    client = TypeSafeSDK.Test.client()
    {:ok, run} = Plexus.start_run(client: client, max_population: 2)

    on_exit(fn ->
      Plexus.stop_run(run)
      TypeSafeSDK.Test.close(client)
    end)

    id = Run.run_id(run)

    {:ok, source} =
      Run.start_actor(run, module: Plexus.PopulationOperatorsTest.Actor, actor_id: :source)

    Population.split(%{run_id: id, actor_id: :source}, :source, a: %{}, b: %{})
    assert Process.alive?(source)
    assert Graph.count(id) == 1
  end
end
