defmodule Plexus.RuntimeAcceptanceTest.Actor do
  use Plexus.Actor

  @impl true
  def init(args) do
    if args[:complete], do: Plexus.Actor.dispatch(args, {:complete, :done})
    {:ok, args}
  end

  @impl true
  def handle_cast({:commands, commands}, state) do
    Plexus.Actor.dispatch(state, commands)
    {:noreply, state}
  end

  @impl true
  def handle_call(:ping, _from, state), do: {:reply, :pong, state}

  @impl true
  def handle_evaluation(_result, _tag, state), do: {:noreply, state}
end

defmodule Plexus.RuntimeAcceptanceTest do
  use ExUnit.Case, async: true

  alias Plexus.{Budget, Graph, Run}
  alias Plexus.Schedule.Quiescence
  alias TypeSafeSDK.Test

  defp run(opts \\ []) do
    client = Test.client()
    {:ok, run} = Plexus.start_run(Keyword.merge([id: make_ref(), client: client], opts))

    on_exit(fn ->
      Plexus.stop_run(run)
      Test.close(client)
    end)

    run
  end

  defp birth(run, id, opts \\ []) do
    Run.start_actor(
      run,
      Keyword.merge([module: Plexus.RuntimeAcceptanceTest.Actor, actor_id: id], opts)
    )
  end

  test "concurrent admission obeys max_population even with a larger explicit budget" do
    run = run(max_population: 4, budgets: [population: 100], actor_partitions: 8)
    results = 1..80 |> Task.async_stream(&birth(run, &1), max_concurrency: 80) |> Enum.to_list()
    assert Enum.count(results, &match?({:ok, {:ok, _}}, &1)) == 4
    assert Graph.count(Run.run_id(run)) == 4
    assert Budget.used(Run.config(run).budget, :population) == 4
  end

  test "completion during init retires the actor exactly once" do
    run = run()
    {:ok, _} = birth(run, :early, init_arg: %{complete: true})
    config = Run.config(run)
    assert Graph.get(config.run_id, :early).status == :complete
    assert Quiescence.quiescent?(config.quiescence)
    assert :ok = Run.terminate_actor(run, :early)
    assert Quiescence.get(config.quiescence, :actors) == 0
    assert Budget.used(config.budget, :population) == 0
  end

  test "actor ids are isolated and births use all configured partitions" do
    a = run(actor_partitions: 4)
    b = run(actor_partitions: 4)
    for id <- 1..32, do: assert({:ok, _} = birth(a, id))
    assert {:ok, other} = birth(b, 1)
    assert {:ok, first} = Run.actor_pid(a, 1)
    refute first == other
    partitions = Supervisor.which_children(Run.config(a).actor_supervisors)
    assert length(partitions) == 4

    assert Enum.all?(partitions, fn {_, pid, _, _} ->
             DynamicSupervisor.count_children(pid).active > 0
           end)
  end
end
