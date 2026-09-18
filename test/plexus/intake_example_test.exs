defmodule Plexus.IntakeExampleTest do
  use ExUnit.Case, async: true

  alias Plexus.Examples.IntakeCoordinator
  alias TypeSafeSDK.Test

  test "root actor performs recursive semantic orchestration through commands" do
    client =
      Test.client()
      |> Test.stub_callback(fn request ->
        body = Jason.decode!(request.body)
        state = body["state"]

        cond do
          Map.has_key?(state, "ticket") ->
            {:answers,
             [
               department: {:choice, "technical", 0.93},
               urgent: {:noul, 0.88},
               evidence: {:noul, 0.91}
             ]}

          Map.has_key?(state, "text") ->
            {:answers, [relevance: {:score, 2, 0.95}, actionable: {:noul, 0.90}]}
        end
      end)

    {:ok, run} = Plexus.start_run(id: make_ref(), client: client, batch: [delay_ms: 1, max: 64])

    on_exit(fn ->
      _ = Plexus.stop_run(run)
      Test.close(client)
    end)

    {:ok, actor} =
      Plexus.start_actor(run,
        module: IntakeCoordinator,
        actor_id: {:ticket, 1},
        init_arg: %{text: "Checkout fails and sign in is broken."}
      )

    Plexus.cast(actor, :classify)

    assert eventually(fn -> GenServer.call(actor, :classification) != nil end)
    children = GenServer.call(actor, :children)
    assert children != []
    assert length(Plexus.subtree(run, {:ticket, 1})) >= 2
  end

  defp eventually(fun, attempts \\ 100)
  defp eventually(fun, 0), do: fun.()

  defp eventually(fun, attempts) do
    if fun.() do
      true
    else
      Process.sleep(10)
      eventually(fun, attempts - 1)
    end
  end
end
