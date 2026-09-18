defmodule Plexus.IntakeExampleTest do
  use ExUnit.Case, async: true

  alias Plexus.Examples.IntakeCoordinator
  alias TypeSafeSDK.Test

  test "root actor performs recursive semantic orchestration" do
    client =
      Test.client()
      |> Test.stub_callback(fn request ->
        body = Jason.decode!(request.body)
        state = body["state"]

        cond do
          Map.has_key?(state, "ticket") ->
            {:answers,
             [
               department: {:choice, "technical", 0.93, %{"technical" => 0.93, "billing" => 0.04, "sales" => 0.03}},
               urgent: {:noul, "yes", 0.88},
               evidence: {:noul, "checkout is broken; user cannot log in", 0.91}
             ]}

          Map.has_key?(state, "text") ->
            {:answers, [relevance: {:score, 5, 0.95}, summary: {:noul, "Key evidence", 0.90}]}
        end
      end)

    {:ok, run} = Plexus.start_run(id: :example, client: client)

    {:ok, actor} =
      Plexus.start_actor(run,
        module: IntakeCoordinator,
        actor_id: {:ticket, 1},
        init_arg: %{text: "Checkout fails and sign in is broken."}
      )

    Plexus.cast(actor, :classify)
    Process.sleep(50)

    classification = GenServer.call(actor, :classification)
    assert classification != nil

    children = GenServer.call(actor, :children)
    assert length(children) >= 1
    assert length(Plexus.subtree(run, {:ticket, 1})) >= 2
  end
end
