defmodule Plexus.Examples.Sample do
  @moduledoc """
  Small end-to-end demo helper for IEx or integration tests.
  """

  alias Plexus.Examples.IntakeCoordinator

  @spec boot(TypeSafeSDK.Client.t(), String.t()) :: {:ok, %{run: pid(), actor: pid()}}
  def boot(client, text) when is_binary(text) do
    {:ok, run} = Plexus.start_run(id: make_ref(), client: client)

    {:ok, actor} =
      Plexus.start_actor(run,
        module: IntakeCoordinator,
        actor_id: {:ticket, 1},
        init_arg: %{text: text}
      )

    Plexus.cast(actor, :classify)
    {:ok, %{run: run, actor: actor}}
  end
end
