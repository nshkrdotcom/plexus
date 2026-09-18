defmodule Plexus.InferenceAdapterTest do
  use ExUnit.Case, async: true

  alias Plexus.Expand.InferenceAdapter

  test "published inference completion preserves structured objects and trace accounting" do
    client =
      Inference.client!(
        adapter: Inference.Adapters.Mock,
        adapter_opts: [response_object: %{"proposals" => []}]
      )

    assert {:ok, response} =
             InferenceAdapter.expand(client, "expand", response_format: {:json, :object})

    assert response.object == %{"proposals" => []}
    assert response.trace.adapter == Inference.Adapters.Mock
    assert response.usage.input_tokens > 0
  end

  test "capability translation preserves unknown and partial support" do
    client =
      Inference.client!(
        adapter: Inference.Adapters.Mock,
        capabilities: [
          Inference.Capability.new(:streaming, :supported),
          Inference.Capability.new(:cancellation, :unknown),
          Inference.Capability.new(:json_schema, :partial)
        ]
      )

    assert InferenceAdapter.capabilities(client) == %{
             streaming: :supported,
             cancellation: :unknown,
             json_schema: :partial
           }
  end

  test "streams neutral events and allows a monitor to stop generation" do
    client =
      Inference.client!(
        adapter: Inference.Adapters.Mock,
        adapter_opts: [response_text: "proposal"]
      )

    owner = self()

    assert {:ok, %{text: "proposal"}} =
             InferenceAdapter.expand(client, "expand", stream: true, on_event: &send(owner, &1))

    assert_receive %Inference.StreamEvent{type: :delta, data: "proposal"}
    assert_receive %Inference.StreamEvent{type: :done}

    assert {:error, :monitor_rejected} =
             InferenceAdapter.expand(client, "expand", stream: true, monitor: fn _ -> :halt end)
  end

  test "an already cancelled expansion never reaches inference" do
    cancellation = Pristine.Cancellation.new()
    Pristine.Cancellation.cancel(cancellation)
    client = Inference.client!(adapter: Inference.Adapters.Mock)

    assert {:error, :cancelled} =
             InferenceAdapter.expand(client, "expand", cancellation: cancellation)
  end
end
