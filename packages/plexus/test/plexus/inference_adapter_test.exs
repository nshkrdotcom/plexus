defmodule Plexus.InferenceAdapterTest do
  use ExUnit.Case, async: true

  alias Plexus.Expand.{InferenceAdapter, Materializer}

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

  test "a TypeSafe monitor evaluates generation deltas and cancels rejected output" do
    semantic = TypeSafeSDK.Test.client() |> TypeSafeSDK.Test.stub(acceptable: {:noul, 0.1})
    on_exit(fn -> TypeSafeSDK.Test.close(semantic) end)

    contract =
      TypeSafeSDK.prepare!(acceptable: TypeSafeSDK.noul("Is the proposed text acceptable?"))

    inference =
      Inference.client!(
        adapter: Inference.Adapters.Mock,
        adapter_opts: [response_text: "proposal"]
      )

    token = Pristine.Cancellation.new()

    monitor = fn
      %Inference.StreamEvent{type: :delta, data: text} ->
        case TypeSafeSDK.evaluate(semantic, %{delta: text}, contract) do
          {:ok, response} ->
            if Plexus.Belief.from(response, :acceptable).value >= 0.5, do: :cont, else: :halt

          {:error, _} ->
            :halt
        end

      _ ->
        :cont
    end

    assert {:error, :monitor_rejected} =
             InferenceAdapter.expand(inference, "expand",
               stream: true,
               monitor: monitor,
               cancellation: token
             )

    assert Pristine.Cancellation.cancelled?(token)
    assert length(TypeSafeSDK.Test.requests(semantic)) == 1
  end

  test "an already cancelled expansion never reaches inference" do
    cancellation = Pristine.Cancellation.new()
    Pristine.Cancellation.cancel(cancellation)
    client = Inference.client!(adapter: Inference.Adapters.Mock)

    assert {:error, :cancelled} =
             InferenceAdapter.expand(client, "expand", cancellation: cancellation)
  end

  @tag capture_log: true
  test "required unknown capabilities fail startup while explicit support starts" do
    semantic = TypeSafeSDK.Test.client()
    on_exit(fn -> TypeSafeSDK.Test.close(semantic) end)
    client = Inference.client!(adapter: Inference.Adapters.Mock)

    assert {:error, _} =
             Plexus.start_run(
               client: semantic,
               inference_client: client,
               expand: [required_capabilities: [:json_schema]]
             )

    supported = %{client | capabilities: [Inference.Capability.new(:json_schema, :supported)]}

    assert {:ok, run} =
             Plexus.start_run(
               client: semantic,
               inference_client: supported,
               expand: [required_capabilities: [:json_schema]]
             )

    Plexus.stop_run(run)
  end

  test "materializer rejects unknown remote atoms and emits normal admitted commands" do
    name = "untrusted_class_#{System.unique_integer([:positive])}"
    object = %{"proposals" => [%{"id" => "x", "class" => name, "content" => "text"}]}

    assert_raise ArgumentError, fn ->
      Materializer.commands(object, fn _ -> __MODULE__ end)
    end

    assert_raise ArgumentError, fn -> String.to_existing_atom(name) end
    object = %{"proposals" => [%{"id" => "x", "class" => "candidate", "content" => "text"}]}

    assert [{:spawn, :candidate, __MODULE__, %{content: "text"}, _}] =
             Materializer.commands(object, fn "candidate" -> __MODULE__ end)
  end
end
