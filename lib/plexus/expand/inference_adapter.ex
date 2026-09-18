defmodule Plexus.Expand.InferenceAdapter do
  @moduledoc """
  Expansion through the published `inference` package.

  Completion returns the neutral response, including `object`, usage, cost and
  trace. Streaming consumes neutral events, calls an optional `on_event` observer
  and a `monitor` callback (`:halt` rejects generation), and retains the final
  response when supplied by the provider. A monitor may use TypeSafe contracts.

  Physical cancellation is provider-dependent: a Pristine token is forwarded in
  request options only when the client explicitly declares `:cancellation` as
  supported. Other clients have local cancellation only. Require that capability
  at run startup when physical cancellation is essential.
  """
  @behaviour Plexus.Expand.Adapter

  alias Inference.{Response, StreamEvent}
  alias Pristine.Cancellation

  @impl true
  def capabilities(client) do
    Map.new(Inference.capabilities(client), &{&1.name, &1.support})
  end

  @impl true
  def expand(client, spec, opts) do
    token = Keyword.get_lazy(opts, :cancellation, &Cancellation.new/0)
    request_opts = Keyword.drop(opts, [:cancellation, :stream, :on_event, :monitor])
    request_opts = cancellation_options(client, token, request_opts)

    cond do
      Cancellation.cancelled?(token) -> {:error, :cancelled}
      Keyword.get(opts, :stream, false) -> stream(client, spec, request_opts, opts, token)
      true -> Inference.complete(client, spec, request_opts)
    end
  end

  defp cancellation_options(client, token, opts) do
    if Map.get(capabilities(client), :cancellation) == :supported do
      Keyword.update(
        opts,
        :options,
        [cancellation: token],
        &Keyword.put(&1, :cancellation, token)
      )
    else
      opts
    end
  end

  defp stream(client, spec, request_opts, opts, token) do
    with {:ok, events} <- Inference.stream(client, spec, request_opts) do
      Enum.reduce_while(events, {:ok, Response.new()}, &consume_event(&1, &2, opts, token))
    end
  end

  defp consume_event(event, acc, opts, token) do
    cond do
      Cancellation.cancelled?(token) ->
        {:halt, {:error, :cancelled}}

      rejected?(event, opts) ->
        Cancellation.cancel(token)
        {:halt, {:error, :monitor_rejected}}

      true ->
        collect(event, acc, opts)
    end
  end

  defp rejected?(event, opts) do
    case Keyword.get(opts, :monitor) do
      nil -> false
      fun -> fun.(event) == :halt
    end
  end

  defp collect(event, {:ok, response}, opts) do
    if observer = Keyword.get(opts, :on_event), do: observer.(event)

    case event do
      %StreamEvent{type: :error, data: reason} ->
        {:halt, {:error, reason}}

      %StreamEvent{type: :delta, data: text} when is_binary(text) ->
        {:cont, {:ok, %{response | text: response.text <> text}}}

      %StreamEvent{type: type, data: %Response{} = final} when type in [:done, :message] ->
        {:cont, {:ok, final}}

      %StreamEvent{} ->
        {:cont, {:ok, response}}

      _ ->
        {:halt, {:error, :invalid_stream_event}}
    end
  end
end
