defmodule Plexus.Contract do
  @moduledoc """
  Shared prepared TypeSafe contracts.

  This module centralizes the parts of `TypeSafeSDK` we want to reuse across
  many actors:

  - `prepare!/1`
  - stable prepared fingerprints
  - `evaluate/4`
  - `evaluate_stream/4`
  - `evaluate_many/4`
  """

  alias TypeSafeSDK.Prepared

  @spec new!(keyword()) :: Prepared.t()
  def new!(questions), do: TypeSafeSDK.prepare!(questions)

  @spec fingerprint(Prepared.t()) :: String.t()
  def fingerprint(prepared), do: Prepared.fingerprint(prepared)

  @spec evaluate(TypeSafeSDK.Client.t(), term(), Prepared.t() | keyword(), keyword()) ::
          {:ok, term()} | {:error, term()}
  def evaluate(client, state, prepared_or_questions, opts \\ []) do
    TypeSafeSDK.evaluate(client, state, prepared_or_questions, opts)
  end

  @spec batch_stream(TypeSafeSDK.Client.t(), Enumerable.t(), Prepared.t() | keyword(), keyword()) ::
          Enumerable.t()
  def batch_stream(client, states, prepared_or_questions, opts \\ []) do
    TypeSafeSDK.evaluate_stream(client, states, prepared_or_questions, opts)
  end

  @spec batch_many(TypeSafeSDK.Client.t(), Enumerable.t(), Prepared.t() | keyword(), keyword()) ::
          list()
  def batch_many(client, states, prepared_or_questions, opts \\ []) do
    TypeSafeSDK.evaluate_many(client, states, prepared_or_questions, opts)
  end

  @spec memo_key(term(), Prepared.t()) :: String.t()
  def memo_key(state, %Prepared{} = prepared) do
    state_digest = :crypto.hash(:sha256, Jason.encode!(state)) |> Base.encode16(case: :lower)
    "#{state_digest}:#{Prepared.fingerprint(prepared)}"
  end

  @doc "Memo identity including effective evaluation options; operational batching controls are excluded."
  @spec memo_key(term(), Prepared.t(), keyword()) :: String.t()
  def memo_key(state, prepared, opts) do
    semantic = opts |> Keyword.drop([:batch, :cancellation, :telemetry_metadata]) |> Enum.sort()
    base = memo_key(state, prepared)

    if semantic == [],
      do: base,
      else:
        base <>
          ":" <>
          (:crypto.hash(:sha256, :erlang.term_to_binary(semantic, [:deterministic]))
           |> Base.encode16(case: :lower))
  end
end
