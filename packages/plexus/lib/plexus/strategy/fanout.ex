defmodule Plexus.Strategy.Fanout do
  @moduledoc """
  Helper for wide semantic fan-out using shared prepared contracts.

  This is intentionally thin: it delegates actual semantic execution to
  `TypeSafeSDK.evaluate_stream/4` and `TypeSafeSDK.evaluate_many/4`.
  """

  alias Plexus.Contract

  @spec stream(
          TypeSafeSDK.Client.t(),
          Enumerable.t(),
          TypeSafeSDK.Prepared.t() | keyword(),
          keyword()
        ) :: Enumerable.t()
  def stream(client, states, prepared, opts \\ []) do
    defaults = [
      max_concurrency:
        Keyword.get(
          opts,
          :max_concurrency,
          Application.get_env(:plexus, :default_batch, []) |> Keyword.get(:max_concurrency, 8)
        ),
      ordered: Keyword.get(opts, :ordered, true),
      on_error: Keyword.get(opts, :on_error, :collect)
    ]

    Contract.batch_stream(client, states, prepared, Keyword.merge(defaults, opts))
  end

  @spec collect(
          TypeSafeSDK.Client.t(),
          Enumerable.t(),
          TypeSafeSDK.Prepared.t() | keyword(),
          keyword()
        ) :: list()
  def collect(client, states, prepared, opts \\ []) do
    stream(client, states, prepared, opts) |> Enum.to_list()
  end

  @spec many(
          TypeSafeSDK.Client.t(),
          Enumerable.t(),
          TypeSafeSDK.Prepared.t() | keyword(),
          keyword()
        ) :: list()
  def many(client, states, prepared, opts \\ []) do
    defaults = [
      max_concurrency:
        Keyword.get(
          opts,
          :max_concurrency,
          Application.get_env(:plexus, :default_batch, []) |> Keyword.get(:max_concurrency, 8)
        ),
      ordered: Keyword.get(opts, :ordered, true),
      on_error: Keyword.get(opts, :on_error, :collect)
    ]

    Contract.batch_many(client, states, prepared, Keyword.merge(defaults, opts))
  end
end
