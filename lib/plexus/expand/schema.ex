defmodule Plexus.Expand.Schema do
  @moduledoc "Standard strict JSON schema for graph-expansion proposals."

  @spec proposals() :: map()
  def proposals do
    %{
      name: "plexus_proposals",
      strict: true,
      schema: %{
        type: "object",
        additionalProperties: false,
        required: ["proposals"],
        properties: %{
          proposals: %{
            type: "array",
            items: %{
              type: "object",
              additionalProperties: false,
              required: ["id", "class", "content"],
              properties: %{
                id: %{type: "string"},
                class: %{type: "string"},
                content: %{type: "string"},
                parent: %{type: ["string", "null"]},
                edges: %{
                  type: "array",
                  items: %{
                    type: "object",
                    additionalProperties: false,
                    required: ["type", "to"],
                    properties: %{
                      type: %{type: "string"},
                      to: %{type: "string"},
                      weight: %{type: "number"}
                    }
                  }
                }
              }
            }
          }
        }
      }
    }
  end
end
