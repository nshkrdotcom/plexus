<p align="center">
  <img src="assets/plexus.svg" alt="Plexus" width="200" height="200"/>
</p>

<p align="center">
  <a href="https://hex.pm/packages/plexus"><img src="https://img.shields.io/hexpm/v/plexus.svg" alt="Hex.pm"/></a>
  <a href="https://hexdocs.pm/plexus"><img src="https://img.shields.io/badge/hex-docs-blue.svg" alt="HexDocs"/></a>
  <a href="https://github.com/nshkrdotcom/plexus"><img src="https://img.shields.io/badge/GitHub-repo-black?logo=github" alt="GitHub"/></a>
  <a href="https://hex.pm/packages/plexus"><img src="https://img.shields.io/hexpm/l/plexus.svg" alt="License"/></a>
</p>

# Plexus

**Plexus** is a BEAM-native semantic actor substrate designed to coordinate stateful AI workflows, dynamic agent hierarchies, and concurrent question evaluation natively on OTP.

Built directly on top of [`TypeSafeSDK`](https://hex.pm/packages/typesafe_sdk) (0.4.0+), Plexus couples TypeSafe's prepared contract evaluations with OTP process primitives. Instead of adding heavy orchestration layers or separate runtimes, Plexus lets you model AI agents as lightweight, supervised BEAM processes that can recursively reason, branch, and fan out semantic work.

---

## Why Plexus?

Modern AI applications often require more than static, one-shot prompt chains. They involve complex decision trees, iterative evaluations, and dynamic hierarchies where parent agents delegate specialized tasks to child workers.

The BEAM actor model is uniquely suited for this pattern:

- **Fault-Isolated Processes**: Every semantic agent runs in its own process. If an evaluation fails or times out, the failure is contained and handled through standard OTP supervision.
- **Asynchronous & Non-Blocking**: Agents yield evaluation requests without blocking their message queues, receiving results via clean OTP callbacks (`handle_evaluation/3`).
- **Bounded Concurrency**: Fan-outs and wide batch sweeps are throttled by a shared `Task.Supervisor`, preventing resource exhaustion and honoring rate limits.
- **Graph-Coordinated State**: Parent-child lifecycles, subtrees, and lineage metadata are tracked in a lightweight, in-memory graph service.

---

## Features

- **Native Semantic Actors**: Thin `use Plexus.Actor` wrapper built on `TypeSafeSDK.OTP.Server`.
- **Prepared Contracts & Fingerprinting**: Pre-compile question schemas using `TypeSafeSDK.prepare!/1` with stable cryptographic fingerprints for deterministic caching.
- **Batched Semantic Sweeps**: Built-in batching and streaming helpers using `TypeSafeSDK.evaluate_stream/4` and `TypeSafeSDK.evaluate_many/4`.
- **Run Coordinator**: Scoped supervisor (`Plexus.Run`) managing actor registries, graph relationships, and bounded task workers per workflow.
- **Dynamic Hierarchy & Subtree Pruning**: Spawn child actors dynamically from parent evaluation callbacks and track/prune entire actor subtrees.
- **First-Class Telemetry**: Telemetry hooks for run lifecycles, actor transitions, batch throughput, and contract evaluation timings.

---

## Architecture

Each run in Plexus isolates an actor population and its execution resources under a unified supervision tree:

```text
Plexus.Run (Coordinator)
├── shared TypeSafeSDK.Client     (HTTP connection pool)
├── shared Task.Supervisor        (bounded concurrent evaluation)
├── DynamicSupervisor             (semantic actor population)
├── Registry                      (local actor addressing)
└── Graph                         (parent/child/subtree relationships)

Actors (use Plexus.Actor)
└── TypeSafeSDK.OTP.Server
    ├── return {:evaluate, ...} without blocking the mailbox
    ├── handle_evaluation/3 receives structured responses
    └── recursively spawn child actors or trigger batched sweeps
```

---

## Installation

Add `plexus` to your list of dependencies in `mix.exs`:

```elixir
def deps do
  [
    {:plexus, "~> 0.1.0"},
    {:typesafe_sdk, "~> 0.4.0"}
  ]
end
```

---

## Quick Start

### 1. Define a Semantic Actor

Use `Plexus.Actor` to define an actor that evaluates incoming data against a prepared contract:

```elixir
defmodule MyApp.TicketCoordinator do
  use Plexus.Actor

  @impl true
  def init(%{ticket: text} = args) do
    prepared =
      TypeSafeSDK.prepare!(
        department:
          TypeSafeSDK.choice(
            "Route this support ticket to the appropriate department.",
            billing: "Invoices, refunds, subscription issues",
            technical: "Bugs, error messages, system downtime",
            account: "Password resets, authentication, access"
          ),
        urgent: TypeSafeSDK.noul("Does this issue require urgent escalation?")
      )

    {:ok, %{ticket: text, context: args, prepared: prepared, result: nil}}
  end

  @impl true
  def handle_cast(:triage, state) do
    # Initiate an asynchronous, non-blocking evaluation
    {:evaluate, {:triage, %{text: state.ticket}, state.prepared}, state}
  end

  @impl true
  def handle_evaluation({:triage, _state_input}, response, state) do
    department = TypeSafeSDK.Response.fetch!(response, :department)
    urgent? = TypeSafeSDK.Response.fetch!(response, :urgent)

    # Make decisions or spawn child worker actors based on semantic result
    {:noreply, %{state | result: %{department: department, urgent: urgent?}}}
  end
end
```

### 2. Start a Run and Dispatch Work

```elixir
# 1. Initialize the TypeSafe client
client = TypeSafeSDK.new_client(api_key: System.fetch_env!("TYPESAFE_API_KEY"))

# 2. Start an isolated run supervisor
{:ok, run} = Plexus.start_run(id: :support_queue, client: client)

# 3. Spawn a root actor
{:ok, actor} =
  Plexus.start_actor(run,
    module: MyApp.TicketCoordinator,
    actor_id: {:ticket, 4821},
    init_arg: %{ticket: "Checkout is failing with 500 internal server error."}
  )

# 4. Trigger actor work
Plexus.cast(actor, :triage)
```

---

## Documentation & Guides

Explore the comprehensive guides for architecture details and advanced recipes:

| Guide | Description |
| :--- | :--- |
| [**Getting Started**](guides/getting-started.md) | Step-by-step walkthrough for your first Plexus workflow. |
| [**Architecture Overview**](guides/architecture.md) | Deep dive into the actor model, supervision, and graph topology. |
| [**Actor Runtime & Lifecycle**](guides/actor-runtime.md) | Non-blocking evaluations, OTP semantics, and process lifecycle. |
| [**TypeSafe Integration**](guides/typesafe-integration.md) | Working with prepared contracts, choices, nouls, and scores. |
| [**Graph & Subtrees**](guides/graph-and-subtrees.md) | Managing dynamic parent-child agent swarms and tree pruning. |
| [**Testing & Release**](guides/testing-and-release.md) | Best practices for testing semantic actors with stubs and fixtures. |

---

## License

Plexus is open source software released under the [MIT License](LICENSE).

