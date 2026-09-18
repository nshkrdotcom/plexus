defmodule Plexus.MixProject do
  use Mix.Project

  @version "0.1.0"
  @source_url "https://github.com/nshkrdotcom/plexus"

  def project do
    [
      app: :plexus,
      version: @version,
      elixir: "~> 1.18",
      start_permanent: Mix.env() == :prod,
      deps: deps(),
      description: description(),
      package: package(),
      docs: docs(),
      source_url: @source_url,
      homepage_url: @source_url,
      name: "Plexus",
      aliases: aliases(),
      test_coverage: [tool: ExCoveralls]
    ]
  end

  def cli do
    [
      preferred_envs: [
        docs: :docs,
        "hex.publish": :docs,
        quality: :test
      ]
    ]
  end

  def application do
    [
      extra_applications: [:logger, :crypto],
      mod: {Plexus.Application, []}
    ]
  end

  defp deps do
    [
      {:typesafe_sdk, "~> 0.4.0"},
      {:inference, "~> 0.4.1"},
      {:pristine, "~> 0.4.0"},
      {:telemetry, "~> 1.3"},
      {:jason, "~> 1.4"},
      {:ex_doc, "~> 0.38", only: :docs, runtime: false},
      {:credo, "~> 1.7", only: [:dev, :test], runtime: false},
      {:dialyxir, "~> 1.4", only: [:dev], runtime: false},
      {:excoveralls, "~> 0.18", only: :test, runtime: false}
    ]
  end

  defp description do
    "BEAM-native semantic actor substrate built on TypeSafeSDK 0.4.0 for bounded fan-out, recursive decisions, and graph-coordinated semantic work."
  end

  defp package do
    [
      licenses: ["MIT"],
      maintainers: ["nshkrdotcom"],
      links: %{
        "GitHub" => @source_url,
        "Guides" => "#{@source_url}/tree/main/guides",
        "TypeSafeSDK" => "https://hex.pm/packages/typesafe_sdk"
      },
      files: [
        ".formatter.exs",
        "CHANGELOG.md",
        "HANDOFF.md",
        "LICENSE",
        "README.md",
        "assets",
        "config",
        "experiments",
        "artifacts",
        "guides",
        "lib",
        "mix.exs",
        "test"
      ]
    ]
  end

  defp docs do
    [
      main: "readme",
      name: "Plexus",
      source_ref: "v#{@version}",
      source_url: @source_url,
      assets: %{"assets" => "assets"},
      logo: "assets/plexus.svg",
      extras: [
        "README.md",
        "guides/index.md",
        "guides/getting-started.md",
        "guides/architecture.md",
        "guides/kernel-primitives.md",
        "guides/actor-runtime.md",
        "guides/typesafe-integration.md",
        "guides/graph-and-subtrees.md",
        "guides/calibration-and-replay.md",
        "guides/expansion.md",
        "guides/testing-and-release.md",
        "guides/experiments.md",
        "HANDOFF.md",
        "CHANGELOG.md",
        "LICENSE"
      ],
      groups_for_extras: [
        Overview: ["README.md"],
        Guides: Path.wildcard("guides/*.md"),
        Project: ["HANDOFF.md", "CHANGELOG.md", "LICENSE"]
      ],
      groups_for_modules: [
        Core: [
          Plexus,
          Plexus.Application,
          Plexus.Run,
          Plexus.Registry,
          Plexus.Graph,
          Plexus.Contract
        ],
        Actor: [
          Plexus.Actor,
          Plexus.Actor.Command,
          Plexus.Actor.Context,
          Plexus.Actor.Interpreter
        ],
        Kernel: [
          Plexus.Budget,
          Plexus.Cache,
          Plexus.Event,
          Plexus.Population,
          Plexus.Provenance,
          Plexus.Record,
          Plexus.Schedule,
          Plexus.Stop,
          Plexus.Reduce,
          Plexus.Belief,
          Plexus.Belief.Calibration,
          Plexus.Measure,
          Plexus.Measure.Coalescer,
          Plexus.Contract.Registry
        ],
        Expansion: [
          Plexus.Expand.Adapter,
          Plexus.Expand.Queue,
          Plexus.Expand.Schema,
          Plexus.Expand.Materializer
        ],
        Strategy: [Plexus.Strategy.Fanout],
        Examples: [
          Plexus.Examples.IntakeCoordinator,
          Plexus.Examples.EvidenceWorker,
          Plexus.Examples.Sample
        ]
      ]
    ]
  end

  defp aliases do
    [
      quality: ["format --check-formatted", "compile --warnings-as-errors", "test"]
    ]
  end
end
