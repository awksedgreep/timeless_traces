defmodule TimelessTraces.MixProject do
  use Mix.Project

  def project do
    [
      app: :timeless_traces,
      version: "0.3.0",
      elixir: "~> 1.18",
      start_permanent: Mix.env() == :prod,
      deps: deps(),
      description: "Embedded OpenTelemetry span storage and compression for Elixir applications.",
      source_url: "https://github.com/awksedgreep/timeless_traces",
      homepage_url: "https://github.com/awksedgreep/timeless_traces",
      package: package(),
      docs: docs()
    ]
  end

  def application do
    [
      extra_applications: [:logger],
      mod: {TimelessTraces.Application, []}
    ]
  end

  defp deps do
    [
      {:exqlite, "~> 0.27"},
      {:ezstd, "~> 1.2"},
      {:opentelemetry_api, "~> 1.4"},
      {:opentelemetry, "~> 1.5"},
      {:bandit, "~> 1.6"},
      {:plug, "~> 1.16"},
      {:jason, "~> 1.4"},
      {:ex_doc, "~> 0.34", only: :dev, runtime: false}
    ]
  end

  defp package do
    [
      maintainers: ["Matt Cotner"],
      licenses: ["MIT"],
      links: %{"GitHub" => "https://github.com/awksedgreep/timeless_traces"},
      files: ~w(lib .formatter.exs mix.exs README.md LICENSE)
    ]
  end

  defp docs do
    [
      main: "readme",
      extras: ["README.md", "LICENSE"]
    ]
  end
end
