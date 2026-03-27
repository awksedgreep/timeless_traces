defmodule TimelessTraces.MixProject do
  use Mix.Project

  def project do
    [
      app: :timeless_traces,
      version: "1.3.2",
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
      extra_applications: [:logger, :inets, :ssl, :public_key, :crypto],
      mod: {TimelessTraces.Application, []}
    ]
  end

  defp deps do
    [
      {:ezstd, "~> 1.2"},
      {:exqlite, "~> 0.27"},
      {:opentelemetry_api, "~> 1.4"},
      {:opentelemetry, "~> 1.5"},
      {:opentelemetry_exporter, "~> 1.7"},
      {:rocket, "~> 0.2"},
      {:telemetry, "~> 1.0"},
      {:ex_doc, "~> 0.34", only: :dev, runtime: false},
      {:ex_openzl, "~> 0.4.0"}
    ]
  end

  defp package do
    [
      maintainers: ["Mark Cotner"],
      licenses: ["MIT"],
      links: %{"GitHub" => "https://github.com/awksedgreep/timeless_traces"},
      files: ~w(lib .formatter.exs mix.exs README.md LICENSE)
    ]
  end

  defp docs do
    [
      main: "readme",
      extras:
        ["README.md", "LICENSE"] ++
          Path.wildcard("docs/*.md")
    ]
  end
end
