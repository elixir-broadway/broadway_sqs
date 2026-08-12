defmodule BroadwaySqs.MixProject do
  use Mix.Project

  @version "0.7.4"
  @description "A SQS connector for Broadway"

  def project do
    [
      app: :broadway_sqs,
      version: @version,
      elixir: "~> 1.16",
      name: "BroadwaySQS",
      description: @description,
      start_permanent: Mix.env() == :prod,
      deps: deps(),
      docs: docs(),
      package: package(),
      hex: [
        ignore_advisories: ["EEF-CVE-2026-43966", "EEF-CVE-2026-43969"]
      ]
    ]
  end

  def application do
    [
      extra_applications: [:logger]
    ]
  end

  defp deps do
    [
      {:broadway, "~> 1.0"},
      {:ex_aws_sqs, "~> 5.0", hex: :beamlab_ex_aws_sqs},
      {:nimble_options, "~> 0.3.7 or ~> 0.4 or ~> 1.0"},
      {:telemetry, "~> 0.4.3 or ~> 1.0"},
      {:jason, "~> 1.0", optional: true},
      {:hackney, "~> 4.0", only: [:dev, :test]},
      {:bypass, "~> 2.1.0", only: :test},
      {:ex_doc, ">= 0.19.0", only: :docs}
    ]
  end

  defp docs do
    [
      main: "BroadwaySQS.Producer",
      source_ref: "v#{@version}",
      source_url: "https://github.com/elixir-broadway/broadway_sqs"
    ]
  end

  defp package do
    %{
      licenses: ["Apache-2.0"],
      links: %{"GitHub" => "https://github.com/elixir-broadway/broadway_sqs"}
    }
  end
end
