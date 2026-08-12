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
      package: package()
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
      {:req, "~> 0.7.2"},
      {:aws_credentials, "~> 1.0"},
      {:nimble_options, "~> 0.3.7 or ~> 0.4 or ~> 1.0"},
      {:telemetry, "~> 0.4.3 or ~> 1.0"},
      {:plug, "~> 1.18", only: :test},
      {:ex_doc, ">= 0.19.0", only: :docs}
    ]
  end

  defp docs do
    [
      main: "BroadwaySQS.Producer",
      source_ref: "v#{@version}",
      source_url: "https://github.com/dashbitco/broadway_sqs"
    ]
  end

  defp package do
    %{
      licenses: ["Apache-2.0"],
      links: %{"GitHub" => "https://github.com/dashbitco/broadway_sqs"}
    }
  end
end
