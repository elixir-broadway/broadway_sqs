import Config

if config_env() == :test do
  config :aws_credentials, credential_providers: []
end
