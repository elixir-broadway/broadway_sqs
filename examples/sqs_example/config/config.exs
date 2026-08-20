use Mix.Config

config :broadway_sqs_example,
  producer_module:
    {BroadwaySQS.Producer,
     config: [
       # access_key_id: "YOUR_AWS_ACCESS_KEY_ID",
       # secret_access_key: "YOUR_AWS_SECRET_ACCESS_KEY"
       region: "us-east-2"
     ]},
  int_queue: "TEST-int-queue",
  string_queue: "TEST-string-queue",
  region: "us-east-2",
  sqs_endpoint: "https://sqs.us-east-2.amazonaws.com"

import_config "#{Mix.env()}.exs"
