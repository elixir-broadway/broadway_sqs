defmodule BroadwaySQS.SQSClient do
  @moduledoc """
  A generic behaviour to implement SQS Clients for `BroadwaySQS.Producer`.

  This module defines callbacks to normalize options and receive message
  from a SQS queue. Modules that implement this behaviour should be passed
  as the `:sqs_client` option from `BroadwaySQS.Producer`.
  """

  alias Broadway.Message

  @type messages :: [Message.t()]

  @doc """
  Should initialize the SQS client.

  This callback is called when the producer is started. It should return
  `{:ok, normalized_opts}` if the client was initialized successfully or
  `{:error, reason}` if there was an error. `normalized_opts` is what gets
  passed to the `c:receive_messages/2` callback.
  """
  @callback init(opts :: any) :: {:ok, normalized_opts :: any} | {:error, reason :: binary}

  @doc """
  Should fetch messages from the configured SQS queue.

  `opts` is the normalized options returned from `c:init/1`. The `demand`
  argument is the number of messages that the producer is asking for.

  Implementations may return `{:error, reason}` when the request fails. The
  producer treats it as an empty receive and adds `error: reason` to the
  `[:broadway_sqs, :receive_messages, :stop]` telemetry metadata.
  """
  @callback receive_messages(demand :: pos_integer, opts :: any) ::
              messages | {:error, reason :: term}
end
