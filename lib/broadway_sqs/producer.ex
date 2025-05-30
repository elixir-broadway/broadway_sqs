defmodule BroadwaySQS.Producer do
  @moduledoc """
  A GenStage producer that continuously polls messages from a SQS queue and
  acknowledge them after being successfully processed.

  By default this producer uses `BroadwaySQS.ExAwsClient` to talk to SQS but
  you can provide your client by implementing the `BroadwaySQS.SQSClient`
  behaviour.

  For a quick getting started on using Broadway with Amazon SQS, please see
  the [Amazon SQS Guide](https://hexdocs.pm/broadway/amazon-sqs.html).

  ## Options

  Aside from `:receive_interval` and `:sqs_client` which are generic and apply to all
  producers (regardless of the client implementation), all other options are specific to
  the `BroadwaySQS.ExAwsClient`, which is the default client.

  #{NimbleOptions.docs(BroadwaySQS.Options.definition())}

  ## Acknowledgments

  You can use the `:on_success` and `:on_failure` options to control how messages are
  acked on SQS. You can set these options when starting the SQS producer or change them
  for each message through `Broadway.Message.configure_ack/2`. By default, successful
  messages are acked (`:ack`) and failed messages are not (`:noop`).

  The possible values for `:on_success` and `:on_failure` are:

    * `:ack` - acknowledge the message. SQS will delete the message from the queue
      and will not redeliver it to any other consumer.

    * `:noop` - do not acknowledge the message. SQS will eventually redeliver the message
      or remove it based on the "Visibility Timeout" and "Max Receive Count" configurations.

    * `{:nack, timeout}` - change the message visibility timeout to `timeout` seconds
      (0 to 43_200). The message will become available again for processing after
      the given amount of time.

    For more information, see:

      * ["Visibility Timeout" page on Amazon SQS](https://docs.aws.amazon.com/AWSSimpleQueueService/latest/SQSDeveloperGuide/sqs-visibility-timeout.html)
      * ["Dead Letter Queue" page on Amazon SQS](https://docs.aws.amazon.com/AWSSimpleQueueService/latest/SQSDeveloperGuide/sqs-dead-letter-queues.html)

  ### Batching

  Even if you are not interested in working with Broadway batches via the
  `handle_batch/3` callback, we recommend all Broadway pipelines with SQS
  producers to define a default batcher with `batch_size` set to 10, so
  messages can be acknowledged in batches, which improves the performance
  and reduce the costs of integrating with SQS.

  ## Example

      Broadway.start_link(MyBroadway,
        name: MyBroadway,
        producer: [
          module: {BroadwaySQS.Producer,
            queue_url: "https://sqs.amazonaws.com/0000000000/my_queue",
            config: [
              access_key_id: "YOUR_AWS_ACCESS_KEY_ID",
              secret_access_key: "YOUR_AWS_SECRET_ACCESS_KEY",
              region: "us-east-2"
            ]
          }
        ],
        processors: [
          default: []
        ],
        batchers: [
          default: [
            batch_size: 10,
            batch_timeout: 2000
          ]
        ]
      )

  The above configuration will set up a producer that continuously receives
  messages from `"my_queue"` and sends them downstream.

  ## Retrieving Metadata

  By default the following information is added to the `metadata` field in the
  `%Message{}` struct:

    * `message_id` - The message id received when the message was sent to the queue
    * `receipt_handle` - The receipt handle
    * `md5_of_body` - An MD5 digest of the message body

  You can access any of that information directly while processing the message:

      def handle_message(_, message, _) do
        receipt = %{
          id: message.metadata.message_id,
          receipt_handle: message.metadata.receipt_handle
        }

        # Do something with the receipt
      end

  If you want to retrieve `attributes` or `message_attributes`, you need to
  configure the `:attributes_names` and `:message_attributes_names` options
  accordingly, otherwise, attributes will not be attached to the response and
  will not be available in the `metadata` field

      producer: [
        module: {BroadwaySQS.Producer,
          queue_url: "https://sqs.amazonaws.com/0000000000/my_queue",
          # Define which attributes/message_attributes you want to be attached
          attribute_names: [:approximate_receive_count],
          message_attribute_names: ["SomeAttribute"]
        }
      ]

  and then in `handle_message`:

      def handle_message(_, message, _) do
        approximate_receive_count = message.metadata.attributes["approximate_receive_count"]
        some_attribute = message.metadata.message_attributes["SomeAttribute"]

        # Do something with the attributes
      end

  For more information on the `:attributes_names` and `:message_attributes_names`
  options, see ["AttributeName.N" and "MessageAttributeName.N" on the ReceiveMessage documentation](https://docs.aws.amazon.com/AWSSimpleQueueService/latest/APIReference/API_ReceiveMessage.html)

  ## Telemetry

  This library exposes the following Telemetry events:

    * `[:broadway_sqs, :receive_messages, :start]` - Dispatched before receiving
      messages from SQS (`c:BroadwaySQS.SQSClient.receive_messages/2`)

      * measurement: `%{time: System.monotonic_time}`
      * metadata: `%{name: atom, demand: integer}`

    * `[:broadway_sqs, :receive_messages, :stop]` - Dispatched after messages have
      been received from SQS and "wrapped".

      * measurement: `%{duration: native_time}`
      * metadata:

        ```
        %{
          name: atom,
          messages: [Broadway.Message.t],
          demand: integer
        }
        ```

    * `[:broadway_sqs, :receive_messages, :exception]` - Dispatched after a failure
      while receiving messages from SQS.

      * measurement: `%{duration: native_time}`
      * metadata:

        ```
        %{
          name: atom,
          demand: integer,
          kind: kind,
          reason: reason,
          stacktrace: stacktrace
        }
        ```

  """

  use GenStage

  require Logger
  alias Broadway.Producer
  alias NimbleOptions.ValidationError

  @behaviour Producer

  @impl true
  def init(opts) do
    receive_interval = opts[:receive_interval]
    sqs_client = opts[:sqs_client]
    {:ok, client_opts} = sqs_client.init(opts)

    {:producer,
     %{
       demand: 0,
       draining: false,
       receive_timer: nil,
       receive_interval: receive_interval,
       sqs_client: {sqs_client, client_opts}
     }}
  end

  @impl true
  def prepare_for_start(_module, broadway_opts) do
    {producer_module, client_opts} = broadway_opts[:producer][:module]

    if Keyword.has_key?(client_opts, :queue_name) do
      Logger.error(
        "The option :queue_name has been removed in order to keep compatibility with " <>
          "ex_aws_sqs >= v3.0.0. Please set the queue URL using the new :queue_url option."
      )

      exit(:invalid_config)
    end

    case NimbleOptions.validate(client_opts, BroadwaySQS.Options.definition()) do
      {:error, error} ->
        raise ArgumentError, format_error(error)

      {:ok, opts} ->
        ack_ref = broadway_opts[:name]

        :persistent_term.put(ack_ref, %{
          queue_url: opts[:queue_url],
          config: opts[:config],
          on_success: opts[:on_success],
          on_failure: opts[:on_failure]
        })

        broadway_opts_with_defaults =
          put_in(broadway_opts, [:producer, :module], {producer_module, opts})

        {[], broadway_opts_with_defaults}
    end
  end

  defp format_error(%ValidationError{keys_path: [], message: message}) do
    "invalid configuration given to SQSBroadway.prepare_for_start/2, " <> message
  end

  defp format_error(%ValidationError{keys_path: keys_path, message: message}) do
    "invalid configuration given to SQSBroadway.prepare_for_start/2 for key #{inspect(keys_path)}, " <>
      message
  end

  @impl true
  def handle_demand(incoming_demand, %{demand: demand} = state) do
    handle_receive_messages(%{state | demand: demand + incoming_demand})
  end

  @impl true
  def handle_info(:receive_messages, state) do
    handle_receive_messages(%{state | receive_timer: nil})
  end

  @impl true
  def handle_info(_, state) do
    {:noreply, [], state}
  end

  @impl Producer
  def prepare_for_draining(state) do
    {:noreply, [], %{state | draining: true}}
  end

  defp handle_receive_messages(%{draining: true} = state) do
    {:noreply, [], state}
  end

  defp handle_receive_messages(%{receive_timer: nil, demand: demand} = state) when demand > 0 do
    messages = receive_messages_from_sqs(state, demand)
    new_demand = demand - length(messages)

    receive_timer =
      case {messages, new_demand} do
        {[], _} -> schedule_receive_messages(state.receive_interval)
        {_, 0} -> nil
        _ -> schedule_receive_messages(0)
      end

    {:noreply, messages, %{state | demand: new_demand, receive_timer: receive_timer}}
  end

  defp handle_receive_messages(state) do
    {:noreply, [], state}
  end

  defp receive_messages_from_sqs(state, total_demand) do
    %{sqs_client: {client, opts}} = state
    metadata = %{name: opts[:ack_ref], demand: total_demand}

    :telemetry.span(
      [:broadway_sqs, :receive_messages],
      metadata,
      fn ->
        messages = client.receive_messages(total_demand, opts)
        {messages, Map.put(metadata, :messages, messages)}
      end
    )
  end

  defp schedule_receive_messages(interval) do
    Process.send_after(self(), :receive_messages, interval)
  end
end
