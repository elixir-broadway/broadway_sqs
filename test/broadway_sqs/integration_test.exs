defmodule BroadwaySQS.BroadwaySQS.IntegrationTest do
  use ExUnit.Case, async: false

  alias Plug.Conn

  defmodule MyConsumer do
    use Broadway

    def start_link(opts), do: GenServer.start_link(__MODULE__, opts)

    def init(opts) do
      {:ok, opts}
    end

    def handle_message(_, message, %{my_pid: my_pid}) do
      send(my_pid, {:message_handled, message.data, message.metadata})
      message
    end

    def handle_batch(_, messages, _, %{my_pid: my_pid}) do
      send(my_pid, {:batch_handled, messages})
      messages
    end
  end

  @receive_message_response """
  {
    "Messages": [
      {
        "MessageId": "7cd4d61a-2d9a-4922-9738-308af6126fea",
        "ReceiptHandle": "receipt-handle-1",
        "MD5OfBody": "8cd6cfc2639481fee178bd04dd3628a7",
        "Body": "hello world"
      },
      {
        "MessageId": "c431bcb8-3275-4cbb-a4a1-7bcbbc5773d1",
        "ReceiptHandle": "receipt-handle-2",
        "MD5OfBody": "35179a54ea587953021400eb0cd23201",
        "Body": "how are you?"
      }
    ]
  }
  """

  # AWS omits "Messages" when the receive returns nothing.
  @receive_message_empty_response "{}"

  @delete_message_response """
  {
    "Successful": [
      {"Id": "my-delete-message-batch-id-1"},
      {"Id": "my-delete-message-batch-id-2"}
    ],
    "Failed": []
  }
  """

  defmodule RequestCounter do
    use Agent

    def start_link(counters) do
      Agent.start_link(fn -> counters end, name: __MODULE__)
    end

    def count_for(request_name) do
      if Process.whereis(__MODULE__) do
        Agent.get(__MODULE__, & &1[request_name])
      end
    end

    def increment_for(request_name) do
      if Process.whereis(__MODULE__) do
        Agent.update(__MODULE__, &Map.put(&1, request_name, &1[request_name] + 1))
      end
    end
  end

  setup do
    bypass = Bypass.open()

    Application.put_env(:ex_aws, :sqs,
      scheme: "http",
      host: "localhost",
      port: bypass.port
    )

    on_exit(fn -> Application.delete_env(:ex_aws, :sqs) end)

    {:ok, bypass: bypass}
  end

  test "consume messages from SQS and ack it", %{bypass: bypass} do
    us = self()

    Bypass.expect(bypass, fn conn ->
      {"x-amz-target", action} = List.keyfind(conn.req_headers, "x-amz-target", 0)
      {:ok, _body, conn} = Plug.Conn.read_body(conn)

      response =
        case action do
          "AmazonSQS.ReceiveMessage" ->
            if RequestCounter.count_for(:receive_message) > 5 do
              @receive_message_empty_response
            else
              RequestCounter.increment_for(:receive_message)
              @receive_message_response
            end

          "AmazonSQS.DeleteMessageBatch" ->
            RequestCounter.increment_for(:delete_message_batch)
            send(us, :messages_deleted)
            @delete_message_response
        end

      conn
      |> Conn.put_resp_header("content-type", "application/x-amz-json-1.0")
      |> Conn.resp(200, response)
    end)

    {:ok, _} = RequestCounter.start_link(%{receive_message: 0, delete_message_batch: 0})

    {:ok, _consumer} = start_fake_consumer(bypass)

    assert_receive {:message_handled, "hello world", %{"ReceiptHandle" => "receipt-handle-1"}},
                   1_000

    assert_receive {:message_handled, "how are you?", %{"ReceiptHandle" => "receipt-handle-2"}}

    assert_receive {:batch_handled, _messages}

    assert_receive :messages_deleted
    assert_receive :messages_deleted
    assert_receive :messages_deleted

    assert RequestCounter.count_for(:receive_message) == 6
    assert RequestCounter.count_for(:delete_message_batch) == 3
  end

  defp start_fake_consumer(bypass) do
    Broadway.start_link(MyConsumer,
      name: MyConsumer,
      producer: [
        module:
          {BroadwaySQS.Producer,
           sqs_client: BroadwaySQS.ExAwsClient,
           max_number_of_messages: 2,
           config: [
             access_key_id: "MY_AWS_ACCESS_KEY_ID",
             secret_access_key: "MY_AWS_SECRET_ACCESS_KEY",
             region: "us-east-2"
           ],
           queue_url: queue_endpoint_url(bypass)},
        concurrency: 1
      ],
      processors: [
        default: [concurrency: 1]
      ],
      batchers: [
        default: [batch_size: 4, batch_timeout: 2000]
      ],
      context: %{my_pid: self()}
    )
  end

  defp queue_endpoint_url(bypass) do
    "http://localhost:#{bypass.port}/my_queue"
  end
end
