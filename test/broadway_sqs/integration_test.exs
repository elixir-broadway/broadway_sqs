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

  @receive_message_response %{
    "Messages" => [
      %{
        "MessageId" => "7cd4d61a-2d9a-4922-9738-308af6126fea",
        "ReceiptHandle" => "receipt-handle-1",
        "MD5OfBody" => "8cd6cfc2639481fee178bd04dd3628a7",
        "Body" => "hello world"
      },
      %{
        "MessageId" => "c431bcb8-3275-4cbb-a4a1-7bcbbc5773d1",
        "ReceiptHandle" => "receipt-handle-2",
        "MD5OfBody" => "35179a54ea587953021400eb0cd23201",
        "Body" => "how are you?"
      }
    ]
  }

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
    Req.Test.set_req_test_to_shared()
    :ok
  end

  test "consume messages from SQS and ack it" do
    us = self()

    Req.Test.expect(__MODULE__, 9, fn conn ->
      payload = Jason.decode!(Req.Test.raw_body(conn))
      action = List.first(Conn.get_req_header(conn, "x-amz-target"))

      response =
        case action do
          "AmazonSQS.ReceiveMessage" ->
            if RequestCounter.count_for(:receive_message) > 5 do
              %{}
            else
              RequestCounter.increment_for(:receive_message)
              @receive_message_response
            end

          "AmazonSQS.DeleteMessageBatch" ->
            assert payload["Entries"]
            RequestCounter.increment_for(:delete_message_batch)
            send(us, :messages_deleted)
            %{"Successful" => Enum.map(payload["Entries"], &Map.take(&1, ["Id"]))}
        end

      Req.Test.json(conn, response)
    end)

    {:ok, _} = RequestCounter.start_link(%{receive_message: 0, delete_message_batch: 0})

    {:ok, _consumer} = start_fake_consumer()

    assert_receive {:message_handled, "hello world", %{receipt_handle: "receipt-handle-1"}}, 1_000
    assert_receive {:message_handled, "how are you?", %{receipt_handle: "receipt-handle-2"}}

    assert_receive {:batch_handled, _messages}

    assert_receive :messages_deleted
    assert_receive :messages_deleted
    assert_receive :messages_deleted

    assert RequestCounter.count_for(:receive_message) == 6
    assert RequestCounter.count_for(:delete_message_batch) == 3
  end

  defp start_fake_consumer do
    Broadway.start_link(MyConsumer,
      name: MyConsumer,
      producer: [
        module:
          {BroadwaySQS.Producer,
           sqs_client: BroadwaySQS.ReqClient,
           max_number_of_messages: 2,
           config: [
             access_key_id: "MY_AWS_ACCESS_KEY_ID",
             secret_access_key: "MY_AWS_SECRET_ACCESS_KEY",
             region: "us-east-2",
             plug: {Req.Test, __MODULE__}
           ],
           queue_url: queue_endpoint_url()},
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

  defp queue_endpoint_url, do: "https://sqs.us-east-2.amazonaws.com/123456789012/my_queue"
end
