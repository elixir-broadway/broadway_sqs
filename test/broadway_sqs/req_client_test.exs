defmodule BroadwaySQS.ReqClientTest do
  use ExUnit.Case, async: true

  import ExUnit.CaptureLog

  alias Broadway.Message
  alias BroadwaySQS.ReqClient

  @config [
    access_key_id: "access-key",
    secret_access_key: "secret-key",
    region: "eu-west-1"
  ]

  setup do
    Req.Test.set_req_test_from_context(__MODULE__)
    Req.Test.stub(__MODULE__, fn conn -> Req.Test.json(conn, %{}) end)
    :ok
  end

  test "receives messages and preserves Broadway metadata" do
    queue_url = queue_url()

    Req.Test.expect(__MODULE__, fn conn ->
      payload = Jason.decode!(Req.Test.raw_body(conn))

      assert payload == %{
               "QueueUrl" => queue_url,
               "MaxNumberOfMessages" => 2,
               "WaitTimeSeconds" => 10,
               "VisibilityTimeout" => 30,
               "AttributeNames" => ["ApproximateReceiveCount"],
               "MessageAttributeNames" => ["TestAttribute"]
             }

      response = %{
        "Messages" => [
          %{
            "MessageId" => "message-id",
            "ReceiptHandle" => "receipt-handle",
            "MD5OfBody" => "body-md5",
            "Body" => "hello",
            "Attributes" => %{"ApproximateReceiveCount" => "5"},
            "MessageAttributes" => %{
              "TestAttribute" => %{
                "StringValue" => "test",
                "DataType" => "String"
              }
            }
          }
        ]
      }

      Req.Test.json(conn, response)
    end)

    {:ok, opts} = ReqClient.init(opts(queue_url))
    [message] = ReqClient.receive_messages(2, opts)

    assert %Message{data: "hello"} = message
    assert message.metadata.message_id == "message-id"
    assert message.metadata.receipt_handle == "receipt-handle"
    assert message.metadata.md5_of_body == "body-md5"
    assert message.metadata.attributes == %{"approximate_receive_count" => 5}

    assert message.metadata.message_attributes == %{
             "TestAttribute" => %{
               name: "TestAttribute",
               data_type: "String",
               string_value: "test",
               binary_value: "",
               value: "test"
             }
           }
  end

  test "logs receive errors" do
    queue_url = queue_url()

    Req.Test.expect(__MODULE__, fn conn ->
      conn
      |> Plug.Conn.put_status(500)
      |> Req.Test.json(%{"message" => "failure"})
    end)

    {:ok, opts} = ReqClient.init(opts(queue_url))

    log = capture_log(fn -> assert ReqClient.receive_messages(1, opts) == [] end)
    assert log =~ "Unable to fetch events from AWS queue #{queue_url}"
  end

  test "acknowledges messages in delete batches" do
    queue_url = queue_url()
    test_pid = self()

    Req.Test.expect(__MODULE__, fn conn ->
      body = Req.Test.raw_body(conn)
      assert Plug.Conn.get_req_header(conn, "x-amz-target") == ["AmazonSQS.DeleteMessageBatch"]

      send(test_pid, {:delete_payload, Jason.decode!(body)})
      Req.Test.json(conn, %{"Successful" => []})
    end)

    {:ok, opts} = ReqClient.init(opts(queue_url, on_success: :ack))
    put_ack_options(opts)

    message = message(opts.ack_ref, "1", "receipt-1")
    ReqClient.ack(opts.ack_ref, [message], [])

    assert_received {:delete_payload,
                     %{
                       "QueueUrl" => ^queue_url,
                       "Entries" => [%{"Id" => "1", "ReceiptHandle" => "receipt-1"}]
                     }}
  end

  test "nacks messages by changing their visibility" do
    queue_url = queue_url()
    test_pid = self()

    Req.Test.expect(__MODULE__, fn conn ->
      body = Req.Test.raw_body(conn)

      assert Plug.Conn.get_req_header(conn, "x-amz-target") == [
               "AmazonSQS.ChangeMessageVisibilityBatch"
             ]

      send(test_pid, {:visibility_payload, Jason.decode!(body)})
      Req.Test.json(conn, %{"Successful" => []})
    end)

    {:ok, opts} = ReqClient.init(opts(queue_url, on_failure: {:nack, 12}))
    put_ack_options(opts)

    message = message(opts.ack_ref, "1", "receipt-1")
    ReqClient.ack(opts.ack_ref, [], [message])

    assert_received {:visibility_payload,
                     %{
                       "QueueUrl" => ^queue_url,
                       "Entries" => [
                         %{
                           "Id" => "1",
                           "ReceiptHandle" => "receipt-1",
                           "VisibilityTimeout" => 12
                         }
                       ]
                     }}
  end

  test "per-message acknowledgement options override producer defaults" do
    queue_url = queue_url()
    test_pid = self()

    Req.Test.expect(__MODULE__, 2, fn conn ->
      body = Req.Test.raw_body(conn)
      send(test_pid, {Plug.Conn.get_req_header(conn, "x-amz-target"), Jason.decode!(body)})
      Req.Test.json(conn, %{"Successful" => []})
    end)

    {:ok, opts} = ReqClient.init(opts(queue_url, on_success: :noop, on_failure: :noop))
    put_ack_options(opts)

    success = message(opts.ack_ref, "success", "success-receipt")
    failure = message(opts.ack_ref, "failure", "failure-receipt")

    success = Message.configure_ack(success, on_success: :ack)
    failure = Message.configure_ack(failure, on_failure: {:nack, 7})

    ReqClient.ack(opts.ack_ref, [success], [failure])

    assert_received {
      ["AmazonSQS.DeleteMessageBatch"],
      %{"Entries" => [%{"Id" => "success"}]}
    }

    assert_received {
      ["AmazonSQS.ChangeMessageVisibilityBatch"],
      %{"Entries" => [%{"Id" => "failure", "VisibilityTimeout" => 7}]}
    }
  end

  test "converts all supported SQS attributes" do
    queue_url = queue_url()

    Req.Test.expect(__MODULE__, fn conn ->
      response = %{
        "Messages" => [
          %{
            "MessageId" => "id",
            "ReceiptHandle" => "receipt",
            "Body" => "body",
            "Attributes" => %{
              "SenderId" => "sender",
              "SentTimestamp" => "123",
              "ApproximateReceiveCount" => "5",
              "ApproximateFirstReceiveTimestamp" => "456",
              "SequenceNumber" => "sequence",
              "MessageDeduplicationId" => "deduplication",
              "MessageGroupId" => "group",
              "AWSTraceHeader" => "trace"
            }
          }
        ]
      }

      Req.Test.json(conn, response)
    end)

    {:ok, opts} = ReqClient.init(opts(queue_url, attribute_names: :all))
    [message] = ReqClient.receive_messages(1, opts)

    assert message.metadata.attributes == %{
             "sender_id" => "sender",
             "sent_timestamp" => 123,
             "approximate_receive_count" => 5,
             "approximate_first_receive_timestamp" => 456,
             "sequence_number" => "sequence",
             "message_deduplication_id" => "deduplication",
             "message_group_id" => "group",
             "aws_trace_header" => "trace"
           }
  end

  test "handles empty and missing Messages responses" do
    queue_url = queue_url()

    Req.Test.expect(__MODULE__, 2, fn conn ->
      response = if Req.Test.raw_body(conn) == "missing", do: %{}, else: %{"Messages" => []}
      Req.Test.json(conn, response)
    end)

    {:ok, opts} = ReqClient.init(opts(queue_url))
    assert ReqClient.receive_messages(1, opts) == []

    assert ReqClient.receive_messages(1, opts) == []
  end

  test "caps receive demand at ten messages" do
    queue_url = queue_url()

    Req.Test.expect(__MODULE__, fn conn ->
      body = Req.Test.raw_body(conn)
      assert Jason.decode!(body)["MaxNumberOfMessages"] == 10
      Req.Test.json(conn, %{"Messages" => []})
    end)

    {:ok, opts} = ReqClient.init(opts(queue_url))
    assert ReqClient.receive_messages(100, opts) == []
  end

  test "omits unset receive options" do
    queue_url = queue_url()

    Req.Test.expect(__MODULE__, fn conn ->
      body = Req.Test.raw_body(conn)

      assert Jason.decode!(body) == %{
               "QueueUrl" => queue_url,
               "MaxNumberOfMessages" => 1
             }

      Req.Test.json(conn, %{"Messages" => []})
    end)

    {:ok, opts} =
      ReqClient.init(
        opts(queue_url,
          wait_time_seconds: nil,
          visibility_timeout: nil,
          attribute_names: nil,
          message_attribute_names: nil
        )
      )

    assert ReqClient.receive_messages(1, opts) == []
  end

  test "splits delete acknowledgements into batches of ten" do
    queue_url = queue_url()
    test_pid = self()

    Req.Test.expect(__MODULE__, 2, fn conn ->
      body = Req.Test.raw_body(conn)
      send(test_pid, {:delete_batch, Jason.decode!(body)["Entries"]})
      Req.Test.json(conn, %{"Successful" => []})
    end)

    {:ok, opts} = ReqClient.init(opts(queue_url))
    put_ack_options(opts)

    messages = Enum.map(1..11, &message(opts.ack_ref, to_string(&1), "receipt-#{&1}"))
    ReqClient.ack(opts.ack_ref, messages, [])

    assert_received {:delete_batch, first_batch}
    assert_received {:delete_batch, second_batch}
    assert length(first_batch) == 10
    assert length(second_batch) == 1
  end

  test "splits visibility changes into batches of ten" do
    queue_url = queue_url()
    test_pid = self()

    Req.Test.expect(__MODULE__, 2, fn conn ->
      body = Req.Test.raw_body(conn)
      send(test_pid, {:visibility_batch, Jason.decode!(body)["Entries"]})
      Req.Test.json(conn, %{"Successful" => []})
    end)

    {:ok, opts} = ReqClient.init(opts(queue_url, on_failure: {:nack, 3}))
    put_ack_options(opts)

    messages = Enum.map(1..11, &message(opts.ack_ref, to_string(&1), "receipt-#{&1}"))
    ReqClient.ack(opts.ack_ref, [], messages)

    assert_received {:visibility_batch, first_batch}
    assert_received {:visibility_batch, second_batch}
    assert length(first_batch) == 10
    assert length(second_batch) == 1
  end

  test "configure merges acknowledgement options" do
    ack_data = %{receipt: %{id: "id", receipt_handle: "receipt"}, on_success: :noop}

    assert {:ok, configured} =
             ReqClient.configure(:ack_ref, ack_data, on_success: :ack, on_failure: :noop)

    assert configured == %{
             receipt: %{id: "id", receipt_handle: "receipt"},
             on_success: :ack,
             on_failure: :noop
           }
  end

  defp opts(queue_url, overrides \\ []) do
    Keyword.merge(
      [
        broadway: [name: unique_ack_ref()],
        queue_url: queue_url,
        config: Keyword.put(@config, :plug, {Req.Test, __MODULE__}),
        on_success: :ack,
        on_failure: :noop,
        max_number_of_messages: 10,
        wait_time_seconds: 10,
        visibility_timeout: 30,
        attribute_names: [:approximate_receive_count],
        message_attribute_names: ["TestAttribute"]
      ],
      overrides
    )
  end

  defp message(ack_ref, id, receipt_handle) do
    %Message{
      data: "data",
      acknowledger: {ReqClient, ack_ref, %{receipt: %{id: id, receipt_handle: receipt_handle}}}
    }
  end

  defp put_ack_options(opts) do
    :persistent_term.put(opts.ack_ref, %{
      queue_url: opts.queue_url,
      config: opts.config,
      on_success: opts.on_success,
      on_failure: opts.on_failure
    })
  end

  defp unique_ack_ref, do: {__MODULE__, make_ref()}

  defp queue_url, do: "https://sqs.eu-west-1.amazonaws.com/123456789012/test-queue"
end
