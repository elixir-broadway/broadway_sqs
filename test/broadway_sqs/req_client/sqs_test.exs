defmodule BroadwaySQS.ReqClient.SQSTest do
  use ExUnit.Case, async: true

  setup context do
    Req.Test.set_req_test_from_context(context)
    :ok
  end

  alias BroadwaySQS.ReqClient.SQS

  @request_options [
    region: "eu-west-1",
    credentials: [access_key_id: "access-key", secret_access_key: "secret-key"]
  ]

  test "receive_message builds the SQS JSON payload" do
    queue_url = queue_url()

    Req.Test.expect(__MODULE__, fn conn ->
      body = Req.Test.raw_body(conn)

      assert Jason.decode!(body) == %{
               "QueueUrl" => queue_url,
               "MaxNumberOfMessages" => 2,
               "WaitTimeSeconds" => 10,
               "VisibilityTimeout" => 30,
               "AttributeNames" => ["ApproximateReceiveCount", "SenderId"],
               "MessageAttributeNames" => ["TestAttribute"]
             }

      assert Plug.Conn.get_req_header(conn, "x-amz-target") == ["AmazonSQS.ReceiveMessage"]
      Req.Test.json(conn, %{"Messages" => []})
    end)

    options = %{
      max_number_of_messages: 2,
      wait_time_seconds: 10,
      visibility_timeout: 30,
      attribute_names: [:approximate_receive_count, :sender_id],
      message_attribute_names: ["TestAttribute"]
    }

    assert {:ok, %{"Messages" => []}} =
             SQS.receive_message(
               queue_url,
               options,
               Keyword.merge(@request_options, endpoint: queue_url, plug: {Req.Test, __MODULE__})
             )
  end

  test "receive_message supports requesting all attributes" do
    queue_url = queue_url()

    Req.Test.expect(__MODULE__, fn conn ->
      body = Req.Test.raw_body(conn)
      assert Jason.decode!(body)["AttributeNames"] == ["All"]
      Req.Test.json(conn, %{"Messages" => []})
    end)

    options = %{max_number_of_messages: 1, attribute_names: :all}

    assert {:ok, %{"Messages" => []}} =
             SQS.receive_message(
               queue_url,
               options,
               Keyword.merge(@request_options, endpoint: queue_url, plug: {Req.Test, __MODULE__})
             )
  end

  test "delete_message_batch builds the delete payload" do
    queue_url = queue_url()
    entries = [%{"Id" => "1", "ReceiptHandle" => "receipt-1"}]

    Req.Test.expect(__MODULE__, fn conn ->
      body = Req.Test.raw_body(conn)

      assert Jason.decode!(body) == %{"QueueUrl" => queue_url, "Entries" => entries}
      assert Plug.Conn.get_req_header(conn, "x-amz-target") == ["AmazonSQS.DeleteMessageBatch"]
      Req.Test.json(conn, %{"Successful" => [%{"Id" => "1"}]})
    end)

    assert {:ok, %{"Successful" => [%{"Id" => "1"}]}} =
             SQS.delete_message_batch(
               queue_url,
               entries,
               Keyword.merge(@request_options, endpoint: queue_url, plug: {Req.Test, __MODULE__})
             )
  end

  test "change_message_visibility_batch builds the visibility payload" do
    queue_url = queue_url()

    entries = [
      %{
        "Id" => "1",
        "ReceiptHandle" => "receipt-1",
        "VisibilityTimeout" => 12
      }
    ]

    Req.Test.expect(__MODULE__, fn conn ->
      body = Req.Test.raw_body(conn)

      assert Jason.decode!(body) == %{"QueueUrl" => queue_url, "Entries" => entries}

      assert Plug.Conn.get_req_header(conn, "x-amz-target") == [
               "AmazonSQS.ChangeMessageVisibilityBatch"
             ]

      Req.Test.json(conn, %{"Successful" => [%{"Id" => "1"}]})
    end)

    assert {:ok, %{"Successful" => [%{"Id" => "1"}]}} =
             SQS.change_message_visibility_batch(
               queue_url,
               entries,
               Keyword.merge(@request_options, endpoint: queue_url, plug: {Req.Test, __MODULE__})
             )
  end

  defp queue_url, do: "https://sqs.eu-west-1.amazonaws.com/123456789012/test-queue"

  test "normalize_message converts SQS messages to the client format" do
    message = %{
      "MessageId" => "message-id",
      "ReceiptHandle" => "receipt-handle",
      "MD5OfBody" => "body-md5",
      "Body" => "hello",
      "Attributes" => %{
        "ApproximateReceiveCount" => "5",
        "SenderId" => "sender"
      },
      "MessageAttributes" => %{
        "TestAttribute" => %{
          "StringValue" => "test",
          "DataType" => "String"
        }
      }
    }

    assert SQS.normalize_message(message) == %{
             data: "hello",
             metadata: %{
               message_id: "message-id",
               receipt_handle: "receipt-handle",
               md5_of_body: "body-md5",
               attributes: %{
                 "approximate_receive_count" => 5,
                 "sender_id" => "sender"
               },
               message_attributes: %{
                 "TestAttribute" => %{
                   name: "TestAttribute",
                   data_type: "String",
                   string_value: "test",
                   binary_value: "",
                   value: "test"
                 }
               }
             },
             receipt: %{id: "message-id", receipt_handle: "receipt-handle"}
           }
  end
end
