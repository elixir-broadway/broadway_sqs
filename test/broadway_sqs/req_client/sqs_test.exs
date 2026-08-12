defmodule BroadwaySQS.ReqClient.SQSTest do
  use ExUnit.Case, async: true

  alias BroadwaySQS.ReqClient.SQS

  @request_options [
    region: "eu-west-1",
    credentials: [access_key_id: "access-key", secret_access_key: "secret-key"]
  ]

  test "receive_message builds the SQS JSON payload" do
    bypass = Bypass.open()
    queue_url = "http://localhost:#{bypass.port}/queue"

    Bypass.expect_once(bypass, "POST", "/queue", fn conn ->
      {:ok, body, conn} = Plug.Conn.read_body(conn)

      assert Jason.decode!(body) == %{
               "QueueUrl" => queue_url,
               "MaxNumberOfMessages" => 2,
               "WaitTimeSeconds" => 10,
               "VisibilityTimeout" => 30,
               "AttributeNames" => ["ApproximateReceiveCount", "SenderId"],
               "MessageAttributeNames" => ["TestAttribute"]
             }

      assert Plug.Conn.get_req_header(conn, "x-amz-target") == ["AmazonSQS.ReceiveMessage"]
      Plug.Conn.resp(conn, 200, Jason.encode!(%{"Messages" => []}))
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
               Keyword.put(@request_options, :endpoint, queue_url)
             )
  end

  test "receive_message supports requesting all attributes" do
    bypass = Bypass.open()
    queue_url = "http://localhost:#{bypass.port}/queue"

    Bypass.expect_once(bypass, "POST", "/queue", fn conn ->
      {:ok, body, conn} = Plug.Conn.read_body(conn)
      assert Jason.decode!(body)["AttributeNames"] == ["All"]
      Plug.Conn.resp(conn, 200, Jason.encode!(%{"Messages" => []}))
    end)

    options = %{max_number_of_messages: 1, attribute_names: :all}

    assert {:ok, %{"Messages" => []}} =
             SQS.receive_message(
               queue_url,
               options,
               Keyword.put(@request_options, :endpoint, queue_url)
             )
  end

  test "delete_message_batch builds the delete payload" do
    bypass = Bypass.open()
    queue_url = "http://localhost:#{bypass.port}/queue"
    entries = [%{"Id" => "1", "ReceiptHandle" => "receipt-1"}]

    Bypass.expect_once(bypass, "POST", "/queue", fn conn ->
      {:ok, body, conn} = Plug.Conn.read_body(conn)

      assert Jason.decode!(body) == %{"QueueUrl" => queue_url, "Entries" => entries}
      assert Plug.Conn.get_req_header(conn, "x-amz-target") == ["AmazonSQS.DeleteMessageBatch"]
      Plug.Conn.resp(conn, 200, Jason.encode!(%{"Successful" => [%{"Id" => "1"}]}))
    end)

    assert {:ok, %{"Successful" => [%{"Id" => "1"}]}} =
             SQS.delete_message_batch(
               queue_url,
               entries,
               Keyword.put(@request_options, :endpoint, queue_url)
             )
  end

  test "change_message_visibility_batch builds the visibility payload" do
    bypass = Bypass.open()
    queue_url = "http://localhost:#{bypass.port}/queue"

    entries = [
      %{
        "Id" => "1",
        "ReceiptHandle" => "receipt-1",
        "VisibilityTimeout" => 12
      }
    ]

    Bypass.expect_once(bypass, "POST", "/queue", fn conn ->
      {:ok, body, conn} = Plug.Conn.read_body(conn)

      assert Jason.decode!(body) == %{"QueueUrl" => queue_url, "Entries" => entries}

      assert Plug.Conn.get_req_header(conn, "x-amz-target") == [
               "AmazonSQS.ChangeMessageVisibilityBatch"
             ]

      Plug.Conn.resp(conn, 200, Jason.encode!(%{"Successful" => [%{"Id" => "1"}]}))
    end)

    assert {:ok, %{"Successful" => [%{"Id" => "1"}]}} =
             SQS.change_message_visibility_batch(
               queue_url,
               entries,
               Keyword.put(@request_options, :endpoint, queue_url)
             )
  end

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
