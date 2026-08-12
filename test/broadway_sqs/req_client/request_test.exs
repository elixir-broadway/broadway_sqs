defmodule BroadwaySQS.ReqClient.RequestTest do
  use ExUnit.Case, async: true

  alias BroadwaySQS.ReqClient.Request

  @credentials [access_key_id: "access-key", secret_access_key: "secret-key"]
  @request_opts [region: "eu-west-1", credentials: @credentials]

  test "sends a signed JSON SQS request and decodes the response" do
    bypass = Bypass.open()

    Bypass.expect_once(bypass, "POST", "/", fn conn ->
      {:ok, body, conn} = Plug.Conn.read_body(conn)
      payload = Jason.decode!(body)

      assert Plug.Conn.get_req_header(conn, "content-type") == ["application/x-amz-json-1.0"]
      assert Plug.Conn.get_req_header(conn, "x-amz-target") == ["AmazonSQS.ReceiveMessage"]
      assert [authorization] = Plug.Conn.get_req_header(conn, "authorization")
      assert String.starts_with?(authorization, "AWS4-HMAC-SHA256 ")
      assert payload == %{"QueueUrl" => "http://localhost/queue"}

      Plug.Conn.resp(conn, 200, Jason.encode!(%{"Messages" => []}))
    end)

    assert {:ok, %{"Messages" => []}} =
             Request.call(
               "AmazonSQS.ReceiveMessage",
               %{"QueueUrl" => "http://localhost/queue"},
               Keyword.merge(@request_opts, endpoint: "http://localhost:#{bypass.port}")
             )
  end

  test "sends a session token" do
    bypass = Bypass.open()

    Bypass.expect_once(bypass, "POST", "/", fn conn ->
      assert Plug.Conn.get_req_header(conn, "x-amz-security-token") == ["session-token"]
      Plug.Conn.resp(conn, 200, Jason.encode!(%{}))
    end)

    assert {:ok, %{}} =
             Request.call(
               "AmazonSQS.DeleteMessageBatch",
               %{"QueueUrl" => "http://localhost/queue"},
               Keyword.merge(@request_opts,
                 endpoint: "http://localhost:#{bypass.port}",
                 credentials: Keyword.put(@credentials, :token, "session-token")
               )
             )
  end

  test "returns decoded AWS errors for non-success responses" do
    bypass = Bypass.open()

    Bypass.expect_once(bypass, "POST", "/", fn conn ->
      Plug.Conn.resp(conn, 400, Jason.encode!(%{"__type" => "InvalidParameterValue"}))
    end)

    assert {:error, {:http_error, 400, %{"__type" => "InvalidParameterValue"}}} =
             Request.call(
               "AmazonSQS.ChangeMessageVisibilityBatch",
               %{"QueueUrl" => "http://localhost/queue"},
               Keyword.merge(@request_opts, endpoint: "http://localhost:#{bypass.port}")
             )
  end

  test "returns an error when credentials are unavailable" do
    assert {:error, :aws_credentials_not_found} =
             Request.call(
               "AmazonSQS.ReceiveMessage",
               %{"QueueUrl" => "http://localhost/queue"},
               credentials: []
             )
  end

  test "returns an error when the AWS region is unavailable" do
    assert {:error, :aws_region_not_found} =
             Request.call(
               "AmazonSQS.ReceiveMessage",
               %{"QueueUrl" => "http://localhost/queue"},
               credentials: [access_key_id: "access-key", secret_access_key: "secret-key"]
             )
  end
end
