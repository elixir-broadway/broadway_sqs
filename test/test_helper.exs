ExUnit.start()

json_codec =
  if Code.ensure_loaded?(JSON) do
    JSON
  else
    Jason
  end

Application.put_env(:ex_aws, :json_codec, json_codec)
