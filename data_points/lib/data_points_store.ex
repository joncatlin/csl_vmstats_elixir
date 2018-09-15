defmodule DataPointsStore do
  
  use GenServer
  require Logger
 
  ## Client API

  def start_link(server_name) do
#    Logger.debug "Files to process are: #{inspect(files)}"
    case GenServer.start_link(__MODULE__, :ok, name: String.to_atom(server_name)) do
      {:ok, pid} -> pid 
      {:error, {:already_started, pid}} -> pid
#      {anything} -> IO.puts "anything =" + anything
    end
  end

  ## Server Callbacks

  def init(:ok) do
    {:ok, %{}}
  end
end
