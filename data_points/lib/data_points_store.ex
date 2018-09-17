defmodule DataPointsStore do
  
  use GenServer
  require Logger

  ## Constants
  @empty_map %{}

  ###################################################################################################################
  ## Client API

  def start_link(machine, %Date{} = date) do
    name = get_name(machine, date.year, date.month, date.day)
    server_name = via_tuple(name)
    case GenServer.start_link(__MODULE__, name, name: server_name) do
      {:ok, pid} -> pid 
      {:error, {:already_started, pid}} -> pid
    end
  end

  defp get_name(machine, year, month, day) do
    "#{machine}-#{year}-#{month}-#{day}"
  end

  defp via_tuple(server_name) do
    {:via, Registry, {DataPointsStoreRegistry, server_name}}
  end

  def stop_link(machine, %Date{} = date) do
    name = get_name(machine, date.year, date.month, date.day)
    GenServer.call(via_tuple(name), :stop)
  end

  # save data in the store
  def save(machine, %Date{} = date, type, time, value) do
    name = get_name(machine, date.year, date.month, date.day)
    data = %{type: type, time: time, value: value}
    GenServer.cast(via_tuple(name), {:save, data})
  end

  # retireve data from the store given its type
  def get(machine, %Date{} = date, type) do
    name = get_name(machine, date.year, date.month, date.day)
    GenServer.call(via_tuple(name), {:get, type})
  end

  # empty any data that is in the store and throw it away
  def empty(machine, %Date{} = date, type) do
    name = get_name(machine, date.year, date.month, date.day)
    GenServer.cast(via_tuple(name), {:empty, type})
  end

  # empty any data that is in the store and throw it away
  def empty_all(machine, %Date{} = date) do
    name = get_name(machine, date.year, date.month, date.day)
    GenServer.cast(via_tuple(name), :empty_all)
  end

  ###################################################################################################################
  ## Server Callbacks

  def init(name) do
    # Check to see if there is a DETS file for this process and load it if there is
    # otherwise create an empty structure

    filename = String.to_charlist("#{name}.dets")
    :dets.open_file(:file_table, [{:file, filename}])

    data = case :dets.lookup(:file_table, :data) do
      [data: anything] -> 
        Logger.debug("restoring state to #{inspect anything}")
        anything
      [] -> 
        Logger.debug("no match found for :data")
        @empty_map
    end

    :dets.close(:file_table)

    Logger.debug("Initialized data store to #{inspect data}")
    {:ok, %{filename: filename, data: data}}
  end


  def handle_info(msg, state) do
    Logger.error("Invalid msg recived, msg=#{inspect msg}")
    {:noreply, state}
  end


  def handle_call(:stop, _from, state) do
    Logger.debug("Stopping process, saving state=#{inspect state}")
    :dets.open_file(:file_table, [{:file, state.filename}])
    :dets.delete_all_objects(:file_table)
    :dets.insert(:file_table, {:data, state.data})
    :dets.close(:file_table)
    {:reply, :ok, state}
  end


  def handle_call({:get, type}, _from, state) do
    Logger.debug("Get for type=#{inspect type} when state=#{inspect state}")

    value = get_in(state, [:data, type])
    Logger.debug("Get value retrieved is = #{inspect value}")
    {:reply, value, state}
  end


  def handle_cast({:save, newdata}, state) do
    Logger.debug("Saving for #{state.filename}, data=#{inspect newdata}")

    # since it is not possible to use put_in when the key or any intermediate keys do not exist, the function
    # below adds the newdata if it does not exist or udates it if it does
    newstate = put_in(state, Enum.map([:data, newdata.type, newdata.time], &Access.key(&1, %{})), newdata.value)
    Logger.debug("newstate=#{inspect newstate}")
    {:noreply, newstate}
  end


  def handle_cast({:empty, type}, state) do
    Logger.debug("Empty for type=#{inspect type}")

    # since it is not possible to use put_in when the key or any intermediate keys do not exist, the function
    # below adds the newdata if it does not exist or udates it if it does
    newstate = put_in(state, Enum.map([:data, type], &Access.key(&1, %{})), @empty_map)
    {:noreply, newstate}
  end


  def handle_cast(:empty_all, state) do
    Logger.debug("Empty_all")

    newstate = put_in(state, [:data], @empty_map)
    {:noreply, newstate}
  end

end
