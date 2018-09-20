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
    Logger.debug("Starting DataPointsStore with name=#{inspect name} and server_name=#{inspect server_name}")
    result = case GenServer.start_link(__MODULE__, %{machine: machine, name: name}, name: server_name) do
      {:ok, pid} -> pid 
      {:error, {:already_started, pid}} -> pid
    end
    Logger.debug("Result=#{inspect result}")
    result
  end

  def get_name(machine, year, month, day) do
    "#{machine}-#{year}-#{month}-#{day}"
  end

  defp via_tuple(server_name) do
    {:via, Registry, {DataPointsStoreRegistry, server_name}}
  end

  def stop_link(machine, %Date{} = date) do
    name = get_name(machine, date.year, date.month, date.day)
    GenServer.call(via_tuple(name), :stop)
    :ok
  end

  # save data in the store
  def save(machine, %Date{} = date, type, time, value) do
    name = get_name(machine, date.year, date.month, date.day)
    data = %{type: type, time: time, value: value}
    GenServer.cast(via_tuple(name), {:save, data})
    :ok
  end

  # save data in the store
  def save_point(machine, %Date{} = date, point) do
    name = get_name(machine, date.year, date.month, date.day)
    GenServer.cast(via_tuple(name), {:save_point, point})
    :ok
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
    :ok
  end

  # empty any data that is in the store and throw it away
  def empty_all(machine, %Date{} = date) do
    name = get_name(machine, date.year, date.month, date.day)
    GenServer.cast(via_tuple(name), :empty_all)
    :ok
  end

  ###################################################################################################################
  ## Server Callbacks

  def init(args) do
    dir = "./DataPointsStore-files/" <> args.machine
    filename = dir <> "/#{args.name}.dets"
    Logger.info("Starting store with filename=#{inspect filename}")

    # read the file
    # if it does not exist ignore it
    # if the dir does not exist then mkdir
    # if it exists return the data
    data = case File.read(filename) do
      {:ok, ""} -> 
        # empty file
        @empty_map
      {:ok, bin} -> 
        try do
          :erlang.binary_to_term(bin)
        rescue
          _ ->
            Logger.warn("Data read from file=#{filename} cannot be converted to term using erlang:bin_to_term. Data read from file is: #{inspect bin}. Setting data to empty map which will cause data loss.")
            @empty_map
        end
      {:error, reason} ->
        Logger.debug("File read error, reason=#{inspect reason}. This is expected if the file does not exist etc")
        :ok = File.mkdir_p(dir)
        @empty_map
    end

    Logger.debug("Initialized data store to #{inspect data}")
    {:ok, %{filename: filename, data: data}}
  end


  def handle_info(msg, state) do
    Logger.error("Invalid msg recived, msg=#{inspect msg}")
    {:noreply, state}
  end


  def handle_call(:stop, _from, state) do
    Logger.info("Stopping store with filename=#{inspect state.filename}")
    bin = :erlang.term_to_binary(state.data)
    File.write!(state.filename, bin)
    {:stop, :normal, :ok, state}
  end


  def handle_call({:get, type}, _from, state) do
    Logger.debug("Get for type=#{inspect type} when state=#{inspect state}")

    value = get_in(state, [:data, type])
    Logger.debug("Get value retrieved is = #{inspect value}")
    {:reply, value, state}
  end


  def handle_cast({:save, newdata}, state) do
    Logger.debug("Saving for #{state.filename}, data=#{inspect newdata}")

    newstate = server_save(newdata.type, newdata.time, newdata.value, state)

    Logger.debug("newstate=#{inspect newstate}")
    {:noreply, newstate}
  end

      # %DataPoints{machine: line.machine, 
      #   date: date,
      #   time: time_in_s, 
      #   data: %{
      #     "mem_max" => to_float(line.mem_max), 
      #     "mem_min" => to_float(line.mem_min), 
      #     "mem_avg" => to_float(line.mem_avg), 
      #     "cpu_max" => to_float(line.cpu_max), 
      #     "cpu_min" => to_float(line.cpu_min), 
      #     "cpu_avg" => to_float(line.cpu_avg), 
      #     "net_max" => to_float(line.net_max), 
      #     "net_min" => to_float(line.net_min), 
      #     "net_avg" => to_float(line.net_avg)
      #   }
      # }

  def handle_cast({:save_point, point}, state) do
    Logger.debug("Saving point for #{state.filename}, data=#{inspect point}")

    Logger.error("before state=#{inspect state}")

    newstate = server_save(point.data, point.time, state)

    # new stuff
    # point.data
    # |> Enum.each(fn ({key, value}) -> state = server_save(key, point.time, value, state) end)
#    |> Enum.each(fn ({key, value}) -> Logger.error("key=#{inspect key}, value=#{inspect value}") end)
    Logger.error("after state=#{inspect newstate}")

#    put_in(%{a: %{}}, Enum.map([:a, :b, :c], &Access.key(&1, %{})), 42)

#    newstate = server_save(newdata, state)
    Logger.debug("newstate=#{inspect newstate}")
    {:noreply, newstate}
  end


  defp server_save([], _time, state) do
    state
  end


  defp server_save(list, time, state) do
    [key, value | tail] = list
    newstate = put_in(state, Enum.map([:data, key, time], &Access.key(&1, %{})), value)
    server_save(tail, time, newstate)
  end


  defp server_save(type, time, value, state) do
    # since it is not possible to use put_in when the key or any intermediate keys do not exist, the function
    # below adds the newdata if it does not exist or updates it if it does
    put_in(state, Enum.map([:data, type, time], &Access.key(&1, %{})), value)
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
