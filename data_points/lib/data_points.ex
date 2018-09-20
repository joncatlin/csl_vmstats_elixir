defmodule DataPoints do
  
  use GenServer
  require Logger

  @interval 10 * 1000
  @headers [:machine, :date, :time, :mem_max, :mem_avg, :mem_min, :cpu_max, :cpu_avg, :cpu_min, :net_min, :net_avg, :net_max]

  defstruct machine: "", date: "", time: "", data: %{}
#    mem_max: 0.0,
#    mem_min: 0.0,
#    mem_avg: 0.0,
#    cpu_max: 0.0,
#    cpu_min: 0.0,
#    cpu_avg: 0.0,
#    net_max: 0.0,
#    net_min: 0.0,
#    net_avg: 0.0

  defp find_new_files(path, existing_files) do
    files = Path.wildcard(path)
    new_files = files -- existing_files

    Logger.debug "Files found while looking in directory #{path} are: #{inspect(files)}"
    Logger.info "The new files discovered are: #{inspect(new_files)}"
    %{:found => files, :new => new_files}
  end

  def process_new_files(files) do
    files
    |> Enum.sort
    |> Enum.chunk_by(&(get_chunk_filename_key(&1)))
#    |> Enum.map(&(IO.puts "file chunk = #{inspect &1}"))
    |> Flow.from_enumerable()
    |> Flow.partition(max_demand: 100, stages: 2)
    |> Flow.map(&process_group_of_files(&1))
#    |> Flow.map(&extract_data(&1))
    |> Flow.run()
    Logger.info "All files processed"
  end

  # def process_new_files_old(files) do
  #   Enum.each files, fn(file) -> extract_data(file) end
  #   Logger.info "All files processed"
  # end


  # defp extract_data2(file) do
  #   file
  #   |> File.stream!(read_ahead: 100_000)
  #   |> Stream.drop(1)
  #   |> CSV.decode!(strip_fields: true, headers: @headers)
  #   |> Enum.chunk_by(&(get_chunk_key(&1)))
  #   |> Flow.from_enumerable()
  #   |> Flow.partition()
  #   |> Flow.map(&(create_dps_for_machine(&1)))
  # end

  defp process_group_of_files(file_list) do
    [head | _] = file_list
    Logger.info("In extract filename for file_list with first element= #{inspect head} in stage=#{inspect self()}")

    file_list
    |> Stream.map(&(&1))
    |> Enum.map(&(process_file(&1)))
#    |> Stream.map(&(process_file(&1)))
#    |> Stream.run
  end
  

  defp process_file(filename) do
    # Logger.info("In extract data for file= #{inspect filename} in stage=#{inspect self()}")

    filename
    |> File.stream!(read_ahead: 100_000)
    |> Stream.drop(1) # ignore the first line as it contains headers
    |> CSV.decode!(strip_fields: true, headers: @headers)
    |> Enum.chunk_by(&(get_chunk_key(&1)))
    |> Enum.map(&(create_dps_for_machine(&1)))
  end

  defp create_dps_for_machine(points) do
    Logger.debug("Create data points for a machine. dps = #{inspect points}")
    
    {machine, date} = points
    |> Enum.map(&(create_data_point(&1)))
    |> Enum.map(&(store(&1)))
    |> Enum.at(0)                      # only get 1 item
#    |> IO.inspect


    #return ok - NOTE this must be here or else the flow stops as it gets a stop from the previous call


    # WARNING this assumes that the points provided to this routine are grouped by machine and date
    # stop the DataPointsStore for this batch
    :ok = DataPointsStore.stop_link(machine, date)
    :ok
  end


  defp get_chunk_key(line) do
    line.machine <> line.date
  end


  defp get_chunk_filename_key(filename) do
    # the key should be the date portion of the filename so extract it
    #[^0-9]*(?<key>[0-9]{6}).*\.
    matches = Regex.named_captures(~r/[^0-9]*(?<key>[0-9]{6}).*\./, filename)
    matches["key"]
  end


  defp create_data_point(line) do
    Logger.debug "Creating a data point from the line with value: #{inspect(line)}"
    [month, day, year] = String.split(line.date, "/")
    [hours, minutes, seconds] = String.split(line.time, ":")
    time_in_s = (String.to_integer(hours) * 60 * 60) + (String.to_integer(minutes) * 60) + String.to_integer(seconds)

    # create the date
    {:ok, date} = Date.new(String.to_integer(year), String.to_integer(month), String.to_integer(day))

    data_point = 
      %DataPoints{machine: line.machine, 
        date: date,
        time: time_in_s, 
        data: [
          "mem_max", to_float(line.mem_max), 
          "mem_min", to_float(line.mem_min), 
          "mem_avg", to_float(line.mem_avg), 
          "cpu_max", to_float(line.cpu_max), 
          "cpu_min", to_float(line.cpu_min), 
          "cpu_avg", to_float(line.cpu_avg), 
          "net_max", to_float(line.net_max), 
          "net_min", to_float(line.net_min), 
          "net_avg", to_float(line.net_avg)
        ]
      }

    Logger.debug "data_point created is: #{inspect(data_point)}"

    # return the data point
    data_point
  end

  defp to_float(string_value) do
    cond do
      string_value == "" -> 
        0.0
      true -> 
        {value, _} = Float.parse(string_value)
        value
    end
  end

  # defp store(point) do
  #   Logger.debug("Data to store = #{inspect point}") 

  #   # create the store incase it does not exist
  #   DataPointsStore.start_link(point.machine, point.date)

  #   # add the data to the store
  #   point.data
  #   |> Enum.map(fn {key, val} -> DataPointsStore.save(point.machine, point.date, key, point.time, val) end)

  #   # return the machine name and date
  #   {point.machine, point.date}
  # end

  defp store(point) do
    Logger.debug("Data to store = #{inspect point}") 

    # create the store incase it does not exist
    DataPointsStore.start_link(point.machine, point.date)

    DataPointsStore.save_point(point.machine, point.date, point)

    # return the machine name and date
    {point.machine, point.date}
  end



  ###################################################################################################################
  ## Client API

  def start_link(path) do
    server_name = via_tuple(:data_points)
    case GenServer.start_link(__MODULE__, path, name: server_name) do
      {:ok, pid} -> {:ok, pid}
      {:error, {:already_started, pid}} -> {:ok, pid}
    end
  end

  defp via_tuple(server_name) do
    {:via, Registry, {DataPointsStoreRegistry, server_name}}
  end

  ###################################################################################################################
  ## Server Callbacks

  @impl true
  def init(_name) do
    dir = "C:/temp/vmstats_data/"
    type = "*.[cC][sS][vV]"
    path = dir <> type

    Logger.info("Starting DataPoints gen_server with path=#{inspect path}")

    schedule_work()

    state = %{path: path, existing_files: []}
    {:ok, state}
  end


  @impl true
  def handle_info(:work, state) do

    Logger.debug("Timer fired, state=#{inspect state}")

    # Do work here
    result = find_new_files(state.path, state.existing_files)
    process_new_files(result.new)

    schedule_work() # Reschedule once more

    # update the state
    newstate = put_in(state, [:existing_files], result.found)
    {:noreply, newstate}
  end

  # Scheulde a msg to be delivered to the process, effectively a timer
  defp schedule_work() do
    Process.send_after(self(), :work, @interval)
  end















end














defmodule MyApp.Periodically do
  use GenServer

  def start_link do
    GenServer.start_link(__MODULE__, %{})
  end

  @impl true
  def init(state) do
    schedule_work() # Schedule work to be performed on start
    {:ok, state}
  end

  @impl true
  def handle_info(:work, state) do
    # Do the desired work here
    schedule_work() # Reschedule once more
    {:noreply, state}
  end

  defp schedule_work() do
    Process.send_after(self(), :work, 2 * 60 * 60 * 1000) # In 2 hours
  end
end