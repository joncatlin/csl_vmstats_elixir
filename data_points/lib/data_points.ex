defmodule DataPoints do
  
  require Logger

  @headers [:machine, :date, :time, :mem_max, :mem_avg, :mem_min, :cpu_max, :cpu_avg, :cpu_min, :net_min, :net_avg, :net_max]

  defstruct machine: "", date: "", time: "",     
    mem_max: 0.0,
    mem_min: 0.0,
    mem_avg: 0.0,
    cpu_max: 0.0,
    cpu_min: 0.0,
    cpu_avg: 0.0,
    net_max: 0.0,
    net_min: 0.0,
    net_avg: 0.0

  def find_new_files(path, existing_files) do
    files = Path.wildcard(path)
    new_files = files -- existing_files

    Logger.debug "Files found while looking in directory #{path} are: #{inspect(files)}"
    Logger.info "The new files discovered are: #{inspect(new_files)}"
    %{:found => files, :new => new_files}
  end

  def process_new_files(files) do
    Logger.debug "Files to process are: #{inspect(files)}"
    Enum.each files, fn(file) -> extract_data(file) end
  end

  defp extract_data(file) do
    file
    |> File.stream!
    |> Stream.drop(1)
    |> CSV.decode!(strip_fields: true, headers: @headers)
    |> Enum.map(&(create_data_point(&1)))
  end

  defp create_data_point(line) do
    Logger.debug "Creating a data point from the line with value: #{inspect(line)}"
    [month, day, year] = String.split(line.date, "/")
    [hours, minutes, seconds] = String.split(line.time, ":")
    time_in_s = (String.to_integer(hours) * 60 * 60) + (String.to_integer(minutes) * 60) + String.to_integer(seconds)

    data_point = 
      %DataPoints{machine: line.machine, \
        date: Date.new(String.to_integer(year), String.to_integer(month), String.to_integer(day)),\
        time: time_in_s, \
        mem_max: to_float(line.mem_max), \
        mem_min: to_float(line.mem_min), \
        mem_avg: to_float(line.mem_avg), \
        cpu_max: to_float(line.cpu_max), \
        cpu_min: to_float(line.cpu_min), \
        cpu_avg: to_float(line.cpu_avg), \
        net_max: to_float(line.net_max), \
        net_min: to_float(line.net_min), \
        net_avg: to_float(line.net_avg)}

    Logger.debug "data_point created is: #{inspect(data_point)}"
    data_point
  end

  defp to_float(string_value) do
    cond do
      string_value == "" -> 0.0
      string_value == "0" -> 0.0
      true -> String.to_float(string_value)
    end
  end
end
