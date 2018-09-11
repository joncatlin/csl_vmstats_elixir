defmodule DataPoints do

  def find_new_files(path, existing_files) do
    files = Path.wildcard(path)
    new_files = files -- existing_files
    %{:found => files, :new => new_files}
  end

  def process_new_files(files) do
    Enum.each files, fn(file) -> extract_data_points(file) end
  end

  def extract_data_points(file) do
    IO.puts "extract_data_points for file: #{file}"
    File.stream!(file)
    |> Stream.map(&(process_line(&1)))
    |> Stream.run()
  end

  def process_line(line) do
    [machine, date, time, mem_max, mem_avg, mem_min, cpu_max, cpu_avg, cpu_min, net_min, net_avg, net_max] = String.split(line, ",")
    create_data_points(mchine, date, time, "mem_max", mem_max)
    #IO.puts "Found line containing: machine name of #{machine}, date of #{date}, net_max: #{net_max}"
  end

  defstruct machine: "", date: %Date{}, time: %Time{}, type: "", value: 0.0


  def create_data_points (machine, date, time, type, value) do
    
  end 
end
