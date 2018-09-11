defmodule DataPointsTest do
  use ExUnit.Case
  doctest DataPoints

  test "find files" do
    dir = "C:/temp/vmstats_data/"
    #type = "*.*"
    type = "*.[cC][sS][vV]"

    path = dir <> type
    initial_files = []
    files = DataPoints.find_new_files(path, initial_files)
    DataPoints.process_new_files(files.new)

    #IO.puts "Files found are: #{files}"
    IO.inspect files

    :timer.sleep(10000)

    new_files = DataPoints.find_new_files(path, files.found)
    DataPoints.process_new_files(new_files.new)

    #IO.puts "New files found are: #{new_files}"
    IO.inspect new_files


#    assert DataPoints.hello() == :world
  end
end
