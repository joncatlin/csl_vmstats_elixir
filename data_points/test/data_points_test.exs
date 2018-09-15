defmodule DataPointsTest do
  use ExUnit.Case
  doctest DataPoints

#  test "find files" do
#    dir = "C:/temp/vmstats_data/"
#    type = "*.[cC][sS][vV]"

#    path = dir <> type
#    initial_files = []
#    files = DataPoints.find_new_files(path, initial_files)
#    DataPoints.process_new_files(files.new)

#    :timer.sleep(10000)

#    new_files = DataPoints.find_new_files(path, files.found)
#    DataPoints.process_new_files(new_files.new)

#  end

  test "init the server" do

    # start the server
    pid = DataPointsStore.start_link("server1")
    IO.puts "pid for 1st attempt server1 = "
    IO.inspect(pid)

    # try and start the ame server again
    pid = DataPointsStore.start_link("server1")
    IO.puts "pid for 2nd attempt server1 = "
    IO.inspect(pid)

  end
end
