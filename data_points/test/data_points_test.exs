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

    date = Date.utc_today

    # # start the server
    # pid = DataPointsStore.start_link("server1", date)
    # IO.puts "pid for 1st attempt server1 = "
    # IO.inspect(pid)

    # # try and start the ame server again
    # pid = DataPointsStore.start_link("server1", date)
    # IO.puts "pid for 2nd attempt server1 = "
    # IO.inspect(pid)

    # # save some data
    # DataPointsStore.save("server1", date, "mem_max", 1, 2.34)
    # DataPointsStore.save("server1", date, "mem_max", 4, 4.34)
    # DataPointsStore.save("server1", date, "mem_max", 3, 3.34)
    # DataPointsStore.save("server1", date, "mem_max", 2, 5.34)
    
    # DataPointsStore.save("server1", date, "mem_max1", 3, 4.34)
    # DataPointsStore.save("server1", date, "mem_max2", 3, 4.34)
    # DataPointsStore.save("server1", date, "mem_max3", 3, 4.34)

    # # stop the server
    # pid = DataPointsStore.stop_link("server1", date)
    # IO.inspect(pid)

  end
end
