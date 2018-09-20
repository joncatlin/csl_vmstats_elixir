defmodule DataPointsStoreTest do
  use ExUnit.Case
  doctest DataPointsStore

  @date ~D[2018-09-16]

#   test "create a DataPointsStore" do

#     # create the store
#     pid = DataPointsStore.start_link("server1", @date) 
#     assert is_pid(pid)

#   end


#   test "check duplicate creates with same values returns the same store" do

#     # create the first store
#     pid1 = DataPointsStore.start_link("server1", @date) 

#     # create the first store
#     pid2 = DataPointsStore.start_link("server1", @date) 
#     assert is_pid(pid2)
#     assert pid1 === pid2
#   end


#   test "check storing and retrieving data" do

#     # create the first store
#     pid = DataPointsStore.start_link("server1", @date) 
#     assert is_pid(pid)

#     # clear out any saved state
#     DataPointsStore.empty_all("server1", @date)
#     assert DataPointsStore.get("server1", @date, "mem_max") == nil

#     # save some points and then retrieve them
#     DataPointsStore.save("server1", @date, "mem_max", 56, 22.34)
#     DataPointsStore.save("server1", @date, "mem_max", 2, 12.34)
#     assert DataPointsStore.get("server1", @date, "mem_max") == %{2 => 12.34, 56 => 22.34}

#     # shutdown the store
#     assert DataPointsStore.stop_link("server1", @date) == :ok

#     # start the store and check that the data has been retrieved
#     DataPointsStore.start_link("server1", @date) 
#     assert DataPointsStore.get("server1", @date, "mem_max") == %{2 => 12.34, 56 => 22.34}
#   end


#   test "check storing and retrieving data to multiple stores" do

#     # create the stores
#     pid = DataPointsStore.start_link("server1", @date) 
#     assert is_pid(pid)
#     pid = DataPointsStore.start_link("server2", @date) 
#     assert is_pid(pid)
#     pid = DataPointsStore.start_link("server3", @date) 
#     assert is_pid(pid)

#     # clear out any saved state
#     DataPointsStore.empty_all("server1", @date)
#     DataPointsStore.empty_all("server2", @date)
#     DataPointsStore.empty_all("server3", @date)
#     assert DataPointsStore.get("server1", @date, "mem_max") == nil
#     assert DataPointsStore.get("server2", @date, "mem_max") == nil
#     assert DataPointsStore.get("server3", @date, "mem_max") == nil

#     # save some points and then retrieve them
#     DataPointsStore.save("server1", @date, "mem_max", 56, 22.34)
#     DataPointsStore.save("server1", @date, "mem_max", 2, 12.34)
#     DataPointsStore.save("server1", @date, "cpu_max", 1, 1.1)
#     DataPointsStore.save("server1", @date, "cpu_max", 2, 2.1)
#     assert DataPointsStore.get("server1", @date, "mem_max") == %{2 => 12.34, 56 => 22.34}
#     assert DataPointsStore.get("server1", @date, "cpu_max") == %{1 => 1.1, 2 => 2.1}
#     assert DataPointsStore.get("server2", @date, "mem_max") == nil
#     assert DataPointsStore.get("server3", @date, "mem_max") == nil
#     DataPointsStore.save("server2", @date, "net_max", 1, 11.1)
#     DataPointsStore.save("server2", @date, "net_max", 2, 21.1)
#     DataPointsStore.save("server2", @date, "net_max", 3, 31.1)
#     assert DataPointsStore.get("server2", @date, "net_max") == %{1 => 11.1, 2 => 21.1, 3 => 31.1}
#     DataPointsStore.save("server3", @date, "net_avg", 11, 11.1)
#     DataPointsStore.save("server3", @date, "net_avg", 12, 21.1)
#     DataPointsStore.save("server3", @date, "net_avg", 13, 31.1)
#     assert DataPointsStore.get("server3", @date, "net_avg") == %{11 => 11.1, 12 => 21.1, 13 => 31.1}

#     # shutdown the store
#     assert DataPointsStore.stop_link("server1", @date) == :ok
#     assert DataPointsStore.stop_link("server2", @date) == :ok
#     assert DataPointsStore.stop_link("server3", @date) == :ok

#     # start the store and check that the data has been retrieved
#     DataPointsStore.start_link("server1", @date) 
#     DataPointsStore.start_link("server2", @date) 
#     DataPointsStore.start_link("server3", @date) 
#     assert DataPointsStore.get("server1", @date, "mem_max") == %{2 => 12.34, 56 => 22.34}
#     assert DataPointsStore.get("server1", @date, "cpu_max") == %{1 => 1.1, 2 => 2.1}
#     assert DataPointsStore.get("server2", @date, "net_max") == %{1 => 11.1, 2 => 21.1, 3 => 31.1}
#     assert DataPointsStore.get("server3", @date, "net_avg") == %{11 => 11.1, 12 => 21.1, 13 => 31.1}
#   end

#   defp check_machine_data(machine, date, type) do
#     IO.puts "Data retrieved for machine=#{inspect machine} for date=#{inspect date} for type=#{inspect type} is:"
#     DataPointsStore.start_link(machine, date)
#     DataPointsStore.get(machine, date, type)
#     |> Enum.sort()
# #    |> Enum.sort_by(&(elem(&1, 0)))
# #    |> Enum.map(&(IO.puts inspect &1))
#     |> IO.inspect
#   end


  test "check storing and retrieving a data point" do

    machine = "server4"
    # create the first store
    pid = DataPointsStore.start_link(machine, @date) 
    assert is_pid(pid)

    # clear out any saved state
    DataPointsStore.empty_all(machine, @date)
    assert DataPointsStore.get(machine, @date, "mem_max") == nil

    point = %DataPoints{machine: machine, 
      date: @date,
      time: 120, 
      data: [
        "mem_max", 1.2, 
        "mem_min", 2.2, 
        "mem_avg", 3.2, 
        "cpu_max", 4.2, 
        "cpu_min", 5.2, 
        "cpu_avg", 6.2, 
        "net_max", 7.2, 
        "net_min", 8.2, 
        "net_avg", 9.2
      ]
    }

    # save some points and then retrieve them
    DataPointsStore.save_point(machine, @date, point)
    assert DataPointsStore.get(machine, @date, "mem_max") == %{120 => 1.2}
    assert DataPointsStore.get(machine, @date, "mem_min") == %{120 => 2.2}
    assert DataPointsStore.get(machine, @date, "mem_avg") == %{120 => 3.2}
    assert DataPointsStore.get(machine, @date, "cpu_max") == %{120 => 4.2}
    assert DataPointsStore.get(machine, @date, "cpu_min") == %{120 => 5.2}
    assert DataPointsStore.get(machine, @date, "cpu_avg") == %{120 => 6.2}
    assert DataPointsStore.get(machine, @date, "net_max") == %{120 => 7.2}
    assert DataPointsStore.get(machine, @date, "net_min") == %{120 => 8.2}
    assert DataPointsStore.get(machine, @date, "net_avg") == %{120 => 9.2}

    # shutdown the store
    assert DataPointsStore.stop_link(machine, @date) == :ok
  end


#   test "check restoring data for V-PReyes ~D[2017-10-13]" do

#     machine = "V-PReyes"
#     date = ~D[2017-10-13]
#     type = "mem_max"
#     check_machine_data(machine, date, type)
#   end


  # test "check restoring data for V-JOlvera range of dates" do

  #   machine = "V-JOlvera"
  #   type = "mem_max"
  #   date = ~D[2017-10-12]
  #   check_machine_data(machine, date, type)
  #   date = ~D[2017-10-13]
  #   check_machine_data(machine, date, type)
  #   date = ~D[2017-10-14]
  #   check_machine_data(machine, date, type)
  # end
end
