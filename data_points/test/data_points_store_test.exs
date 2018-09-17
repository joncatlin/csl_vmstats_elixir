defmodule DataPointsStoreTest do
  use ExUnit.Case
  doctest DataPointsStore

  @date ~D[2018-09-16]

  test "create a DataPointsStore" do

    # create the store
    pid = DataPointsStore.start_link("server1", @date) 
    assert is_pid(pid)

  end


  test "check duplicate creates with same values returns the same store" do

    # create the first store
    pid1 = DataPointsStore.start_link("server1", @date) 

    # create the first store
    pid2 = DataPointsStore.start_link("server1", @date) 
    assert is_pid(pid2)
    assert pid1 === pid2
  end


  test "check storing and retrieving data" do

    # create the first store
    pid = DataPointsStore.start_link("server1", @date) 
    assert is_pid(pid)

    # clear out any saved state
    DataPointsStore.empty_all("server1", @date)
    assert DataPointsStore.get("server1", @date, "mem_max") == nil

    # save some points and then retrieve them
    DataPointsStore.save("server1", @date, "mem_max", 56, 22.34)
    DataPointsStore.save("server1", @date, "mem_max", 2, 12.34)
    assert DataPointsStore.get("server1", @date, "mem_max") == %{2 => 12.34, 56 => 22.34}

    # shutdown the store
    assert DataPointsStore.stop_link("server1", @date) == :ok

    # start the store and check that the data has been retrieved
    DataPointsStore.start_link("server1", @date) 
    assert DataPointsStore.get("server1", @date, "mem_max") == %{2 => 12.34, 56 => 22.34}
  end


  test "check storing and retrieving data to multiple stores" do

    # create the stores
    pid = DataPointsStore.start_link("server1", @date) 
    assert is_pid(pid)
    pid = DataPointsStore.start_link("server2", @date) 
    assert is_pid(pid)
    pid = DataPointsStore.start_link("server3", @date) 
    assert is_pid(pid)

    # clear out any saved state
    DataPointsStore.empty_all("server1", @date)
    DataPointsStore.empty_all("server2", @date)
    DataPointsStore.empty_all("server3", @date)
    assert DataPointsStore.get("server1", @date, "mem_max") == nil
    assert DataPointsStore.get("server2", @date, "mem_max") == nil
    assert DataPointsStore.get("server3", @date, "mem_max") == nil

    # save some points and then retrieve them
    DataPointsStore.save("server1", @date, "mem_max", 56, 22.34)
    DataPointsStore.save("server1", @date, "mem_max", 2, 12.34)
    DataPointsStore.save("server1", @date, "cpu_max", 1, 1.1)
    DataPointsStore.save("server1", @date, "cpu_max", 2, 2.1)
    assert DataPointsStore.get("server1", @date, "mem_max") == %{2 => 12.34, 56 => 22.34}
    assert DataPointsStore.get("server1", @date, "cpu_max") == %{1 => 1.1, 2 => 2.1}
    assert DataPointsStore.get("server2", @date, "mem_max") == nil
    assert DataPointsStore.get("server3", @date, "mem_max") == nil
    DataPointsStore.save("server2", @date, "net_max", 1, 11.1)
    DataPointsStore.save("server2", @date, "net_max", 2, 21.1)
    DataPointsStore.save("server2", @date, "net_max", 3, 31.1)
    assert DataPointsStore.get("server2", @date, "net_max") == %{1 => 11.1, 2 => 21.1, 3 => 31.1}
    DataPointsStore.save("server3", @date, "net_avg", 11, 11.1)
    DataPointsStore.save("server3", @date, "net_avg", 12, 21.1)
    DataPointsStore.save("server3", @date, "net_avg", 13, 31.1)
    assert DataPointsStore.get("server3", @date, "net_avg") == %{11 => 11.1, 12 => 21.1, 13 => 31.1}

    # shutdown the store
    assert DataPointsStore.stop_link("server1", @date) == :ok
    assert DataPointsStore.stop_link("server2", @date) == :ok
    assert DataPointsStore.stop_link("server3", @date) == :ok

    # start the store and check that the data has been retrieved
    DataPointsStore.start_link("server1", @date) 
    DataPointsStore.start_link("server2", @date) 
    DataPointsStore.start_link("server3", @date) 
    assert DataPointsStore.get("server1", @date, "mem_max") == %{2 => 12.34, 56 => 22.34}
    assert DataPointsStore.get("server1", @date, "cpu_max") == %{1 => 1.1, 2 => 2.1}
    assert DataPointsStore.get("server2", @date, "net_max") == %{1 => 11.1, 2 => 21.1, 3 => 31.1}
    assert DataPointsStore.get("server3", @date, "net_avg") == %{11 => 11.1, 12 => 21.1, 13 => 31.1}
  end

end
