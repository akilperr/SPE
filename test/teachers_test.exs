defmodule SPETest do
  require Logger

  use ExUnit.Case, async: false


  @tag :submit_bad_jobs
  test "submit_bad_jobs" do
    assert {:ok, sup} = SPE.start_link([])
    assert {:error, _} = SPE.submit_job("bad")
    assert {:error, _} = SPE.submit_job(%{"name" => :olle})
    assert {:error, _} = SPE.submit_job(%{"name" => "nisse"})
    assert {:error, _} = SPE.submit_job(%{"name" => "nisse", "tasks" => []})
    task = %{"enables" => [], "exec" => fn _ -> 1 + 2 end, "timeout" => :infinity}
    assert {:error, _} = SPE.submit_job(%{"name" => "nisse", "tasks" => [task]})
    task = %{"name" => "t0", "enables" => [], "timeout" => :infinity}
    assert {:error, _} = SPE.submit_job(%{"name" => "nisse", "tasks" => [task]})
    task = %{"name" => :t0, "enables" => [], "exec" => fn _ -> 1 + 2 end, "timeout" => :infinity}
    assert {:error, _} = SPE.submit_job(%{"name" => "nisse", "tasks" => [task]})
    task = %{"name" => "t0", "enables" => [], "exec" => fn _ -> 1 + 2 end, "timeout" => nil}
    assert {:error, _} = SPE.submit_job(%{"name" => "nisse", "tasks" => [task]})

    task = %{
      "name" => "t0",
      "enables" => ["t1"],
      "exec" => fn _ -> 1 + 2 end,
      "timeout" => :infinity
    }

    assert {:error, _} = SPE.submit_job(%{"name" => "nisse", "tasks" => [task]})
    task = %{"name" => "t0", "enables" => [], "exec" => fn _ -> 1 + 2 end, "timeout" => :infinity}
    assert {:error, _} = SPE.submit_job(%{"name" => "nisse", "tasks" => [task, task]})
    assert :ok = Supervisor.stop(sup)
  end


  @tag :submit_good_job
  test "submit_good_job" do
    assert {:ok, sup} = SPE.start_link([])
    task = %{"name" => "t0", "enables" => [], "exec" => fn _ -> 1 + 2 end, "timeout" => :infinity}
    assert {:ok, _} = SPE.submit_job(%{"name" => "nisse", "tasks" => [task]})
    assert :ok = Supervisor.stop(sup)
  end


  @tag :submit_good_jobs
  test "submit_good_jobs" do
    assert {:ok, sup} = SPE.start_link([])
    task = %{"name" => "t0", "enables" => [], "exec" => fn _ -> 1 + 2 end, "timeout" => :infinity}
    assert {:ok, _} = SPE.submit_job(%{"name" => "nisse", "tasks" => [task]})
    task = %{"name" => "t0", "enables" => [], "exec" => fn _ -> 1 + 2 end, "timeout" => :infinity}
    assert {:ok, _} = SPE.submit_job(%{"name" => "nisse", "tasks" => [task]})
    assert :ok = Supervisor.stop(sup)
  end

  @tag :start_job1
  test "start_job1" do
    assert {:ok, sup} = SPE.start_link([])
    task = %{"name" => "t0", "enables" => [], "exec" => fn _ -> 1 + 2 end, "timeout" => :infinity}
    result = SPE.submit_job(%{"name" => "nisse", "tasks" => [task]})
    assert {:ok, id} = result
    Phoenix.PubSub.subscribe(SPE.PubSub, id)
    SPE.start_job(id)
    all_broadcasts = get_all_broadcasts(300)
    assert {:succeeded, %{"t0" => {:result, 3}}} = get_result(id, all_broadcasts)
    Phoenix.PubSub.unsubscribe(SPE.PubSub, id)
    assert :ok = Supervisor.stop(sup)
  end


  @tag :start_job2
  test "start_job2" do
    assert {:ok, sup} = SPE.start_link([])
    task = %{"name" => "t0", "enables" => [], "exec" => fn _ -> 3 / 0 end, "timeout" => :infinity}
    result = SPE.submit_job(%{"name" => "nisse", "tasks" => [task]})
    assert {:ok, id} = result
    Phoenix.PubSub.subscribe(SPE.PubSub, id)
    SPE.start_job(id)
    all_broadcasts = get_all_broadcasts(300)
    assert {:failed, %{"t0" => {:failed, _}}} = get_result(id, all_broadcasts)
    Phoenix.PubSub.unsubscribe(SPE.PubSub, id)
    assert :ok = Supervisor.stop(sup)
  end


  @tag :start_job3
  test "start_job3" do
    assert {:ok, sup} = SPE.start_link(num_workers: 1)
    task1 = %{"name" => "t0", "enables" => [], "exec" => fn _ -> 1 end, "timeout" => :infinity}
    task2 = %{"name" => "t1", "enables" => [], "exec" => fn _ -> 2 end, "timeout" => :infinity}
    result = SPE.submit_job(%{"name" => "nisse", "tasks" => [task1, task2]})
    assert {:ok, id} = result
    Phoenix.PubSub.subscribe(SPE.PubSub, id)
    SPE.start_job(id)
    all_broadcasts = get_all_broadcasts(300)

    assert {:succeeded, %{"t0" => {:result, 1}, "t1" => {:result, 2}}} =
             get_result(id, all_broadcasts)

    Phoenix.PubSub.unsubscribe(SPE.PubSub, id)
    assert :ok = Supervisor.stop(sup)
  end


  @tag :start_job4
  test "start_job4" do
    assert {:ok, sup} = SPE.start_link(num_workers: 1)
    task1 = %{"name" => "t0", "enables" => [], "exec" => fn _ -> 1 end, "timeout" => :infinity}
    task2 = %{"name" => "t1", "enables" => [], "exec" => fn _ -> 2 end, "timeout" => :infinity}
    result = SPE.submit_job(%{"name" => "nisse", "tasks" => [task1, task2]})
    assert {:ok, id} = result
    Phoenix.PubSub.subscribe(SPE.PubSub, id)
    SPE.start_job(id)
    all_broadcasts = get_all_broadcasts(300)

    assert {:succeeded, %{"t0" => {:result, 1}, "t1" => {:result, 2}}} =
             get_result(id, all_broadcasts)

    Phoenix.PubSub.unsubscribe(SPE.PubSub, id)
    assert :ok = Supervisor.stop(sup)
  end


  @tag :start_job5
  test "start_job5" do
    assert {:ok, sup} = SPE.start_link(num_workers: 1)

    task1 = %{
      "name" => "t0",
      "enables" => [],
      "exec" => fn _ -> 1 + 2 end,
      "timeout" => :infinity
    }

    task2 = %{
      "name" => "t1",
      "enables" => [],
      "exec" => fn _ -> 3 + 4 end,
      "timeout" => :infinity
    }

    result = SPE.submit_job(%{"name" => "nisse", "tasks" => [task1, task2]})
    assert {:ok, id} = result
    Phoenix.PubSub.subscribe(SPE.PubSub, id)
    SPE.start_job(id)
    all_broadcasts = get_all_broadcasts(300)

    assert {:succeeded, %{"t0" => {:result, 3}, "t1" => {:result, 7}}} =
             get_result(id, all_broadcasts)

    Phoenix.PubSub.unsubscribe(SPE.PubSub, id)
    assert :ok = Supervisor.stop(sup)
  end


  @tag :failing
  test "start_job_failing" do
    assert {:ok, sup} = SPE.start_link(num_workers: 1)

    task0 = %{
      "name" => "t0",
      "enables" => [],
      "exec" => fn _ -> Process.exit(self(), :brutal_kill) end,
      "timeout" => :infinity
    }

    task1 = %{
      "name" => "t1",
      "enables" => [],
      "exec" => fn _ -> Process.exit(self(), :because_i_am_bad) end,
      "timeout" => :infinity
    }

    task2 = %{
      "name" => "t2",
      "enables" => [],
      "exec" => fn _ -> raise "no_future" end,
      "timeout" => :infinity
    }

    task3 = %{
      "name" => "t3",
      "enables" => [],
      "exec" => fn _ -> throw("dont catch me") end,
      "timeout" => :infinity
    }

    result = SPE.submit_job(%{"name" => "nisse", "tasks" => [task0, task1, task2, task3]})
    assert {:ok, id} = result
    Phoenix.PubSub.subscribe(SPE.PubSub, id)
    SPE.start_job(id)
    all_broadcasts = get_all_broadcasts(300)

    assert {:failed,
            %{
              "t0" => {:failed, _},
              "t1" => {:failed, _},
              "t2" => {:failed, _},
              "t3" => {:failed, _}
            }} = get_result(id, all_broadcasts)

    Phoenix.PubSub.unsubscribe(SPE.PubSub, id)
    assert :ok = Supervisor.stop(sup)
  end


  @tag :job_enables1
  test "job_enables1" do
    assert {:ok, sup} = SPE.start_link(num_workers: 1)

    task1 = %{
      "name" => "t0",
      "enables" => [],
      "exec" => fn %{"t1" => value} -> value + 2 end,
      "timeout" => :infinity
    }

    task2 = %{
      "name" => "t1",
      "enables" => ["t0"],
      "exec" => fn _ -> 3 + 4 end,
      "timeout" => :infinity
    }

    assert {:ok, job_id} = SPE.submit_job(%{"name" => "nisse", "tasks" => [task1, task2]})
    Phoenix.PubSub.subscribe(SPE.PubSub, job_id)
    assert {:ok, ^job_id} = SPE.start_job(job_id)

    assert {:succeeded, %{"t0" => {:result, 9}, "t1" => {:result, 7}}} =
             get_result(job_id, get_all_broadcasts(500))

    Phoenix.PubSub.unsubscribe(SPE.PubSub, job_id)
    assert :ok = Supervisor.stop(sup)
  end


  @tag :job_enables_fails
  test "job_enables_fails" do
    assert {:ok, sup} = SPE.start_link(num_workers: 1)

    task1 = %{
      "name" => "t0",
      "enables" => [],
      "exec" => fn %{"t1" => value} -> value + 2 end,
      "timeout" => :infinity
    }

    task2 = %{
      "name" => "t1",
      "enables" => ["t0"],
      "exec" => fn _ -> 2 / 0 end,
      "timeout" => :infinity
    }

    result = SPE.submit_job(%{"name" => "nisse", "tasks" => [task1, task2]})
    assert {:ok, id} = result
    Phoenix.PubSub.subscribe(SPE.PubSub, id)
    SPE.start_job(id)
    all_broadcasts = get_all_broadcasts(500)

    assert {:failed, %{"t0" => :not_run, "t1" => {:failed, _}}} = get_result(id, all_broadcasts)

    Phoenix.PubSub.unsubscribe(SPE.PubSub, id)
    assert :ok = Supervisor.stop(sup)
  end


  @tag :job_enables_timeout
  test "job_enables_timeout" do
    assert {:ok, sup} = SPE.start_link(num_workers: 1)

    task1 = %{
      "name" => "t0",
      "enables" => [],
      "exec" => fn %{"t1" => value} -> value + 2 end,
      "timeout" => :infinity
    }

    task2 = %{
      "name" => "t1",
      "enables" => ["t0"],
      "exec" => fn _ -> Process.sleep(2000) end,
      "timeout" => 200
    }

    result = SPE.submit_job(%{"name" => "nisse", "tasks" => [task1, task2]})
    assert {:ok, id} = result
    Phoenix.PubSub.subscribe(SPE.PubSub, id)
    SPE.start_job(id)
    all_broadcasts = get_all_broadcasts(500)

    assert {:failed, %{"t0" => :not_run, "t1" => {:failed, _}}} = get_result(id, all_broadcasts)

    Phoenix.PubSub.unsubscribe(SPE.PubSub, id)
    assert :ok = Supervisor.stop(sup)
  end


  @tag :job_enables2
  test "job_enables2" do
    assert {:ok, sup} = SPE.start_link(num_workers: 1)

    task1 = %{
      "name" => "t0",
      "enables" => [],
      "exec" => fn m ->
        %{"t1" => t1, "t2" => t2} = m
        t1 + t2 + 2
      end,
      "timeout" => :infinity
    }

    task2 = %{
      "name" => "t1",
      "enables" => ["t0"],
      "exec" => fn _ -> 3 + 4 end,
      "timeout" => :infinity
    }

    task3 = %{
      "name" => "t2",
      "enables" => ["t0"],
      "exec" => fn _ -> 8 + 9 end,
      "timeout" => :infinity
    }

    result = SPE.submit_job(%{"name" => "nisse", "tasks" => [task1, task2, task3]})
    assert {:ok, id} = result
    Phoenix.PubSub.subscribe(SPE.PubSub, id)
    SPE.start_job(id)
    all_broadcasts = get_all_broadcasts(500)

    assert {:succeeded, %{"t0" => {:result, 26}, "t1" => {:result, 7}, "t2" => {:result, 17}}} =
             get_result(id, all_broadcasts)

    Phoenix.PubSub.unsubscribe(SPE.PubSub, id)
    assert :ok = Supervisor.stop(sup)
  end


  @tag :job_enables_transitive
  test "job_enables_transitive" do
    assert {:ok, sup} = SPE.start_link(num_workers: 1)

    task1 = %{
      "name" => "t0",
      "enables" => [],
      "exec" => fn %{"t1" => t1, "t2" => t2} -> t1 + t2 + 2 end,
      "timeout" => :infinity
    }

    task2 = %{
      "name" => "t1",
      "enables" => ["t0"],
      "exec" => fn %{"t2" => t2} -> t2 + 4 end,
      "timeout" => :infinity
    }

    task3 = %{
      "name" => "t2",
      "enables" => ["t1"],
      "exec" => fn _ -> 8 + 9 end,
      "timeout" => :infinity
    }

    result = SPE.submit_job(%{"name" => "nisse", "tasks" => [task1, task2, task3]})
    assert {:ok, id} = result
    Phoenix.PubSub.subscribe(SPE.PubSub, id)
    SPE.start_job(id)
    all_broadcasts = get_all_broadcasts(500)

    assert {:succeeded, %{"t2" => {:result, 17}, "t1" => {:result, 21}, "t0" => {:result, 40}}} =
             get_result(id, all_broadcasts)

    Phoenix.PubSub.unsubscribe(SPE.PubSub, id)
    assert :ok = Supervisor.stop(sup)
  end


  @tag :timeout_job1
  test "timeout_job1" do
    assert {:ok, sup} = SPE.start_link([])

    task0 = %{
      "name" => "t0",
      "enables" => ["t1"],
      "exec" => fn _ ->
        Process.sleep(1000)
        2
      end,
      "timeout" => 200
    }

    task1 = %{"name" => "t1", "exec" => fn _ -> 3 end}
    result = SPE.submit_job(%{"name" => "nisse", "tasks" => [task0, task1]})
    assert {:ok, id} = result
    Phoenix.PubSub.subscribe(SPE.PubSub, id)
    SPE.start_job(id)
    all_broadcasts = get_all_broadcasts(500)
    IO.puts("all_broadcasts=#{inspect(all_broadcasts)}")

    assert {:failed, %{"t0" => {:failed, :timeout}, "t1" => :not_run}} =
             get_result(id, all_broadcasts)

    Phoenix.PubSub.unsubscribe(SPE.PubSub, id)
    assert :ok = Supervisor.stop(sup)
  end


  @tag :big_test
  test "big_test" do
    assert {:ok, sup} = SPE.start_link([])
    limit = 3

    tasks =
      for i <- 1..limit do
        %{
          "name" => "t_" <> Integer.to_string(i),
          "enables" => [],
          "exec" => fn _ -> i end,
          "timeout" => :infinity
        }
      end

    result = SPE.submit_job(%{"name" => "nisse", "tasks" => tasks})
    assert {:ok, id} = result
    Phoenix.PubSub.subscribe(SPE.PubSub, id)
    SPE.start_job(id)
    all_broadcasts = get_all_broadcasts(2000)
    assert {:succeeded, map} = get_result(id, all_broadcasts)
    assert ^limit = map_size(map)
    Phoenix.PubSub.unsubscribe(SPE.PubSub, id)
    assert :ok = Supervisor.stop(sup)
  end


  @tag :big_job_test
  test "big_job_test" do
    assert {:ok, sup} = SPE.start_link([])
    limit = 100

    jobs =
      for _ <- 1..limit do
        task1 = %{
          "name" => "t0",
          "enables" => [],
          "exec" => fn %{"t1" => value} -> value + 2 end,
          "timeout" => :infinity
        }

        task2 = %{
          "name" => "t1",
          "enables" => ["t0"],
          "exec" => fn _ -> 3 + 4 end,
          "timeout" => :infinity
        }

        %{"name" => "stina", "tasks" => [task1, task2]}
      end

    job_ids =
      Enum.map(jobs, fn job ->
        assert {:ok, id} = SPE.submit_job(job)
        id
      end)

    Enum.each(job_ids, fn job_id -> Phoenix.PubSub.subscribe(SPE.PubSub, job_id) end)
    Enum.each(job_ids, fn job_id -> SPE.start_job(job_id) end)

    all_broadcasts = get_all_broadcasts(1000)
    results = get_all_results(all_broadcasts)
    assert ^limit = length(results)

    Enum.each(results, fn result ->
      assert {_id, :result, {:succeeded, %{"t0" => {:result, 9}, "t1" => {:result, 7}}}} = result
    end)

    Enum.each(job_ids, fn job_id -> Phoenix.PubSub.unsubscribe(SPE.PubSub, job_id) end)
    assert :ok = Supervisor.stop(sup)
  end


  @tag :multiple_jobs
  test "multiple_jobs" do
    assert {:ok, sup} = SPE.start_link(num_workers: 10)

    task1 = %{
      "name" => "t0",
      "enables" => [],
      "exec" => fn %{"t1" => value} -> value + 2 end,
      "timeout" => :infinity
    }

    task2 = %{
      "name" => "t1",
      "enables" => ["t0"],
      "exec" => fn _ ->
        Process.sleep(100)
        3 + 4
      end,
      "timeout" => :infinity
    }

    result1 = SPE.submit_job(%{"name" => "nisse", "tasks" => [task1, task2]})

    task1 = %{
      "name" => "t0",
      "enables" => [],
      "exec" => fn %{"t1" => value} -> value + 2 end,
      "timeout" => :infinity
    }

    task2 = %{
      "name" => "t1",
      "enables" => ["t0"],
      "exec" => fn _ -> 3 + 4 end,
      "timeout" => :infinity
    }

    result2 = SPE.submit_job(%{"name" => "nisse", "tasks" => [task1, task2]})

    assert {:ok, id1} = result1
    assert {:ok, id2} = result2

    Phoenix.PubSub.subscribe(SPE.PubSub, id1)
    Phoenix.PubSub.subscribe(SPE.PubSub, id2)

    SPE.start_job(id1)
    SPE.start_job(id2)
    all_broadcasts = get_all_broadcasts(1000)

    assert {:succeeded, %{"t0" => {:result, 9}, "t1" => {:result, 7}}} =
             get_result(id1, all_broadcasts)

    assert {:succeeded, %{"t0" => {:result, 9}, "t1" => {:result, 7}}} =
             get_result(id2, all_broadcasts)

    Phoenix.PubSub.unsubscribe(SPE.PubSub, id1)
    Phoenix.PubSub.unsubscribe(SPE.PubSub, id2)
    assert :ok = Supervisor.stop(sup)
  end


  @tag :task_timing1
  test "task_timing1" do
    assert {:ok, sup} = SPE.start_link(num_workers: 1)

    task1 = %{
      "name" => "t0",
      "enables" => [],
      "exec" => fn _ ->
        Process.sleep(500)
        2 + 1
      end,
      "timeout" => :infinity
    }

    task2 = %{
      "name" => "t1",
      "enables" => [],
      "exec" => fn _ ->
        Process.sleep(600)
        3 + 4
      end,
      "timeout" => :infinity
    }

    assert {:ok, id} = SPE.submit_job(%{"name" => "nisse", "tasks" => [task1, task2]})
    Phoenix.PubSub.subscribe(SPE.PubSub, id)
    SPE.start_job(id)
    all_broadcasts = get_all_broadcasts(1500)

    assert {:succeeded, %{"t0" => {:result, 3}, "t1" => {:result, 7}}} =
             get_result(id, all_broadcasts)

    assert {:ok, start_t0} = task_start_time(all_broadcasts, id, "t0")
    assert {:ok, start_t1} = task_start_time(all_broadcasts, id, "t1")
    assert {:ok, end_t0} = task_termination_time(all_broadcasts, id, "t0")
    assert {:ok, end_t1} = task_termination_time(all_broadcasts, id, "t1")

    # End times occur after start times
    assert end_t0 >= start_t0
    assert end_t1 >= start_t1
    # No overlap between tasks
    assert start_t1 >= end_t0 or start_t0 >= end_t1
    # Around 1 second has passed when executing both tasks
    min_start = min(start_t0, start_t1)
    max_end = max(end_t0, end_t1)
    assert max_end - min_start >= 900

    Phoenix.PubSub.unsubscribe(SPE.PubSub, id)
    assert :ok = Supervisor.stop(sup)
  end


  @tag :task_timing2
  test "task_timing2" do
    assert {:ok, sup} = SPE.start_link(num_workers: 2)

    task1 = %{
      "name" => "t0",
      "enables" => [],
      "exec" => fn _ ->
        Process.sleep(500)
        2 + 1
      end,
      "timeout" => :infinity
    }

    task2 = %{
      "name" => "t1",
      "enables" => [],
      "exec" => fn _ ->
        Process.sleep(500)
        3 + 4
      end,
      "timeout" => :infinity
    }

    assert {:ok, id} = SPE.submit_job(%{"name" => "nisse", "tasks" => [task1, task2]})
    Phoenix.PubSub.subscribe(SPE.PubSub, id)
    SPE.start_job(id)
    all_broadcasts = get_all_broadcasts(1500)

    assert {:succeeded, %{"t0" => {:result, 3}, "t1" => {:result, 7}}} =
             get_result(id, all_broadcasts)

    assert {:ok, start_t0} = task_start_time(all_broadcasts, id, "t0")
    assert {:ok, start_t1} = task_start_time(all_broadcasts, id, "t1")
    assert {:ok, end_t0} = task_termination_time(all_broadcasts, id, "t0")
    assert {:ok, end_t1} = task_termination_time(all_broadcasts, id, "t1")

    # End times occur after start times
    assert end_t0 >= start_t0
    assert end_t1 >= start_t1

    # At least 500 milliseconds has passed when executing both tasks, and less than 750 milliseconds in total
    # (task overlap necessary)
    min_start = min(start_t0, start_t1)
    max_end = max(end_t0, end_t1)
    assert max_end - min_start >= 500
    assert max_end - min_start <= 750

    Phoenix.PubSub.unsubscribe(SPE.PubSub, id)
    assert :ok = Supervisor.stop(sup)
  end

  def task_start_time(all_broadcasts, job_id, task_id) do
    assert [time] =
             Enum.reduce(all_broadcasts, [], fn broadcast, acc ->
               case broadcast do
                 {:spe, time, {^job_id, :task_started, ^task_id}} -> [time | acc]
                 _ -> acc
               end
             end)

    {:ok, time}
  end

  def task_termination_time(all_broadcasts, job_id, task_id) do
    assert [time] =
             Enum.reduce(all_broadcasts, [], fn broadcast, acc ->
               case broadcast do
                 {:spe, time, {^job_id, :task_terminated, ^task_id}} -> [time | acc]
                 _ -> acc
               end
             end)

    {:ok, time}
  end

  def get_all_results(all_broadcasts) do
    Enum.reduce(all_broadcasts, [], fn broadcast, acc ->
      case broadcast do
        {:spe, _, msg = {_id, :result, _}} -> [msg | acc]
        _ -> acc
      end
    end)
  end

  def get_result(id, all_broadcasts) do
    assert [{^id, :result, result}] =
             Enum.filter(get_all_results(all_broadcasts), fn result ->
               case result do
                 {^id, :result, _} -> true
                 _ -> false
               end
             end)

    result
  end

  def get_all_broadcasts(timeout), do: get_all_broadcasts(timeout, [])

  def get_all_broadcasts(timeout, broadcasts) do
    now = :erlang.monotonic_time(:millisecond)

    receive do
      broadcast = {:spe, _time, _} ->
        later = :erlang.monotonic_time(:millisecond)
        elapsed = later - now

        if elapsed >= timeout do
          Enum.reverse(broadcasts)
        else
          get_all_broadcasts(timeout - elapsed, [broadcast | broadcasts])
        end
    after
      timeout -> Enum.reverse(broadcasts)
    end
  end
end
