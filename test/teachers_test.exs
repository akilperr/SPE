defmodule SPETest do
  require Logger

  use ExUnit.Case, async: false

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
