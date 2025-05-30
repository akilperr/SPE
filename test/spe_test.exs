defmodule SpeTest do
  use ExUnit.Case
  alias SPE.Task
  alias SPE.JobValidator
  require Logger
  use ExUnit.Case, async: false

  doctest SPE

  # Tasks validation tests
  describe "Task.normalize_task/1" do
    test "1 - Validates correct task" do
      task = %{"name" => "task1", "exec" => fn _ -> 1 + 2 end}

      assert Task.normalize_task(task) == %{
               "name" => "task1",
               "exec" => task["exec"],
               "enables" => [],
               "timeout" => :infinity
             }
    end

    test "2 - Verifies validation fails if incorrect name" do
      task = %{"exec" => fn _ -> 1 + 2 end}
      assert Task.normalize_task(task) == {:error, :invalid_task_name}
    end

    test "3 - Verifies if validation fails with wrong arity" do
      task = %{"name" => "task1", "exec" => fn -> 1 + 2 end}
      assert Task.normalize_task(task) == {:error, :invalid_task_exec}
    end

    test "4 - Verifies if validation fails with incorrect timeout" do
      task = %{"name" => "task1", "exec" => fn _ -> 1 + 2 end, "timeout" => -5}
      assert Task.normalize_task(task) == {:error, :invalid_task_timeout}
    end

    test "5 - Verifies validation fails with incorrect enables list" do
      task = %{"name" => "task1", "exec" => fn _ -> 1 + 2 end, "enables" => "task2"}
      assert Task.normalize_task(task) == {:error, :invalid_task_enables}
    end
  end

  # Jobs validation test
  describe "JobValidator.validate_job/1" do
    test "1 - Verifies if validation succeeds with correct job" do
      job = %{"name" => "job1",
        "tasks" => [
          %{"name" => "task1", "exec" => fn _ -> 1 end},
          %{"name" => "task2", "exec" => fn %{"task1" => v} -> v + 2 end, "enables" => ["task1"]}
        ]
      }

      assert JobValidator.validate_job(job) ==
               {:ok,
                [%{
                    "name" => "task2",
                    "exec" => job["tasks"] |> tl() |> hd() |> Map.get("exec"),
                    "enables" => ["task1"],
                    "timeout" => :infinity
                  },
                  %{
                    "name" => "task1",
                    "exec" => job["tasks"] |> hd() |> Map.get("exec"),
                    "enables" => [],
                    "timeout" => :infinity
                  }

                ]}
    end

    test "2 - Verifies if validation fails with an incorrect task" do
      job = %{
        "name" => "hola",
        "tasks" => [
          %{"name" => "task1", "exec" => fn _ -> 1 end},
          # missing name
          %{"exec" => fn _ -> 2 end}
        ]
      }

      assert JobValidator.validate_job(job) == {:error, :invalid_task_name}
    end

    test "3 - Verifies if validation fails with incoherent dependencies" do
      job = %{"name" => "test_job",
        "tasks" => [
          %{"name" => "task1", "exec" => fn _ -> 1 end},
          %{"name" => "task2", "exec" => fn %{"taskX" => v} -> v + 2 end, "enables" => ["taskX"]}
        ]
      }

      assert JobValidator.validate_job(job) == {:error, :invalid_dependencies}
    end
  end

  # SPE.start_link/1 validation tests
  describe "SPE.start_link/1" do
    test "1 - starts SPE with no options" do
      assert {:ok, sup} = SPE.start_link([])
      assert :ok = Supervisor.stop(sup)
    end

    test "2 -starts SPE with num_workers option" do
      assert {:ok, sup} = SPE.start_link(num_workers: 2)
      assert :ok = Supervisor.stop(sup)
    end

    test "3 - fails if SPE is already started" do
      assert {:ok, _pid} = SPE.start_link([])
      assert {:error, {:already_started, _}} = SPE.start_link([])
    end

  end

  # SPE.submit_job/1 validation tests
  describe "1 custom tests" do

    test "1 - accepts minimal valid job" do
      {:ok, _sup} = SPE.start_link([])
      task = %{
        "name" => "t1",
        "exec" => fn _ -> 42 end
      }
      job = %{
        "name" => "my_job",
        "tasks" => [task]
      }
      assert {:ok, _job_id} = SPE.submit_job(job)
      Supervisor.stop(SPE)
    end

    test "2 - rejects job with duplicate task names" do
      {:ok, _sup} = SPE.start_link([])
      task1 = %{"name" => "t1", "exec" => fn _ -> 1 end}
      task2 = %{"name" => "t1", "exec" => fn _ -> 2 end}  # mismo nombre
      job = %{
        "name" => "dup_tasks_job",
        "tasks" => [task1, task2]
      }
      assert {:error, :invalid_tasks} = SPE.submit_job(job)
      Supervisor.stop(SPE)
    end

    test "3 - allows submitting multiple jobs" do
      {:ok, _sup} = SPE.start_link([])
      task = %{"name" => "t1", "exec" => fn _ -> 1 end}
      job = %{"name" => "job", "tasks" => [task]}

      assert {:ok, job_id1} = SPE.submit_job(job)
      assert {:ok, job_id2} = SPE.submit_job(job)
      assert job_id1 != job_id2
      Supervisor.stop(SPE)
    end

  end


  # SPE.start_job/1 validation tests
  describe "SPE.start_job/1" do

    test "1 - fails if job_id does not exist" do
      {:ok, _sup} = SPE.start_link([])
      assert {:error, :job_not_found} = SPE.start_job("non_existing_id")
      Supervisor.stop(SPE)
    end

    test "2 - fails if job has already been started" do
      {:ok, _sup} = SPE.start_link([])
      job = %{
        "name" => "test_job",
        "tasks" => [
          %{"name" => "t1", "exec" => fn _ -> 1 end}
        ]
      }
      {:ok, job_id} = SPE.submit_job(job)
      assert {:ok, ^job_id} = SPE.start_job(job_id)
      assert {:error, :job_already_started} = SPE.start_job(job_id)
      Supervisor.stop(SPE)
    end

    test "3 - starts a submitted job and updates status" do
      {:ok, _sup} = SPE.start_link([])
      job = %{
        "name" => "simple_job",
        "tasks" => [
          %{"name" => "t1", "exec" => fn _ -> 42 end}
        ]
      }
      {:ok, job_id} = SPE.submit_job(job)
      assert {:ok, ^job_id} = SPE.start_job(job_id)
      # Recuperamos el estado interno para verificar que se actualizó
      state = :sys.get_state(SPE.Server)
      job_data = Map.get(state.jobs, job_id)
      assert job_data.status == :running
      Supervisor.stop(SPE)
    end

  end

end
