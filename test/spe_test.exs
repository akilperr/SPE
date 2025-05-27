defmodule SpeTest do
  use ExUnit.Case
  alias SPE.TaskDef
  alias SPE.JobValidator
  require Logger
  use ExUnit.Case, async: false

  doctest SPE

  # Tasks validation tests
  describe "TaskDef.normalize_task/1" do
    test "TaskDef 1 - Validates correct task" do
      task = %{"name" => "task1", "exec" => fn _ -> 1 + 2 end}

      assert TaskDef.normalize_task(task) == %{
               "name" => "task1",
               "exec" => task["exec"],
               "enables" => [],
               "timeout" => :infinity
             }
    end

    test "TaskDef 2 - Verifies validation fails if incorrect name" do
      task = %{"exec" => fn _ -> 1 + 2 end}
      assert TaskDef.normalize_task(task) == {:error, :invalid_task_name}
    end

    test "TaskDef 3 - Verifies if validation fails with wrong arity" do
      task = %{"name" => "task1", "exec" => fn -> 1 + 2 end}
      assert TaskDef.normalize_task(task) == {:error, :invalid_task_exec}
    end

    test "TaskDef 4 - Verifies if validation fails with incorrect timeout" do
      task = %{"name" => "task1", "exec" => fn _ -> 1 + 2 end, "timeout" => -5}
      assert TaskDef.normalize_task(task) == {:error, :invalid_task_timeout}
    end

    test "TaskDef 5 - Verifies validation fails with incorrect enables list" do
      task = %{"name" => "task1", "exec" => fn _ -> 1 + 2 end, "enables" => "task2"}
      assert TaskDef.normalize_task(task) == {:error, :invalid_task_enables}
    end
  end

  # Jobs validation test
  describe "JobValidator.validate_job/1" do
    test "JobValidator 1 - Verifies if validation succeeds with correct job" do
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

    test "JobValidator 2 - Verifies if validation fails with an incorrect task" do
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

    test "JobValidator 3 - Verifies if validation fails with incoherent dependencies" do
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
    test "start_link 1 - starts SPE with no options" do
      assert {:ok, sup} = SPE.start_link([])
      assert :ok = Supervisor.stop(sup)
    end

    test "start_link 2 -starts SPE with num_workers option" do
      assert {:ok, sup} = SPE.start_link(num_workers: 2)
      assert :ok = Supervisor.stop(sup)
    end

    test "start_link 3 - fails if SPE is already started" do
      assert {:ok, _pid} = SPE.start_link([])
      assert {:error, {:already_started, _}} = SPE.start_link([])
    end

  end

  # SPE.submit_job/1 validation tests
  describe "submit_job/1 custom tests" do

    test "submit_job 1 - accepts minimal valid job" do
      start_supervised!({SPE, []})
      task = %{
        "name" => "t1",
        "exec" => fn _ -> 42 end
      }
      job = %{
        "name" => "my_job",
        "tasks" => [task]
      }
      assert {:ok, _job_id} = SPE.submit_job(job)
    end

  end


  # SPE.start_job/1 validation tests

end
