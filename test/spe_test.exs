defmodule SpeTest do
  use ExUnit.Case
  alias SPE.TaskDef
  alias SPE.JobValidator
  require Logger
  use ExUnit.Case, async: false

  doctest SPE

  # Tasks validation tests
  describe "TaskDef.normalize_task/1" do
    test "Validates correct task" do
      task = %{"name" => "task1", "exec" => fn _ -> 1 + 2 end}

      assert TaskDef.normalize_task(task) == %{
               "name" => "task1",
               "exec" => task["exec"],
               "enables" => [],
               "timeout" => :infinity
             }
    end

    test "Verifies validation fails if incorrect name" do
      task = %{"exec" => fn _ -> 1 + 2 end}
      assert TaskDef.normalize_task(task) == {:error, :invalid_task_name}
    end

    test "Verifies if validation fails with wrong arity" do
      task = %{"name" => "task1", "exec" => fn -> 1 + 2 end}
      assert TaskDef.normalize_task(task) == {:error, :invalid_task_exec}
    end

    test "Verifies if validation fails with incorrect timeout" do
      task = %{"name" => "task1", "exec" => fn _ -> 1 + 2 end, "timeout" => -5}
      assert TaskDef.normalize_task(task) == {:error, :invalid_task_timeout}
    end

    test "Verifies validation fails with incorrect enables list" do
      task = %{"name" => "task1", "exec" => fn _ -> 1 + 2 end, "enables" => "task2"}
      assert TaskDef.normalize_task(task) == {:error, :invalid_task_enables}
    end
  end

  # Jobs validation test
  describe "JobValidator.validate_job/1" do
    test "Verifies if validation succeeds with correct job" do
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

    test "Verifies if validation fails with an incorrect task" do
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

    test "Veifies if validation fails with incoherent dependencies" do
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
    test "starts SPE with no options" do
      assert {:ok, sup} = SPE.start_link([])
      assert :ok = Supervisor.stop(sup)
    end

    test "starts SPE with num_workers option" do
      assert {:ok, sup} = SPE.start_link(num_workers: 2)
      assert :ok = Supervisor.stop(sup)
    end

    test "fails if SPE is already started" do
      assert {:ok, _pid} = SPE.start_link([])
      assert {:error, {:already_started, _}} = SPE.start_link([])
      assert :ok = Supervisor.stop(sup)
    end

  end

  # SPE.submit_job/1 validation tests
  

  # SPE.start_job/1 validation tests

end
