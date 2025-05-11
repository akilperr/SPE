defmodule SpeTest do
  use ExUnit.Case
  alias Spe.TaskDef
  alias Spe.JobValidator
  doctest Spe


    # Tasks validation tests
    describe "TaskDef.normalize_task/1" do
      test "Validates correct task" do
        task = %{"name" => "task1", "exec" => fn _ -> 1 + 2 end}
        assert TaskDef.normalize_task(task) == %{"name" => "task1", "exec" => task["exec"], "enables" => [], "timeout" => :infinity}
      end

      test "Verifies validation fails if incorrect name" do
        task = %{"exec" => fn _ -> 1 + 2 end}
        assert TaskDef.normalize_task(task) == {:error, :invalid_task}
      end

      test "Verifies if validation fails with wrong arity" do
        task = %{"name" => "task1", "exec" => fn -> 1 + 2 end}
        assert TaskDef.normalize_task(task) == {:error, :invalid_task}
      end

      test "Verifies if validation fails with incorrect timeout" do
        task = %{"name" => "task1", "exec" => fn _ -> 1 + 2 end, "timeout" => -5}
        assert TaskDef.normalize_task(task) == {:error, :invalid_task}
      end

      test "Verifies validation fails with incorrect enables list" do
        task = %{"name" => "task1", "exec" => fn _ -> 1 + 2 end, "enables" => "task2"}
        assert TaskDef.normalize_task(task) == {:error, :invalid_task}
      end
    end

    # Jobs validation test
    describe "JobValidator.validate_job/1" do
      test "Verifies if validation succeeds with correct job" do
        job = %{"tasks" => [
          %{"name" => "task1", "exec" => fn _ -> 1 end},
          %{"name" => "task2", "exec" => fn %{"task1" => v} -> v + 2 end, "enables" => ["task1"]}
        ]}

        assert JobValidator.validate_job(job) == {:ok, [
          %{"name" => "task1", "exec" => job["tasks"] |> hd() |> Map.get("exec"), "enables" => [], "timeout" => :infinity},
          %{"name" => "task2", "exec" => job["tasks"] |> tl() |> hd() |> Map.get("exec"), "enables" => ["task1"], "timeout" => :infinity}
        ]}

      end

      test "Verifies if validation fails with an incorrect task" do
        job = %{"tasks" => [
          %{"name" => "task1", "exec" => fn _ -> 1 end},
          %{"exec" => fn _ -> 2 end}  # missing name
        ]}

        assert JobValidator.validate_job(job) == {:error, :invalid_job}
      end

      test "Veifies if validation fails with incoherent dependencies" do
        job = %{"tasks" => [
          %{"name" => "task1", "exec" => fn _ -> 1 end},
          %{"name" => "task2", "exec" => fn %{"taskX" => v} -> v + 2 end, "enables" => ["taskX"]}
        ]}

        assert JobValidator.validate_job(job) == {:error, :invalid_dependencies}
      end
    end

end
