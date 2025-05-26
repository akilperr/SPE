defmodule SPE do
  use Supervisor


  def start_link(options) do
    Supervisor.start_link(__MODULE__, options, name: __MODULE__)
  end

  def init(options) do
    num_workers = Keyword.get(options, :num_workers, :infinity)

    children = [
      # Inicia Phoenix.PubSub
      {Phoenix.PubSub, name: SPE.PubSub},

      # Inicia el GenServer principal que maneja los jobs
      {SPE.Server, %{num_workers: num_workers}}
    ]

    Supervisor.init(children, strategy: :one_for_one)
  end


  def submit_job(job_description) do
    GenServer.call(SPE.Server, {:submit_job, job_description})
  end


  defmodule TaskDef do
  @moduledoc """
  This module manages the tasks validation (synthactically correct)
  """

    def normalize_task(task) do # Verifies correct fields and completes omitted ones
      if is_map(task) do
        name = Map.get(task, "name")
        exec = Map.get(task, "exec")
        enables = Map.get(task, "enables", [])
        timeout = Map.get(task, "timeout", :infinity)

        if is_binary(name) and is_function(exec, 1) and valid_timeout?(timeout) and is_list(enables) do # For the moment, we only verify that enables is a list
          %{
            "name" => name,
            "exec" => exec,
            "enables" => enables,
            "timeout" => timeout
          }
        else
          {:error, :invalid_task}
        end
      else
        {:error, :invalid_task}
      end
    end

    defp valid_timeout?(:infinity), do: true # Valid timeout: infinity or a positive number
    defp valid_timeout?(t) when is_integer(t) and t >= 0, do: true
    defp valid_timeout?(_), do: false
    def valid_enables?(list, defined_names) when is_list(list) do # Valid enables list: all task names have to be in the task list, used when all job tasks defined
      Enum.all?(list, fn name -> is_binary(name) and name in defined_names end)
    end
    def valid_enables?(_, _), do: false
  end

  defmodule JobValidator do
  @moduledoc """
  This module manages the jobs validation (synthactically correct)
  """

    alias SPE.TaskDef

    def validate_job(job_description) do
      if is_map(job_description) do

        tasks = Map.get(job_description, "tasks")
        if tasks == %{} do
          {:error, :invalid_job_no_task}
        end

        validated_tasks = Enum.map(tasks, &TaskDef.normalize_task/1)

        if Enum.any?(validated_tasks, fn t -> t == {:error, :invalid_task} end) do
          {:error, :invalid_job}
        else
          task_names = Enum.map(validated_tasks, fn t -> t["name"] end)
          validate_task_dependencies(validated_tasks, task_names)
        end
      else
        {:error, :invalid_job_not_map}
      end
    end

    def validate_task_dependencies(tasks, task_names) do
      if Enum.all?(tasks, fn task -> TaskDef.valid_enables?(task["enables"], task_names) end) do
        {:ok, tasks}
      else
        {:error, :invalid_dependencies}
      end
    end
  end



end
