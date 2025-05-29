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

  def start_job(job_id) do
    GenServer.call(SPE.Server, {:start_job, job_id})
  end

  defmodule TaskDef do
    def normalize_task(task) when is_map(task) do
      with {:ok, name} <- validate_name(Map.get(task, "name")),
          {:ok, exec} <- validate_exec(Map.get(task, "exec")),
          {:ok, enables} <- validate_enables(Map.get(task, "enables", [])),
          {:ok, timeout} <- validate_timeout(Map.get(task, "timeout", :infinity)) do
        %{
          "name" => name,
          "exec" => exec,
          "enables" => enables,
          "timeout" => timeout
        }
      else
        {:error, reason} -> {:error, reason}
      end
    end

    def normalize_task(_), do: {:error, "Task must be a map"}

    defp validate_name(name) when is_binary(name) and name != "", do: {:ok, name}
    defp validate_name(_), do: {:error, :invalid_task_name}

    defp validate_exec(fun) when is_function(fun, 1), do: {:ok, fun}
    defp validate_exec(_), do: {:error, :invalid_task_exec}

    defp validate_enables(enables) when is_list(enables), do: {:ok, enables}
    defp validate_enables(_enables), do: {:error, :invalid_task_enables}

    defp validate_timeout(:infinity), do: {:ok, :infinity}
    defp validate_timeout(timeout) when is_integer(timeout) and timeout > 0, do: {:ok, timeout}
    defp validate_timeout(_), do: {:error, :invalid_task_timeout}
  end


  defmodule JobValidator do
    @moduledoc """
    This module manages the jobs validation (synthactically correct)
    """

    alias SPE.TaskDef

    def validate_job(job_description) do
      with {:ok, valid_job} <- validate_structure(job_description),
           {:ok, validated_tasks} <- validate_tasks(valid_job.tasks),
           {:ok, _} <- validate_task_dependencies(validated_tasks) do
        {:ok, validated_tasks}
      end
    end

    defp validate_structure(%{"name" => name, "tasks" => tasks})
         when is_binary(name) and name != "" and is_list(tasks) do
      {:ok, %{name: name, tasks: tasks}}
    end

    defp validate_structure(%{"name" => name}) when not is_binary(name) do
      {:error, :job_name_not_a_string}
    end

    defp validate_structure(%{"name" => ""}) do
      {:error, :job_name_empty}
    end

    defp validate_structure(%{}) do
      {:error, :missing_tasks_key}
    end

    defp validate_structure(_) do
      {:error, :invalid_job_format}
    end

    defp validate_tasks([]) do
      {:error, :invalid_tasks}
    end

    defp validate_tasks(tasks) when is_list(tasks) do
      validated =
        tasks
        |> Enum.map(&TaskDef.normalize_task/1)
        |> Enum.reduce({:ok, []}, fn
          {:error, reason}, _ -> {:error, reason}
          task, {:ok, acc} -> {:ok, [task | acc]}
          _, error -> error
        end)

      case validated do
        {:ok, tasks} ->
          task_names = Enum.map(tasks, & &1["name"])

          if length(task_names) == length(Enum.uniq(task_names)) do
            {:ok, tasks}
          else
      {:error, :invalid_tasks}
          end

        error ->
          error
      end
    end

    defp validate_tasks(nil) do
      {:error, :invalid_tasks}
    end

    defp validate_tasks(_) do
      {:error, :invalid_tasks}
    end

    defp validate_task_dependencies(tasks) do
      task_names = Enum.map(tasks, & &1["name"])

      if Enum.all?(tasks, fn task ->
           valid_enables?(task["enables"], task_names)
         end) do
        {:ok, :dependencies_valid}
      else
      {:error, :invalid_dependencies}
      end
    end

    defp valid_enables?(enables, all_task_names) do
      Enum.all?(enables, fn task_name ->
        task_name in all_task_names
      end)
    end
  end

end
