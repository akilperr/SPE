defmodule SPE.Task do

  @moduledoc """
  Defines and validates individual tasks within a job.

  Ensures each task has a valid structure, including name, function,
  dependencies, and timeout configuration.
  """

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
  defp validate_enables(_), do: {:error, :invalid_task_enables}

  defp validate_timeout(:infinity), do: {:ok, :infinity}
  defp validate_timeout(timeout) when is_integer(timeout) and timeout > 0, do: {:ok, timeout}
  defp validate_timeout(_), do: {:error, :invalid_task_timeout}
end
