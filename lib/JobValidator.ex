defmodule SPE.JobValidator do
  alias SPE.Task

  @moduledoc """
  Handles validation of job descriptions and dependency analysis.

  This module ensures that a submitted job is syntactically correct, that all tasks
  are well-formed, and that task dependencies are valid and acyclic. It also builds
  a Directed Acyclic Graph (DAG) used to determine task execution order.
  """

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
    {:error, :invalid_job}
  end

  defp validate_structure(%{"name" => ""}) do
    {:error, :invalid_job}
  end

  defp validate_structure(%{}) do
    {:error, :invalid_job}
  end

  defp validate_structure(_) do
    {:error, :invalid_job}
  end

  defp validate_tasks([]) do
    {:error, :invalid_tasks}
  end

  defp validate_tasks(tasks) when is_list(tasks) do
    validated =
      tasks
      |> Enum.map(&Task.normalize_task/1)
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
    dag = build_dag(tasks)

    if Enum.all?(tasks, fn t -> Enum.all?(t["enables"], &(&1 in task_names)) end) do
      if is_dag?(dag) do
        {:ok, :dependencies_valid}
      else
        {:error, :invalid_dependencies}
      end
    else
      {:error, :invalid_dependencies}
    end
  end

  def build_dag(tasks) do
    for task <- tasks, into: %{} do
      name = task["name"]

      depends_on =
        Enum.filter(tasks, fn t -> name in (t["enables"] || []) end)
        |> Enum.map(& &1["name"])

      {name, depends_on}
    end
  end

  defp is_dag?(%{}) do
    true
  end

  defp is_dag?(dag) do
    try do
      topological_sort(dag)
      true
    rescue
      _ -> false
    end
  end

  defp topological_sort(dag) do
    dag
    |> Enum.reduce(%{}, fn {node, _deps}, acc ->
      acc
      |> Map.put_new(node, 0)
      |> Enum.reduce(acc, fn dep, inner_acc ->
        Map.update(inner_acc, dep, 1, &(&1 + 1))
      end)
    end)
    |> do_topological_sort(dag, [])
  end

  defp do_topological_sort(counts, _dag, result) when map_size(counts) == 0 do
    Enum.reverse(result)
  end

  defp do_topological_sort(counts, dag, result) do
    {ready, new_counts} =
      Enum.split_with(counts, fn {_node, count} -> count == 0 end)

    if ready == [] do
      raise "Cycle detected"
    end

    ready_nodes = Enum.map(ready, &elem(&1, 0))

    updated_counts =
      Enum.reduce(ready_nodes, new_counts, fn node, acc ->
        Enum.reduce(dag[node], acc, fn dep, inner_acc ->
          Map.update(inner_acc, dep, 0, &(&1 - 1))
        end)
      end)

    do_topological_sort(updated_counts, dag, ready_nodes ++ result)
  end
end
