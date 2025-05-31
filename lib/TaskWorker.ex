defmodule SPE.TaskWorker do
  use GenServer

  def start_link(options) do
    GenServer.start_link(__MODULE__, options)
  end

  def init(%{server_pid: server_pid, job_id: job_id, task: task, dependencies: dependencies}) do
    # Filter dependencies to only include the ones this task depends on
    Process.flag(:trap_exit, true)
    Process.flag(:priority, :low)


   task_ref = Task.Supervisor.async_nolink(SPE.TaskWorkerSupervisor, fn ->
    # Add small delay to prevent resource contention
    Process.sleep(1)
    execute_task(task, dependencies)
  end)

    {:ok, %{server_pid: server_pid, job_id: job_id, task: task, task_ref: task_ref}}
  end

  def handle_info({ref, result}, state) when ref == state.task_ref.ref do
    # Task completed normally
    Process.demonitor(ref, [:flush])
    report_result(state.server_pid,state.job_id, state.task["name"], result)
    {:stop, :normal, state}
  end

  def handle_info({:DOWN, ref, :process, _pid, reason}, state) when ref == state.task_ref.ref do
    # Task crashed
    report_result(state.server_pid,state.job_id, state.task["name"], {:failed, {:crashed, reason}})
    {:stop, :normal, state}
  end

  defp execute_task(task, dependencies) do
  #  IO.inspect({:execute_task, task["name"], dependencies}, label: "TaskWorker")
  try do
    case task["exec"].(dependencies) do
      result when is_integer(result) -> {:result, result}
      other -> {:failed, {:invalid_return, other}}
    end
  rescue
    e -> {:failed, {:exception, Exception.message(e)}}
  catch
    :exit, reason -> {:failed, {:exit, reason}}
    what, value -> {:failed, {:throw, {what, value}}}
  end
end

defp report_result(server_pid, job_id, task_name, result) do
    # IO.inspect({:report_result,server_pid, job_id, task_name, result}, label: "TaskWorker")

    case result do
      {:exit, :timeout} ->
        send(server_pid, {:task_completed, job_id, task_name, {:failed, :timeout}})

      {:result, value} ->
        send(server_pid, {:task_completed, job_id, task_name, {:result, value}})

      {:failed, reason} ->
        send(server_pid, {:task_completed, job_id, task_name, {:failed, reason}})

      other ->
        send(
          server_pid,
          {:task_completed, job_id, task_name, {:failed, {:unexpected_result, other}}}
        )
    end
  end

  def terminate(_reason, state) do
  if state.task_ref && Process.alive?(state.task_ref.pid) do
    Task.shutdown(state.task_ref, :brutal_kill)
  end
  :ok
end
end
