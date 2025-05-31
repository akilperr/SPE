defmodule SPE.TaskWorker do
  use GenServer

  @moduledoc """
  Worker module that executes a single task in isolation.

  Receives task and its dependencies, runs it, and reports the result
  back to the main server. Handles exceptions and timeouts gracefully.
  """

  def child_spec(args) do
    %{
      id: __MODULE__,
      start: {__MODULE__, :start_link, [args]},
      restart: :temporary
    }
  end

  def start_link(options) do
    GenServer.start_link(__MODULE__, options)
  end

  def init(%{server_pid: server_pid, job_id: job_id, task: task, dependencies: dependencies}) do
    Process.flag(:trap_exit, true)
    Process.flag(:priority, :low)

    time = :erlang.monotonic_time(:millisecond)

    Phoenix.PubSub.local_broadcast(
      SPE.PubSub,
      job_id,
      {:spe, time, {job_id, :task_started, task["name"]}}
    )

    task_ref =
      Task.Supervisor.async_nolink(SPE.TaskWorkerSupervisor, fn ->
        Process.sleep(1)
        execute_task(task, dependencies)
      end)

    timeout =
      case Map.get(task, "timeout", :infinity) do
        :infinity ->
          nil

        t when is_integer(t) and t > 0 ->
          Process.send_after(self(), {:timeout, task_ref.ref}, t)
      end

    {:ok,
     %{
       server_pid: server_pid,
       job_id: job_id,
       task: task,
       task_ref: task_ref,
       timeout_timer: timeout
     }}
  end

  def handle_info({ref, result}, state) when ref == state.task_ref.ref do
    if state.timeout_timer, do: Process.cancel_timer(state.timeout_timer)

    Process.demonitor(ref, [:flush])
    report_result(state.server_pid, state.job_id, state.task["name"], result)
    {:stop, :normal, state}
  end

  def handle_info({:DOWN, ref, :process, _pid, reason}, state) when ref == state.task_ref.ref do
    if state.timeout_timer, do: Process.cancel_timer(state.timeout_timer)

    report_result(
      state.server_pid,
      state.job_id,
      state.task["name"],
      {:failed, {:crashed, reason}}
    )

    {:stop, :normal, state}
  end

  def handle_info({:timeout, ref}, state) when ref == state.task_ref.ref do
    Task.shutdown(state.task_ref, :brutal_kill)
    report_result(state.server_pid, state.job_id, state.task["name"], {:exit, :timeout})
    {:stop, :normal, state}
  end

  defp execute_task(task, dependencies) do
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
