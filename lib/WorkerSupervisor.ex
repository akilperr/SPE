defmodule SPE.WorkerSupervisor do
  use Supervisor

  @moduledoc """
  Supervisor that initializes and oversees the task supervision infrastructure.

  Starts both a Task.Supervisor and a DynamicSupervisor used to spawn and manage
  individual TaskWorker processes during job execution.
  """

  def start_link(_opts) do
    Supervisor.start_link(__MODULE__, :ok, name: __MODULE__)
  end

  def init(:ok) do
    children = [
      {
        Task.Supervisor,
        name: SPE.TaskWorkerSupervisor, max_restarts: 1000, max_seconds: 5
      },
      {
        DynamicSupervisor,
        name: SPE.TaskSupervisor, strategy: :one_for_one, max_restarts: 1000, max_seconds: 5
      }
    ]

    Supervisor.init(children, strategy: :one_for_one)
  end

  def start_task(server_pid, job_id, task, dependencies) do
    task_args = %{
      server_pid: server_pid,
      job_id: job_id,
      task: task,
      dependencies: dependencies
    }

    DynamicSupervisor.start_child(
      SPE.TaskSupervisor,
      {SPE.TaskWorker, task_args}
    )
  end
end
