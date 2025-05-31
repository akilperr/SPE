defmodule SPE do
  use Supervisor

  @moduledoc """
  Main public interface of SPE.

  Provides high-level functions for starting the system,
  submitting jobs, and triggering execution.
  """

  def start_link(options) do
    Supervisor.start_link(__MODULE__, options, name: __MODULE__)
  end

  def submit_job(job_description) do
    GenServer.call(SPE.Server, {:submit_job, job_description})
  end

  def start_job(job_id) do
    GenServer.call(SPE.Server, {:start_job, job_id})
  end

  def init(options) do
    num_workers = Keyword.get(options, :num_workers, :infinity)

    children = [
      {Phoenix.PubSub, name: SPE.PubSub},
      {SPE.WorkerSupervisor, []},
      {SPE.Server, %{num_workers: num_workers}}
    ]

    Supervisor.init(children, strategy: :one_for_one)
  end
end
