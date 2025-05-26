defmodule SPE.Server do
  use GenServer

  def start_link(state) do
    GenServer.start_link(__MODULE__, state, name: __MODULE__)
  end


  alias SPE.JobValidator

  # Public function (puedes omitir si solo usas desde SPE.ex)
  def submit_job(job_description) do
    GenServer.call(__MODULE__, {:submit_job, job_description})
  end

  def init(state) do
    state = Map.put_new(state, :jobs, %{})
    {:ok, state}
  end

  # Callback
  def handle_call({:submit_job, job_description}, _from, state) do
    case JobValidator.validate_job(job_description) do
      {:ok, validated_tasks} ->
        job_id = make_job_id()
        job_data = %{
          id: job_id,
          name: job_description["name"],
          tasks: validated_tasks,
          status: :submitted
        }

        new_jobs = Map.put(state.jobs, job_id, job_data)
        new_state = %{state | jobs: new_jobs}

        {:reply, {:ok, job_id}, new_state}

      {:error, reason} ->
        {:reply, {:error, reason}, state}
    end
  end

  defp make_job_id do
    :erlang.unique_integer([:positive]) |> Integer.to_string()
  end
end
