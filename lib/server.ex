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

  defp make_job_id do
    :erlang.unique_integer([:positive]) |> Integer.to_string()
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


  def handle_call({:start_job, job_id}, _from, state) do
    case Map.get(state.jobs, job_id) do
      nil ->
        {:reply, {:error, :job_not_found}, state}

      %{status: :started} ->
        {:reply, {:error, :job_already_started}, state}

      job ->
        Phoenix.PubSub.broadcast(SPE.PubSub, job_id, {:spe, now(), {job_id, :job_started}})

        tasks = job.tasks

        # Proceso recolector
        collector =
          spawn(fn ->
            results = collect_results(%{}, length(tasks))

            status =
              if Enum.any?(results, fn {_k, v} -> match?({:failed, _}, v) end),
                do: :failed,
                else: :succeeded

            Phoenix.PubSub.broadcast(SPE.PubSub, job_id, {:spe, now(), {job_id, :result, {status, results}}})
          end)

        Enum.each(tasks, fn task ->
          spawn(fn ->
            Phoenix.PubSub.broadcast(SPE.PubSub, job_id, {:spe, now(), {job_id, :task_started, task["name"]}})

            result =
              try do
                res = maybe_with_timeout(task["timeout"], fn -> task["exec"].(%{}) end)
                {:result, res}
              rescue
                e -> {:failed, Exception.message(e)}
              catch
                kind, val -> {:failed, {kind, val}}
              end

            Phoenix.PubSub.broadcast(SPE.PubSub, job_id, {:spe, now(), {job_id, :task_terminated, task["name"]}})
            send(collector, {:task_done, task["name"], result})
          end)
        end)

        updated_job = Map.put(job, :status, :started)
        new_state = %{state | jobs: Map.put(state.jobs, job_id, updated_job)}
        {:reply, :ok, new_state}
    end
  end

  defp collect_results(results, 0), do: results

  defp collect_results(results, remaining) do
    receive do
      {:task_done, task_name, result} ->
        collect_results(Map.put(results, task_name, result), remaining - 1)
    end
  end


  defp now, do: System.system_time(:millisecond)

  defp maybe_with_timeout(:infinity, fun), do: fun.()

  defp maybe_with_timeout(ms, fun) do
    parent = self()

    task = Task.async(fn ->
      send(parent, {:task_result, self(), fun.()})
    end)

    task_pid = task.pid

    receive do
      {:task_result, ^task_pid, result} -> result
    after
      ms ->
        Process.exit(task_pid, :kill)
        {:failed, :timeout}
    end
  end

end
