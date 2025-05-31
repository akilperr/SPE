defmodule SPE.Server do
  use GenServer
  alias SPE.{JobValidator}

  @moduledoc """
  Central GenServer responsible for managing job lifecycle and scheduling.

  Handles submission, validation, dependency analysis, and dispatching
  tasks according to the job's DAG.
  """

  defstruct [
    :num_workers,
    jobs: %{},
    running_tasks: 0,
    pending_tasks: %{},
    task_results: %{},
    executing_tasks: %{}
  ]

  def start_link(opts) do
    GenServer.start_link(__MODULE__, opts, name: __MODULE__)
  end

  def init(state) do
    {:ok,
     %__MODULE__{
       num_workers: state.num_workers,
       jobs: %{},
       running_tasks: 0,
       pending_tasks: %{},
       task_results: %{},
       executing_tasks: %{}
     }}
  end

  def handle_call({:submit_job, job_description}, _from, state) do
    case JobValidator.validate_job(job_description) do
      {:ok, validated_tasks} ->
        job_id = make_job_id()

        dag = SPE.JobValidator.build_dag(validated_tasks)

        job_data = %{
          id: job_id,
          name: job_description["name"],
          tasks: validated_tasks,
          status: :submitted,
          task_map: build_task_map(validated_tasks),
          dag: dag
        }

        new_jobs = Map.put(state.jobs, job_id, job_data)
        {:reply, {:ok, job_id}, %{state | jobs: new_jobs}}

      {:error, reason} ->
        {:reply, {:error, reason}, state}
    end
  end

  def handle_call({:start_job, job_id}, _from, state) do
    case Map.get(state.jobs, job_id) do
      nil ->
        {:reply, {:error, :job_not_found}, state}

      job when job.status != :submitted ->
        {:reply, {:error, :job_already_started}, state}

      job ->
        dag = SPE.JobValidator.build_dag(job.tasks)

        initial_tasks =
          Enum.filter(job.tasks, fn task ->
            dag[task["name"]] == []
          end)

        new_jobs = Map.put(state.jobs, job_id, %{job | status: :running})

        new_pending = Map.put(state.pending_tasks, job_id, initial_tasks)

        new_state = %{state | jobs: new_jobs, pending_tasks: new_pending}

        {:reply, {:ok, job_id}, schedule_tasks(new_state)}
    end
  end

  def handle_info({:task_completed, job_id, task_name, result}, state) do
    :erlang.garbage_collect(self())

    time = :erlang.monotonic_time(:millisecond)

    Phoenix.PubSub.local_broadcast(
      SPE.PubSub,
      job_id,
      {:spe, time, {job_id, :task_terminated, task_name}}
    )

    already_done = Map.get(state.task_results, job_id, %{}) |> Map.has_key?(task_name)

    if already_done do
      {:noreply, state}
    else

      new_results =
        Map.update(
          state.task_results,
          job_id,
          %{task_name => result},
          &Map.put(&1, task_name, result)
        )

      new_running = max(state.running_tasks - 1, 0)

      pending = Map.get(state.pending_tasks, job_id, [])

      new_pending_tasks =
        if Enum.any?(pending, fn t -> t["name"] == task_name end) do
          Enum.reject(pending, fn t -> t["name"] == task_name end)
        else
          pending
        end

      new_pending = Map.put(state.pending_tasks, job_id, new_pending_tasks)

      executing = Map.get(state.executing_tasks, job_id, MapSet.new())
      new_executing = Map.put(state.executing_tasks, job_id, MapSet.delete(executing, task_name))

      job = state.jobs[job_id]
      results = new_results[job_id] || %{}
      executing_names = MapSet.to_list(Map.get(state.executing_tasks, job_id, MapSet.new()))
      pending_names = Map.get(state.pending_tasks, job_id, []) |> Enum.map(& &1["name"])

      completed_names = Map.keys(results)

      ready_tasks =
        job.tasks
        |> Enum.filter(fn task ->
          dependencies = job.dag[task["name"]]
          name = task["name"]

          name not in pending_names and
            name not in executing_names and
            name not in completed_names and
            Enum.all?(dependencies, &match?({:result, _}, results[&1]))
        end)

      existing_completed_names = Map.keys(results)

      filtered_ready_tasks =
        Enum.reject(ready_tasks, fn task ->
          name = task["name"]
          name in existing_completed_names
        end)

      new_pending2 =
        Map.update(new_pending, job_id, filtered_ready_tasks, fn old ->
          old ++ filtered_ready_tasks
        end)

      new_state = %{
        state
        | running_tasks: new_running,
          task_results: new_results,
          pending_tasks: new_pending2,
          executing_tasks: new_executing
      }

      job_completed = check_job_completion(job_id, new_state)

      job = new_state.jobs[job_id]

      if job_completed and job.status != :completed do
        final_results = compile_job_results(job_id, new_state)

        status =
          if Enum.any?(final_results, fn {_, res} -> match?({:failed, _}, res) end),
            do: :failed,
            else: :succeeded

        Phoenix.PubSub.local_broadcast(
          SPE.PubSub,
          job_id,
          {:spe, time, {job_id, :result, {status, final_results}}}
        )

        new_jobs = Map.put(new_state.jobs, job_id, %{job | status: :completed})
        {:noreply, schedule_tasks(%{new_state | jobs: new_jobs})}
      else
        {:noreply, schedule_tasks(new_state)}
      end
    end
  end

  defp remove_task(pending_tasks, job_id, task) do
    Map.update!(pending_tasks, job_id, fn tasks ->
      Enum.reject(tasks, &(&1["name"] == task["name"]))
    end)
  end

  defp schedule_tasks(state) do
    state = %{state | running_tasks: max(state.running_tasks, 0)}

    max_workers =
      case state.num_workers do
        :infinity -> 1_000_000_000
        n -> n
      end

    if state.running_tasks > max_workers * 0.75 do
      Process.sleep(5)
    end

    ready =
      for {job_id, tasks} <- state.pending_tasks,
          task <- tasks,
          state.running_tasks < max_workers,
          job = state.jobs[job_id],
          results = state.task_results[job_id] || %{},
          executing = Map.get(state.executing_tasks, job_id, MapSet.new()),
          not MapSet.member?(executing, task["name"]),
          not Map.has_key?(results, task["name"]),
          dependencies = job.dag[task["name"]],
          Enum.all?(dependencies, fn dep -> match?({:result, _}, results[dep]) end) do
        {job_id, task}
      end

    case ready do
      [] ->
        state

      [{job_id, task} | _rest] ->
        dep_map =
          (state.task_results[job_id] || %{})
          |> Enum.filter(fn {_k, v} -> match?({:result, _}, v) end)
          |> Enum.map(fn {k, {:result, v}} -> {k, v} end)
          |> Map.new()

        new_pending = remove_task(state.pending_tasks, job_id, task)
        executing = Map.get(state.executing_tasks, job_id, MapSet.new())

        new_executing =
          Map.put(state.executing_tasks, job_id, MapSet.put(executing, task["name"]))

        new_state = %{
          state
          | pending_tasks: new_pending,
            executing_tasks: new_executing,
            running_tasks: state.running_tasks + 1
        }

        {:ok, _pid} = SPE.WorkerSupervisor.start_task(self(), job_id, task, dep_map)

        schedule_tasks(new_state)
    end
  end

  defp check_job_completion(job_id, state) do
    job = state.jobs[job_id]
    results = state.task_results[job_id] || %{}

    Enum.all?(job.tasks, fn task ->
      Map.has_key?(results, task["name"]) ||
        Enum.any?(job.dag[task["name"]], fn dep ->
          case results[dep] do
            {:failed, _} -> true
            _ -> false
          end
        end)
    end)
  end

  defp compile_job_results(job_id, state) do
    job = state.jobs[job_id]
    results = state.task_results[job_id] || %{}

    Enum.reduce(job.tasks, %{}, fn task, acc ->
      deps = job.dag[task["name"]]

      result =
        case results[task["name"]] do
          nil ->
            if Enum.any?(deps, fn dep ->
                 case results[dep] do
                   {:failed, _} -> true
                   _ -> false
                 end
               end) do
              :not_run
            else
              nil
            end

          res ->
            res
        end

      if result != nil do
        Map.put(acc, task["name"], result)
      else
        acc
      end
    end)
  end

  defp build_task_map(tasks) do
    task_map =
      tasks
      |> Enum.reduce(%{}, fn task, acc ->
        Map.put(acc, task["name"], task)
      end)

    # Then enrich with predecessors
    Enum.reduce(tasks, task_map, fn task, acc ->
      predecessors =
        Enum.filter(tasks, fn t ->
          task["name"] in t["enables"]
        end)
        |> Enum.map(& &1["name"])

      Map.update!(acc, task["name"], &Map.put(&1, "predecessors", predecessors))
    end)
  end

  defp make_job_id do
    :erlang.unique_integer([:positive]) |> Integer.to_string()
  end

end
