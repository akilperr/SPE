# SPE – School Process Engine

> Universidad Politécnica de Madrid · Programming Scalable Systems · 2024-2025

## Description

SPE is a job processing engine implemented in Elixir. It allows users to define and execute jobs composed of multiple tasks with potential dependencies between them. Tasks are executed concurrently when possible, following dependency constraints.

The system is built using core OTP components like `GenServer` and `Supervisor`, and uses `Phoenix.PubSub` to publish task and job execution events.

---

## Project Structure

- `_build/`: generated build artifacts (should not be edited manually).
- `deps/`: downloaded project dependencies.
  - `dep_from_git/`: placeholder or test dependency.
  - `phoenix_pubsub/`: Phoenix PubSub library for broadcasting events.

- `lib/`: main source code
  - `Spe.ex`: public interface for the job processing system.
  - `Server.ex`: internal GenServer that manages jobs and tasks.
  - `Task.ex`: handles task validation and normalization.
  - `JobValidator.ex`: validates job structure and builds the DAG.
  - `TaskWorker.ex`: executes individual tasks and reports results.
  - `WorkerSupervisor.ex`: supervises running task workers.

- `test/`: test suite
  - `our_tests.exs`: custom tests covering core logic and validation.
  - `spe_test.exs`: official test cases from instructors
  - `spe_test_with_tag.exs`: official test cases from instructors, supports `@tag` filtering.
  - `test_helper.exs`: shared configuration for test setup.

- `.formatter.exs`: Elixir formatting configuration for `mix format`.
- `.gitignore`: files and folders excluded from version control.
- `AUTHORS`: list of project contributors with UPM emails.
- `erl_crash.dump`: crash log created by Erlang VM (only if a failure occurs).
- `mix.exs`: Mix project definition with dependencies and entry point.
- `mix.lock`: dependency lock file for reproducible builds.
- `README.md`: main documentation for building, testing, and using the project.

---

## Installation

```bash
mix deps.get
```

---

## Running the Application

```bash
iex -S mix
```

---

## Running Tests

To run all tests:

```bash
mix test
```

### Running a specific test file

To run only a specific test file (e.g. your custom tests or teacher tests):

```bash
mix test test/spe_test.exs
mix test test/teachers_test.exs
```

### Running a single test using tags

In `teachers_test.exs`, `@tag` annotations are added to help debug individual cases:

```elixir
@tag :submit_good_jobs
test "submit good jobs" do
  ...
end
```

To run only that test:

```bash
mix test --only submit_good_jobs
```

---

## System Requirements

- **Elixir**: tested with version >= 1.14
- **Erlang/OTP**: compatible with OTP 24/25
- **Main dependencies**:
  - `{:phoenix_pubsub, "~> 2.1"}` (included in `mix.exs`)

---

## Persistence

Persistence is not implemented. The entire system runs in memory. No database or Ecto integration is used, so no additional configuration is required.

---

## Phoenix.PubSub Events

The system emits two types of events:

- `{:task_terminated, task_name}` – when a task finishes (success, timeout, or crash)
- `{:result, {status, results}}` – when a job completes

Subscribe to events using:

```elixir
Phoenix.PubSub.subscribe(SPE.PubSub, job_id)
```

---

## Job Example

```elixir
job = %{
  "name" => "math_job",
  "tasks" => [
    %{"name" => "a", "exec" => fn _ -> 2 end},
    %{"name" => "b", "exec" => fn %{"a" => val} -> val + 1 end, "enables" => ["a"]}
  ]
}

{:ok, _} = SPE.start_link(num_workers: 2)
{:ok, job_id} = SPE.submit_job(job)
Phoenix.PubSub.subscribe(SPE.PubSub, job_id)
SPE.start_job(job_id)
```

---

## Authors

- Carlos Velázquez Arranz <carlos.velazquez@alumnos.upm.es>
- Inés Pérez Rincón <ines.perez.rincon@alumnos.upm.es>
- Lucía Liu Wang <lucia.liu@alumnos.upm.es>
