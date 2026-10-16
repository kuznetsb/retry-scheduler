# Retry Scheduler

Retry Scheduler accepts jobs containing a destination URL and JSON payload,
stores their state in MongoDB, and delivers them asynchronously using HTTP
`POST`. Redis and SAQ dispatch the work to one or more workers.

For the detailed lifecycle, retry policy, concurrency behavior, and design
trade-offs, see [Job lifecycle and retries](docs/job-lifecycle-and-retries.md).

## Technology overview

| Component | Role | Why it is used |
| --- | --- | --- |
| FastAPI | HTTP API for creating and reading jobs | Provides async endpoints, request validation, and generated API docs. |
| MongoDB (PyMongo Async) | Durable job records and lifecycle state | Stores queryable job state, attempts, and retry due times independently of queue state. |
| Redis | SAQ queue backend | Provides the shared queue used by the API and worker processes. |
| SAQ | Async job queue and worker | Provides Redis-backed dispatch, attempt tracking and delayed retries, plus a monitoring UI and worker health check. This avoids implementing queue consumption and scheduling directly. |
| aiohttp | Outbound job delivery | Sends asynchronous HTTP requests with an explicit request timeout. |
| Pydantic Settings | Environment-based configuration | Keeps MongoDB and Redis connection settings configurable without hardcoding deployment-specific values. |
| uv / Ruff | Dependency and development tooling | uv installs the locked dependency set; Ruff provides linting and formatting. |

SAQ is intentionally used as the execution and scheduling mechanism, not as
the authoritative job record. MongoDB holds the application-visible status;
Redis/SAQ holds queued and scheduled executions. This separation allows the
API to report job status, but it also means MongoDB updates and Redis scheduling
are not one atomic transaction. The business-logic document describes the
implications and current recovery limits.

## Run locally with Docker Compose

Docker Compose is the simplest way to run the full stack. It starts the API,
worker, Redis, and MongoDB; the API and worker wait for their dependencies to
become healthy.

Requirements: Docker with the Compose plugin and GNU Make.

```bash
make build
make run
```

The API is available at <http://localhost:8000>. Interactive API documentation
is at <http://localhost:8000/docs>, and the SAQ monitoring UI is at
<http://localhost:8000/monitor>.

To stop the stack, press `Ctrl+C` or run `docker compose down` in another
terminal. MongoDB data is kept in the `mongo_data` named volume.

Submit a sample job:

```bash
curl -X POST http://localhost:8000/jobs \
  -H 'Content-Type: application/json' \
  -d '{
    "url": "https://httpbin.org/post",
    "payload": {"event": "demo"}
  }'
```

The response contains the job ID. Check its status with:

```bash
curl http://localhost:8000/jobs/<job-id>
```

## Project structure

```text
app/
  handlers/       Job orchestration, status transitions, and retry decisions
  repositories/   MongoDB and Redis connections, models, and queries
  tasks/          SAQ task functions and HTTP delivery
  main.py         FastAPI application and SAQ monitoring UI
  routes.py       HTTP API routes
  schemas.py      API request and response models
  settings.py     MongoDB and Redis settings
  worker.py       SAQ worker entry point
docs/
  job-lifecycle-and-retries.md
tests/
  conftest.py      Shared test doubles and fixtures
  test_*.py        Handler, API, and HTTP task tests
Dockerfile        Container image used by the API and worker
docker-compose.yaml
pyproject.toml    Project metadata and dependencies
uv.lock           Locked dependency versions
Makefile          Build, run, test, lint, and format commands
```

## API and observability

- `POST /jobs` accepts a destination URL and JSON payload and returns a job ID.
- `GET /jobs/{job_id}` returns the job's current status, attempt count, and
  next retry time.
- `/docs` provides the generated OpenAPI interface.
- `/monitor` exposes the SAQ monitoring UI.
- The task logs delivery failures and duplicate/already-claimed deliveries.
- Docker Compose health-checks MongoDB, Redis, and the worker.

This is basic operational visibility: it does not currently include metrics,
distributed tracing, alerts, or a separate dead-letter queue.

## Development checks

Requirements: Python 3.15 or newer and [uv](https://docs.astral.sh/uv/).
Install the locked application and development dependencies:

```bash
uv sync --frozen
```

Run the checks:

```bash
make test
make lint
```

`make test` runs the pytest suite. The tests use a fake MongoDB collection and
a local HTTP test server, so they do not require MongoDB, Redis, or a running
Compose stack. Use `make format` to format Python files with Ruff.

GitHub Actions runs the same test and lint checks for pushes and pull requests.
