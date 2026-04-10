# GPU Service Manager

`gpu-service-manager` is a small FastAPI service that keeps a single GPU host predictable by allowing only one Docker Compose service stack to hold the GPU lease at a time.

The practical goal is simple: if you have one NVIDIA RTX Pro 4000 Blackwell and several heavy stacks that can each push VRAM usage hard, this gives you a clean way to run one known stack at a time and avoid accidental overlap and OOM churn.

## What It Does

- Discovers candidate service stacks under `services/<target>/`
- Starts exactly one target stack at a time with `docker compose up -d`
- Waits for one designated container healthcheck before declaring the stack ready
- Persists lease and queue state on disk
- Serializes access so callers cannot accidentally bring up multiple GPU-heavy stacks at once
- Supports queued handoff when the GPU is busy

## How It Works

Each managed target is a Docker Compose project. A client calls `POST /acquire` to request a target. If the GPU is idle, that target is started and a lease is issued. If the GPU is already leased, the caller can optionally join a priority queue.

When the active lease is released, the queue head gets a short claim window. Only that queued token can claim the GPU during that window. Fresh callers cannot skip the queue.

The manager enforces a single active stack. If a new target is acquired, any other managed targets are brought down before the new target is started.

## Service Contract

Each target must live in its own directory under `services/` and include one supported Compose filename:

- `docker-compose.yml`
- `docker-compose.yaml`
- `compose.yml`
- `compose.yaml`

Exactly one service in that Compose project must be marked as the readiness master:

```yaml
services:
  api:
    labels:
      gpu.healthcheck-master: "true"
    healthcheck:
      test: ["CMD", "curl", "-f", "http://127.0.0.1:8080/healthz"]
      interval: 5s
      timeout: 3s
      retries: 20
```

That labeled container is the one inspected for Docker health. Acquire fails if:

- no service has the label
- more than one service has the label
- the labeled service has no Docker `healthcheck`
- the labeled service becomes unhealthy or exits

## Repository Layout

```text
.
├── app.py
├── docker-compose.yml
├── requirements.txt
├── requirements-dev.txt
└── tests/
    └── fixtures/
        ├── runtime/
        └── services/
            └── dummy-ok/
                └── docker-compose.yml
```

The repository includes `tests/fixtures/services/dummy-*` targets used for local integration and stress testing. Production service stacks should live outside the repository in whatever host directory you mount as `${GPU_HOST_SERVICES_DIR}`.

## Configuration

The manager uses these environment variables:

| Variable | Required | Default | Description |
| --- | --- | --- | --- |
| `GPU_HOST_SERVICES_DIR` | yes | none | Host path containing target Compose projects and optional `.env` |
| `GPU_HOST_RUNTIME_DIR` | yes | none | Host path used for persisted lease and queue state |
| `GPU_SERVICES_DIR` | no | `/services` | Services path inside the manager container |
| `GPU_RUNTIME_DIR` | no | `/runtime` | Runtime state path inside the manager container |
| `GPU_ENV_FILE` | no | `/services/.env` | Optional env file passed to every `docker compose` invocation |
| `DEFAULT_WAIT_S` | no | `900` | Default readiness wait timeout for `acquire` |
| `DEFAULT_LEASE_TTL_S` | no | `1800` | Default lease lifetime |
| `QUEUE_CLAIM_WINDOW_S` | no | `10` | How long the queue head has to claim the GPU after release |
| `DOCKER_SOCK` | no | `/var/run/docker.sock` | Docker socket path |
| `HEALTHCHECK_MASTER_LABEL` | no | `gpu.healthcheck-master` | Label key used to choose readiness master |
| `HEALTHCHECK_MASTER_VALUE` | no | `true` | Label value used to choose readiness master |

If `${GPU_ENV_FILE}` exists, it is passed to every `docker compose` invocation with `--env-file`.

## Running It

The latest published container image is:

```text
ghcr.io/nashspence/gpu-service-manager:latest
```

For a normal deployment, use a minimal `docker-compose.yml` and `.env` like this:

`docker-compose.yml`

```yaml
services:
  gpu-service-manager:
    image: ghcr.io/nashspence/gpu-service-manager:latest
    container_name: gpu-service-manager
    restart: unless-stopped
    ports:
      - "8080:8080"
    environment:
      GPU_HOST_SERVICES_DIR: ${GPU_HOST_SERVICES_DIR}
      GPU_HOST_RUNTIME_DIR: ${GPU_HOST_RUNTIME_DIR}
    volumes:
      - ${GPU_HOST_SERVICES_DIR}:/services
      - ${GPU_HOST_RUNTIME_DIR}:/runtime
      - /var/run/docker.sock:/var/run/docker.sock
```

`.env`

```dotenv
GPU_HOST_SERVICES_DIR=/opt/gpu-service-manager/services
GPU_HOST_RUNTIME_DIR=/opt/gpu-service-manager/runtime
```

Then start the manager:

```bash
docker compose up -d
```

This configuration runs the manager on port `8080` and mounts:

- `${GPU_HOST_SERVICES_DIR}` at `/services`
- `${GPU_HOST_RUNTIME_DIR}` at `/runtime`
- `/var/run/docker.sock`

For local development from this repository, you can still build and run the included top-level Compose file:

```bash
export GPU_HOST_SERVICES_DIR="$PWD/tests/fixtures/services"
export GPU_HOST_RUNTIME_DIR="$PWD/tests/fixtures/runtime"
docker compose up -d --build
```

Those paths are test fixtures for local development only. For real usage, point `${GPU_HOST_SERVICES_DIR}` and `${GPU_HOST_RUNTIME_DIR}` at host directories outside the repository.

## API

### `GET /healthz`

Simple manager liveness check.

### `GET /status`

Returns:

- current public lease state
- current queue state
- current running service status, if any
- discovered services

### `POST /acquire`

Acquire a target or refresh an existing lease.

Example:

```bash
curl -X POST http://localhost:8080/acquire \
  -H 'content-type: application/json' \
  -d '{"target":"my-stack","owner":"me"}'
```

Refresh an active lease:

```bash
curl -X POST http://localhost:8080/acquire \
  -H 'content-type: application/json' \
  -d '{"target":"my-stack","lease_token":"<active-token>"}'
```

Join the queue when the GPU is busy:

```bash
curl -X POST http://localhost:8080/acquire \
  -H 'content-type: application/json' \
  -d '{"target":"my-stack","owner":"batch-job","priority":100}'
```

Request body:

| Field | Required | Description |
| --- | --- | --- |
| `target` | yes | Service target directory name under `services/` |
| `owner` | no | Human-readable owner string |
| `lease_token` | no | Existing active lease token or queued token |
| `lease_ttl_s` | no | Lease TTL override |
| `wait_s` | no | Readiness timeout override |
| `wait_ready` | no | Wait for readiness before returning, defaults to `true` |
| `priority` | no | Queue priority when the GPU is busy |

Behavior:

- If idle, the target is started and a lease is granted.
- If the same active token is presented again for the same target, the lease is refreshed.
- If busy and `priority` is set, the caller is added to the queue.
- If busy and no queue priority is supplied, the call is rejected with `409`.
- If a queued token reaches the front after release, that token can claim the GPU during the claim window.

### `POST /release`

Release the active lease.

```bash
curl -X POST http://localhost:8080/release \
  -H 'content-type: application/json' \
  -d '{"lease_token":"<token>"}'
```

Force release without a token:

```bash
curl -X POST http://localhost:8080/release \
  -H 'content-type: application/json' \
  -d '{"force":true}'
```

Notes:

- Releasing removes the lease.
- Releasing does not immediately stop the current target stack.
- A different target acquisition will bring other managed targets down before starting the new one.

## Queue Semantics

- Higher `priority` wins.
- Equal priority is FIFO.
- The queue head only gets a claim deadline after the active lease is released.
- While a queue head is waiting to claim, other callers cannot jump ahead.
- Queued ownership is preserved when that queued token later claims the GPU.

## State Files

Runtime state is stored under:

- `<configured runtime dir>/state.json`
- `<configured runtime dir>/lease.lock`

This lets the manager survive restarts without losing the lease and queue model.

## Development

Install dependencies:

```bash
python3 -m pip install -r requirements-dev.txt
```

Run the full test suite:

```bash
python3 -m pytest --cov=app --cov-report=term-missing
```

Run the live HTTP stress test only:

```bash
python3 -m pytest tests/test_gpu_service_manager_stress.py -q
```

## Test Coverage

The test suite exercises:

- service discovery across all supported Compose filenames
- happy-path acquire, status, refresh, and release
- readiness master validation failures
- queue fairness and claim-window behavior
- live multi-worker stress with concurrent enqueue, status polling, bad-token retries, and claim races

## Limitations

- This manager coordinates only the Compose projects it knows about under `services/`.
- It does not stop unrelated containers running outside that set.
- It assumes Docker healthchecks are a reliable signal for stack readiness.
- It is designed for one managed GPU host, not distributed scheduling across multiple machines.
