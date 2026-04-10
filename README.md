# GPU Service Manager

`gpu-service-manager` is a small FastAPI service that keeps a single GPU host predictable by allowing only one Docker Compose stack to hold the GPU lease at a time.

The practical goal is simple: if you have one GPU box and several heavyweight Compose stacks that can each consume most of the VRAM, this gives you a clean way to run one known stack at a time and avoid overlap and OOM churn.

## What It Does

- Starts exactly one configured Compose stack at a time with `docker compose up -d`
- Waits until every container in the selected Compose stack is either healthy or running without a Docker healthcheck
- Persists lease and queue state on disk
- Serializes access so callers cannot accidentally bring up multiple GPU-heavy stacks at once
- Supports queued handoff when the GPU is busy

## How It Works

Each managed target is configured in `GPU_SERVICE_CONFIG_YAML`. A client calls `POST /acquire` to request a target. If the GPU is idle, that stack is started and a lease is issued. If the GPU is already leased, the caller can optionally join a priority queue.

When the active lease is released, the queue head gets a short claim window. Only that queued token can claim the GPU during that window. Fresh callers cannot skip the queue.

The manager enforces a single active stack. If a new target is acquired, any other managed targets are brought down before the new target is started.

## Service Contract

Managed services are defined by YAML stored in the `GPU_SERVICE_CONFIG_YAML` environment variable. Each entry supplies:

- `path`: the absolute path to that stack’s Compose file on the host
- `name`: optional target name used by the API. If omitted, the manager uses the Compose file’s parent directory name.
- A list entry may also be just a string path to the Compose file.

Example:

```yaml
services:
  - name: comfyui
    path: /opt/stacks/comfyui/docker-compose.yml
  - /srv/ml/whisperx/compose.yaml
```

The manager does not require any custom labels inside the managed stacks. Existing Compose projects can stay unchanged. Readiness succeeds once every container in the stack is either:

- `healthy`
- `running` with no Docker healthcheck defined

Acquire fails if:

- the configured Compose file is not visible inside the manager container
- any container in the stack becomes unhealthy or exits

## Relative Paths

To keep relative paths inside existing Compose files working unchanged, mount each managed stack into the manager container at the same absolute path it has on the host.

This matters for Compose features like:

- `build: .`
- `env_file: .env`
- relative bind mounts such as `./models:/models`
- includes or other file references resolved relative to the project directory

The manager invokes Docker as:

- `docker compose -f <configured path>`
- with `--project-directory <compose file parent>`

That preserves Compose’s normal relative-path behavior, but only if the manager container can see the stack at that same absolute path.

A good convention is to mount a common parent directory once, for example:

```yaml
volumes:
  - /opt/stacks:/opt/stacks:ro
```

If stacks live in unrelated places, mount each relevant parent directory individually at its original absolute path.

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

The repository includes `tests/fixtures/services/dummy-*` targets used for local integration and stress testing.

## Configuration

The manager uses these environment variables:

| Variable | Required | Default | Description |
| --- | --- | --- | --- |
| `GPU_SERVICE_CONFIG_YAML` | yes | none | YAML that defines the managed Compose stacks |
| `GPU_RUNTIME_DIR` | no | `/runtime` | Runtime state path inside the manager container |
| `GPU_ENV_FILE` | no | none | Optional extra env file passed to every `docker compose` invocation |
| `DEFAULT_WAIT_S` | no | `900` | Default readiness wait timeout for `acquire` |
| `DEFAULT_LEASE_TTL_S` | no | `1800` | Default lease lifetime |
| `QUEUE_CLAIM_WINDOW_S` | no | `10` | How long the queue head has to claim the GPU after release |
| `DOCKER_SOCK` | no | `/var/run/docker.sock` | Docker socket path |

If `GPU_ENV_FILE` is set, that file must also be visible inside the manager container at the same absolute path as the host.

## Running It

The latest published container image is:

```text
ghcr.io/nashspence/gpu-service-manager:latest
```

For a normal deployment, use a minimal `docker-compose.yml` like this:

```yaml
services:
  gpu-service-manager:
    image: ghcr.io/nashspence/gpu-service-manager:latest
    container_name: gpu-service-manager
    restart: unless-stopped
    ports:
      - "8080:8080"
    environment:
      GPU_RUNTIME_DIR: /runtime
      GPU_SERVICE_CONFIG_YAML: |
        services:
          - name: comfyui
            path: /opt/stacks/comfyui/docker-compose.yml
          - /opt/stacks/whisperx/compose.yaml
    volumes:
      - /opt/stacks:/opt/stacks:ro
      - /opt/gpu-service-manager/runtime:/runtime
      - /var/run/docker.sock:/var/run/docker.sock
```

Then start the manager:

```bash
docker compose up -d
```

This configuration mounts:

- `/opt/stacks` at the same absolute path so existing relative paths in the managed stacks still work
- `/opt/gpu-service-manager/runtime` at `/runtime`
- `/var/run/docker.sock`

For local development from this repository:

```bash
export REPO_ROOT="$PWD"
export GPU_HOST_RUNTIME_DIR="$PWD/tests/fixtures/runtime"
docker compose up -d --build
```

The included top-level [`docker-compose.yml`](/workspaces/gpu-service-manager/docker-compose.yml) is a local-dev example that configures the fixture stacks through `GPU_SERVICE_CONFIG_YAML`.

## API

### `GET /healthz`

Simple manager liveness check.

### `GET /status`

Returns:

- current public lease state
- current queue state
- current running service status, if any
- configured managed services

### `POST /acquire`

Acquire a target or refresh an existing lease.

Example:

```bash
curl -X POST http://localhost:8080/acquire \
  -H 'content-type: application/json' \
  -d '{"target":"comfyui","owner":"me"}'
```

Refresh an active lease:

```bash
curl -X POST http://localhost:8080/acquire \
  -H 'content-type: application/json' \
  -d '{"target":"comfyui","lease_token":"<active-token>"}'
```

Join the queue when the GPU is busy:

```bash
curl -X POST http://localhost:8080/acquire \
  -H 'content-type: application/json' \
  -d '{"target":"whisperx","owner":"batch-job","priority":100}'
```

Request body:

| Field | Required | Description |
| --- | --- | --- |
| `target` | yes | Managed target name from `GPU_SERVICE_CONFIG_YAML` |
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
