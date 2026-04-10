# GPU Service Manger

## API

```bash
curl http://localhost:8080/status

curl -X POST http://localhost:8080/acquire \
  -H 'content-type: application/json' \
  -d '{"target":"my-service","owner":"me"}'

curl -X POST http://localhost:8080/acquire \
  -H 'content-type: application/json' \
  -d '{"target":"my-service","lease_token":"<active-token>"}'

curl -X POST http://localhost:8080/acquire \
  -H 'content-type: application/json' \
  -d '{"target":"my-service","owner":"me","priority":100}'

curl -X POST http://localhost:8080/release \
  -H 'content-type: application/json' \
  -d '{"lease_token":"<token>"}'

curl -X POST http://localhost:8080/release \
  -H 'content-type: application/json' \
  -d '{"force":true}'
```

## Readiness selection

The compose project must mark exactly one service as the readiness master:

```yaml
services:
  web:
    labels:
      gpu.healthcheck-master: "true"
    healthcheck:
      test: ["CMD", "curl", "-f", "http://localhost:8000/healthz"]
      interval: 5s
      timeout: 3s
      retries: 20
```

That labeled service is the one inspected for Docker health. If no service is labeled, more than one service is labeled, or the labeled service has no Docker `healthcheck`, acquire fails with a clear error.

## Notes

- Services are discovered under `/gpu/services/<target>`.
- If `/gpu/.env` exists, it is passed to every `docker compose` invocation with `--env-file /gpu/.env`.
- Exactly one target is managed at a time, but each target may contain multiple Compose services.
- While waiting for readiness, the manager streams logs from the full compose project to stdout.
- Lease refresh happens by calling `/acquire` with the current active `lease_token`.
- Releasing with `force: true` does not require a token.
- If `priority` is set on `/acquire` and the GPU is busy, the caller is queued. Higher priority wins and ties are FIFO.
- When a queued caller reaches the front, it has 10 seconds to call `/acquire` with its queued token.
- Queue advancement happens on normal API activity such as `status`, `acquire`, and `release`.
- Lease and queue state are persisted under `/gpu/runtime/state.json`.
