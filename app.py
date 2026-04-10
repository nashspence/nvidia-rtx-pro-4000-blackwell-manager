from __future__ import annotations

import fcntl
import json
import os
import subprocess
import time
import uuid
from contextlib import contextmanager
from pathlib import Path
from typing import Any, Iterator

from fastapi import FastAPI, HTTPException
from pydantic import BaseModel


def now() -> int:
    return int(time.time())


def run(
    args: list[str],
    *,
    check: bool = True,
    capture_output: bool = False,
    quiet: bool = False,
    env: dict[str, str] | None = None,
) -> subprocess.CompletedProcess[str]:
    kwargs: dict[str, Any] = {
        "check": check,
        "text": True,
        "capture_output": capture_output,
        "env": env,
    }
    if quiet:
        kwargs["stdout"] = subprocess.DEVNULL
        kwargs["stderr"] = subprocess.DEVNULL
        kwargs.pop("capture_output", None)
    return subprocess.run(args, **kwargs)


def normalize_seconds(value: int | float | str | None, default: float, name: str) -> float:
    if value in (None, ""):
        return default
    seconds = float(value)
    if seconds <= 0:
        raise ValueError(f"{name} must be > 0")
    return seconds


CONTAINER_GPU_ROOT = Path(os.getenv("GPU_CONTAINER_ROOT", "/gpu"))
ROOT = CONTAINER_GPU_ROOT / "runtime"
LOCK_FILE = ROOT / "lease.lock"
STATE_FILE = ROOT / "state.json"
SERVICES_DIR = CONTAINER_GPU_ROOT / "services"

HOST_GPU_ROOT = Path(os.environ["GPU_HOST_ROOT"])
HOST_SERVICES_DIR = HOST_GPU_ROOT / "services"

DEFAULT_WAIT_S = float(os.getenv("DEFAULT_WAIT_S", "900"))
DEFAULT_LEASE_TTL_S = float(os.getenv("DEFAULT_LEASE_TTL_S", "1800"))
QUEUE_CLAIM_WINDOW_S = int(os.getenv("QUEUE_CLAIM_WINDOW_S", "10"))
DOCKER_SOCK = os.getenv("DOCKER_SOCK", "/var/run/docker.sock")
HEALTHCHECK_MASTER_LABEL = os.getenv("HEALTHCHECK_MASTER_LABEL", "gpu.healthcheck-master")
HEALTHCHECK_MASTER_VALUE = os.getenv("HEALTHCHECK_MASTER_VALUE", "true")

COMPOSE_FILENAMES = (
    "docker-compose.yml",
    "docker-compose.yaml",
    "compose.yml",
    "compose.yaml",
)
FAILED_STATES = {"exited", "dead"}


class AcquireRequest(BaseModel):
    target: str
    owner: str = ""
    lease_token: str = ""
    lease_ttl_s: int | float | str | None = DEFAULT_LEASE_TTL_S
    wait_s: int | float | str | None = DEFAULT_WAIT_S
    wait_ready: bool = True
    priority: int | None = None


class ReleaseRequest(BaseModel):
    lease_token: str = ""
    force: bool = False


app = FastAPI(title="gpu-service-manager", version="2.0.0")


def ensure_dirs() -> None:
    ROOT.mkdir(parents=True, exist_ok=True)


def discover_services() -> dict[str, str]:
    services: dict[str, str] = {}
    if not SERVICES_DIR.exists():
        return services
    for child in sorted(SERVICES_DIR.iterdir()):
        if not child.is_dir():
            continue
        compose_path = next((child / name for name in COMPOSE_FILENAMES if (child / name).exists()), None)
        if compose_path:
            services[child.name] = str(compose_path)
    return services


def compose_file(target: str) -> Path:
    services = discover_services()
    if target not in services:
        raise ValueError(f"unknown target: {target}")
    return Path(services[target])


def docker_env(extra: dict[str, str] | None = None) -> dict[str, str]:
    env = dict(os.environ)
    env.update(
        {
            "DOCKER_HOST": f"unix://{DOCKER_SOCK}",
            "GPU_HOST_ROOT": str(HOST_GPU_ROOT),
            "GPU_HOST_SERVICES_DIR": str(HOST_SERVICES_DIR),
            "GPU_CONTAINER_ROOT": str(CONTAINER_GPU_ROOT),
            "GPU_SERVICES_DIR": str(SERVICES_DIR),
        }
    )
    if extra:
        env.update(extra)
    return env


def docker(*args: str, check: bool = True, capture_output: bool = False, quiet: bool = False) -> subprocess.CompletedProcess[str]:
    return run(["docker", *args], check=check, capture_output=capture_output, quiet=quiet, env=docker_env())


def compose_env_file_args() -> list[str]:
    env_file = CONTAINER_GPU_ROOT / ".env"
    return ["--env-file", str(env_file)] if env_file.exists() else []


def compose(target: str, *args: str, check: bool = True, capture_output: bool = False, quiet: bool = False) -> subprocess.CompletedProcess[str]:
    return docker(
        "compose",
        *compose_env_file_args(),
        "-f",
        str(compose_file(target)),
        *args,
        check=check,
        capture_output=capture_output,
        quiet=quiet,
    )


def inspect_maybe(name: str | None) -> dict[str, Any] | None:
    if not name:
        return None
    cp = docker("inspect", name, check=False, capture_output=True)
    if cp.returncode != 0 or not cp.stdout.strip():
        return None
    try:
        [info] = json.loads(cp.stdout)
        return info
    except Exception:
        return None


def project_container_ids(target: str) -> list[str]:
    cp = compose(target, "ps", "-q", check=False, capture_output=True)
    return [line.strip() for line in cp.stdout.splitlines() if line.strip()]


def find_healthcheck_master_container_id(target: str) -> str:
    matches: list[str] = []
    for container_id in project_container_ids(target):
        info = inspect_maybe(container_id)
        labels = ((info or {}).get("Config") or {}).get("Labels") or {}
        if labels.get(HEALTHCHECK_MASTER_LABEL) == HEALTHCHECK_MASTER_VALUE:
            matches.append(container_id)
    if not matches:
        raise RuntimeError(
            error_payload(
                target,
                message=(
                    f"compose project must define exactly one service with label "
                    f"{HEALTHCHECK_MASTER_LABEL}={HEALTHCHECK_MASTER_VALUE}"
                ),
            )
        )
    if len(matches) > 1:
        raise RuntimeError(
            error_payload(
                target,
                message=(
                    f"compose project has multiple services with label "
                    f"{HEALTHCHECK_MASTER_LABEL}={HEALTHCHECK_MASTER_VALUE}"
                ),
            )
        )
    return matches[0]


def inspect_target_master_maybe(target: str) -> dict[str, Any] | None:
    try:
        return inspect_maybe(find_healthcheck_master_container_id(target))
    except RuntimeError:
        return None


def active_lease(state: dict[str, Any]) -> dict[str, Any] | None:
    lease = state.get("lease")
    if not isinstance(lease, dict):
        return None
    return lease if int(lease.get("expires_at", 0)) > now() else None


def queue_entries(state: dict[str, Any]) -> list[dict[str, Any]]:
    queue = state.get("queue")
    return queue if isinstance(queue, list) else []


def public_state(state: dict[str, Any]) -> dict[str, Any]:
    out = dict(state)
    lease = active_lease(state)
    if lease:
        out["lease"] = {"owner": lease.get("owner"), "expires_at": lease["expires_at"]}
    else:
        out.pop("lease", None)
    out["queue"] = [
        {
            "token": entry.get("token"),
            "owner": entry.get("owner"),
            "priority": entry.get("priority"),
            "target": entry.get("target"),
            "claim_expires_at": entry.get("claim_expires_at"),
        }
        for entry in queue_entries(state)
    ]
    return out


def load_state() -> dict[str, Any]:
    if not STATE_FILE.exists():
        return {}
    try:
        state = json.loads(STATE_FILE.read_text())
    except Exception:
        return {}
    if not isinstance(state, dict) or state.get("mode") != "service":
        return {}
    target = str(state.get("target") or "")
    if target and target not in discover_services():
        return {}
    if target:
        info = inspect_target_master_maybe(target)
        if info and info.get("State", {}).get("Status") in FAILED_STATES:
            return {}
    if state.get("lease") and not active_lease(state):
        state = dict(state)
        state.pop("lease", None)
    state["queue"] = queue_entries(state)
    return state


def save_state(state: dict[str, Any]) -> None:
    ensure_dirs()
    tmp = STATE_FILE.with_suffix(".tmp")
    tmp.write_text(json.dumps(state, indent=2, sort_keys=True))
    os.replace(tmp, STATE_FILE)


@contextmanager
def locked() -> Iterator[None]:
    ensure_dirs()
    with LOCK_FILE.open("a+") as fh:
        fcntl.flock(fh, fcntl.LOCK_EX)
        yield


def require_lease_token(state: dict[str, Any], lease_token: str) -> dict[str, Any]:
    lease = active_lease(state)
    if not lease:
        raise RuntimeError("no active lease")
    if not lease_token or lease.get("token") != lease_token:
        raise RuntimeError("lease mismatch")
    return lease


def endpoint_for(target: str) -> dict[str, Any]:
    return {"compose_file": str(compose_file(target))}


def down_target(target: str) -> None:
    compose(target, "down", "--remove-orphans", check=False, quiet=True)


def down_all_targets(except_target: str | None = None) -> None:
    for target in discover_services():
        if target != except_target:
            down_target(target)


def up_target(target: str) -> None:
    compose(target, "up", "-d")


def current_logs_tail(target: str, lines: int = 80) -> str:
    cp = compose(target, "logs", "--no-color", "--tail", str(lines), check=False, capture_output=True)
    return ((cp.stdout or "") + (cp.stderr or "")).strip()


def error_payload(target: str, *, message: str | None = None, state: dict[str, Any] | None = None) -> str:
    payload: dict[str, Any] = {"logs_tail": current_logs_tail(target)}
    if message:
        payload["message"] = message
    if state is not None:
        payload["state"] = state
    return json.dumps(payload)


@contextmanager
def stream_compose_logs(target: str) -> Iterator[None]:
    proc = subprocess.Popen(
        [
            "docker",
            "compose",
            *compose_env_file_args(),
            "-f",
            str(compose_file(target)),
            "logs",
            "-f",
            "--tail",
            "0",
            "--no-color",
        ],
        text=True,
        env=docker_env(),
    )
    try:
        yield
    finally:
        if proc.poll() is None:
            proc.terminate()
            try:
                proc.wait(timeout=3)
            except subprocess.TimeoutExpired:
                proc.kill()
                proc.wait()


def wait_service(target: str, timeout_s: int | float | str | None) -> dict[str, Any]:
    deadline = time.time() + normalize_seconds(timeout_s, DEFAULT_WAIT_S, "wait_s")
    while time.time() < deadline:
        master_id = find_healthcheck_master_container_id(target)
        info = inspect_maybe(master_id)
        if not info:
            time.sleep(1)
            continue
        state = info.get("State", {})
        status = state.get("Status")
        health = state.get("Health")
        health_status = health.get("Status") if health else None
        if status in FAILED_STATES:
            raise RuntimeError(error_payload(target, state=state))
        if not health:
            raise RuntimeError(
                error_payload(
                    target,
                    message=(
                        f"healthcheck master is missing a Docker healthcheck: "
                        f"label {HEALTHCHECK_MASTER_LABEL}={HEALTHCHECK_MASTER_VALUE}"
                    ),
                    state=state,
                )
            )
        if health_status == "unhealthy":
            raise RuntimeError(error_payload(target, state=state))
        if status == "running" and health_status == "healthy":
            return state
        time.sleep(2)
    raise TimeoutError(error_payload(target, message="service not ready"))


def cleanup_queue(state: dict[str, Any]) -> tuple[dict[str, Any], bool]:
    state = dict(state)
    queue = queue_entries(state)
    changed = False

    if active_lease(state):
        for entry in queue:
            if entry.get("claim_expires_at") is not None:
                entry["claim_expires_at"] = None
                changed = True
        state["queue"] = queue
        return state, changed

    while queue:
        head = queue[0]
        claim_expires_at = int(head.get("claim_expires_at") or 0)
        if claim_expires_at and claim_expires_at <= now():
            queue.pop(0)
            changed = True
            continue
        break

    if queue and not queue[0].get("claim_expires_at"):
        queue[0]["claim_expires_at"] = now() + QUEUE_CLAIM_WINDOW_S
        changed = True

    for entry in queue[1:]:
        if entry.get("claim_expires_at") is not None:
            entry["claim_expires_at"] = None
            changed = True

    state["queue"] = queue
    return state, changed


def queue_index(state: dict[str, Any], token: str) -> int | None:
    for i, entry in enumerate(queue_entries(state)):
        if entry.get("token") == token:
            return i
    return None


def ensure_project_running(target: str, state: dict[str, Any]) -> dict[str, Any]:
    if state.get("mode") != "service" or str(state.get("target") or "") != target:
        down_all_targets()
        up_target(target)
        state = {"mode": "service", "target": target, "at": now(), "queue": queue_entries(state)}
    return state


def service_status(state: dict[str, Any]) -> dict[str, Any]:
    target = str(state.get("target") or "")
    if target not in discover_services():
        return {}
    info = inspect_target_master_maybe(target)
    if not info:
        return {}
    return {
        **info.get("State", {}),
        "logs_tail": current_logs_tail(target, 40),
        "endpoint": endpoint_for(target),
        "healthcheck_master_label": f"{HEALTHCHECK_MASTER_LABEL}={HEALTHCHECK_MASTER_VALUE}",
        "container_count": len(project_container_ids(target)),
    }


def status() -> dict[str, Any]:
    with locked():
        state = load_state()
        state, changed = cleanup_queue(state)
        if changed:
            save_state(state)
    return {"state": public_state(state), "service": service_status(state) if state.get("mode") == "service" else {}, "services": discover_services()}


def acquire(*, target: str, owner: str, lease_token: str, lease_ttl_s: int | float | str | None, wait_s: int | float | str | None, wait_ready: bool, priority: int | None) -> dict[str, Any]:
    if target not in discover_services():
        raise ValueError(f"unknown target: {target}")

    token = lease_token or str(uuid.uuid4())
    expires_at = int(now() + normalize_seconds(lease_ttl_s, DEFAULT_LEASE_TTL_S, "lease_ttl_s"))
    should_wait = False
    refreshed_lease = False
    effective_owner = owner or None

    with locked():
        state = load_state()
        state, _ = cleanup_queue(state)
        lease = active_lease(state)

        if lease and lease.get("token") == lease_token:
            active_target = str(state.get("target") or "")
            if active_target != target:
                raise RuntimeError(f"target mismatch: active:{active_target}; requested:{target}")
            lease["expires_at"] = expires_at
            save_state(state)
            refreshed_lease = True
        else:
            idx = queue_index(state, lease_token) if lease_token else None
            if lease:
                if idx is not None:
                    if idx != 0:
                        raise RuntimeError(f"gpu busy: service:{state['target']}; queue_index:{idx}")
                    head = queue_entries(state)[0]
                    if int(head.get("claim_expires_at") or 0) <= now():
                        state, _ = cleanup_queue(state)
                        save_state(state)
                        raise RuntimeError("queued token expired")
                    raise RuntimeError(f"gpu busy: service:{state['target']}; queue_index:0")
                if priority is None:
                    raise RuntimeError(f"gpu busy: service:{state['target']}")
                queue = queue_entries(state)
                entry = {
                    "token": token,
                    "owner": owner or None,
                    "target": target,
                    "priority": int(priority),
                    "enqueued_at": now(),
                    "claim_expires_at": None,
                }
                queue.append(entry)
                queue.sort(key=lambda item: (-int(item.get("priority", 0)), int(item.get("enqueued_at", 0))))
                state["queue"] = queue
                state, _ = cleanup_queue(state)
                save_state(state)
                new_idx = queue_index(state, token)
                return {"queued": True, "lease_token": token, "queue_index": new_idx, "state": public_state(state), "service": service_status(state) if state.get("mode") == "service" else {}, "services": discover_services()}

            if idx is not None:
                head = queue_entries(state)[0]
                queued_target = str(head.get("target") or "")
                if queued_target and queued_target != target:
                    raise RuntimeError(f"target mismatch: queued:{queued_target}; requested:{target}")
                if idx != 0:
                    raise RuntimeError(f"gpu busy: queue_index:{idx}")
                if int(head.get("claim_expires_at") or 0) <= now():
                    state, _ = cleanup_queue(state)
                    save_state(state)
                    raise RuntimeError("queued token expired")
                effective_owner = effective_owner or head.get("owner")
                queue = queue_entries(state)
                queue.pop(0)
                state["queue"] = queue
            elif queue_entries(state):
                raise RuntimeError("gpu busy: queued claimant pending")

            state = ensure_project_running(target, state)
            state["lease"] = {"token": token, "owner": effective_owner, "expires_at": expires_at}
            save_state(state)
            should_wait = wait_ready

    if refreshed_lease:
        out = status()
        out["lease_token"] = lease_token
        out["lease_expires_at"] = expires_at
        out["endpoint"] = endpoint_for(target)
        return out

    try:
        if should_wait:
            with stream_compose_logs(target):
                wait_service(target, wait_s)
    except Exception:
        with locked():
            state = load_state()
            if state.get("mode") == "service" and state.get("target") == target:
                lease = active_lease(state)
                if lease and lease.get("token") == token:
                    down_target(target)
                    state.pop("lease", None)
                    save_state(state)
        raise

    out = status()
    out["lease_token"] = token
    out["lease_expires_at"] = expires_at
    out["endpoint"] = endpoint_for(target)
    return out


def release(*, lease_token: str, force: bool) -> dict[str, Any]:
    with locked():
        state = load_state()
        if not force:
            require_lease_token(state, lease_token)
        state.pop("lease", None)
        state, _ = cleanup_queue(state)
        save_state(state)
    return status()


def api_call(fn):
    try:
        return fn()
    except ValueError as e:
        raise HTTPException(status_code=400, detail=str(e)) from e
    except TimeoutError as e:
        raise HTTPException(status_code=504, detail=str(e)) from e
    except RuntimeError as e:
        message = str(e)
        code = 409 if any(s in message for s in ("gpu busy", "lease", "queued", "queue_index", "expired", "target mismatch")) else 500
        raise HTTPException(status_code=code, detail=message) from e
    except subprocess.CalledProcessError as e:
        detail = ((e.stdout or "") + (e.stderr or "")).strip()
        raise HTTPException(status_code=500, detail=detail or str(e)) from e


@app.get("/healthz")
def healthz() -> dict[str, str]:
    return {"ok": "true"}


@app.get("/status")
def get_status() -> dict[str, Any]:
    return api_call(status)


@app.post("/acquire")
def post_acquire(body: AcquireRequest) -> dict[str, Any]:
    return api_call(lambda: acquire(target=body.target, owner=body.owner, lease_token=body.lease_token, lease_ttl_s=body.lease_ttl_s, wait_s=body.wait_s, wait_ready=body.wait_ready, priority=body.priority))


@app.post("/release")
def post_release(body: ReleaseRequest) -> dict[str, Any]:
    return api_call(lambda: release(lease_token=body.lease_token, force=body.force))
