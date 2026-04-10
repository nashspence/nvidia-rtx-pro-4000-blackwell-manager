from __future__ import annotations

import fcntl
import json
import os
import subprocess
import time
import uuid
from contextlib import contextmanager
from dataclasses import dataclass
from pathlib import Path
from typing import Any, Iterator

import yaml
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


RUNTIME_DIR = Path(os.getenv("GPU_RUNTIME_DIR", "/runtime"))
LOCK_FILE = RUNTIME_DIR / "lease.lock"
STATE_FILE = RUNTIME_DIR / "state.json"

SERVICE_CONFIG_ENV_VAR = "GPU_SERVICE_CONFIG_YAML"
SERVICE_CONFIG_YAML = os.getenv(SERVICE_CONFIG_ENV_VAR, "")
ENV_FILE = Path(os.getenv("GPU_ENV_FILE")).expanduser() if os.getenv("GPU_ENV_FILE") else None

DEFAULT_WAIT_S = float(os.getenv("DEFAULT_WAIT_S", "900"))
DEFAULT_LEASE_TTL_S = float(os.getenv("DEFAULT_LEASE_TTL_S", "1800"))
QUEUE_CLAIM_WINDOW_S = int(os.getenv("QUEUE_CLAIM_WINDOW_S", "10"))
DOCKER_SOCK = os.getenv("DOCKER_SOCK", "/var/run/docker.sock")
FAILED_STATES = {"exited", "dead"}


@dataclass(frozen=True)
class ManagedService:
    name: str
    compose_file: Path


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
    RUNTIME_DIR.mkdir(parents=True, exist_ok=True)


def load_managed_services() -> dict[str, ManagedService]:
    raw = SERVICE_CONFIG_YAML.strip()
    if not raw:
        return {}
    try:
        loaded = yaml.safe_load(raw)
    except yaml.YAMLError as exc:
        raise ValueError(f"{SERVICE_CONFIG_ENV_VAR} is not valid YAML: {exc}") from exc
    if loaded in (None, ""):
        return {}

    entries = loaded.get("services") if isinstance(loaded, dict) and "services" in loaded else loaded
    normalized_entries: list[dict[str, Any]] = []

    if isinstance(entries, dict):
        for name, value in entries.items():
            if isinstance(value, str):
                normalized_entries.append({"name": str(name), "path": value})
                continue
            if not isinstance(value, dict):
                raise ValueError(f"{SERVICE_CONFIG_ENV_VAR} entry for {name!r} must be a mapping or a string path")
            item = dict(value)
            item.setdefault("name", str(name))
            normalized_entries.append(item)
    elif isinstance(entries, list):
        for value in entries:
            if isinstance(value, str):
                normalized_entries.append({"path": value})
                continue
            if not isinstance(value, dict):
                raise ValueError(f"{SERVICE_CONFIG_ENV_VAR} list entries must be mappings or string paths")
            normalized_entries.append(dict(value))
    else:
        raise ValueError(f"{SERVICE_CONFIG_ENV_VAR} must be a mapping or a list of mappings")

    services: dict[str, ManagedService] = {}
    for item in normalized_entries:
        compose_file_value = item.get("path")
        if not compose_file_value:
            entry_name = str(item.get("name") or "<unnamed>").strip()
            raise ValueError(f"{SERVICE_CONFIG_ENV_VAR} entry {entry_name!r} must define path")

        compose_file = Path(str(compose_file_value)).expanduser()
        if not compose_file.is_absolute():
            raise ValueError(
                f"{SERVICE_CONFIG_ENV_VAR} entry path must be absolute: {compose_file}"
            )
        service_name = str(item.get("name") or compose_file.parent.name).strip()
        if not service_name:
            raise ValueError(f"{SERVICE_CONFIG_ENV_VAR} entries must define name or use a path with a parent directory")
        if service_name in services:
            raise ValueError(f"{SERVICE_CONFIG_ENV_VAR} contains duplicate name: {service_name}")

        services[service_name] = ManagedService(
            name=service_name,
            compose_file=compose_file,
        )
    return services


MANAGED_SERVICES = load_managed_services()


def discover_services() -> dict[str, dict[str, str]]:
    return {
        name: {
            "compose_file": str(service.compose_file),
            "name": service.name,
        }
        for name, service in MANAGED_SERVICES.items()
    }


def service_config(target: str) -> ManagedService:
    try:
        return MANAGED_SERVICES[target]
    except KeyError as exc:
        raise ValueError(f"unknown target: {target}") from exc


def compose_file(target: str) -> Path:
    return service_config(target).compose_file


def compose_file_for_cli(target: str) -> Path:
    compose_path = compose_file(target)
    if compose_path.exists():
        return compose_path
    raise RuntimeError(
        (
            f"compose file is not visible inside gpu-service-manager: {compose_path}. "
            f"Bind mount the compose file's parent directory into the manager container "
            f"at the same absolute path as the host."
        )
    )


def compose_project_dir_for_cli(target: str) -> Path:
    return compose_file_for_cli(target).parent


def docker_env(extra: dict[str, str] | None = None) -> dict[str, str]:
    env = dict(os.environ)
    env.update(
        {
            "DOCKER_HOST": f"unix://{DOCKER_SOCK}",
            "GPU_RUNTIME_DIR": str(RUNTIME_DIR),
            SERVICE_CONFIG_ENV_VAR: SERVICE_CONFIG_YAML,
        }
    )
    if ENV_FILE:
        env["GPU_ENV_FILE"] = str(ENV_FILE)
    if extra:
        env.update(extra)
    return env


def docker(*args: str, check: bool = True, capture_output: bool = False, quiet: bool = False) -> subprocess.CompletedProcess[str]:
    return run(["docker", *args], check=check, capture_output=capture_output, quiet=quiet, env=docker_env())


def compose_env_file_for_cli() -> Path | None:
    if not ENV_FILE:
        return None
    if ENV_FILE.exists():
        return ENV_FILE
    raise RuntimeError(
        (
            f"GPU_ENV_FILE is not visible inside gpu-service-manager: {ENV_FILE}. "
            f"Bind mount that file or its parent directory into the manager container "
            f"at the same absolute path as the host."
        )
    )


def compose_env_file_args() -> list[str]:
    env_file = compose_env_file_for_cli()
    return ["--env-file", str(env_file)] if env_file else []


def compose(target: str, *args: str, check: bool = True, capture_output: bool = False, quiet: bool = False) -> subprocess.CompletedProcess[str]:
    compose_path = compose_file_for_cli(target)
    return docker(
        "compose",
        *compose_env_file_args(),
        "--project-directory",
        str(compose_path.parent),
        "-f",
        str(compose_path),
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


def project_container_ids(target: str, service_name: str | None = None) -> list[str]:
    args = ["ps", "-q"]
    if service_name:
        args.append(service_name)
    cp = compose(target, *args, check=False, capture_output=True)
    return [line.strip() for line in cp.stdout.splitlines() if line.strip()]


def configured_service_names(target: str) -> list[str]:
    cp = compose(target, "config", "--services", check=False, capture_output=True)
    output = ((cp.stdout or "") + (cp.stderr or "")).strip()
    if cp.returncode != 0:
        raise RuntimeError(output or error_payload(target, message="unable to inspect compose services"))
    return [line.strip() for line in cp.stdout.splitlines() if line.strip()]


def inspect_project_containers(target: str) -> list[dict[str, Any]]:
    return [info for container_id in project_container_ids(target) if (info := inspect_maybe(container_id))]


def inspect_target_maybe(target: str) -> list[dict[str, Any]] | None:
    try:
        infos = inspect_project_containers(target)
        return infos or None
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
        infos = inspect_target_maybe(target)
        if infos and any(info.get("State", {}).get("Status") in FAILED_STATES for info in infos):
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
    managed_service = service_config(target)
    return {
        "compose_file": str(managed_service.compose_file),
        "name": managed_service.name,
    }


def down_target(target: str) -> None:
    compose(target, "down", "--remove-orphans", check=False, quiet=True)


def down_all_targets(except_target: str | None = None) -> None:
    for target in discover_services():
        if target != except_target:
            down_target(target)


def up_target(target: str) -> None:
    compose(target, "up", "-d")


def current_logs_tail(target: str, lines: int = 80) -> str:
    try:
        cp = compose(target, "logs", "--no-color", "--tail", str(lines), check=False, capture_output=True)
    except RuntimeError:
        return ""
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
    compose_path = compose_file_for_cli(target)
    proc = subprocess.Popen(
        [
            "docker",
            "compose",
            *compose_env_file_args(),
            "--project-directory",
            str(compose_path.parent),
            "-f",
            str(compose_path),
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


def wait_service(target: str, timeout_s: int | float | str | None) -> list[dict[str, Any]]:
    deadline = time.time() + normalize_seconds(timeout_s, DEFAULT_WAIT_S, "wait_s")
    expected_services = configured_service_names(target)
    while time.time() < deadline:
        containers_by_service: dict[str, list[dict[str, Any]]] = {}
        for service_name in expected_services:
            infos = [info for container_id in project_container_ids(target, service_name) if (info := inspect_maybe(container_id))]
            if infos:
                containers_by_service[service_name] = infos

        if len(containers_by_service) < len(expected_services):
            time.sleep(1)
            continue

        all_infos = [info for infos in containers_by_service.values() for info in infos]
        all_ready = True
        for info in all_infos:
            state = info.get("State", {})
            status = state.get("Status")
            health = state.get("Health")
            health_status = health.get("Status") if health else None
            if status in FAILED_STATES:
                raise RuntimeError(error_payload(target, state=state))
            if health_status == "unhealthy":
                raise RuntimeError(error_payload(target, state=state))
            if status != "running":
                all_ready = False
                continue
            if health and health_status != "healthy":
                all_ready = False
        if all_ready:
            return all_infos
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
    infos = inspect_target_maybe(target)
    if not infos:
        return {}
    healthy_count = 0
    unhealthy_count = 0
    no_healthcheck_count = 0
    for info in infos:
        container_state = info.get("State", {})
        health = container_state.get("Health")
        if not health:
            no_healthcheck_count += 1
            continue
        if health.get("Status") == "healthy":
            healthy_count += 1
        elif health.get("Status") == "unhealthy":
            unhealthy_count += 1
    return {
        **infos[0].get("State", {}),
        "logs_tail": current_logs_tail(target, 40),
        "endpoint": endpoint_for(target),
        "healthy_container_count": healthy_count,
        "unhealthy_container_count": unhealthy_count,
        "no_healthcheck_container_count": no_healthcheck_count,
        "container_count": len(infos),
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
