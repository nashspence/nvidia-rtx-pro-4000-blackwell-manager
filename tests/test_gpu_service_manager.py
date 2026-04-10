from __future__ import annotations

import importlib
import json
import shutil
import subprocess
import sys
from pathlib import Path

import pytest
from fastapi.testclient import TestClient


REPO_ROOT = Path(__file__).resolve().parents[1]
DUMMY_SERVICES = (
    "dummy-ok",
    "dummy-alt",
    "dummy-no-health",
    "dummy-multi-master",
    "dummy-unhealthy",
)


def _copy_dummy_services(root: Path) -> None:
    services_root = root / "services"
    services_root.mkdir(parents=True, exist_ok=True)
    for name in DUMMY_SERVICES:
        shutil.copytree(REPO_ROOT / "services" / name, services_root / name)


def _docker_compose_down(root: Path) -> None:
    for service_dir in (root / "services").iterdir():
        if not service_dir.is_dir():
            continue
        for filename in ("docker-compose.yml", "docker-compose.yaml", "compose.yml", "compose.yaml"):
            compose_path = service_dir / filename
            if compose_path.exists():
                subprocess.run(
                    ["docker", "compose", "-f", str(compose_path), "down", "--remove-orphans", "--volumes"],
                    check=False,
                    capture_output=True,
                    text=True,
                )
                break


@pytest.fixture
def app_module(tmp_path: Path, monkeypatch: pytest.MonkeyPatch):
    root = tmp_path / "gpu-root"
    _copy_dummy_services(root)
    (root / "runtime").mkdir(parents=True, exist_ok=True)

    monkeypatch.setenv("GPU_HOST_ROOT", str(root))
    monkeypatch.setenv("GPU_CONTAINER_ROOT", str(root))
    monkeypatch.setenv("DEFAULT_WAIT_S", "20")
    monkeypatch.setenv("DEFAULT_LEASE_TTL_S", "60")
    monkeypatch.setenv("QUEUE_CLAIM_WINDOW_S", "2")

    sys.modules.pop("app", None)
    module = importlib.import_module("app")
    yield module
    try:
        _docker_compose_down(root)
    finally:
        sys.modules.pop("app", None)


@pytest.fixture
def client(app_module):
    with TestClient(app_module.app) as test_client:
        yield test_client


def test_status_discovers_dummy_services(client, app_module) -> None:
    payload = client.get("/status").json()

    assert payload["state"] == {"queue": []}
    assert payload["service"] == {}
    assert payload["services"] == {
        name: str(app_module.SERVICES_DIR / name / filename)
        for name, filename in {
            "dummy-alt": "compose.yaml",
            "dummy-multi-master": "compose.yml",
            "dummy-no-health": "docker-compose.yaml",
            "dummy-ok": "docker-compose.yml",
            "dummy-unhealthy": "docker-compose.yml",
        }.items()
    }


def test_acquire_status_and_release_happy_path(client, app_module) -> None:
    acquire_response = client.post("/acquire", json={"target": "dummy-ok", "owner": "alice"})
    assert acquire_response.status_code == 200

    acquire_payload = acquire_response.json()
    lease_token = acquire_payload["lease_token"]

    assert acquire_payload["state"]["target"] == "dummy-ok"
    assert acquire_payload["state"]["lease"] == {
        "owner": "alice",
        "expires_at": acquire_payload["lease_expires_at"],
    }
    assert acquire_payload["service"]["Status"] == "running"
    assert acquire_payload["service"]["Health"]["Status"] == "healthy"
    assert acquire_payload["service"]["container_count"] == 2
    assert acquire_payload["endpoint"] == {
        "compose_file": str(app_module.SERVICES_DIR / "dummy-ok" / "docker-compose.yml")
    }

    state_on_disk = json.loads(app_module.STATE_FILE.read_text())
    assert state_on_disk["lease"]["token"] == lease_token

    status_payload = client.get("/status").json()
    assert status_payload["state"]["lease"]["owner"] == "alice"
    assert "token" not in status_payload["state"]["lease"]

    release_response = client.post("/release", json={"lease_token": lease_token})
    assert release_response.status_code == 200
    assert release_response.json()["state"] == {
        "mode": "service",
        "target": "dummy-ok",
        "at": state_on_disk["at"],
        "queue": [],
    }


def test_priority_queue_claim_can_switch_targets(client, app_module) -> None:
    active = client.post("/acquire", json={"target": "dummy-ok", "owner": "active"})
    assert active.status_code == 200
    active_token = active.json()["lease_token"]

    queued = client.post("/acquire", json={"target": "dummy-alt", "owner": "queued", "priority": 50})
    assert queued.status_code == 200
    queued_payload = queued.json()
    queued_token = queued_payload["lease_token"]

    assert queued_payload["queued"] is True
    assert queued_payload["queue_index"] == 0
    assert queued_payload["state"]["queue"][0]["target"] == "dummy-alt"

    release = client.post("/release", json={"lease_token": active_token})
    assert release.status_code == 200
    claim_expires_at = release.json()["state"]["queue"][0]["claim_expires_at"]
    assert isinstance(claim_expires_at, int)

    claimed = client.post("/acquire", json={"target": "dummy-alt", "lease_token": queued_token})
    assert claimed.status_code == 200
    claimed_payload = claimed.json()

    assert claimed_payload["state"]["target"] == "dummy-alt"
    assert claimed_payload["state"]["lease"]["owner"] == "queued"
    assert claimed_payload["service"]["Health"]["Status"] == "healthy"
    assert claimed_payload["endpoint"] == {
        "compose_file": str(app_module.SERVICES_DIR / "dummy-alt" / "compose.yaml")
    }


def test_queue_claim_window_only_starts_after_release(client) -> None:
    active = client.post("/acquire", json={"target": "dummy-ok", "owner": "active"})
    assert active.status_code == 200

    queued = client.post("/acquire", json={"target": "dummy-alt", "owner": "queued", "priority": 50})
    assert queued.status_code == 200
    queued_payload = queued.json()

    queue_entry = queued_payload["state"]["queue"][0]
    assert queue_entry["target"] == "dummy-alt"
    assert queue_entry["claim_expires_at"] is None

    status_payload = client.get("/status").json()
    assert status_payload["state"]["queue"][0]["claim_expires_at"] is None


def test_non_queued_caller_cannot_skip_the_queue(client) -> None:
    active = client.post("/acquire", json={"target": "dummy-ok", "owner": "active"})
    assert active.status_code == 200
    active_token = active.json()["lease_token"]

    queued = client.post("/acquire", json={"target": "dummy-alt", "owner": "queued", "priority": 50})
    assert queued.status_code == 200

    released = client.post("/release", json={"lease_token": active_token})
    assert released.status_code == 200
    assert isinstance(released.json()["state"]["queue"][0]["claim_expires_at"], int)

    skipped = client.post("/acquire", json={"target": "dummy-alt", "owner": "newcomer", "wait_ready": False})

    assert skipped.status_code == 409
    assert "queued claimant pending" in skipped.json()["detail"]


def test_refreshing_a_lease_with_a_different_target_is_rejected(client) -> None:
    acquired = client.post("/acquire", json={"target": "dummy-ok", "owner": "alice"})
    assert acquired.status_code == 200
    lease_token = acquired.json()["lease_token"]

    refresh = client.post("/acquire", json={"target": "dummy-alt", "lease_token": lease_token, "wait_ready": False})

    assert refresh.status_code == 409
    assert "target mismatch" in refresh.json()["detail"]


def test_claiming_a_queued_token_for_the_wrong_target_is_rejected(client) -> None:
    active = client.post("/acquire", json={"target": "dummy-ok", "owner": "alice"})
    assert active.status_code == 200
    active_token = active.json()["lease_token"]

    queued = client.post("/acquire", json={"target": "dummy-alt", "owner": "bob", "priority": 25})
    assert queued.status_code == 200
    queued_token = queued.json()["lease_token"]

    release = client.post("/release", json={"lease_token": active_token})
    assert release.status_code == 200

    wrong_target = client.post("/acquire", json={"target": "dummy-ok", "lease_token": queued_token, "wait_ready": False})

    assert wrong_target.status_code == 409
    assert "target mismatch" in wrong_target.json()["detail"]


@pytest.mark.parametrize(
    ("target", "expected_status", "expected_text"),
    [
        ("dummy-no-health", 500, "missing a Docker healthcheck"),
        ("dummy-multi-master", 500, "multiple services"),
        ("dummy-unhealthy", 500, '"Status": "unhealthy"'),
    ],
)
def test_readiness_failures_return_clear_errors(client, target: str, expected_status: int, expected_text: str) -> None:
    response = client.post("/acquire", json={"target": target, "owner": "alice", "wait_s": 10})

    assert response.status_code == expected_status
    assert expected_text in response.json()["detail"]


def test_force_release_does_not_require_a_token(client) -> None:
    acquired = client.post("/acquire", json={"target": "dummy-ok", "owner": "alice"})
    assert acquired.status_code == 200

    release = client.post("/release", json={"force": True})

    assert release.status_code == 200
    assert release.json()["state"]["queue"] == []
