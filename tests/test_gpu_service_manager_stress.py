from __future__ import annotations

import json
import os
import shutil
import signal
import socket
import subprocess
import sys
import time
from concurrent.futures import ThreadPoolExecutor
from pathlib import Path

import httpx
import pytest


REPO_ROOT = Path(__file__).resolve().parents[1]
TEST_FIXTURES_ROOT = Path(__file__).resolve().parent / "fixtures"
SERVICES_FIXTURES_ROOT = TEST_FIXTURES_ROOT / "services"
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
        shutil.copytree(SERVICES_FIXTURES_ROOT / name, services_root / name)


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


def _free_port() -> int:
    with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as sock:
        sock.bind(("127.0.0.1", 0))
        return int(sock.getsockname()[1])


@pytest.fixture
def live_server(tmp_path: Path):
    services_root = tmp_path / "services"
    runtime_root = tmp_path / "runtime"
    _copy_dummy_services(tmp_path)
    runtime_root.mkdir(parents=True, exist_ok=True)

    port = _free_port()
    base_url = f"http://127.0.0.1:{port}"
    log_path = tmp_path / "uvicorn.log"
    env = dict(os.environ)
    env.update(
        {
            "GPU_HOST_SERVICES_DIR": str(services_root),
            "GPU_HOST_RUNTIME_DIR": str(runtime_root),
            "GPU_SERVICES_DIR": str(services_root),
            "GPU_RUNTIME_DIR": str(runtime_root),
            "GPU_ENV_FILE": str(services_root / ".env"),
            "DEFAULT_WAIT_S": "20",
            "DEFAULT_LEASE_TTL_S": "120",
            "QUEUE_CLAIM_WINDOW_S": "3",
        }
    )

    with log_path.open("w") as log_file:
        proc = subprocess.Popen(
            [
                sys.executable,
                "-m",
                "uvicorn",
                "app:app",
                "--host",
                "127.0.0.1",
                "--port",
                str(port),
                "--workers",
                "2",
            ],
            cwd=REPO_ROOT,
            env=env,
            stdout=log_file,
            stderr=subprocess.STDOUT,
            text=True,
            start_new_session=True,
        )

        deadline = time.time() + 30
        while time.time() < deadline:
            if proc.poll() is not None:
                pytest.fail(log_path.read_text())
            try:
                response = httpx.get(f"{base_url}/healthz", timeout=1.0)
                if response.status_code == 200:
                    break
            except httpx.HTTPError:
                pass
            time.sleep(0.25)
        else:
            os.killpg(proc.pid, signal.SIGTERM)
            proc.wait(timeout=10)
            pytest.fail(log_path.read_text())

        yield {"base_url": base_url, "runtime_root": runtime_root}

        os.killpg(proc.pid, signal.SIGTERM)
        proc.wait(timeout=10)

    _docker_compose_down(tmp_path)


def test_live_server_stress_queue_and_claims(live_server) -> None:
    base_url = live_server["base_url"]
    runtime_root = live_server["runtime_root"]

    with httpx.Client(base_url=base_url, timeout=30.0) as client:
        acquired = client.post("/acquire", json={"target": "dummy-ok", "owner": "seed"})
        assert acquired.status_code == 200
        active_token = acquired.json()["lease_token"]

    priorities = {f"queued-{index}": 200 - index for index in range(10)}

    def enqueue(owner: str) -> tuple[str, httpx.Response]:
        with httpx.Client(base_url=base_url, timeout=30.0) as client:
            response = client.post(
                "/acquire",
                json={"target": "dummy-alt", "owner": owner, "priority": priorities[owner], "wait_ready": False},
            )
            return owner, response

    def bad_refresh(attempts: int) -> list[int]:
        codes: list[int] = []
        with httpx.Client(base_url=base_url, timeout=30.0) as client:
            for _ in range(attempts):
                response = client.post(
                    "/acquire",
                    json={"target": "dummy-alt", "lease_token": active_token, "wait_ready": False},
                )
                codes.append(response.status_code)
        return codes

    def poll_status(attempts: int) -> list[int]:
        queue_lengths: list[int] = []
        with httpx.Client(base_url=base_url, timeout=30.0) as client:
            for _ in range(attempts):
                response = client.get("/status")
                assert response.status_code == 200
                queue_lengths.append(len(response.json()["state"]["queue"]))
        return queue_lengths

    with ThreadPoolExecutor(max_workers=16) as executor:
        enqueue_futures = [executor.submit(enqueue, owner) for owner in priorities]
        refresh_futures = [executor.submit(bad_refresh, 6) for _ in range(3)]
        status_futures = [executor.submit(poll_status, 15) for _ in range(3)]

    enqueue_results = [future.result() for future in enqueue_futures]
    refresh_results = [future.result() for future in refresh_futures]
    status_results = [future.result() for future in status_futures]

    queued_tokens: dict[str, str] = {}
    for owner, response in enqueue_results:
        assert response.status_code == 200
        payload = response.json()
        assert payload["queued"] is True
        queued_tokens[owner] = payload["lease_token"]

    assert all(code == 409 for result in refresh_results for code in result)
    assert all(all(length >= 0 for length in result) for result in status_results)

    with httpx.Client(base_url=base_url, timeout=30.0) as client:
        status_response = client.get("/status")
        assert status_response.status_code == 200
        status_payload = status_response.json()

    queue = status_payload["state"]["queue"]
    assert len(queue) == len(priorities)
    expected_order = [owner for owner, _priority in sorted(priorities.items(), key=lambda item: -item[1])]
    assert [entry["owner"] for entry in queue] == expected_order
    assert [queued_tokens[entry["owner"]] for entry in queue] == [entry["token"] for entry in queue]

    state_on_disk = json.loads((runtime_root / "state.json").read_text())
    assert len(state_on_disk["queue"]) == len(priorities)
    assert state_on_disk["lease"]["token"] == active_token

    with httpx.Client(base_url=base_url, timeout=30.0) as client:
        released = client.post("/release", json={"lease_token": active_token})
        assert released.status_code == 200
        release_payload = released.json()

    head = release_payload["state"]["queue"][0]
    assert isinstance(head["claim_expires_at"], int)

    def claim(token: str) -> tuple[str, httpx.Response]:
        with httpx.Client(base_url=base_url, timeout=30.0) as client:
            response = client.post("/acquire", json={"target": "dummy-alt", "lease_token": token, "wait_s": 20})
            return token, response

    def poll_during_claim(attempts: int) -> list[int]:
        lengths: list[int] = []
        with httpx.Client(base_url=base_url, timeout=30.0) as client:
            for _ in range(attempts):
                response = client.get("/status")
                assert response.status_code == 200
                lengths.append(len(response.json()["state"]["queue"]))
        return lengths

    with ThreadPoolExecutor(max_workers=16) as executor:
        claim_futures = [executor.submit(claim, token) for token in queued_tokens.values()]
        poll_future = executor.submit(poll_during_claim, 20)

    claim_results = [future.result() for future in claim_futures]
    _ = poll_future.result()

    successes = [(token, response) for token, response in claim_results if response.status_code == 200]
    conflicts = [(token, response) for token, response in claim_results if response.status_code == 409]

    assert len(successes) == 1
    assert len(conflicts) == len(priorities) - 1
    assert successes[0][0] == head["token"]
    assert all(
        any(
            marker in response.json()["detail"]
            for marker in ("queue_index", "gpu busy", "queued claimant pending", "expired")
        )
        for _, response in conflicts
    )

    claimed_payload = successes[0][1].json()
    assert claimed_payload["state"]["target"] == "dummy-alt"
    assert claimed_payload["service"]["Status"] == "running"
    assert claimed_payload["service"]["Health"]["Status"] == "healthy"
    assert claimed_payload["service"]["container_count"] == 1
    assert len(claimed_payload["state"]["queue"]) == len(priorities) - 1

    with httpx.Client(base_url=base_url, timeout=30.0) as client:
        final_status = client.get("/status")
        assert final_status.status_code == 200
        final_payload = final_status.json()
        assert final_payload["state"]["target"] == "dummy-alt"
        assert final_payload["state"]["lease"]["owner"] == head["owner"]

        force_release = client.post("/release", json={"force": True})
        assert force_release.status_code == 200
        released_payload = force_release.json()
        assert "lease" not in released_payload["state"]
