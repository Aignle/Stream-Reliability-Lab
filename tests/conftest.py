"""Shared real-network fixtures for browser and scenario verification."""

from __future__ import annotations

import os
import socket
import subprocess
import sys
import time
from collections.abc import Iterator
from pathlib import Path

import httpx
import pytest

SERVER_START_ATTEMPTS = 3
SERVER_START_TIMEOUT_SECONDS = 15


def _free_port() -> int:
    with socket.socket() as listener:
        listener.bind(("127.0.0.1", 0))
        return int(listener.getsockname()[1])


def _stop_process(process: subprocess.Popen[bytes]) -> None:
    if process.poll() is not None:
        return
    process.terminate()
    try:
        process.wait(timeout=5)
    except subprocess.TimeoutExpired:
        process.kill()
        process.wait(timeout=5)


@pytest.fixture
def live_server(tmp_path) -> Iterator[tuple[str, str]]:
    """Start an isolated real-network API for browser and simulator clients."""
    environment = os.environ.copy()
    environment["STREAMLAB_DB_PATH"] = str(tmp_path / "live.duckdb")
    creation_flags = subprocess.CREATE_NO_WINDOW if os.name == "nt" else 0
    diagnostics: list[str] = []
    process: subprocess.Popen[bytes] | None = None
    log_stream = None
    api_url = ""
    ws_url = ""

    for attempt in range(1, SERVER_START_ATTEMPTS + 1):
        port = _free_port()
        api_url = f"http://127.0.0.1:{port}"
        ws_url = f"ws://127.0.0.1:{port}"
        log_path = tmp_path / f"uvicorn-start-{attempt}.log"
        log_stream = log_path.open("w", encoding="utf-8")
        process = subprocess.Popen(
            [
                sys.executable,
                "-m",
                "uvicorn",
                "streamlab.main:app",
                "--host",
                "127.0.0.1",
                "--port",
                str(port),
                "--log-level",
                "warning",
            ],
            cwd=Path(__file__).parents[1],
            env=environment,
            stdout=log_stream,
            stderr=subprocess.STDOUT,
            creationflags=creation_flags,
        )
        deadline = time.monotonic() + SERVER_START_TIMEOUT_SECONDS
        healthy = False
        while time.monotonic() < deadline:
            if process.poll() is not None:
                break
            try:
                response = httpx.get(f"{api_url}/health", timeout=0.5)
                if response.status_code == 200:
                    time.sleep(0.05)
                    healthy = process.poll() is None
                    break
            except httpx.HTTPError:
                pass
            time.sleep(0.05)
        if healthy:
            break

        exited_early = process.poll() is not None
        _stop_process(process)
        exit_code = process.returncode
        log_stream.close()
        output = log_path.read_text(encoding="utf-8").strip() or "(no output)"
        diagnostics.append(
            f"attempt {attempt} on port {port} exited {exit_code}: {output}"
        )
        process = None
        log_stream = None
        if not exited_early:
            break

    if process is None or log_stream is None:
        detail = " | ".join(diagnostics)
        raise RuntimeError(
            f"Uvicorn did not become healthy after {len(diagnostics)} attempt(s): "
            f"{detail}"
        )

    try:
        yield api_url, ws_url
    finally:
        _stop_process(process)
        log_stream.close()
