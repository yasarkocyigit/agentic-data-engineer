import asyncio
import contextlib
import json
import os
import re
import signal
import struct
import subprocess
import termios
import fcntl
import pty

from fastapi import APIRouter, HTTPException, Query, WebSocket, WebSocketDisconnect
from pydantic import BaseModel

router = APIRouter()

ALLOWED_ACTIONS = {"start", "stop", "restart"}
_DOCKER_FULL_CLI_ENABLED = os.getenv("DOCKER_FULL_CLI", "true").strip().lower() in {"1", "true", "yes", "on"}
_DOCKER_CLI_DEFAULT_TIMEOUT_SECONDS = 90
_DOCKER_CLI_MAX_TIMEOUT_SECONDS = 300
_TERMINAL_DEFAULT_SHELL = os.getenv("DOCKER_TERMINAL_SHELL", "bash")


class DockerCliRequest(BaseModel):
    command: str
    timeout_seconds: int | None = None


def _set_winsize(fd: int, rows: int, cols: int) -> None:
    safe_rows = max(10, min(rows, 200))
    safe_cols = max(20, min(cols, 400))
    packed = struct.pack("HHHH", safe_rows, safe_cols, 0, 0)
    fcntl.ioctl(fd, termios.TIOCSWINSZ, packed)


@router.post("/docker/{action}/{container_name}")
async def manage_container(action: str, container_name: str):
    """
    Executes a docker lifecycle command (start, stop, restart) on a specified container.
    """
    if action not in ALLOWED_ACTIONS:
        raise HTTPException(status_code=400, detail=f"Invalid action. Supported actions: {ALLOWED_ACTIONS}")

    # We only allow alphanumeric, hyphens, and underscores for container names to prevent injection
    if not container_name.replace("-", "").replace("_", "").isalnum():
        raise HTTPException(status_code=400, detail="Invalid container name format.")

    try:
        # Run docker command asynchronously to prevent blocking the event loop
        cmd = ["docker", action, container_name]
        process = await asyncio.create_subprocess_exec(
            *cmd,
            stdout=subprocess.PIPE,
            stderr=subprocess.PIPE,
        )

        stdout, stderr = await process.communicate()

        if process.returncode != 0:
            error_msg = stderr.decode().strip() or stdout.decode().strip()
            raise HTTPException(
                status_code=500,
                detail=f"Failed to {action} container '{container_name}': {error_msg}",
            )

        return {
            "status": "success",
            "message": f"Successfully executed '{action}' on '{container_name}'",
            "output": stdout.decode().strip(),
        }

    except Exception as e:
        if isinstance(e, HTTPException):
            raise e
        raise HTTPException(status_code=500, detail=str(e))


@router.post("/docker/cli")
async def execute_docker_cli(request: DockerCliRequest):
    """
    Execute a shell command from the Docker CLI page terminal.
    Full command mode is intended for local development.
    """
    if not _DOCKER_FULL_CLI_ENABLED:
        raise HTTPException(status_code=403, detail="Docker full CLI mode is disabled")

    command = (request.command or "").strip()
    if not command:
        raise HTTPException(status_code=400, detail="command is required")

    timeout_seconds = request.timeout_seconds or _DOCKER_CLI_DEFAULT_TIMEOUT_SECONDS
    timeout_seconds = max(1, min(timeout_seconds, _DOCKER_CLI_MAX_TIMEOUT_SECONDS))

    process = await asyncio.create_subprocess_exec(
        "bash",
        "-o",
        "pipefail",
        "-lc",
        command,
        stdout=subprocess.PIPE,
        stderr=subprocess.PIPE,
    )

    try:
        stdout, stderr = await asyncio.wait_for(process.communicate(), timeout=timeout_seconds)
    except asyncio.TimeoutError:
        process.kill()
        with contextlib.suppress(Exception):
            await process.communicate()
        return {
            "status": "timeout",
            "command": command,
            "exit_code": None,
            "stdout": "",
            "stderr": f"Command timed out after {timeout_seconds}s",
        }

    return {
        "status": "success" if process.returncode == 0 else "error",
        "command": command,
        "exit_code": process.returncode,
        "stdout": stdout.decode("utf-8", errors="replace"),
        "stderr": stderr.decode("utf-8", errors="replace"),
    }


@router.websocket("/docker/pty")
async def docker_pty_terminal(
    websocket: WebSocket,
    container: str | None = Query(default=None),
    shell: str | None = Query(default=None),
    rows: int = Query(default=24),
    cols: int = Query(default=100),
):
    """
    Interactive PTY terminal over WebSocket.
    - container provided: docker exec -it <container> <shell>
    - container omitted: local shell in openclaw-api container
    """
    if not _DOCKER_FULL_CLI_ENABLED:
        await websocket.accept()
        await websocket.send_text("Docker full CLI mode is disabled.\r\n")
        await websocket.close(code=1008)
        return

    await websocket.accept()

    chosen_shell = (shell or _TERMINAL_DEFAULT_SHELL).strip() or "bash"
    if container:
        # Prefer bash if available, otherwise fallback to sh inside the target container.
        cmd = [
            "docker",
            "exec",
            "-it",
            container,
            "sh",
            "-lc",
            f"command -v {chosen_shell} >/dev/null 2>&1 && exec {chosen_shell} || exec sh",
        ]
    else:
        cmd = ["bash", "-lc", f"exec {chosen_shell}"]

    master_fd, slave_fd = pty.openpty()
    process: asyncio.subprocess.Process | None = None
    out_task: asyncio.Task | None = None
    in_task: asyncio.Task | None = None

    try:
        _set_winsize(slave_fd, rows, cols)
        process = await asyncio.create_subprocess_exec(
            *cmd,
            stdin=slave_fd,
            stdout=slave_fd,
            stderr=slave_fd,
            preexec_fn=os.setsid,
        )
        os.close(slave_fd)
        slave_fd = -1

        async def stream_output() -> None:
            loop = asyncio.get_running_loop()
            while True:
                try:
                    chunk = await loop.run_in_executor(None, os.read, master_fd, 4096)
                except OSError:
                    break
                if not chunk:
                    break
                await websocket.send_text(chunk.decode("utf-8", errors="replace"))

        async def stream_input() -> None:
            while True:
                message = await websocket.receive_text()
                # Special control message for terminal resize from frontend
                try:
                    payload = json.loads(message)
                    if isinstance(payload, dict) and payload.get("type") == "resize":
                        next_rows = int(payload.get("rows", rows))
                        next_cols = int(payload.get("cols", cols))
                        _set_winsize(master_fd, next_rows, next_cols)
                        continue
                except (json.JSONDecodeError, ValueError, TypeError):
                    pass
                os.write(master_fd, message.encode("utf-8", errors="replace"))

        out_task = asyncio.create_task(stream_output())
        in_task = asyncio.create_task(stream_input())

        done, pending = await asyncio.wait(
            [out_task, in_task],
            return_when=asyncio.FIRST_COMPLETED,
        )
        for task in pending:
            task.cancel()
            with contextlib.suppress(Exception):
                await task
        for task in done:
            with contextlib.suppress(Exception):
                task.result()

    except WebSocketDisconnect:
        pass
    except Exception as e:
        with contextlib.suppress(Exception):
            await websocket.send_text(f"\r\n[terminal-error] {e}\r\n")
    finally:
        if in_task and not in_task.done():
            in_task.cancel()
            with contextlib.suppress(Exception):
                await in_task
        if out_task and not out_task.done():
            out_task.cancel()
            with contextlib.suppress(Exception):
                await out_task

        if process and process.returncode is None:
            with contextlib.suppress(ProcessLookupError):
                os.killpg(process.pid, signal.SIGTERM)
            with contextlib.suppress(Exception):
                await asyncio.wait_for(process.wait(), timeout=2)
            if process.returncode is None:
                with contextlib.suppress(ProcessLookupError):
                    os.killpg(process.pid, signal.SIGKILL)
                with contextlib.suppress(Exception):
                    await process.wait()

        with contextlib.suppress(OSError):
            if master_fd >= 0:
                os.close(master_fd)
        with contextlib.suppress(OSError):
            if slave_fd >= 0:
                os.close(slave_fd)
        with contextlib.suppress(Exception):
            await websocket.close()


def parse_memory_string(mem_str: str) -> float:
    """
    Parses a string like '96.65MiB' or '1.5GiB' or '500kB'
    Returns the value in bytes (float).
    """
    mem_str = mem_str.strip()
    if not mem_str:
        return 0.0

    # Mapping of suffixes to multipliers (binary and decimal bases)
    multipliers = {
        "B": 1,
        "kB": 1e3,
        "MB": 1e6,
        "GB": 1e9,
        "TB": 1e12,
        "KiB": 1024,
        "MiB": 1024**2,
        "GiB": 1024**3,
        "TiB": 1024**4,
    }

    match = re.search(r"([\d\.]+)\s*([a-zA-Z]+)?", mem_str)
    if not match:
        return 0.0

    val = float(match.group(1))
    unit = match.group(2)

    if unit in multipliers:
        return val * multipliers[unit]
    return val


@router.get("/docker/stats")
async def get_docker_stats():
    """
    Retrieves aggregated CPU and Memory usage for all running containers.
    """
    try:
        # Get host info for total CPUs and Memory
        info_proc = await asyncio.create_subprocess_exec(
            "docker",
            "info",
            "--format",
            "{{json .}}",
            stdout=subprocess.PIPE,
            stderr=subprocess.PIPE,
        )
        info_stdout, _ = await info_proc.communicate()

        info_data = {}
        try:
            info_data = json.loads(info_stdout.decode().strip())
        except Exception:
            pass

        total_cpus = info_data.get("NCPU", 0)
        total_mem_bytes = info_data.get("MemTotal", 0)

        # Get stats for all running containers
        stats_proc = await asyncio.create_subprocess_exec(
            "docker",
            "stats",
            "--no-stream",
            "--format",
            "{{json .}}",
            stdout=subprocess.PIPE,
            stderr=subprocess.PIPE,
        )
        stats_stdout, _ = await stats_proc.communicate()

        if stats_proc.returncode != 0:
            raise HTTPException(status_code=500, detail="Failed to fetch docker stats")

        total_cpu_perc = 0.0
        used_mem_bytes = 0.0
        container_count = 0

        lines = stats_stdout.decode().strip().split("\n")
        for line in lines:
            if not line.strip():
                continue
            container_count += 1
            try:
                stat = json.loads(line)

                # Parse CPU (e.g., "1.06%")
                cpu_str = stat.get("CPUPerc", "0%").replace("%", "")
                total_cpu_perc += float(cpu_str)

                # Parse Mem (e.g., "96.65MiB / 7.653GiB")
                mem_usage_str = stat.get("MemUsage", "0B / 0B")
                used_part = mem_usage_str.split("/")[0]
                used_mem_bytes += parse_memory_string(used_part)

                # If total_mem_bytes wasn't found from info, grab it here
                if total_mem_bytes == 0 and "/" in mem_usage_str:
                    total_part = mem_usage_str.split("/")[1]
                    total_mem_bytes = parse_memory_string(total_part)

            except Exception:
                continue

        return {
            "status": "success",
            "containers": container_count,
            "cpu": {
                "used_percent": round(total_cpu_perc, 2),
                "total_percent": total_cpus * 100,
                "cores": total_cpus,
            },
            "memory": {
                "used_bytes": used_mem_bytes,
                "total_bytes": total_mem_bytes,
                "used_gb": round(used_mem_bytes / (1024**3), 2),
                "total_gb": round(total_mem_bytes / (1024**3), 2),
            },
        }

    except Exception as e:
        if isinstance(e, HTTPException):
            raise e
        raise HTTPException(status_code=500, detail=str(e))
