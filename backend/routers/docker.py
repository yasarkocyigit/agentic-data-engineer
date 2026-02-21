from fastapi import APIRouter, HTTPException
import subprocess
import asyncio

router = APIRouter()

ALLOWED_ACTIONS = {"start", "stop", "restart"}

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
            stderr=subprocess.PIPE
        )
        
        stdout, stderr = await process.communicate()
        
        if process.returncode != 0:
            error_msg = stderr.decode().strip() or stdout.decode().strip()
            raise HTTPException(
                status_code=500, 
                detail=f"Failed to {action} container '{container_name}': {error_msg}"
            )
            
        return {
            "status": "success", 
            "message": f"Successfully executed '{action}' on '{container_name}'",
            "output": stdout.decode().strip()
        }
        
        
    except Exception as e:
        if isinstance(e, HTTPException):
            raise e
        raise HTTPException(status_code=500, detail=str(e))

import json

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
        'B': 1,
        'kB': 1e3, 'MB': 1e6, 'GB': 1e9, 'TB': 1e12,
        'KiB': 1024, 'MiB': 1024**2, 'GiB': 1024**3, 'TiB': 1024**4
    }
    
    import re
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
            "docker", "info", "--format", "{{json .}}",
            stdout=subprocess.PIPE,
            stderr=subprocess.PIPE
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
            "docker", "stats", "--no-stream", "--format", "{{json .}}",
            stdout=subprocess.PIPE,
            stderr=subprocess.PIPE
        )
        stats_stdout, stats_stderr = await stats_proc.communicate()
        
        if stats_proc.returncode != 0:
            raise HTTPException(status_code=500, detail="Failed to fetch docker stats")
            
        total_cpu_perc = 0.0
        used_mem_bytes = 0.0
        container_count = 0
        
        lines = stats_stdout.decode().strip().split('\n')
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
                "cores": total_cpus
            },
            "memory": {
                "used_bytes": used_mem_bytes,
                "total_bytes": total_mem_bytes,
                "used_gb": round(used_mem_bytes / (1024**3), 2),
                "total_gb": round(total_mem_bytes / (1024**3), 2)
            }
        }

    except Exception as e:
        if isinstance(e, HTTPException):
            raise e
        raise HTTPException(status_code=500, detail=str(e))
