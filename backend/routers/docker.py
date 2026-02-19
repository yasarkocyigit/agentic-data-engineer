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
