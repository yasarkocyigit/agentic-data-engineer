"""
Gitea Router — Gitea REST API v1 proxy.
Provides native access to repositories, branches, commits, file contents,
pull requests, and Actions workflow runs via the Gitea API.
"""
import base64
import logging
import os
from typing import Optional

import httpx
from fastapi import APIRouter, HTTPException, Query
from pydantic import BaseModel

logger = logging.getLogger(__name__)

router = APIRouter()

GITEA_URL = os.getenv("GITEA_URL", "http://localhost:3030")
GITEA_TOKEN = os.getenv("GITEA_TOKEN", "")


# ─── Core Fetch Helper ───


async def gitea_fetch(path: str, method: str = "GET", body: dict = None) -> dict:
    """Request to Gitea REST API v1."""
    url = f"{GITEA_URL}/api/v1{path}"

    headers = {"Content-Type": "application/json"}
    if GITEA_TOKEN:
        headers["Authorization"] = f"token {GITEA_TOKEN}"

    async with httpx.AsyncClient(timeout=30.0) as client:
        kwargs = {"headers": headers}
        if body is not None:
            kwargs["json"] = body

        if method == "GET":
            res = await client.get(url, **kwargs)
        elif method == "POST":
            res = await client.post(url, **kwargs)
        elif method == "PATCH":
            res = await client.patch(url, **kwargs)
        elif method == "PUT":
            res = await client.put(url, **kwargs)
        elif method == "DELETE":
            res = await client.delete(url, **kwargs)
        else:
            raise ValueError(f"Unsupported method: {method}")

    if not res.is_success:
        err_msg = f"Gitea {res.status_code}"
        try:
            err_body = res.json()
            if "message" in err_body:
                err_msg += ": " + err_body["message"]
            else:
                err_msg += ": " + res.text
        except Exception:
            err_msg += ": " + res.text
        raise HTTPException(status_code=res.status_code, detail=err_msg)

    content_type = res.headers.get("content-type", "")
    if "application/json" in content_type:
        return res.json()
    return {"text": res.text}


# ─── Request Models ───


class CreatePRRequest(BaseModel):
    title: str
    body: str = ""
    head: str
    base: str


class MergePRRequest(BaseModel):
    merge_message_field: str = ""
    merge_message_field_style: str = "merge"  # merge | rebase | squash | fast-forward-only
    delete_branch_after_merge: bool = False


class CreateBranchRequest(BaseModel):
    new_branch_name: str
    old_branch_name: str = "main"


class FileUpdateRequest(BaseModel):
    path: str
    content: str       # base64-encoded content
    message: str       # commit message
    sha: str           # current file SHA (required by Gitea)
    branch: str = "main"


# ─── Routes ───


@router.get("/gitea/health")
async def gitea_health():
    """Check Gitea connectivity by fetching version info."""
    try:
        data = await gitea_fetch("/version")
        return {"status": "online", "version": data.get("version", "unknown")}
    except Exception as e:
        return {"status": "offline", "error": str(e)}


@router.get("/gitea/repos")
async def list_repos(
    q: Optional[str] = Query(default=None),
    limit: int = Query(default=50),
    page: int = Query(default=1),
):
    """Search/list repositories."""
    params = f"?limit={limit}&page={page}"
    if q:
        params += f"&q={q}"
    try:
        data = await gitea_fetch(f"/repos/search{params}")
        repos = data.get("data", []) if isinstance(data, dict) else data
        return {"repos": repos, "ok": True}
    except HTTPException:
        raise
    except Exception as e:
        logger.error("[Gitea] GET /repos error: %s", e)
        raise HTTPException(status_code=500, detail=str(e))


@router.get("/gitea/repos/{owner}/{repo}")
async def get_repo(owner: str, repo: str):
    """Get repository detail."""
    try:
        return await gitea_fetch(f"/repos/{owner}/{repo}")
    except HTTPException:
        raise
    except Exception as e:
        logger.error("[Gitea] GET /repos/%s/%s error: %s", owner, repo, e)
        raise HTTPException(status_code=500, detail=str(e))


# ─── Branches ───


@router.get("/gitea/repos/{owner}/{repo}/branches")
async def list_branches(owner: str, repo: str):
    """List branches for a repository."""
    try:
        data = await gitea_fetch(f"/repos/{owner}/{repo}/branches")
        return {"branches": data if isinstance(data, list) else []}
    except HTTPException:
        raise
    except Exception as e:
        logger.error("[Gitea] GET /branches error: %s", e)
        raise HTTPException(status_code=500, detail=str(e))


@router.post("/gitea/repos/{owner}/{repo}/branches")
async def create_branch(owner: str, repo: str, req: CreateBranchRequest):
    """Create a new branch."""
    try:
        data = await gitea_fetch(
            f"/repos/{owner}/{repo}/branches",
            method="POST",
            body={"new_branch_name": req.new_branch_name, "old_branch_name": req.old_branch_name},
        )
        return data
    except HTTPException:
        raise
    except Exception as e:
        logger.error("[Gitea] POST /branches error: %s", e)
        raise HTTPException(status_code=500, detail=str(e))


# ─── Commits ───


@router.get("/gitea/repos/{owner}/{repo}/commits")
async def list_commits(
    owner: str,
    repo: str,
    sha: Optional[str] = Query(default=None),
    limit: int = Query(default=20),
    page: int = Query(default=1),
):
    """List commits for a repository."""
    params = f"?limit={limit}&page={page}"
    if sha:
        params += f"&sha={sha}"
    try:
        data = await gitea_fetch(f"/repos/{owner}/{repo}/commits{params}")
        commits = data if isinstance(data, list) else []
        return {"commits": commits}
    except HTTPException:
        raise
    except Exception as e:
        logger.error("[Gitea] GET /commits error: %s", e)
        raise HTTPException(status_code=500, detail=str(e))


@router.get("/gitea/repos/{owner}/{repo}/commits/{sha}")
async def get_commit_detail(owner: str, repo: str, sha: str):
    """Get a single commit with diff/stats."""
    try:
        data = await gitea_fetch(f"/repos/{owner}/{repo}/git/commits/{sha}")
        return data
    except HTTPException:
        raise
    except Exception as e:
        logger.error("[Gitea] GET /commits/%s error: %s", sha, e)
        raise HTTPException(status_code=500, detail=str(e))


# ─── File Tree & Content ───


@router.get("/gitea/repos/{owner}/{repo}/tree")
async def get_file_tree(
    owner: str,
    repo: str,
    ref: Optional[str] = Query(default=None),
    path: str = Query(default=""),
):
    """Get directory listing (file tree) for a path."""
    api_path = f"/repos/{owner}/{repo}/contents/"
    if path:
        api_path += path
    params = f"?ref={ref}" if ref else ""
    try:
        data = await gitea_fetch(f"{api_path}{params}")
        # Gitea returns a list for directories, a dict for single files
        if isinstance(data, list):
            entries = []
            for item in data:
                entries.append({
                    "name": item.get("name"),
                    "path": item.get("path"),
                    "type": item.get("type"),  # "file" or "dir"
                    "size": item.get("size", 0),
                    "sha": item.get("sha"),
                })
            # Sort: dirs first, then files, both alphabetically
            entries.sort(key=lambda x: (0 if x["type"] == "dir" else 1, x["name"].lower()))
            return {"entries": entries, "path": path or "/"}
        # Single file
        return {"entries": [data], "path": path}
    except HTTPException:
        raise
    except Exception as e:
        logger.error("[Gitea] GET /tree error: %s", e)
        raise HTTPException(status_code=500, detail=str(e))


@router.get("/gitea/repos/{owner}/{repo}/file")
async def get_file_content(
    owner: str,
    repo: str,
    path: str = Query(...),
    ref: Optional[str] = Query(default=None),
):
    """Get decoded file content."""
    params = f"?ref={ref}" if ref else ""
    try:
        data = await gitea_fetch(f"/repos/{owner}/{repo}/contents/{path}{params}")
        content = ""
        if isinstance(data, dict) and data.get("content"):
            try:
                content = base64.b64decode(data["content"]).decode("utf-8", errors="replace")
            except Exception:
                content = "[Binary file — cannot display]"
        return {
            "name": data.get("name", path.split("/")[-1]),
            "path": path,
            "size": data.get("size", 0),
            "sha": data.get("sha", ""),
            "content": content,
            "encoding": data.get("encoding", ""),
        }
    except HTTPException:
        raise
    except Exception as e:
        logger.error("[Gitea] GET /file error: %s", e)
        raise HTTPException(status_code=500, detail=str(e))


@router.put("/gitea/repos/{owner}/{repo}/file")
async def update_file_content(owner: str, repo: str, req: FileUpdateRequest):
    """Update (commit) a file's content."""
    try:
        data = await gitea_fetch(
            f"/repos/{owner}/{repo}/contents/{req.path}",
            method="PUT",
            body={
                "message": req.message,
                "content": req.content,
                "sha": req.sha,
                "branch": req.branch,
            },
        )
        return {"ok": True, "commit": data.get("commit", {})}
    except HTTPException:
        raise
    except Exception as e:
        logger.error("[Gitea] PUT /file error: %s", e)
        raise HTTPException(status_code=500, detail=str(e))


@router.get("/gitea/repos/{owner}/{repo}/tags")
async def list_tags(
    owner: str,
    repo: str,
    limit: int = Query(default=20),
    page: int = Query(default=1),
):
    """List repository tags."""
    try:
        data = await gitea_fetch(f"/repos/{owner}/{repo}/tags?limit={limit}&page={page}")
        return data if isinstance(data, list) else []
    except HTTPException:
        raise
    except Exception as e:
        logger.error("[Gitea] GET /tags error: %s", e)
        raise HTTPException(status_code=500, detail=str(e))


@router.get("/gitea/repos/{owner}/{repo}/contents/{path:path}")
async def get_contents(owner: str, repo: str, path: str, ref: Optional[str] = Query(default=None)):
    """Get file or directory contents (raw Gitea response)."""
    params = f"?ref={ref}" if ref else ""
    try:
        return await gitea_fetch(f"/repos/{owner}/{repo}/contents/{path}{params}")
    except HTTPException:
        raise
    except Exception as e:
        logger.error("[Gitea] GET /contents error: %s", e)
        raise HTTPException(status_code=500, detail=str(e))


# ─── Pull Requests ───


@router.get("/gitea/repos/{owner}/{repo}/pulls")
async def list_pulls(
    owner: str,
    repo: str,
    state: str = Query(default="open"),
    limit: int = Query(default=20),
    page: int = Query(default=1),
):
    """List pull requests."""
    try:
        data = await gitea_fetch(
            f"/repos/{owner}/{repo}/pulls?state={state}&limit={limit}&page={page}"
        )
        prs = data if isinstance(data, list) else []
        return {"pulls": prs}
    except HTTPException:
        raise
    except Exception as e:
        logger.error("[Gitea] GET /pulls error: %s", e)
        raise HTTPException(status_code=500, detail=str(e))





async def _fetch_and_parse_diff(owner: str, repo: str, pull_id: int) -> dict:
    """Fetch raw diff and parse patches for files."""
    try:
        # Note: .diff endpoint is NOT under /api/v1
        url = f"{GITEA_URL}/{owner}/{repo}/pulls/{pull_id}.diff"
        async with httpx.AsyncClient(timeout=30.0) as client:
            res = await client.get(url)
            if not res.is_success:
                return {}
            diff_text = res.text
            
            # Simple parser for Unified Diff
            patches = {}
            current_file = None
            current_patch = []
            
            for line in diff_text.splitlines():
                if line.startswith("diff --git"):
                    if current_file and current_patch:
                        patches[current_file] = "\n".join(current_patch)
                    current_file = None
                    current_patch = []
                elif line.startswith("+++ b/"):
                    current_file = line[6:].strip()
                elif line.startswith("+++ a/"): # fallback for deleted files? usually /dev/null
                    pass
                elif line.startswith("@@") and current_file:
                    current_patch.append(line)
                elif current_file and current_patch:
                    current_patch.append(line)
            
            # Flush last file
            if current_file and current_patch:
                patches[current_file] = "\n".join(current_patch)
                
            return patches
    except Exception as e:
        logger.error("Failed to fetch/parse diff: %s", e)
        return {}


@router.get("/gitea/repos/{owner}/{repo}/pulls/{pull_id}")
async def get_pull_detail(owner: str, repo: str, pull_id: int):
    """Get pull request detail."""
    try:
        pr = await gitea_fetch(f"/repos/{owner}/{repo}/pulls/{pull_id}")
        # Also fetch changed files
        files_list = []
        try:
            files = await gitea_fetch(f"/repos/{owner}/{repo}/pulls/{pull_id}/files")
            files_list = files if isinstance(files, list) else []
            
            # Check if patch is missing (Gitea API quirk)
            if files_list and "patch" not in files_list[0]:
                patches = await _fetch_and_parse_diff(owner, repo, pull_id)
                for f in files_list:
                    if f.get("filename") in patches:
                        f["patch"] = patches[f["filename"]]
        except Exception as e:
            logger.error("Error fetching pull files: %s", e)
            
        return {"pull": pr, "files": files_list}
    except HTTPException:
        raise
    except Exception as e:
        logger.error("[Gitea] GET /pulls/%s error: %s", pull_id, e)
        raise HTTPException(status_code=500, detail=str(e))


@router.post("/gitea/repos/{owner}/{repo}/pulls")
async def create_pull(owner: str, repo: str, req: CreatePRRequest):
    """Create a pull request."""
    try:
        data = await gitea_fetch(
            f"/repos/{owner}/{repo}/pulls",
            method="POST",
            body={"title": req.title, "body": req.body, "head": req.head, "base": req.base},
        )
        return data
    except HTTPException:
        raise
    except Exception as e:
        logger.error("[Gitea] POST /pulls error: %s", e)
        raise HTTPException(status_code=500, detail=str(e))


@router.post("/gitea/repos/{owner}/{repo}/pulls/{pull_id}/merge")
async def merge_pull(owner: str, repo: str, pull_id: int, req: MergePRRequest):
    """Merge a pull request."""
    try:
        body: dict = {
            "Do": req.merge_message_field_style,
            "delete_branch_after_merge": req.delete_branch_after_merge,
        }
        if req.merge_message_field:
            body["merge_message_field"] = req.merge_message_field
        await gitea_fetch(
            f"/repos/{owner}/{repo}/pulls/{pull_id}/merge",
            method="POST",
            body=body,
        )
        return {"ok": True, "message": "PR merged successfully"}
    except HTTPException:
        raise
    except Exception as e:
        logger.error("[Gitea] POST /pulls/%s/merge error: %s", pull_id, e)
        raise HTTPException(status_code=500, detail=str(e))


@router.patch("/gitea/repos/{owner}/{repo}/pulls/{pull_id}/close")
async def close_pull(owner: str, repo: str, pull_id: int):
    """Close (reject) a pull request."""
    try:
        data = await gitea_fetch(
            f"/repos/{owner}/{repo}/pulls/{pull_id}",
            method="PATCH",
            body={"state": "closed"},
        )
        return {"ok": True, "pull": data}
    except HTTPException:
        raise
    except Exception as e:
        logger.error("[Gitea] PATCH /pulls/%s close error: %s", pull_id, e)
        raise HTTPException(status_code=500, detail=str(e))


@router.post("/gitea/repos/{owner}/{repo}/pulls/{pull_id}/reviews")
async def approve_pull(owner: str, repo: str, pull_id: int):
    """Approve a pull request (submit APPROVED review)."""
    try:
        data = await gitea_fetch(
            f"/repos/{owner}/{repo}/pulls/{pull_id}/reviews",
            method="POST",
            body={"event": "APPROVED", "body": "LGTM ✅"},
        )
        return {"ok": True, "review": data}
    except HTTPException:
        raise
    except Exception as e:
        logger.error("[Gitea] POST /pulls/%s/reviews error: %s", pull_id, e)
        raise HTTPException(status_code=500, detail=str(e))


@router.delete("/gitea/repos/{owner}/{repo}/branches/{branch}")
async def delete_branch(owner: str, repo: str, branch: str):
    """Delete a branch."""
    try:
        await gitea_fetch(
            f"/repos/{owner}/{repo}/branches/{branch}",
            method="DELETE",
        )
        return {"ok": True, "message": f"Branch '{branch}' deleted"}
    except HTTPException:
        raise
    except Exception as e:
        logger.error("[Gitea] DELETE /branches/%s error: %s", branch, e)
        raise HTTPException(status_code=500, detail=str(e))


# ─── Actions ───


@router.get("/gitea/repos/{owner}/{repo}/actions/runs")
async def list_action_runs(
    owner: str,
    repo: str,
    limit: int = Query(default=20),
    page: int = Query(default=1),
):
    """List Actions workflow runs."""
    try:
        try:
            data = await gitea_fetch(
                f"/repos/{owner}/{repo}/actions/tasks?limit={limit}&page={page}"
            )
        except HTTPException as he:
            if he.status_code == 404:
                return {"workflow_runs": [], "total_count": 0}
            raise
        if isinstance(data, list):
            return {"workflow_runs": data, "total_count": len(data)}
        return data
    except HTTPException:
        raise
    except Exception as e:
        logger.error("[Gitea] GET /actions/runs error: %s", e)
        raise HTTPException(status_code=500, detail=str(e))


@router.get("/gitea/repos/{owner}/{repo}/actions/runs/{run_id}/jobs")
async def get_action_jobs(owner: str, repo: str, run_id: int):
    """Get jobs for an Actions workflow run."""
    try:
        return await gitea_fetch(
            f"/repos/{owner}/{repo}/actions/tasks/{run_id}"
        )
    except HTTPException:
        raise
    except Exception as e:
        logger.error("[Gitea] GET /actions/jobs error: %s", e)
        raise HTTPException(status_code=500, detail=str(e))

