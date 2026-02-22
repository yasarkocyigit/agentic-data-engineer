"""
Gitea Router — Gitea REST API v1 proxy.
Provides native access to repositories, branches, commits, file contents,
pull requests, and Actions workflow runs via the Gitea API.
"""
import base64
import logging
import os
from typing import Optional
from urllib.parse import quote, unquote

import httpx
from fastapi import APIRouter, HTTPException, Query
from pydantic import BaseModel

logger = logging.getLogger(__name__)

router = APIRouter()

DEFAULT_GITEA_URL = "http://gitea:3000" if os.path.exists("/.dockerenv") else "http://localhost:3030"
GITEA_URL = os.getenv("GITEA_URL", DEFAULT_GITEA_URL)
GITEA_TOKEN = os.getenv("GITEA_TOKEN", "")
GITEA_USERNAME = os.getenv("GITEA_USERNAME", "")
GITEA_PASSWORD = os.getenv("GITEA_PASSWORD", "")


# ─── Core Fetch Helper ───


def _encode_repo_path(path: str) -> str:
    """URL-encode repository file path while preserving directory separators."""
    if not path:
        return ""
    return quote(path.lstrip("/"), safe="/")


async def gitea_fetch(path: str, method: str = "GET", body: dict = None) -> dict:
    """Request to Gitea REST API v1."""
    url = f"{GITEA_URL.rstrip('/')}/api/v1{path}"

    headers = {"Content-Type": "application/json"}
    if GITEA_TOKEN:
        headers["Authorization"] = f"token {GITEA_TOKEN}"
    elif GITEA_USERNAME and GITEA_PASSWORD:
        creds = f"{GITEA_USERNAME}:{GITEA_PASSWORD}".encode("utf-8")
        headers["Authorization"] = f"Basic {base64.b64encode(creds).decode('utf-8')}"

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
            res = await client.request("DELETE", url, **kwargs)
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


class PullReviewRequest(BaseModel):
    event: str = "APPROVED"  # APPROVED | REQUEST_CHANGES | COMMENT
    body: str = ""


class PullInlineCommentRequest(BaseModel):
    body: str
    path: str
    new_position: int = 0
    old_position: int = 0
    commit_id: str = ""


class CreateBranchRequest(BaseModel):
    new_branch_name: str
    old_branch_name: str = "main"


class BranchProtectionUpsertRequest(BaseModel):
    branch_name: str
    rule_name: str = ""
    enable_status_check: bool = True
    status_check_contexts: list[str] = []
    required_approvals: int = 1
    dismiss_stale_approvals: bool = True
    block_on_rejected_reviews: bool = True
    block_on_outdated_branch: bool = True
    block_on_official_review_requests: bool = False
    enable_force_push: bool = False
    require_signed_commits: bool = False


class FileUpdateRequest(BaseModel):
    path: str
    content: str       # base64-encoded content
    message: str       # commit message
    sha: str = ""      # current file SHA (required for updates, ignored on create)
    branch: str = "main"


class FileDeleteRequest(BaseModel):
    path: str
    message: str       # commit message
    sha: str           # current file SHA (required by Gitea for deletion)
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


# ─── Branch Protections ───


def _branch_protection_payload(req: BranchProtectionUpsertRequest) -> dict:
    contexts = [c.strip() for c in req.status_check_contexts if c and c.strip()]
    return {
        "branch_name": req.branch_name,
        "rule_name": req.rule_name or req.branch_name,
        "enable_status_check": req.enable_status_check,
        "status_check_contexts": contexts,
        "required_approvals": req.required_approvals,
        "dismiss_stale_approvals": req.dismiss_stale_approvals,
        "block_on_rejected_reviews": req.block_on_rejected_reviews,
        "block_on_outdated_branch": req.block_on_outdated_branch,
        "block_on_official_review_requests": req.block_on_official_review_requests,
        "enable_force_push": req.enable_force_push,
        "require_signed_commits": req.require_signed_commits,
    }


@router.get("/gitea/repos/{owner}/{repo}/branch-protections")
async def list_branch_protections(owner: str, repo: str):
    """List branch protection rules for a repository."""
    try:
        data = await gitea_fetch(f"/repos/{owner}/{repo}/branch_protections")
        return {"protections": data if isinstance(data, list) else []}
    except HTTPException:
        raise
    except Exception as e:
        logger.error("[Gitea] GET /branch_protections error: %s", e)
        raise HTTPException(status_code=500, detail=str(e))


@router.get("/gitea/repos/{owner}/{repo}/branch-protections/{branch:path}")
async def get_branch_protection(owner: str, repo: str, branch: str):
    """Get branch protection rule by branch/rule name."""
    try:
        decoded = unquote(branch)
        encoded = quote(decoded, safe="")
        data = await gitea_fetch(f"/repos/{owner}/{repo}/branch_protections/{encoded}")
        return {"protection": data}
    except HTTPException:
        raise
    except Exception as e:
        logger.error("[Gitea] GET /branch_protections/%s error: %s", branch, e)
        raise HTTPException(status_code=500, detail=str(e))


@router.put("/gitea/repos/{owner}/{repo}/branch-protections/{branch:path}")
async def upsert_branch_protection(owner: str, repo: str, branch: str, req: BranchProtectionUpsertRequest):
    """Create or update branch protection rule for a branch."""
    decoded = unquote(branch)
    if req.branch_name != decoded:
        raise HTTPException(status_code=400, detail="branch_name must match branch path")
    encoded = quote(decoded, safe="")
    payload = _branch_protection_payload(req)
    try:
        data = await gitea_fetch(
            f"/repos/{owner}/{repo}/branch_protections/{encoded}",
            method="PATCH",
            body=payload,
        )
        return {"ok": True, "protection": data, "mode": "updated"}
    except HTTPException as he:
        if he.status_code != 404:
            raise
    try:
        data = await gitea_fetch(
            f"/repos/{owner}/{repo}/branch_protections",
            method="POST",
            body=payload,
        )
        return {"ok": True, "protection": data, "mode": "created"}
    except HTTPException:
        raise
    except Exception as e:
        logger.error("[Gitea] PUT /branch_protections/%s error: %s", branch, e)
        raise HTTPException(status_code=500, detail=str(e))


@router.delete("/gitea/repos/{owner}/{repo}/branch-protections/{branch:path}")
async def delete_branch_protection(owner: str, repo: str, branch: str):
    """Delete branch protection rule."""
    try:
        decoded = unquote(branch)
        encoded = quote(decoded, safe="")
        await gitea_fetch(
            f"/repos/{owner}/{repo}/branch_protections/{encoded}",
            method="DELETE",
        )
        return {"ok": True, "message": f"Branch protection '{decoded}' deleted"}
    except HTTPException:
        raise
    except Exception as e:
        logger.error("[Gitea] DELETE /branch_protections/%s error: %s", branch, e)
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


@router.get("/gitea/repos/{owner}/{repo}/commits/{ref}/status")
async def get_commit_status(owner: str, repo: str, ref: str):
    """Get combined status/check contexts for a commit/ref."""
    try:
        data = await gitea_fetch(f"/repos/{owner}/{repo}/commits/{ref}/status")
        if isinstance(data, dict):
            return {
                "state": data.get("state", "unknown"),
                "statuses": data.get("statuses", []),
                "sha": data.get("sha", ref),
                "total_count": data.get("total_count", len(data.get("statuses", []))),
            }
        return {"state": "unknown", "statuses": []}
    except HTTPException:
        raise
    except Exception as e:
        logger.error("[Gitea] GET /commits/%s/status error: %s", ref, e)
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
    encoded_path = _encode_repo_path(path)
    api_path = f"/repos/{owner}/{repo}/contents"
    if encoded_path:
        api_path += f"/{encoded_path}"
    else:
        api_path += "/"
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
        encoded_path = _encode_repo_path(path)
        data = await gitea_fetch(f"/repos/{owner}/{repo}/contents/{encoded_path}{params}")
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


@router.post("/gitea/repos/{owner}/{repo}/file")
async def create_file_content(owner: str, repo: str, req: FileUpdateRequest):
    """Create a new file."""
    # For creation, SHA is not needed/should be ignored.
    # Gitea API uses POST for creation.
    try:
        body = {
            "message": req.message,
            "content": req.content,
            "branch": req.branch,
        }
        # If the Pydantic model requires SHA, we just ignore it here.
        # But we should probably make SHA optional in the model if we want to be clean,
        # but for now we can reuse the model and just not send SHA to Gitea.
        
        encoded_path = _encode_repo_path(req.path)
        data = await gitea_fetch(
            f"/repos/{owner}/{repo}/contents/{encoded_path}",
            method="POST",
            body=body,
        )
        return {"ok": True, "commit": data.get("commit", {})}
    except HTTPException:
        raise
    except Exception as e:
        logger.error("[Gitea] POST /file error: %s", e)
        raise HTTPException(status_code=500, detail=str(e))


@router.put("/gitea/repos/{owner}/{repo}/file")
async def update_file_content(owner: str, repo: str, req: FileUpdateRequest):
    """Update (commit) a file's content."""
    try:
        encoded_path = _encode_repo_path(req.path)
        data = await gitea_fetch(
            f"/repos/{owner}/{repo}/contents/{encoded_path}",
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


@router.delete("/gitea/repos/{owner}/{repo}/file")
async def delete_file_content(owner: str, repo: str, req: FileDeleteRequest):
    """Delete a file from the repository."""
    try:
        encoded_path = _encode_repo_path(req.path)
        data = await gitea_fetch(
            f"/repos/{owner}/{repo}/contents/{encoded_path}",
            method="DELETE",
            body={
                "message": req.message,
                "sha": req.sha,
                "branch": req.branch,
            },
        )
        return {"ok": True, "commit": data.get("commit", {})}
    except HTTPException:
        raise
    except Exception as e:
        logger.error("[Gitea] DELETE /file error: %s", e)
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
        encoded_path = _encode_repo_path(path)
        return await gitea_fetch(f"/repos/{owner}/{repo}/contents/{encoded_path}{params}")
    except HTTPException:
        raise
    except Exception as e:
        logger.error("[Gitea] GET /contents error: %s", e)
        raise HTTPException(status_code=500, detail=str(e))


@router.get("/gitea/repos/{owner}/{repo}/languages")
async def get_languages(owner: str, repo: str):
    """Get language breakdown for a repository."""
    try:
        data = await gitea_fetch(f"/repos/{owner}/{repo}/languages")
        return data  # dict like {"Python": 12345, "YAML": 678}
    except HTTPException:
        raise
    except Exception as e:
        logger.error("[Gitea] GET /languages error: %s", e)
        raise HTTPException(status_code=500, detail=str(e))


@router.get("/gitea/repos/{owner}/{repo}/topics")
async def get_topics(owner: str, repo: str):
    """Get repository topics."""
    try:
        data = await gitea_fetch(f"/repos/{owner}/{repo}/topics")
        return data  # {"topics": ["data-engineering", "airflow", ...]}
    except HTTPException:
        raise
    except Exception as e:
        logger.error("[Gitea] GET /topics error: %s", e)
        raise HTTPException(status_code=500, detail=str(e))


@router.get("/gitea/repos/{owner}/{repo}/contributors")
async def get_contributors(owner: str, repo: str):
    """Get repository contributors."""
    try:
        data = await gitea_fetch(f"/repos/{owner}/{repo}/contributors")
        return data  # list of contributor objects
    except HTTPException:
        raise
    except Exception as e:
        logger.error("[Gitea] GET /contributors error: %s", e)
        raise HTTPException(status_code=500, detail=str(e))


# ─── Issues / Releases ───


@router.get("/gitea/repos/{owner}/{repo}/issues")
async def list_issues(
    owner: str,
    repo: str,
    state: str = Query(default="open"),
    limit: int = Query(default=20),
    page: int = Query(default=1),
):
    """List repository issues."""
    try:
        data = await gitea_fetch(
            f"/repos/{owner}/{repo}/issues?state={state}&type=issues&limit={limit}&page={page}"
        )
        issues = data if isinstance(data, list) else []
        return {"issues": issues}
    except HTTPException:
        raise
    except Exception as e:
        logger.error("[Gitea] GET /issues error: %s", e)
        raise HTTPException(status_code=500, detail=str(e))


@router.get("/gitea/repos/{owner}/{repo}/releases")
async def list_releases(
    owner: str,
    repo: str,
    limit: int = Query(default=20),
    page: int = Query(default=1),
):
    """List repository releases."""
    try:
        data = await gitea_fetch(
            f"/repos/{owner}/{repo}/releases?limit={limit}&page={page}"
        )
        releases = data if isinstance(data, list) else []
        return {"releases": releases}
    except HTTPException:
        raise
    except Exception as e:
        logger.error("[Gitea] GET /releases error: %s", e)
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
        url = f"{GITEA_URL.rstrip('/')}/{owner}/{repo}/pulls/{pull_id}.diff"
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


@router.get("/gitea/repos/{owner}/{repo}/pulls/{pull_id}/activity")
async def get_pull_activity(owner: str, repo: str, pull_id: int):
    """Get PR comments and reviews for timeline-style rendering."""
    try:
        comments = await gitea_fetch(
            f"/repos/{owner}/{repo}/issues/{pull_id}/comments?limit=100&page=1"
        )
        reviews = await gitea_fetch(
            f"/repos/{owner}/{repo}/pulls/{pull_id}/reviews?limit=100&page=1"
        )
        inline_comments: list[dict] = []
        for review in reviews if isinstance(reviews, list) else []:
            review_id = review.get("id")
            if not review_id:
                continue
            try:
                review_comments = await gitea_fetch(
                    f"/repos/{owner}/{repo}/pulls/{pull_id}/reviews/{review_id}/comments"
                )
                if isinstance(review_comments, list):
                    for item in review_comments:
                        if isinstance(item, dict):
                            item["review_id"] = review_id
                            inline_comments.append(item)
            except Exception:
                # Inline comments are best-effort to avoid blocking the whole PR view.
                continue
        return {
            "comments": comments if isinstance(comments, list) else [],
            "reviews": reviews if isinstance(reviews, list) else [],
            "inline_comments": inline_comments,
        }
    except HTTPException:
        raise
    except Exception as e:
        logger.error("[Gitea] GET /pulls/%s/activity error: %s", pull_id, e)
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


@router.post("/gitea/repos/{owner}/{repo}/pulls/{pull_id}/update")
async def update_pull(owner: str, repo: str, pull_id: int, style: str = Query(default="rebase")):
    """Update PR head branch from base branch (merge/rebase)."""
    try:
        update_style = (style or "rebase").lower()
        if update_style not in {"merge", "rebase"}:
            raise HTTPException(status_code=400, detail="style must be 'merge' or 'rebase'")
        await gitea_fetch(
            f"/repos/{owner}/{repo}/pulls/{pull_id}/update?style={update_style}",
            method="POST",
        )
        return {"ok": True, "message": f"PR #{pull_id} updated with {update_style}"}
    except HTTPException:
        raise
    except Exception as e:
        logger.error("[Gitea] POST /pulls/%s/update error: %s", pull_id, e)
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


@router.post("/gitea/repos/{owner}/{repo}/pulls/{pull_id}/inline-comments")
async def create_inline_pull_comment(owner: str, repo: str, pull_id: int, req: PullInlineCommentRequest):
    """Create file/line inline comment on a PR via review API."""
    try:
        if not req.path.strip():
            raise HTTPException(status_code=400, detail="path is required")
        if req.new_position <= 0 and req.old_position <= 0:
            raise HTTPException(status_code=400, detail="new_position or old_position must be > 0")

        payload: dict = {
            "event": "COMMENT",
            "body": req.body or "",
            "comments": [
                {
                    "path": req.path,
                    "new_position": max(0, int(req.new_position)),
                    "old_position": max(0, int(req.old_position)),
                    "body": req.body or "",
                }
            ],
        }
        if req.commit_id:
            payload["commit_id"] = req.commit_id

        data = await gitea_fetch(
            f"/repos/{owner}/{repo}/pulls/{pull_id}/reviews",
            method="POST",
            body=payload,
        )
        return {"ok": True, "review": data}
    except HTTPException:
        raise
    except Exception as e:
        logger.error("[Gitea] POST /pulls/%s/inline-comments error: %s", pull_id, e)
        raise HTTPException(status_code=500, detail=str(e))


@router.post("/gitea/repos/{owner}/{repo}/pulls/{pull_id}/reviews")
async def submit_pull_review(owner: str, repo: str, pull_id: int, req: PullReviewRequest):
    """Submit pull request review (approve/request changes/comment)."""
    try:
        event = (req.event or "APPROVED").upper()
        if event not in {"APPROVED", "REQUEST_CHANGES", "COMMENT"}:
            raise HTTPException(status_code=400, detail=f"Unsupported review event: {event}")
        data = await gitea_fetch(
            f"/repos/{owner}/{repo}/pulls/{pull_id}/reviews",
            method="POST",
            body={"event": event, "body": req.body or ""},
        )
        return {"ok": True, "review": data}
    except HTTPException:
        raise
    except Exception as e:
        logger.error("[Gitea] POST /pulls/%s/reviews error: %s", pull_id, e)
        raise HTTPException(status_code=500, detail=str(e))


@router.delete("/gitea/repos/{owner}/{repo}/branches/{branch:path}")
async def delete_branch(owner: str, repo: str, branch: str):
    """Delete a branch."""
    return await _delete_branch_impl(owner, repo, branch)


@router.delete("/gitea/repos/{owner}/{repo}/branches")
async def delete_branch_query(owner: str, repo: str, branch: str = Query(..., min_length=1)):
    """Delete a branch (query format to support slashes safely)."""
    return await _delete_branch_impl(owner, repo, branch)


async def _delete_branch_impl(owner: str, repo: str, branch: str):
    try:
        # Tolerate both raw and pre-encoded branch names coming from clients.
        decoded_branch = unquote(branch)
        encoded_branch = quote(decoded_branch, safe="")
        await gitea_fetch(
            f"/repos/{owner}/{repo}/branches/{encoded_branch}",
            method="DELETE",
        )
        return {"ok": True, "message": f"Branch '{decoded_branch}' deleted"}
    except HTTPException:
        raise
    except Exception as e:
        logger.error("[Gitea] DELETE /branches/%s error: %s", branch, e)
        raise HTTPException(status_code=500, detail=str(e))


# ─── Actions ───


def _normalize_action_run_item(run: dict) -> dict:
    if not isinstance(run, dict):
        return run
    item = dict(run)
    path = str(item.get("path") or "")
    if not item.get("workflow_id") and "@" in path:
        item["workflow_id"] = path.split("@", 1)[0]
    if not item.get("head_branch"):
        if "@refs/heads/" in path:
            item["head_branch"] = path.split("@refs/heads/", 1)[1]
        elif "@refs/" in path:
            item["head_branch"] = path.split("@", 1)[1]
    return item


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
                f"/repos/{owner}/{repo}/actions/runs?limit={limit}&page={page}"
            )
        except HTTPException as he:
            if he.status_code == 404:
                data = await gitea_fetch(
                    f"/repos/{owner}/{repo}/actions/tasks?limit={limit}&page={page}"
                )
            else:
                raise
        if isinstance(data, dict) and isinstance(data.get("workflow_runs"), list):
            runs = [_normalize_action_run_item(r) for r in data.get("workflow_runs", [])]
            return {"workflow_runs": runs, "total_count": data.get("total_count", len(runs))}
        if isinstance(data, dict) and isinstance(data.get("data"), list):
            runs = [_normalize_action_run_item(r) for r in data.get("data", [])]
            return {"workflow_runs": runs, "total_count": data.get("total_count", len(runs))}
        if isinstance(data, list):
            runs = [_normalize_action_run_item(r) for r in data]
            return {"workflow_runs": runs, "total_count": len(runs)}
        return {"workflow_runs": [], "total_count": 0}
    except HTTPException:
        raise
    except Exception as e:
        logger.error("[Gitea] GET /actions/runs error: %s", e)
        raise HTTPException(status_code=500, detail=str(e))


@router.get("/gitea/repos/{owner}/{repo}/actions/runs/{run_id}/jobs")
async def get_action_jobs(owner: str, repo: str, run_id: int):
    """Get jobs for an Actions workflow run."""
    try:
        try:
            data = await gitea_fetch(
                f"/repos/{owner}/{repo}/actions/runs/{run_id}/jobs"
            )
        except HTTPException as he:
            if he.status_code == 404:
                data = await gitea_fetch(
                    f"/repos/{owner}/{repo}/actions/tasks/{run_id}"
                )
            else:
                raise
        if isinstance(data, dict):
            if isinstance(data.get("jobs"), list):
                return {"jobs": data.get("jobs", [])}
            if isinstance(data.get("tasks"), list):
                return {"jobs": data.get("tasks", [])}
            if isinstance(data.get("data"), list):
                return {"jobs": data.get("data", [])}
        if isinstance(data, list):
            return {"jobs": data}
        return {"jobs": []}
    except HTTPException:
        raise
    except Exception as e:
        logger.error("[Gitea] GET /actions/jobs error: %s", e)
        raise HTTPException(status_code=500, detail=str(e))


@router.post("/gitea/repos/{owner}/{repo}/actions/runs/{run_id}/cancel")
async def cancel_action_run(owner: str, repo: str, run_id: int):
    """Cancel (or remove) a workflow run."""
    try:
        # Gitea 1.25 may not expose an explicit cancel endpoint; deleting the run
        # is the documented API action and effectively stops it from the UI flow.
        await gitea_fetch(
            f"/repos/{owner}/{repo}/actions/runs/{run_id}",
            method="DELETE",
        )
        return {"ok": True, "message": f"Run #{run_id} cancelled/deleted"}
    except HTTPException:
        raise
    except Exception as e:
        logger.error("[Gitea] POST /actions/runs/%s/cancel error: %s", run_id, e)
        raise HTTPException(status_code=500, detail=str(e))


@router.post("/gitea/repos/{owner}/{repo}/actions/runs/{run_id}/rerun")
async def rerun_action_run(owner: str, repo: str, run_id: int):
    """Re-run workflow run by dispatching its workflow on the same ref."""
    try:
        run = _normalize_action_run_item(await gitea_fetch(f"/repos/{owner}/{repo}/actions/runs/{run_id}"))
        workflow_id = run.get("workflow_id") or run.get("workflow", {}).get("id")
        ref = run.get("head_branch") or run.get("head_sha") or run.get("ref")
        path = str(run.get("path") or "")
        if not ref and "@" in path:
            ref = path.split("@", 1)[1]
        if isinstance(ref, str) and ref.startswith("refs/heads/"):
            ref = ref.split("refs/heads/", 1)[1]
        if not workflow_id:
            raise HTTPException(status_code=400, detail="workflow_id not found for run")
        if not ref:
            raise HTTPException(status_code=400, detail="ref not found for run")

        try:
            await gitea_fetch(
                f"/repos/{owner}/{repo}/actions/workflows/{workflow_id}/dispatches",
                method="POST",
                body={"ref": ref},
            )
        except HTTPException as he:
            fallback_ref = str(run.get("repository", {}).get("default_branch") or "main")
            if he.status_code == 404 and isinstance(ref, str) and ref.startswith("refs/pull/") and fallback_ref:
                await gitea_fetch(
                    f"/repos/{owner}/{repo}/actions/workflows/{workflow_id}/dispatches",
                    method="POST",
                    body={"ref": fallback_ref},
                )
                return {
                    "ok": True,
                    "message": f"Re-run dispatched for run #{run_id} using fallback ref '{fallback_ref}'",
                    "workflow_id": workflow_id,
                    "ref": fallback_ref,
                    "fallback": True,
                }
            raise
        return {"ok": True, "message": f"Re-run dispatched for run #{run_id}", "workflow_id": workflow_id, "ref": ref}
    except HTTPException:
        raise
    except Exception as e:
        logger.error("[Gitea] POST /actions/runs/%s/rerun error: %s", run_id, e)
        raise HTTPException(status_code=500, detail=str(e))
