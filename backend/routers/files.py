"""
Files Router — Project directory tree and file content reader.
Ported from web-ui/src/app/api/files/route.ts
"""
import logging
import os
from pathlib import Path
from fastapi import APIRouter, HTTPException, Query
from typing import Optional

logger = logging.getLogger(__name__)

router = APIRouter()

# ─── Project Root ───
PROJECT_ROOT = Path(__file__).resolve().parent.parent.parent  # backend/../.. = project root

# ─── Directories & files to exclude ───
EXCLUDE_DIRS = {
    ".git", "node_modules", "__pycache__", ".next", ".vscode",
    "logs", ".DS_Store", "extra-jars", "jars", "plugins",
    ".system_generated", ".gemini", "log.txt", ".env",
}

EXCLUDE_EXTENSIONS = {".pyc", ".pyo", ".class", ".o", ".so", ".dylib"}

# ─── Language Mapping ───
LANG_MAP = {
    "py": "python", "ts": "typescript", "tsx": "typescript",
    "js": "javascript", "jsx": "javascript", "sql": "sql",
    "md": "markdown", "json": "json", "yml": "yaml", "yaml": "yaml",
    "sh": "shell", "bash": "shell", "html": "html", "css": "css",
    "xml": "xml", "toml": "toml", "csv": "plaintext", "txt": "plaintext",
    "cfg": "ini", "properties": "ini", "conf": "ini",
    "dockerfile": "dockerfile",
}


def read_directory(dir_path: Path, relative_path: str, depth: int, max_depth: int) -> list:
    """Recursively read directory contents."""
    if depth > max_depth:
        return []

    try:
        entries = sorted(
            dir_path.iterdir(),
            key=lambda e: (not e.is_dir(), e.name.lower()),
        )
    except PermissionError:
        return []

    nodes = []
    for entry in entries:
        if entry.name in EXCLUDE_DIRS:
            continue
        if entry.name.startswith(".") and entry.name not in (".env.example", ".gitignore"):
            continue

        ext = entry.suffix.lower()
        if ext in EXCLUDE_EXTENSIONS:
            continue

        rel_path = os.path.join(relative_path, entry.name)

        if entry.is_dir():
            children = read_directory(entry, rel_path, depth + 1, max_depth)
            nodes.append({
                "name": entry.name,
                "path": rel_path,
                "type": "directory",
                "children": children,
            })
        elif entry.is_file():
            node = {
                "name": entry.name,
                "path": rel_path,
                "type": "file",
                "extension": ext.lstrip(".") or None,
            }
            try:
                node["size"] = entry.stat().st_size
            except OSError:
                pass
            nodes.append(node)

    return nodes


@router.get("/files")
async def files_handler(
    action: str = Query(default="tree"),
    path: str = Query(default=""),
    maxDepth: int = Query(default=3),
    filePath: Optional[str] = Query(default=None),
):
    """Read project file tree or file contents."""
    try:
        # ── Read File Content ──
        if action == "readFile":
            if not filePath:
                raise HTTPException(status_code=400, detail="Missing filePath parameter")

            # Normalize: strip leading './' if present
            clean_path = filePath.lstrip("./")
            target_path = PROJECT_ROOT / clean_path

            # Security: prevent traversal above project root
            try:
                target_path.resolve().relative_to(PROJECT_ROOT.resolve())
            except ValueError:
                raise HTTPException(status_code=400, detail="Invalid path")

            if not target_path.exists() or not target_path.is_file():
                raise HTTPException(status_code=404, detail="File not found")

            stat = target_path.stat()
            ext = target_path.suffix.lower().lstrip(".")
            file_name = target_path.name

            # Only preview text files under 2MB
            MAX_SIZE = 2 * 1024 * 1024
            if stat.st_size > MAX_SIZE:
                raise HTTPException(status_code=413, detail="File too large for preview")

            content = target_path.read_text(encoding="utf-8", errors="replace")
            language = "dockerfile" if file_name.lower() == "dockerfile" else LANG_MAP.get(ext, "plaintext")

            return {
                "name": file_name,
                "path": clean_path,
                "content": content,
                "language": language,
                "extension": ext,
                "size": stat.st_size,
                "lastModified": stat.st_mtime,
                "lineCount": content.count("\n") + 1,
            }

        # ── List Directory Tree ──
        target_path = PROJECT_ROOT / path
        try:
            target_path.resolve().relative_to(PROJECT_ROOT.resolve())
        except ValueError:
            raise HTTPException(status_code=400, detail="Invalid path")

        tree = read_directory(target_path, path or ".", 0, maxDepth)

        return {
            "root": PROJECT_ROOT.name,
            "path": path or ".",
            "tree": tree,
        }

    except HTTPException:
        raise
    except Exception as e:
        logger.error("[Files API Error] %s", e)
        raise HTTPException(status_code=500, detail=str(e))
