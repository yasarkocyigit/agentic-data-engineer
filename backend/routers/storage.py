"""
Storage Router — MinIO/S3 bucket and object management.
Ported from web-ui/src/app/api/storage/route.ts
"""
import logging
import math
from fastapi import APIRouter, HTTPException, Query
from typing import Optional
import boto3
from botocore.config import Config as BotoConfig

logger = logging.getLogger(__name__)

router = APIRouter()

# ─── S3 Client (MinIO) ───
s3 = boto3.client(
    "s3",
    endpoint_url="http://localhost:9000",
    aws_access_key_id="admin",
    aws_secret_access_key="admin123",
    region_name="us-east-1",
    config=BotoConfig(signature_version="s3v4", s3={"addressing_style": "path"}),
)


def format_bytes(size: int) -> str:
    """Format bytes into human-readable string."""
    if size == 0:
        return "0 B"
    names = ["B", "KB", "MB", "GB", "TB"]
    i = int(math.floor(math.log(size, 1024)))
    p = math.pow(1024, i)
    return f"{round(size / p, 2)} {names[i]}"


@router.get("/storage")
async def storage_handler(
    action: Optional[str] = Query(None),
    bucket: Optional[str] = Query(None),
    prefix: str = Query(default=""),
    key: Optional[str] = Query(None),
):
    """Manage MinIO/S3 storage: list buckets, list objects, get object preview."""
    try:
        # ── List Buckets ──
        if action == "listBuckets":
            result = s3.list_buckets()
            buckets = result.get("Buckets", [])

            enriched = []
            for b in buckets:
                try:
                    objects = s3.list_objects_v2(Bucket=b["Name"], MaxKeys=1000)
                    contents = objects.get("Contents", [])
                    total_size = sum(obj.get("Size", 0) for obj in contents)
                    enriched.append(
                        {
                            "name": b["Name"],
                            "creationDate": b["CreationDate"].isoformat() if b.get("CreationDate") else None,
                            "objectCount": objects.get("KeyCount", 0),
                            "totalSize": total_size,
                            "totalSizeFormatted": format_bytes(total_size),
                        }
                    )
                except Exception:
                    enriched.append(
                        {
                            "name": b["Name"],
                            "creationDate": b["CreationDate"].isoformat() if b.get("CreationDate") else None,
                            "objectCount": 0,
                            "totalSize": 0,
                            "totalSizeFormatted": "0 B",
                        }
                    )

            return {"buckets": enriched}

        # ── List Objects ──
        elif action == "listObjects":
            if not bucket:
                raise HTTPException(status_code=400, detail="Missing bucket parameter")

            result = s3.list_objects_v2(
                Bucket=bucket, Prefix=prefix, Delimiter="/", MaxKeys=1000
            )

            # Folders (common prefixes)
            folders = [
                {
                    "type": "folder",
                    "name": p["Prefix"].replace(prefix, "").rstrip("/"),
                    "key": p["Prefix"],
                    "size": 0,
                    "sizeFormatted": "--",
                    "lastModified": None,
                }
                for p in result.get("CommonPrefixes", [])
            ]

            # Files
            files = []
            for obj in result.get("Contents", []):
                if obj["Key"] == prefix:
                    continue  # exclude the prefix itself
                name = obj["Key"].replace(prefix, "")
                ext = name.rsplit(".", 1)[-1].lower() if "." in name else ""
                files.append(
                    {
                        "type": "file",
                        "name": name,
                        "key": obj["Key"],
                        "size": obj.get("Size", 0),
                        "sizeFormatted": format_bytes(obj.get("Size", 0)),
                        "lastModified": obj["LastModified"].isoformat() if obj.get("LastModified") else None,
                        "etag": obj.get("ETag", "").strip('"'),
                        "extension": ext,
                    }
                )

            return {
                "bucket": bucket,
                "prefix": prefix,
                "folders": folders,
                "files": files,
                "totalObjects": result.get("KeyCount", 0),
                "isTruncated": result.get("IsTruncated", False),
            }

        # ── Get Object Preview ──
        elif action == "getObject":
            if not bucket or not key:
                raise HTTPException(status_code=400, detail="Missing bucket or key parameter")

            result = s3.get_object(Bucket=bucket, Key=key)
            content_type = result.get("ContentType", "application/octet-stream")
            size = result.get("ContentLength", 0)

            # Only preview text-like files under 1MB
            text_types = ["text/", "application/json", "application/xml", "application/csv", "application/x-yaml", "application/yaml"]
            text_extensions = [".csv", ".json", ".yaml", ".yml", ".txt", ".py", ".sql", ".md"]

            is_text = any(content_type.startswith(t) for t in text_types) or any(
                key.endswith(ext) for ext in text_extensions
            )

            preview = None
            if is_text and size < 1024 * 1024:
                preview = result["Body"].read().decode("utf-8", errors="replace")
            else:
                # Still need to consume the body
                result["Body"].close()

            return {
                "bucket": bucket,
                "key": key,
                "contentType": content_type,
                "size": size,
                "sizeFormatted": format_bytes(size),
                "lastModified": result["LastModified"].isoformat() if result.get("LastModified") else None,
                "etag": result.get("ETag", "").strip('"'),
                "preview": preview,
                "isPreviewable": is_text and size < 1024 * 1024,
            }

        else:
            raise HTTPException(
                status_code=400,
                detail="Unknown action. Use: listBuckets, listObjects, getObject",
            )

    except HTTPException:
        raise
    except Exception as e:
        logger.error("[Storage API Error] %s", e)
        raise HTTPException(status_code=500, detail=str(e))
