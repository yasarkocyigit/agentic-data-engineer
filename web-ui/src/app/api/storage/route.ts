import { NextResponse } from 'next/server';
import {
    S3Client,
    ListBucketsCommand,
    ListObjectsV2Command,
    GetObjectCommand,
    HeadBucketCommand,
} from '@aws-sdk/client-s3';

// ─── S3 Client (MinIO) ───
const s3 = new S3Client({
    endpoint: 'http://localhost:9000',
    region: 'us-east-1',
    forcePathStyle: true,
    credentials: {
        accessKeyId: process.env.MINIO_ROOT_USER || 'admin',
        secretAccessKey: process.env.MINIO_ROOT_PASSWORD || 'admin123',
    },
});

// ─── Helper: Format Bytes ───
function formatBytes(bytes: number): string {
    if (bytes === 0) return '0 B';
    const k = 1024;
    const sizes = ['B', 'KB', 'MB', 'GB', 'TB'];
    const i = Math.floor(Math.log(bytes) / Math.log(k));
    return parseFloat((bytes / Math.pow(k, i)).toFixed(2)) + ' ' + sizes[i];
}

// ─── GET Handler ───
export async function GET(request: Request) {
    const { searchParams } = new URL(request.url);
    const action = searchParams.get('action');

    try {
        // ── List Buckets ──
        if (action === 'listBuckets') {
            const result = await s3.send(new ListBucketsCommand({}));
            const buckets = result.Buckets || [];

            // Enrich each bucket with object count & size
            const enriched = await Promise.all(
                buckets.map(async (bucket) => {
                    try {
                        const objects = await s3.send(
                            new ListObjectsV2Command({
                                Bucket: bucket.Name!,
                                MaxKeys: 1000,
                            })
                        );
                        const totalSize = (objects.Contents || []).reduce(
                            (sum, obj) => sum + (obj.Size || 0),
                            0
                        );
                        return {
                            name: bucket.Name,
                            creationDate: bucket.CreationDate?.toISOString(),
                            objectCount: objects.KeyCount || 0,
                            totalSize,
                            totalSizeFormatted: formatBytes(totalSize),
                        };
                    } catch {
                        return {
                            name: bucket.Name,
                            creationDate: bucket.CreationDate?.toISOString(),
                            objectCount: 0,
                            totalSize: 0,
                            totalSizeFormatted: '0 B',
                        };
                    }
                })
            );

            return NextResponse.json({ buckets: enriched });
        }

        // ── List Objects ──
        if (action === 'listObjects') {
            const bucket = searchParams.get('bucket');
            const prefix = searchParams.get('prefix') || '';

            if (!bucket) {
                return NextResponse.json(
                    { error: 'Missing bucket parameter' },
                    { status: 400 }
                );
            }

            const result = await s3.send(
                new ListObjectsV2Command({
                    Bucket: bucket,
                    Prefix: prefix,
                    Delimiter: '/',
                    MaxKeys: 1000,
                })
            );

            // Folders (common prefixes)
            const folders = (result.CommonPrefixes || []).map((p) => ({
                type: 'folder' as const,
                name: p.Prefix!.replace(prefix, '').replace(/\/$/, ''),
                key: p.Prefix!,
                size: 0,
                sizeFormatted: '--',
                lastModified: null,
            }));

            // Files
            const files = (result.Contents || [])
                .filter((obj) => obj.Key !== prefix) // exclude the prefix itself
                .map((obj) => {
                    const name = obj.Key!.replace(prefix, '');
                    const ext = name.split('.').pop()?.toLowerCase() || '';
                    return {
                        type: 'file' as const,
                        name,
                        key: obj.Key!,
                        size: obj.Size || 0,
                        sizeFormatted: formatBytes(obj.Size || 0),
                        lastModified: obj.LastModified?.toISOString() || null,
                        etag: obj.ETag?.replace(/"/g, '') || '',
                        extension: ext,
                    };
                });

            return NextResponse.json({
                bucket,
                prefix,
                folders,
                files,
                totalObjects: result.KeyCount || 0,
                isTruncated: result.IsTruncated || false,
            });
        }

        // ── Get Object Preview ──
        if (action === 'getObject') {
            const bucket = searchParams.get('bucket');
            const key = searchParams.get('key');

            if (!bucket || !key) {
                return NextResponse.json(
                    { error: 'Missing bucket or key parameter' },
                    { status: 400 }
                );
            }

            const result = await s3.send(
                new GetObjectCommand({ Bucket: bucket, Key: key })
            );

            const contentType = result.ContentType || 'application/octet-stream';
            const size = result.ContentLength || 0;

            // Only preview text-like files under 1MB
            let preview: string | null = null;
            const textTypes = [
                'text/',
                'application/json',
                'application/xml',
                'application/csv',
                'application/x-yaml',
                'application/yaml',
            ];
            const isText =
                textTypes.some((t) => contentType.startsWith(t)) ||
                key.endsWith('.csv') ||
                key.endsWith('.json') ||
                key.endsWith('.yaml') ||
                key.endsWith('.yml') ||
                key.endsWith('.txt') ||
                key.endsWith('.py') ||
                key.endsWith('.sql') ||
                key.endsWith('.md');

            if (isText && size < 1024 * 1024) {
                const body = await result.Body?.transformToString();
                preview = body || null;
            }

            return NextResponse.json({
                bucket,
                key,
                contentType,
                size,
                sizeFormatted: formatBytes(size),
                lastModified: result.LastModified?.toISOString(),
                etag: result.ETag?.replace(/"/g, ''),
                preview,
                isPreviewable: isText && size < 1024 * 1024,
            });
        }

        return NextResponse.json(
            { error: 'Unknown action. Use: listBuckets, listObjects, getObject' },
            { status: 400 }
        );
    } catch (error: any) {
        console.error('[Storage API Error]', error);
        return NextResponse.json(
            { error: error.message || 'Storage API error' },
            { status: 500 }
        );
    }
}
