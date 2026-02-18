
import React, { useState, useEffect, useCallback } from 'react';
import { Sidebar } from '@/components/Sidebar';
import clsx from 'clsx';
import {
    HardDrive, Folder, FileText, ChevronRight, RefreshCw, Download,
    Database, Archive, Layers, Box, Clock, Hash, X, Maximize2, Minimize2,
    FileCode, File, Image as ImageIcon, Table, AlertCircle, Loader2
} from 'lucide-react';

// ─── Types ───
type BucketInfo = {
    name: string;
    creationDate: string;
    objectCount: number;
    totalSize: number;
    totalSizeFormatted: string;
};

type ObjectItem = {
    type: 'folder' | 'file';
    name: string;
    key: string;
    size: number;
    sizeFormatted: string;
    lastModified: string | null;
    etag?: string;
    extension?: string;
};

type ObjectPreview = {
    bucket: string;
    key: string;
    contentType: string;
    size: number;
    sizeFormatted: string;
    lastModified: string;
    etag: string;
    preview: string | null;
    isPreviewable: boolean;
};

// ─── Bucket Colors ───
const BUCKET_STYLES: Record<string, { color: string; bg: string; icon: React.ElementType }> = {
    bronze: { color: '#cd7f32', bg: '#cd7f32/10', icon: Database },
    silver: { color: '#8c8e9e', bg: '#8c8e9e/10', icon: Layers },
    gold: { color: '#e5c07b', bg: '#e5c07b/10', icon: Layers },
    warehouse: { color: '#3574f0', bg: '#3574f0/10', icon: Archive },
    checkpoints: { color: '#9b59b6', bg: '#9b59b6/10', icon: Box },
};

// ─── File Icon Helper ───
function getFileIcon(ext: string) {
    if (['py', 'sql', 'ts', 'js', 'sh'].includes(ext)) return FileCode;
    if (['parquet', 'csv', 'tsv'].includes(ext)) return Table;
    if (['json', 'yaml', 'yml', 'xml', 'txt', 'md'].includes(ext)) return FileText;
    if (['png', 'jpg', 'jpeg', 'gif', 'svg'].includes(ext)) return ImageIcon;
    return File;
}

// ─── Time Ago Helper ───
function timeAgo(dateStr: string | null): string {
    if (!dateStr) return '--';
    const d = new Date(dateStr);
    const now = new Date();
    const diffMs = now.getTime() - d.getTime();
    const diffMin = Math.floor(diffMs / 60000);
    if (diffMin < 60) return `${diffMin}m ago`;
    const diffHr = Math.floor(diffMin / 60);
    if (diffHr < 24) return `${diffHr}h ago`;
    const diffDays = Math.floor(diffHr / 24);
    return `${diffDays}d ago`;
}

// ─── Main Component ───
export default function StoragePage() {
    const [buckets, setBuckets] = useState<BucketInfo[]>([]);
    const [bucketsLoading, setBucketsLoading] = useState(true);
    const [bucketsError, setBucketsError] = useState<string | null>(null);

    const [selectedBucket, setSelectedBucket] = useState<string | null>(null);
    const [currentPrefix, setCurrentPrefix] = useState('');
    const [objects, setObjects] = useState<ObjectItem[]>([]);
    const [objectsLoading, setObjectsLoading] = useState(false);

    const [selectedFile, setSelectedFile] = useState<ObjectItem | null>(null);
    const [filePreview, setFilePreview] = useState<ObjectPreview | null>(null);
    const [previewLoading, setPreviewLoading] = useState(false);

    const [fullscreenPreview, setFullscreenPreview] = useState(false);

    // ─── Fetch Buckets ───
    const fetchBuckets = useCallback(async () => {
        setBucketsLoading(true);
        setBucketsError(null);
        try {
            const res = await fetch('/api/storage?action=listBuckets');
            const data = await res.json();
            if (!res.ok) throw new Error(data.error);
            setBuckets(data.buckets || []);
        } catch (err: any) {
            setBucketsError(err.message);
        } finally {
            setBucketsLoading(false);
        }
    }, []);

    useEffect(() => { fetchBuckets(); }, [fetchBuckets]);

    // ─── Fetch Objects ───
    const fetchObjects = useCallback(async (bucket: string, prefix: string) => {
        setObjectsLoading(true);
        try {
            const res = await fetch(`/api/storage?action=listObjects&bucket=${encodeURIComponent(bucket)}&prefix=${encodeURIComponent(prefix)}`);
            const data = await res.json();
            if (!res.ok) throw new Error(data.error);
            setObjects([...(data.folders || []), ...(data.files || [])]);
        } catch (err: any) {
            setObjects([]);
        } finally {
            setObjectsLoading(false);
        }
    }, []);

    // ─── Navigate Bucket ───
    const openBucket = (name: string) => {
        setSelectedBucket(name);
        setCurrentPrefix('');
        setSelectedFile(null);
        setFilePreview(null);
        fetchObjects(name, '');
    };

    const openFolder = (key: string) => {
        if (!selectedBucket) return;
        setCurrentPrefix(key);
        setSelectedFile(null);
        setFilePreview(null);
        fetchObjects(selectedBucket, key);
    };

    const goBackToBuckets = () => {
        setSelectedBucket(null);
        setCurrentPrefix('');
        setObjects([]);
        setSelectedFile(null);
        setFilePreview(null);
    };

    // ─── Breadcrumb Parts ───
    const breadcrumbs = currentPrefix
        ? currentPrefix.split('/').filter(Boolean).map((part, i, arr) => ({
            label: part,
            prefix: arr.slice(0, i + 1).join('/') + '/',
        }))
        : [];

    // ─── File Preview ───
    const openFilePreview = async (item: ObjectItem) => {
        if (!selectedBucket) return;
        setSelectedFile(item);
        setPreviewLoading(true);
        try {
            const res = await fetch(`/api/storage?action=getObject&bucket=${encodeURIComponent(selectedBucket)}&key=${encodeURIComponent(item.key)}`);
            const data = await res.json();
            if (!res.ok) throw new Error(data.error);
            setFilePreview(data);
        } catch {
            setFilePreview(null);
        } finally {
            setPreviewLoading(false);
        }
    };

    // ─── Stats ───
    const totalObjects = buckets.reduce((s, b) => s + b.objectCount, 0);
    const totalSize = buckets.reduce((s, b) => s + b.totalSize, 0);
    const formatBytes = (bytes: number) => {
        if (bytes === 0) return '0 B';
        const k = 1024;
        const sizes = ['B', 'KB', 'MB', 'GB', 'TB'];
        const i = Math.floor(Math.log(bytes) / Math.log(k));
        return parseFloat((bytes / Math.pow(k, i)).toFixed(2)) + ' ' + sizes[i];
    };

    return (
        <div className="flex h-screen bg-[#1e1f22] text-[#bcbec4] font-sans overflow-hidden">
            <Sidebar />

            <main className="flex-1 flex flex-col min-w-0 overflow-hidden">

                {/* ─── Top Bar ─── */}
                <header className="h-9 bg-[#2b2d30] border-b border-[#393b40] flex items-center justify-between px-4 shrink-0">
                    <div className="flex items-center gap-2">
                        <HardDrive className="w-3.5 h-3.5 text-[#3574f0]" />
                        <span className="text-[12px] font-bold text-[#bcbec4]">Storage Layer</span>
                        <span className="text-[10px] text-[#6c707e]">MinIO S3</span>
                    </div>
                    <div className="flex items-center gap-3 text-[10px] text-[#6c707e]">
                        <span>{buckets.length} buckets</span>
                        <span>•</span>
                        <span>{totalObjects} objects</span>
                        <span>•</span>
                        <span>{formatBytes(totalSize)}</span>
                        <button
                            onClick={() => { fetchBuckets(); if (selectedBucket) fetchObjects(selectedBucket, currentPrefix); }}
                            className="p-1 hover:bg-[#3c3f41] rounded text-[#6c707e] hover:text-[#bcbec4] ml-2"
                        >
                            <RefreshCw className="w-3.5 h-3.5" />
                        </button>
                    </div>
                </header>

                {/* ─── Main Content ─── */}
                <div className="flex-1 flex overflow-hidden">

                    {/* ─── Bucket/Object Browser ─── */}
                    <div className={clsx(
                        "flex-1 flex flex-col overflow-hidden",
                        selectedFile && !fullscreenPreview && "border-r border-[#393b40]"
                    )}>

                        {/* Breadcrumb */}
                        {selectedBucket && (
                            <div className="h-8 bg-[#2b2d30] border-b border-[#393b40] flex items-center px-3 gap-1 text-[11px] shrink-0">
                                <button
                                    onClick={goBackToBuckets}
                                    className="text-[#3574f0] hover:text-[#5a9cf5] hover:underline"
                                >
                                    Buckets
                                </button>
                                <ChevronRight className="w-3 h-3 text-[#6c707e]" />
                                <button
                                    onClick={() => openFolder('')}
                                    className={clsx(
                                        breadcrumbs.length === 0 ? "text-[#bcbec4]" : "text-[#3574f0] hover:text-[#5a9cf5] hover:underline"
                                    )}
                                >
                                    {selectedBucket}
                                </button>
                                {breadcrumbs.map((bc, i) => (
                                    <React.Fragment key={bc.prefix}>
                                        <ChevronRight className="w-3 h-3 text-[#6c707e]" />
                                        <button
                                            onClick={() => openFolder(bc.prefix)}
                                            className={clsx(
                                                i === breadcrumbs.length - 1 ? "text-[#bcbec4]" : "text-[#3574f0] hover:text-[#5a9cf5] hover:underline"
                                            )}
                                        >
                                            {bc.label}
                                        </button>
                                    </React.Fragment>
                                ))}
                            </div>
                        )}

                        {/* Content Area */}
                        <div className="flex-1 overflow-auto bg-[#1e1f22]">

                            {/* ─── Bucket Cards ─── */}
                            {!selectedBucket && (
                                <div className="p-4">
                                    {bucketsLoading ? (
                                        <div className="flex items-center justify-center py-20 text-[#6c707e]">
                                            <Loader2 className="w-5 h-5 animate-spin mr-2" />
                                            <span className="text-[12px]">Connecting to MinIO...</span>
                                        </div>
                                    ) : bucketsError ? (
                                        <div className="flex items-center justify-center py-20 text-[#ff5261]">
                                            <AlertCircle className="w-5 h-5 mr-2" />
                                            <span className="text-[12px]">{bucketsError}</span>
                                        </div>
                                    ) : (
                                        <>
                                            <div className="text-[11px] text-[#6c707e] font-bold uppercase tracking-wider mb-3 px-1">
                                                Lakehouse Buckets
                                            </div>
                                            <div className="grid grid-cols-1 md:grid-cols-2 lg:grid-cols-3 xl:grid-cols-5 gap-3">
                                                {buckets.map((bucket) => {
                                                    const style = BUCKET_STYLES[bucket.name] || { color: '#6c707e', bg: '#6c707e/10', icon: Archive };
                                                    const Icon = style.icon;
                                                    return (
                                                        <div
                                                            key={bucket.name}
                                                            onClick={() => openBucket(bucket.name)}
                                                            className="group bg-[#2b2d30] border border-[#393b40] rounded-lg p-4 cursor-pointer hover:border-[#3574f0]/40 hover:bg-[#2b2d30]/80 transition-all"
                                                        >
                                                            <div className="flex items-start justify-between mb-3">
                                                                <div
                                                                    className="w-9 h-9 rounded-lg flex items-center justify-center"
                                                                    style={{ backgroundColor: `${style.color}15` }}
                                                                >
                                                                    <Icon className="w-4.5 h-4.5" style={{ color: style.color }} />
                                                                </div>
                                                                <ChevronRight className="w-3.5 h-3.5 text-[#6c707e] opacity-0 group-hover:opacity-100 transition-opacity mt-1" />
                                                            </div>
                                                            <div className="text-[13px] font-semibold text-[#ced0d6] mb-1">
                                                                {bucket.name}
                                                            </div>
                                                            <div className="flex items-center gap-3 text-[10px] text-[#6c707e]">
                                                                <span>{bucket.objectCount} objects</span>
                                                                <span>•</span>
                                                                <span>{bucket.totalSizeFormatted}</span>
                                                            </div>
                                                        </div>
                                                    );
                                                })}
                                            </div>
                                        </>
                                    )}
                                </div>
                            )}

                            {/* ─── Object Table ─── */}
                            {selectedBucket && (
                                <>
                                    {objectsLoading ? (
                                        <div className="flex items-center justify-center py-20 text-[#6c707e]">
                                            <Loader2 className="w-5 h-5 animate-spin mr-2" />
                                            <span className="text-[12px]">Loading objects...</span>
                                        </div>
                                    ) : objects.length === 0 ? (
                                        <div className="flex flex-col items-center justify-center py-20 text-[#6c707e]">
                                            <Archive className="w-8 h-8 mb-3 opacity-30" />
                                            <span className="text-[12px]">This directory is empty</span>
                                        </div>
                                    ) : (
                                        <table className="w-full text-left border-collapse">
                                            <thead className="sticky top-0 bg-[#3c3f41] shadow-sm z-10">
                                                <tr>
                                                    <th className="p-1 px-3 border-r border-[#393b40] border-b text-[11px] text-[#bcbec4] font-normal w-[50%]">Name</th>
                                                    <th className="p-1 px-3 border-r border-[#393b40] border-b text-[11px] text-[#bcbec4] font-normal w-[15%]">Size</th>
                                                    <th className="p-1 px-3 border-r border-[#393b40] border-b text-[11px] text-[#bcbec4] font-normal w-[15%]">Modified</th>
                                                    <th className="p-1 px-3 border-b text-[11px] text-[#bcbec4] font-normal w-[20%]">Type</th>
                                                </tr>
                                            </thead>
                                            <tbody className="text-[12px] font-mono">
                                                {objects.map((item) => {
                                                    const isFolder = item.type === 'folder';
                                                    const Icon = isFolder ? Folder : getFileIcon(item.extension || '');
                                                    const isSelected = selectedFile?.key === item.key;
                                                    return (
                                                        <tr
                                                            key={item.key}
                                                            onClick={() => isFolder ? openFolder(item.key) : openFilePreview(item)}
                                                            className={clsx(
                                                                "cursor-pointer border-b border-[#393b40]",
                                                                isSelected ? "bg-[#214283]" : "hover:bg-[#2b2d30]"
                                                            )}
                                                        >
                                                            <td className="p-1.5 px-3 border-r border-[#393b40]">
                                                                <div className="flex items-center gap-2">
                                                                    <Icon className={clsx(
                                                                        "w-4 h-4 shrink-0",
                                                                        isFolder ? "text-[#e5c07b]" : "text-[#6c707e]"
                                                                    )} />
                                                                    <span className={clsx(
                                                                        "truncate",
                                                                        isFolder ? "text-[#ced0d6]" : "text-[#a9b7c6]"
                                                                    )}>
                                                                        {item.name}
                                                                    </span>
                                                                </div>
                                                            </td>
                                                            <td className="p-1.5 px-3 border-r border-[#393b40] text-[#6c707e]">
                                                                {item.sizeFormatted}
                                                            </td>
                                                            <td className="p-1.5 px-3 border-r border-[#393b40] text-[#6c707e]">
                                                                {timeAgo(item.lastModified)}
                                                            </td>
                                                            <td className="p-1.5 px-3 text-[#6c707e]">
                                                                {isFolder ? (
                                                                    <span className="text-[10px] px-1.5 py-0.5 rounded bg-[#e5c07b]/10 text-[#e5c07b] font-semibold uppercase">DIR</span>
                                                                ) : (
                                                                    <span className="text-[10px] px-1.5 py-0.5 rounded bg-[#3574f0]/10 text-[#3574f0] font-semibold uppercase">
                                                                        {item.extension || 'file'}
                                                                    </span>
                                                                )}
                                                            </td>
                                                        </tr>
                                                    );
                                                })}
                                            </tbody>
                                        </table>
                                    )}
                                </>
                            )}
                        </div>
                    </div>

                    {/* ─── File Preview Sidebar ─── */}
                    {selectedFile && (
                        <div className={clsx(
                            "flex flex-col bg-[#1e1f22] shrink-0 transition-all duration-200",
                            fullscreenPreview
                                ? "fixed inset-0 z-50 w-full"
                                : "w-[380px]"
                        )}>
                            {/* Preview Header */}
                            <div className={clsx(
                                "bg-[#2b2d30] border-b border-[#393b40] flex items-center justify-between shrink-0",
                                fullscreenPreview ? "h-12 px-6" : "h-10 px-4"
                            )}>
                                <div className="flex items-center gap-2 min-w-0">
                                    <FileText className={clsx("text-[#3574f0] shrink-0", fullscreenPreview ? "w-4 h-4" : "w-3.5 h-3.5")} />
                                    <span className={clsx("font-bold text-[#ced0d6] truncate", fullscreenPreview ? "text-sm" : "text-[11px]")}>
                                        {selectedFile.name}
                                    </span>
                                </div>
                                <div className="flex items-center gap-1">
                                    <button
                                        onClick={() => setFullscreenPreview(!fullscreenPreview)}
                                        className="p-1 hover:bg-[#3c3f41] rounded text-[#6c707e] hover:text-[#bcbec4] transition-colors"
                                        title={fullscreenPreview ? 'Minimize' : 'Maximize'}
                                    >
                                        {fullscreenPreview
                                            ? <Minimize2 className="w-3.5 h-3.5" />
                                            : <Maximize2 className="w-3.5 h-3.5" />
                                        }
                                    </button>
                                    <button
                                        onClick={() => { setSelectedFile(null); setFilePreview(null); setFullscreenPreview(false); }}
                                        className="p-1 hover:bg-[#3c3f41] rounded text-[#6c707e] hover:text-[#bcbec4] text-[12px]"
                                    >
                                        ✕
                                    </button>
                                </div>
                            </div>

                            {/* Preview Body */}
                            <div className="flex-1 overflow-auto">
                                {previewLoading ? (
                                    <div className="flex items-center justify-center py-20 text-[#6c707e]">
                                        <Loader2 className="w-5 h-5 animate-spin mr-2" />
                                        <span className="text-[12px]">Loading preview...</span>
                                    </div>
                                ) : filePreview ? (
                                    <div className="flex flex-col h-full">
                                        {/* Metadata */}
                                        <div className={clsx(
                                            "border-b border-[#393b40] space-y-2",
                                            fullscreenPreview ? "p-6" : "p-3"
                                        )}>
                                            <div className="text-[10px] text-[#6c707e] font-bold uppercase tracking-wider mb-2">Object Details</div>
                                            <div className="grid grid-cols-2 gap-2 text-[11px]">
                                                <div>
                                                    <div className="text-[9px] text-[#6c707e] uppercase mb-0.5">Key</div>
                                                    <div className="text-[#a9b7c6] font-mono text-[10px] break-all">{filePreview.key}</div>
                                                </div>
                                                <div>
                                                    <div className="text-[9px] text-[#6c707e] uppercase mb-0.5">Bucket</div>
                                                    <div className="text-[#a9b7c6] font-mono text-[10px]">{filePreview.bucket}</div>
                                                </div>
                                                <div>
                                                    <div className="text-[9px] text-[#6c707e] uppercase mb-0.5">Size</div>
                                                    <div className="text-[#a9b7c6] font-mono text-[10px]">{filePreview.sizeFormatted}</div>
                                                </div>
                                                <div>
                                                    <div className="text-[9px] text-[#6c707e] uppercase mb-0.5">Content Type</div>
                                                    <div className="text-[#a9b7c6] font-mono text-[10px]">{filePreview.contentType}</div>
                                                </div>
                                                <div>
                                                    <div className="text-[9px] text-[#6c707e] uppercase mb-0.5">Last Modified</div>
                                                    <div className="text-[#a9b7c6] font-mono text-[10px]">
                                                        {filePreview.lastModified ? new Date(filePreview.lastModified).toLocaleString() : '--'}
                                                    </div>
                                                </div>
                                                <div>
                                                    <div className="text-[9px] text-[#6c707e] uppercase mb-0.5">ETag</div>
                                                    <div className="text-[#a9b7c6] font-mono text-[10px] truncate">{filePreview.etag || '--'}</div>
                                                </div>
                                            </div>
                                        </div>

                                        {/* Content Preview */}
                                        {filePreview.isPreviewable && filePreview.preview && (
                                            <div className="flex-1 flex flex-col overflow-hidden">
                                                <div className={clsx(
                                                    "bg-[#2b2d30] border-b border-[#393b40] flex items-center justify-between shrink-0",
                                                    fullscreenPreview ? "px-6 py-2" : "px-3 py-1"
                                                )}>
                                                    <span className="text-[10px] text-[#6c707e] font-bold uppercase">Content Preview</span>
                                                    <span className="text-[9px] text-[#6c707e]">{filePreview.preview.split('\n').length} lines</span>
                                                </div>
                                                <div className="flex-1 overflow-auto bg-[#1a1b1e]">
                                                    <pre className="p-0 m-0">
                                                        <code className={clsx("font-mono leading-relaxed", fullscreenPreview ? "text-[13px]" : "text-[11px]")}>
                                                            {filePreview.preview.split('\n').map((line, i) => (
                                                                <div key={i} className="flex hover:bg-[#2b2d30]/50">
                                                                    <span className={clsx(
                                                                        "text-right pr-3 text-[#6c707e]/50 select-none shrink-0",
                                                                        fullscreenPreview ? "w-14 text-[12px]" : "w-8 text-[10px]"
                                                                    )} style={{ lineHeight: fullscreenPreview ? '24px' : '18px' }}>
                                                                        {i + 1}
                                                                    </span>
                                                                    <span className="flex-1 whitespace-pre text-[#a9b7c6]" style={{ lineHeight: fullscreenPreview ? '24px' : '18px' }}>
                                                                        {line}
                                                                    </span>
                                                                </div>
                                                            ))}
                                                        </code>
                                                    </pre>
                                                </div>
                                            </div>
                                        )}

                                        {/* Binary file notice */}
                                        {!filePreview.isPreviewable && (
                                            <div className="flex-1 flex flex-col items-center justify-center text-[#6c707e] p-6">
                                                <File className="w-10 h-10 mb-3 opacity-30" />
                                                <span className="text-[12px] mb-1">Binary file — preview not available</span>
                                                <span className="text-[10px]">{filePreview.contentType}</span>
                                            </div>
                                        )}
                                    </div>
                                ) : (
                                    <div className="flex items-center justify-center py-20 text-[#6c707e] text-[12px]">
                                        Failed to load preview
                                    </div>
                                )}
                            </div>
                        </div>
                    )}
                </div>
            </main>
        </div>
    );
}
