
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
        <div className="flex h-screen bg-obsidian-bg text-foreground font-sans overflow-hidden">
            <Sidebar />

            <main className="flex-1 flex flex-col min-w-0 overflow-hidden">

                {/* ─── Top Bar ─── */}
                <header className="h-10 bg-black/60 border-b border-obsidian-border/30 flex items-center justify-between px-5 shrink-0 shadow-sm transition-all relative">
                    <div className="absolute top-0 left-0 right-0 h-[1px] bg-gradient-to-r from-obsidian-info to-obsidian-purple opacity-40"></div>
                    <div className="flex items-center gap-2">
                        <HardDrive className="w-4 h-4 text-obsidian-info drop-shadow-[0_0_8px_rgba(34,211,238,0.5)]" />
                        <span className="text-[12px] font-bold text-foreground tracking-widest uppercase">Storage Layer</span>
                        <span className="text-[10px] text-obsidian-muted uppercase tracking-wider ml-1 px-2 py-0.5 bg-white/5 rounded-md border border-white/10">MinIO S3</span>
                    </div>
                    <div className="flex items-center gap-4 text-[11px] text-obsidian-muted font-medium">
                        <div className="flex bg-black/40 px-3 py-1 rounded-md border border-obsidian-border/30 gap-3 shadow-[inset_0_1px_0_rgba(255,255,255,0.05)]">
                            <span>{buckets.length} buckets</span>
                            <span className="text-obsidian-border">•</span>
                            <span>{totalObjects} objects</span>
                            <span className="text-obsidian-border">•</span>
                            <span className="text-obsidian-info glow-text font-bold tracking-wide">{formatBytes(totalSize)}</span>
                        </div>
                        <button
                            onClick={() => { fetchBuckets(); if (selectedBucket) fetchObjects(selectedBucket, currentPrefix); }}
                            className="p-1.5 hover:bg-white/10 rounded-md text-obsidian-muted hover:text-white transition-all active:scale-90 border border-transparent hover:border-obsidian-border/50"
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
                        selectedFile && !fullscreenPreview && "border-r border-obsidian-border"
                    )}>

                        {/* Breadcrumb */}
                        {selectedBucket && (
                            <div className="h-10 bg-black/40 border-b border-obsidian-border/30 flex items-center px-4 gap-2 text-[12px] shrink-0 shadow-sm">
                                <button
                                    onClick={goBackToBuckets}
                                    className="text-obsidian-info hover:text-[#5a9cf5] hover:underline font-medium transition-colors border border-obsidian-info/20 bg-obsidian-info/5 px-2 py-0.5 rounded"
                                >
                                    Buckets
                                </button>
                                <ChevronRight className="w-3.5 h-3.5 text-obsidian-muted/60" />
                                <button
                                    onClick={() => openFolder('')}
                                    className={clsx(
                                        breadcrumbs.length === 0 ? "text-foreground font-bold tracking-wide" : "text-obsidian-muted hover:text-[#5a9cf5] transition-colors"
                                    )}
                                >
                                    {selectedBucket}
                                </button>
                                {breadcrumbs.map((bc, i) => (
                                    <React.Fragment key={bc.prefix}>
                                        <ChevronRight className="w-3.5 h-3.5 text-obsidian-muted/60" />
                                        <button
                                            onClick={() => openFolder(bc.prefix)}
                                            className={clsx(
                                                i === breadcrumbs.length - 1 ? "text-foreground font-bold tracking-wide" : "text-obsidian-muted hover:text-[#5a9cf5] transition-colors"
                                            )}
                                        >
                                            {bc.label}
                                        </button>
                                    </React.Fragment>
                                ))}
                            </div>
                        )}

                        {/* Content Area */}
                        <div className="flex-1 overflow-auto bg-obsidian-bg">

                            {/* ─── Bucket Cards ─── */}
                            {!selectedBucket && (
                                <div className="p-4">
                                    {bucketsLoading ? (
                                        <div className="flex items-center justify-center py-20 text-obsidian-muted">
                                            <Loader2 className="w-5 h-5 animate-spin mr-2" />
                                            <span className="text-[12px]">Connecting to MinIO...</span>
                                        </div>
                                    ) : bucketsError ? (
                                        <div className="flex items-center justify-center py-20 text-obsidian-danger">
                                            <AlertCircle className="w-5 h-5 mr-2" />
                                            <span className="text-[12px]">{bucketsError}</span>
                                        </div>
                                    ) : (
                                        <>
                                            <div className="text-[10px] text-obsidian-muted font-bold uppercase tracking-widest mb-4 px-1 flex items-center gap-2">
                                                Lakehouse Buckets
                                                <div className="h-[1px] bg-obsidian-border/50 flex-1 ml-2"></div>
                                            </div>
                                            <div className="grid grid-cols-1 md:grid-cols-2 lg:grid-cols-3 xl:grid-cols-4 gap-4">
                                                {buckets.map((bucket) => {
                                                    const style = BUCKET_STYLES[bucket.name] || { color: '#6c707e', bg: '#6c707e/10', icon: Archive };
                                                    const Icon = style.icon;
                                                    return (
                                                        <div
                                                            key={bucket.name}
                                                            onClick={() => openBucket(bucket.name)}
                                                            className="group bg-obsidian-panel/60 backdrop-blur-md border border-obsidian-border/80 rounded-xl p-5 cursor-pointer hover:border-obsidian-info/50 hover:bg-white/5 transition-all shadow-lg hover:shadow-[0_8px_30px_rgba(0,0,0,0.6)] active:scale-95"
                                                        >
                                                            <div className="flex items-start justify-between mb-4">
                                                                <div
                                                                    className="w-10 h-10 rounded-xl flex items-center justify-center shadow-inner"
                                                                    style={{ backgroundColor: `${style.color}20`, border: `1px solid ${style.color}40` }}
                                                                >
                                                                    <Icon className="w-5 h-5 group-hover:scale-110 transition-transform" style={{ color: style.color, filter: `drop-shadow(0 0 8px ${style.color}80)` }} />
                                                                </div>
                                                                <div className="p-1 rounded bg-black/20 opacity-0 group-hover:opacity-100 transition-all transform translate-x-1 group-hover:translate-x-0">
                                                                    <ChevronRight className="w-4 h-4 text-obsidian-info" />
                                                                </div>
                                                            </div>
                                                            <div className="text-[14px] font-bold text-foreground mb-1 tracking-wide group-hover:text-white transition-colors">
                                                                {bucket.name}
                                                            </div>
                                                            <div className="flex items-center gap-3 text-[11px] text-obsidian-muted/80 font-medium">
                                                                <span>{bucket.objectCount} objects</span>
                                                                <span className="text-obsidian-border">•</span>
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
                                        <div className="flex items-center justify-center py-20 text-obsidian-muted">
                                            <Loader2 className="w-5 h-5 animate-spin mr-2" />
                                            <span className="text-[12px]">Loading objects...</span>
                                        </div>
                                    ) : objects.length === 0 ? (
                                        <div className="flex flex-col items-center justify-center py-20 text-obsidian-muted">
                                            <Archive className="w-8 h-8 mb-3 opacity-30" />
                                            <span className="text-[12px]">This directory is empty</span>
                                        </div>
                                    ) : (
                                        <table className="w-full text-left border-collapse">
                                            <thead className="sticky top-0 bg-black/60 backdrop-blur-md shadow-sm z-10">
                                                <tr>
                                                    <th className="p-2 px-4 border-b border-obsidian-border/30 text-[10px] text-obsidian-muted font-bold uppercase tracking-widest w-[50%]">Name</th>
                                                    <th className="p-2 px-4 border-b border-obsidian-border/30 text-[10px] text-obsidian-muted font-bold uppercase tracking-widest w-[15%]">Size</th>
                                                    <th className="p-2 px-4 border-b border-obsidian-border/30 text-[10px] text-obsidian-muted font-bold uppercase tracking-widest w-[15%]">Modified</th>
                                                    <th className="p-2 px-4 border-b border-obsidian-border/30 text-[10px] text-obsidian-muted font-bold uppercase tracking-widest w-[20%]">Type</th>
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
                                                                "cursor-pointer group transition-all",
                                                                isSelected ? "bg-obsidian-info/10 border-l-2 border-l-obsidian-info" : "hover:bg-white/5 border-l-2 border-l-transparent hover:border-l-obsidian-border"
                                                            )}
                                                        >
                                                            <td className="p-2.5 px-4 border-b border-obsidian-border/20">
                                                                <div className="flex items-center gap-3">
                                                                    <Icon className={clsx(
                                                                        "w-4 h-4 shrink-0 transition-transform group-hover:scale-110",
                                                                        isFolder ? "text-obsidian-warning drop-shadow-[0_0_8px_rgba(255,203,107,0.4)]" : "text-obsidian-info"
                                                                    )} />
                                                                    <span className={clsx(
                                                                        "truncate font-medium transition-colors",
                                                                        isFolder ? "text-foreground group-hover:text-white" : "text-foreground/90 group-hover:text-white"
                                                                    )}>
                                                                        {item.name}
                                                                    </span>
                                                                </div>
                                                            </td>
                                                            <td className="p-2.5 px-4 border-b border-obsidian-border/20 text-obsidian-muted/80 group-hover:text-obsidian-muted transition-colors">
                                                                {item.sizeFormatted}
                                                            </td>
                                                            <td className="p-2.5 px-4 border-b border-obsidian-border/20 text-obsidian-muted/80 group-hover:text-obsidian-muted transition-colors">
                                                                {timeAgo(item.lastModified)}
                                                            </td>
                                                            <td className="p-2.5 px-4 border-b border-obsidian-border/20 text-obsidian-muted">
                                                                {isFolder ? (
                                                                    <span className="text-[9px] px-2 py-0.5 rounded-md bg-obsidian-warning/10 border border-obsidian-warning/30 text-obsidian-warning font-bold tracking-widest shadow-[inset_0_1px_0_rgba(255,255,255,0.1)]">DIR</span>
                                                                ) : (
                                                                    <span className="text-[9px] px-2 py-0.5 rounded-md bg-obsidian-info/10 border border-obsidian-info/30 text-obsidian-info font-bold tracking-widest uppercase shadow-[inset_0_1px_0_rgba(255,255,255,0.1)]">
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
                            "flex flex-col bg-obsidian-panel/30 shrink-0 transition-all duration-300 shadow-[-10px_0_30px_rgba(0,0,0,0.6)] backdrop-blur-xl border-l border-white/5",
                            fullscreenPreview
                                ? "fixed inset-0 z-50 w-full"
                                : "w-[420px]"
                        )}>
                            {/* Preview Header */}
                            <div className={clsx(
                                "bg-black/60 border-b border-white/10 flex items-center justify-between shrink-0 shadow-sm relative",
                                fullscreenPreview ? "h-12 px-6" : "h-10 px-4"
                            )}>
                                {/* Glowing Top Edge */}
                                <div className="absolute top-0 left-0 right-0 h-[1px] bg-gradient-to-r from-obsidian-purple to-obsidian-info opacity-60"></div>
                                <div className="flex items-center gap-3 min-w-0">
                                    <FileText className={clsx("text-obsidian-info shrink-0 drop-shadow-[0_0_8px_rgba(34,211,238,0.5)]", fullscreenPreview ? "w-4 h-4" : "w-4 h-4")} />
                                    <span className={clsx("font-bold text-white tracking-wide truncate drop-shadow-md", fullscreenPreview ? "text-[14px]" : "text-[12px]")}>
                                        {selectedFile.name}
                                    </span>
                                </div>
                                <div className="flex items-center gap-1.5">
                                    <button
                                        onClick={() => setFullscreenPreview(!fullscreenPreview)}
                                        className="p-1.5 hover:bg-white/10 rounded-md text-obsidian-muted hover:text-white transition-all active:scale-90"
                                        title={fullscreenPreview ? 'Minimize' : 'Maximize'}
                                    >
                                        {fullscreenPreview
                                            ? <Minimize2 className="w-3.5 h-3.5" />
                                            : <Maximize2 className="w-3.5 h-3.5" />
                                        }
                                    </button>
                                    <button
                                        onClick={() => { setSelectedFile(null); setFilePreview(null); setFullscreenPreview(false); }}
                                        className="p-1.5 hover:bg-white/10 rounded-md text-obsidian-muted hover:text-obsidian-danger hover:bg-obsidian-danger/10 transition-all active:scale-90"
                                    >
                                        <X className="w-4 h-4" />
                                    </button>
                                </div>
                            </div>

                            {/* Preview Body */}
                            <div className="flex-1 overflow-auto">
                                {previewLoading ? (
                                    <div className="flex items-center justify-center py-20 text-obsidian-muted">
                                        <Loader2 className="w-5 h-5 animate-spin mr-2" />
                                        <span className="text-[12px]">Loading preview...</span>
                                    </div>
                                ) : filePreview ? (
                                    <div className="flex flex-col h-full">
                                        {/* Metadata */}
                                        <div className={clsx(
                                            "border-b border-obsidian-border space-y-2",
                                            fullscreenPreview ? "p-6" : "p-3"
                                        )}>
                                            <div className="text-[10px] text-obsidian-muted font-bold uppercase tracking-wider mb-2">Object Details</div>
                                            <div className="grid grid-cols-2 gap-2 text-[11px]">
                                                <div>
                                                    <div className="text-[9px] text-obsidian-muted uppercase mb-0.5">Key</div>
                                                    <div className="text-foreground font-mono text-[10px] break-all">{filePreview.key}</div>
                                                </div>
                                                <div>
                                                    <div className="text-[9px] text-obsidian-muted uppercase mb-0.5">Bucket</div>
                                                    <div className="text-foreground font-mono text-[10px]">{filePreview.bucket}</div>
                                                </div>
                                                <div>
                                                    <div className="text-[9px] text-obsidian-muted uppercase mb-0.5">Size</div>
                                                    <div className="text-foreground font-mono text-[10px]">{filePreview.sizeFormatted}</div>
                                                </div>
                                                <div>
                                                    <div className="text-[9px] text-obsidian-muted uppercase mb-0.5">Content Type</div>
                                                    <div className="text-foreground font-mono text-[10px]">{filePreview.contentType}</div>
                                                </div>
                                                <div>
                                                    <div className="text-[9px] text-obsidian-muted uppercase mb-0.5">Last Modified</div>
                                                    <div className="text-foreground font-mono text-[10px]">
                                                        {filePreview.lastModified ? new Date(filePreview.lastModified).toLocaleString() : '--'}
                                                    </div>
                                                </div>
                                                <div>
                                                    <div className="text-[9px] text-obsidian-muted uppercase mb-0.5">ETag</div>
                                                    <div className="text-foreground font-mono text-[10px] truncate">{filePreview.etag || '--'}</div>
                                                </div>
                                            </div>
                                        </div>

                                        {/* Content Preview */}
                                        {filePreview.isPreviewable && filePreview.preview && (
                                            <div className="flex-1 flex flex-col overflow-hidden m-4 rounded-xl border border-white/10 shadow-[0_8px_24px_rgba(0,0,0,0.6)]">
                                                <div className={clsx(
                                                    "bg-black/60 border-b border-white/10 flex items-center justify-between shrink-0",
                                                    fullscreenPreview ? "px-6 py-2.5" : "px-4 py-1.5"
                                                )}>
                                                    <span className="text-[10px] text-obsidian-muted font-bold uppercase tracking-widest">Raw Output</span>
                                                    <span className="text-[9px] text-obsidian-muted/80 bg-black/40 px-2 py-0.5 rounded border border-white/5">{filePreview.preview.split('\n').length} lines</span>
                                                </div>
                                                <div className="flex-1 overflow-auto bg-[#0a0a0c] custom-scrollbar">
                                                    <pre className="p-0 m-0">
                                                        <code className={clsx("font-mono leading-relaxed", fullscreenPreview ? "text-[13px]" : "text-[11px]")}>
                                                            {filePreview.preview.split('\n').map((line, i) => (
                                                                <div key={i} className="flex hover:bg-obsidian-panel/50">
                                                                    <span className={clsx(
                                                                        "text-right pr-3 text-obsidian-muted/50 select-none shrink-0",
                                                                        fullscreenPreview ? "w-14 text-[12px]" : "w-8 text-[10px]"
                                                                    )} style={{ lineHeight: fullscreenPreview ? '24px' : '18px' }}>
                                                                        {i + 1}
                                                                    </span>
                                                                    <span className="flex-1 whitespace-pre text-foreground" style={{ lineHeight: fullscreenPreview ? '24px' : '18px' }}>
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
                                            <div className="flex-1 flex flex-col items-center justify-center text-obsidian-muted p-6">
                                                <File className="w-10 h-10 mb-3 opacity-30" />
                                                <span className="text-[12px] mb-1">Binary file — preview not available</span>
                                                <span className="text-[10px]">{filePreview.contentType}</span>
                                            </div>
                                        )}
                                    </div>
                                ) : (
                                    <div className="flex items-center justify-center py-20 text-obsidian-muted text-[12px]">
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
