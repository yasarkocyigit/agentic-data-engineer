
import React, { useState, useEffect, useCallback, useRef } from 'react';
import {
    GitPullRequest, ExternalLink, RefreshCw, Loader2, AlertCircle,
    CheckCircle2, Maximize2, Minimize2, Search, Star, Clock,
    GitBranch, GitCommit, Play, XCircle, ChevronRight, X, PackageOpen,
    Folder, FileCode, FileText, File as FileIcon, FolderOpen,
    Plus, ArrowUpRight, GitMerge, MessageSquare, ChevronDown, Eye, Code2, Trash2, Copy,
    BookOpen, Scale, Users, Tag, Link as LinkIcon, Clipboard, Check, LayoutPanelLeft
} from 'lucide-react';
import clsx from 'clsx';
import { Sidebar } from '@/components/Sidebar';
import { Prism as SyntaxHighlighter } from 'react-syntax-highlighter';
import { vscDarkPlus } from 'react-syntax-highlighter/dist/esm/styles/prism';
import ReactMarkdown from 'react-markdown';
import remarkGfm from 'remark-gfm';
import Editor, { OnMount } from "@monaco-editor/react";

function resolveGiteaUrl(): string {
    if (typeof window !== 'undefined') {
        const runtimeUrl = (window as Window & { __OPENCLAW_GITEA_URL__?: string }).__OPENCLAW_GITEA_URL__?.trim();
        if (runtimeUrl) return runtimeUrl;
        return `${window.location.protocol}//${window.location.hostname}:3030`;
    }
    return 'http://localhost:3030';
}

function resolveGiteaSshHost(giteaUrl: string): string {
    try {
        return new URL(giteaUrl).hostname;
    } catch {
        return typeof window !== 'undefined' ? window.location.hostname : 'localhost';
    }
}

function resolveGiteaSshPort(giteaUrl: string): number {
    if (typeof window !== 'undefined') {
        const runtimePort = (window as Window & { __OPENCLAW_GITEA_SSH_PORT__?: string | number }).__OPENCLAW_GITEA_SSH_PORT__;
        if (runtimePort !== undefined && runtimePort !== null) {
            const parsed = Number(runtimePort);
            if (Number.isFinite(parsed) && parsed > 0) return parsed;
        }
    }
    try {
        const host = new URL(giteaUrl).hostname;
        if (host === 'localhost' || host === '127.0.0.1') return 2222;
    } catch {
        // no-op fallback below
    }
    return 22;
}

const GITEA_URL = resolveGiteaUrl();
const GITEA_SSH_HOST = resolveGiteaSshHost(GITEA_URL);
const GITEA_SSH_PORT = resolveGiteaSshPort(GITEA_URL);

// ─── Types ───

interface GiteaRepo {
    id: number;
    full_name: string;
    name: string;
    owner: { login: string; avatar_url: string };
    description: string;
    language: string;
    stars_count: number;
    forks_count: number;
    updated_at: string;
    default_branch: string;
    html_url: string;
    empty: boolean;
}

interface GiteaBranch {
    name: string;
    commit: { id: string; message: string };
    protected: boolean;
}

interface GiteaCommit {
    sha: string;
    commit: { message: string; author: { name: string; date: string } };
    author?: { avatar_url?: string; login?: string };
    html_url: string;
}

interface TreeEntry {
    name: string;
    path: string;
    type: 'file' | 'dir';
    size: number;
    sha: string;
}

interface CommitDetail {
    sha: string;
    commit: { message: string; author: { name: string; date: string; email: string } };
    stats: { total: number; additions: number; deletions: number };
    files: { filename: string; status: string; additions: number; deletions: number; patch?: string }[];
}

interface PullRequest {
    id: number;
    number: number;
    title: string;
    body: string;
    state: string;
    user: { login: string; avatar_url: string };
    head: { label: string; ref: string; sha?: string };
    base: { label: string; ref: string };
    created_at: string;
    updated_at: string;
    merged: boolean;
    merged_at: string | null;
    comments: number;
    changed_files: number;
    additions: number;
    deletions: number;
}

interface CommitStatusItem {
    id?: number;
    context?: string;
    state?: string;
    description?: string;
    target_url?: string;
    created_at?: string;
    updated_at?: string;
}

interface ActionRun {
    id: number;
    name: string;
    status: string;
    conclusion: string;
    head_branch: string;
    head_sha?: string;
    workflow_id?: string | number;
    created_at: string;
    updated_at: string;
    run_number: number;
}

interface ActionJob {
    id?: number;
    name?: string;
    status?: string;
    conclusion?: string;
    started_at?: string;
    completed_at?: string;
    runner_name?: string;
    steps?: { number?: number; name?: string; status?: string; conclusion?: string }[];
}

interface IssueItem {
    id: number;
    number: number;
    title: string;
    state: string;
    created_at: string;
    updated_at: string;
    body?: string;
    comments?: number;
    user?: { login?: string; avatar_url?: string };
    labels?: { id: number; name: string; color?: string }[];
}

interface ReleaseItem {
    id: number;
    tag_name: string;
    name?: string;
    body?: string;
    draft?: boolean;
    prerelease?: boolean;
    created_at?: string;
    published_at?: string;
    author?: { login?: string };
}

interface PullComment {
    id: number;
    body?: string;
    created_at?: string;
    user?: { login?: string; avatar_url?: string };
}

interface PullReview {
    id: number;
    body?: string;
    submitted_at?: string;
    state?: string;
    user?: { login?: string; avatar_url?: string };
}

interface ParsedPatchLine {
    text: string;
    oldPosition: number;
    newPosition: number;
    kind: 'hunk' | 'add' | 'del' | 'ctx' | 'meta';
}

interface PullInlineComment {
    id: number;
    body?: string;
    path?: string;
    new_position?: number;
    old_position?: number;
    created_at?: string;
    user?: { login?: string; avatar_url?: string };
    review_id?: number;
}

interface BranchProtectionRule {
    branch_name?: string;
    rule_name?: string;
    enable_status_check?: boolean;
    status_check_contexts?: string[];
    required_approvals?: number;
    dismiss_stale_approvals?: boolean;
    block_on_rejected_reviews?: boolean;
    block_on_outdated_branch?: boolean;
    block_on_official_review_requests?: boolean;
    enable_force_push?: boolean;
    require_signed_commits?: boolean;
}

// ─── Utility ───

function parsePatchLines(patch: string): ParsedPatchLine[] {
    const lines = patch.split('\n');
    const parsed: ParsedPatchLine[] = [];
    let oldPos = 0;
    let newPos = 0;
    for (const line of lines) {
        if (line.startsWith('@@')) {
            const m = line.match(/^@@ -(\d+)(?:,\d+)? \+(\d+)(?:,\d+)? @@/);
            oldPos = m ? Number(m[1]) : 0;
            newPos = m ? Number(m[2]) : 0;
            parsed.push({ text: line, oldPosition: 0, newPosition: 0, kind: 'hunk' });
            continue;
        }
        if (line.startsWith('+') && !line.startsWith('+++')) {
            parsed.push({ text: line, oldPosition: 0, newPosition: newPos, kind: 'add' });
            newPos += 1;
            continue;
        }
        if (line.startsWith('-') && !line.startsWith('---')) {
            parsed.push({ text: line, oldPosition: oldPos, newPosition: 0, kind: 'del' });
            oldPos += 1;
            continue;
        }
        if (line.startsWith('diff --git') || line.startsWith('index ') || line.startsWith('--- ') || line.startsWith('+++ ')) {
            parsed.push({ text: line, oldPosition: 0, newPosition: 0, kind: 'meta' });
            continue;
        }
        if (oldPos > 0 || newPos > 0) {
            parsed.push({ text: line, oldPosition: oldPos, newPosition: newPos, kind: 'ctx' });
            oldPos += 1;
            newPos += 1;
            continue;
        }
        parsed.push({ text: line, oldPosition: 0, newPosition: 0, kind: 'ctx' });
    }
    return parsed;
}

function timeAgo(dateStr: string | null): string {
    if (!dateStr) return '—';
    const now = Date.now();
    const then = new Date(dateStr).getTime();
    const diffMs = now - then;
    if (diffMs < 0) return 'soon';
    const secs = Math.floor(diffMs / 1000);
    if (secs < 60) return `${secs}s ago`;
    const mins = Math.floor(secs / 60);
    if (mins < 60) return `${mins}m ago`;
    const hrs = Math.floor(mins / 60);
    if (hrs < 24) return `${hrs}h ago`;
    const days = Math.floor(hrs / 24);
    return `${days}d ago`;
}

function langColor(lang: string | null): string {
    if (!lang) return '#6c707e';
    const map: Record<string, string> = {
        Python: '#3572a5', JavaScript: '#f1e05a', TypeScript: '#3178c6',
        Go: '#00add8', Rust: '#dea584', Java: '#b07219', Shell: '#89e051',
        SQL: '#e38c00', HTML: '#e34c26', CSS: '#563d7c', Dockerfile: '#384d54',
    };
    return map[lang] || '#6c707e';
}

function fileIcon(name: string): typeof FileIcon {
    const ext = name.split('.').pop()?.toLowerCase();
    if (!ext) return FileIcon;
    if (['py'].includes(ext)) return FileCode;
    if (['ts', 'tsx', 'js', 'jsx', 'json', 'yml', 'yaml', 'toml', 'sh', 'html', 'css'].includes(ext)) return FileCode;
    if (['md', 'txt', 'csv', 'log'].includes(ext)) return FileText;
    return FileIcon;
}

function extToLang(name: string): string {
    const ext = name.split('.').pop()?.toLowerCase() || '';
    const basename = name.toLowerCase();
    const map: Record<string, string> = {
        py: 'python', ts: 'typescript', tsx: 'tsx', js: 'javascript', jsx: 'jsx',
        json: 'json', yml: 'yaml', yaml: 'yaml', md: 'markdown', mdx: 'markdown',
        sh: 'bash', bash: 'bash', zsh: 'bash', fish: 'bash',
        sql: 'sql', html: 'html', htm: 'html', css: 'css', scss: 'scss', less: 'less',
        xml: 'xml', svg: 'xml', toml: 'toml', ini: 'ini', cfg: 'ini',
        rs: 'rust', go: 'go', java: 'java', kt: 'kotlin', swift: 'swift',
        rb: 'ruby', php: 'php', cs: 'csharp', cpp: 'cpp', c: 'c', h: 'c',
        r: 'r', lua: 'lua', perl: 'perl', ps1: 'powershell',
        dockerfile: 'docker', makefile: 'makefile', cmake: 'cmake',
        graphql: 'graphql', gql: 'graphql', proto: 'protobuf',
        tf: 'hcl', hcl: 'hcl', env: 'bash',
    };
    if (basename === 'dockerfile' || basename.startsWith('dockerfile.')) return 'docker';
    if (basename === 'makefile') return 'makefile';
    if (basename === '.gitignore' || basename === '.dockerignore' || basename === '.env' || basename === '.env.example') return 'bash';
    return map[ext] || 'text';
}

function isMarkdown(name: string): boolean {
    const ext = name.split('.').pop()?.toLowerCase();
    return ext === 'md' || ext === 'mdx';
}

function isImage(name: string): boolean {
    const ext = name.split('.').pop()?.toLowerCase();
    return ['png', 'jpg', 'jpeg', 'gif', 'svg', 'webp', 'ico', 'bmp'].includes(ext || '');
}

function fileColor(name: string): string {
    const ext = name.split('.').pop()?.toLowerCase();
    if (!ext) return '#6c707e';
    const map: Record<string, string> = {
        py: '#4e94c0', ts: '#4b8ec2', tsx: '#4b8ec2', js: '#b8a84e', jsx: '#b8a84e',
        json: '#8a9a6e', yml: '#8a6ea0', yaml: '#8a6ea0', md: '#6ea870',
        sh: '#6ea870', bash: '#6ea870', sql: '#82aaff', html: '#c07a60', css: '#6e6ea0',
        dockerfile: '#4e8a7e', gitignore: '#6c707e', env: '#6c707e',
    };
    return map[ext] || '#6c707e';
}

function formatBytes(bytes: number): string {
    if (bytes < 1024) return `${bytes} B`;
    if (bytes < 1024 * 1024) return `${(bytes / 1024).toFixed(1)} KB`;
    return `${(bytes / (1024 * 1024)).toFixed(1)} MB`;
}

const LANG_COLORS: Record<string, string> = {
    Python: '#3572A5', TypeScript: '#3178c6', JavaScript: '#f1e05a', HTML: '#e34c26',
    CSS: '#563d7c', Shell: '#89e051', Go: '#00ADD8', Rust: '#dea584',
    Java: '#b07219', Ruby: '#701516', PHP: '#4F5D95', Swift: '#F05138',
    Kotlin: '#A97BFF', 'C#': '#178600', C: '#555555', 'C++': '#f34b7d',
    Dockerfile: '#384d54', YAML: '#cb171e', JSON: '#292929', Makefile: '#427819',
    SQL: '#e38c00', Lua: '#000080', R: '#198CE7', Scala: '#c22d40',
    HCL: '#844FBA', Perl: '#0298c3', Markdown: '#083fa1',
};

async function fetchJsonWithTimeout(url: string, timeoutMs = 10000) {
    const controller = new AbortController();
    const timer = window.setTimeout(() => controller.abort(), timeoutMs);
    try {
        const res = await fetch(url, { signal: controller.signal });
        if (!res.ok) throw new Error(`${res.status} ${res.statusText}`);
        return await res.json();
    } finally {
        window.clearTimeout(timer);
    }
}

function encodeBase64Utf8(value: string): string {
    const bytes = new TextEncoder().encode(value);
    let binary = '';
    const chunkSize = 0x8000;
    for (let i = 0; i < bytes.length; i += chunkSize) {
        binary += String.fromCharCode(...bytes.subarray(i, i + chunkSize));
    }
    return btoa(binary);
}

function normalizeCheckState(value?: string | null): string {
    return (value || 'unknown').toLowerCase();
}

function isBlockingCheckState(value?: string | null): boolean {
    const state = normalizeCheckState(value);
    return state === 'failure' || state === 'error' || state === 'failed';
}

function checkStateStyle(value?: string | null): { bg: string; text: string; border: string } {
    const state = normalizeCheckState(value);
    if (state === 'success') return { bg: 'bg-obsidian-success/10', text: 'text-obsidian-success', border: 'border-obsidian-success/20' };
    if (state === 'pending' || state === 'running' || state === 'waiting') return { bg: 'bg-obsidian-warning/10', text: 'text-obsidian-warning', border: 'border-obsidian-warning/20' };
    if (isBlockingCheckState(state)) return { bg: 'bg-obsidian-danger/10', text: 'text-obsidian-danger', border: 'border-obsidian-danger/20' };
    return { bg: 'bg-obsidian-border-active/30', text: 'text-obsidian-muted', border: 'border-white/10' };
}

// ─── Health Dot ───

function HealthDot({ status }: { status: 'online' | 'offline' | 'checking' }) {
    if (status === 'checking') return (
        <div className="flex items-center gap-1 text-obsidian-muted">
            <Loader2 className="w-3.5 h-3.5 animate-spin" /><span className="text-[10px]">Checking...</span>
        </div>
    );
    if (status === 'online') return (
        <div className="flex items-center gap-1 text-obsidian-success">
            <CheckCircle2 className="w-3.5 h-3.5" /><span className="text-[10px]">Connected</span>
        </div>
    );
    return (
        <div className="flex items-center gap-1 text-obsidian-danger">
            <AlertCircle className="w-3.5 h-3.5" /><span className="text-[10px]">Offline</span>
        </div>
    );
}

// ─── Action Status Badge ───

function ActionBadge({ status, conclusion }: { status: string; conclusion?: string }) {
    const state = conclusion || status;
    const config: Record<string, { bg: string; text: string; border: string; icon: typeof CheckCircle2 }> = {
        success: { bg: 'bg-obsidian-success/10', text: 'text-obsidian-success', border: 'border-obsidian-success/20', icon: CheckCircle2 },
        failure: { bg: 'bg-obsidian-danger/10', text: 'text-obsidian-danger', border: 'border-obsidian-danger/20', icon: XCircle },
        running: { bg: 'bg-obsidian-info/10', text: 'text-obsidian-info', border: 'border-obsidian-info/20', icon: Loader2 },
        waiting: { bg: 'bg-obsidian-warning/10', text: 'text-obsidian-warning', border: 'border-obsidian-warning/20', icon: Clock },
        cancelled: { bg: 'bg-obsidian-muted/10', text: 'text-obsidian-muted', border: 'border-obsidian-muted/20', icon: XCircle },
    };
    const c = config[state] || config.waiting;
    const Icon = c.icon;
    return (
        <span className={clsx("inline-flex items-center gap-1.5 rounded-md px-2 py-0.5 text-[10px] font-medium border", c.bg, c.text, c.border)}>
            <Icon className={clsx("w-3 h-3", state === 'running' && "animate-spin")} />
            {state.charAt(0).toUpperCase() + state.slice(1)}
        </span>
    );
}

// ─── PR Status Badge ───

function PRBadge({ pr }: { pr: PullRequest }) {
    if (pr.merged) return (
        <span className="inline-flex items-center gap-1.5 rounded-md px-2 py-0.5 text-[10px] font-medium border bg-obsidian-primary/10 text-obsidian-primary border-obsidian-primary/20 transition-all active:scale-95">
            <GitMerge className="w-3.5 h-3.5" /> Merged
        </span>
    );
    if (pr.state === 'closed') return (
        <span className="inline-flex items-center gap-1.5 rounded-md px-2 py-0.5 text-[10px] font-medium border bg-obsidian-danger/10 text-obsidian-danger border-obsidian-danger/20 transition-all active:scale-95">
            <XCircle className="w-3.5 h-3.5" /> Closed
        </span>
    );
    return (
        <span className="inline-flex items-center gap-1.5 rounded-md px-2 py-0.5 text-[10px] font-medium border bg-obsidian-success/10 text-obsidian-success border-obsidian-success/20 transition-all active:scale-95">
            <GitPullRequest className="w-3.5 h-3.5" /> Open
        </span>
    );
}


// ─── Diff Viewer ───

function DiffBlock({ patch, filename }: { patch: string; filename: string }) {
    if (!patch) return <div className="px-4 py-2 text-[10px] text-obsidian-muted italic">No diff available</div>;
    const lines = patch.split('\n');
    return (
        <div className="font-mono text-[11px] leading-[18px] overflow-x-auto">
            {lines.map((line, i) => {
                let bg = '';
                let color = '#bcbec4';
                if (line.startsWith('+') && !line.startsWith('+++')) { bg = 'bg-[#1a3a2a]'; color = '#6aab73'; }
                else if (line.startsWith('-') && !line.startsWith('---')) { bg = 'bg-[#3a1a1a]'; color = '#ff5261'; }
                else if (line.startsWith('@@')) { bg = 'bg-[#1a2a3a]'; color = '#3574f0'; }
                return (
                    <div key={`${filename}-${i}`} className={clsx("px-4 py-0 whitespace-pre", bg)} style={{ color }}>
                        {line}
                    </div>
                );
            })}
        </div>
    );
}


// ─── Main Page ───

type DetailTab = 'files' | 'commits' | 'pulls' | 'branches' | 'actions' | 'tags' | 'issues' | 'releases' | 'security' | 'insights';
type UiNoticeTone = 'success' | 'error' | 'info';

export default function CICDPage() {
    const [health, setHealth] = useState<'checking' | 'online' | 'offline'>('checking');
    const [giteaVersion, setGiteaVersion] = useState<string>('');
    const [repos, setRepos] = useState<GiteaRepo[]>([]);
    const [loading, setLoading] = useState(true);
    const [error, setError] = useState<string | null>(null);
    const [search, setSearch] = useState('');
    const [isFullscreen, setIsFullscreen] = useState(false);

    // Detail panel state
    const [selectedRepo, setSelectedRepo] = useState<GiteaRepo | null>(null);
    const [detailTab, setDetailTab] = useState<DetailTab>('files');
    const [branches, setBranches] = useState<GiteaBranch[]>([]);
    const [commits, setCommits] = useState<GiteaCommit[]>([]);
    const [commitsPage, setCommitsPage] = useState(1);
    const [commitsHasMore, setCommitsHasMore] = useState(false);
    const [commitsLoadingMore, setCommitsLoadingMore] = useState(false);
    const [actionRuns, setActionRuns] = useState<ActionRun[]>([]);
    const [actionsPage, setActionsPage] = useState(1);
    const [actionsHasMore, setActionsHasMore] = useState(false);
    const [actionsLoadingMore, setActionsLoadingMore] = useState(false);
    const [selectedActionRun, setSelectedActionRun] = useState<number | null>(null);
    const [actionJobs, setActionJobs] = useState<ActionJob[]>([]);
    const [actionJobsLoading, setActionJobsLoading] = useState(false);
    const [actionRunCommand, setActionRunCommand] = useState<{ runId: number; action: 'rerun' | 'cancel' } | null>(null);
    const [detailLoading, setDetailLoading] = useState(false);
    const [activeBranch, setActiveBranch] = useState<string>('');
    const [showBranchDropdown, setShowBranchDropdown] = useState(false);

    // Tags
    const [tags, setTags] = useState<{ name: string; commit: { sha: string }; tarball_url: string }[]>([]);
    const [tagsPage, setTagsPage] = useState(1);
    const [tagsHasMore, setTagsHasMore] = useState(false);
    const [tagsLoadingMore, setTagsLoadingMore] = useState(false);

    // File tree state
    const [treeEntries, setTreeEntries] = useState<Record<string, TreeEntry[]>>({});

    const [currentPath, setCurrentPath] = useState<string>('');
    const [readmeContent, setReadmeContent] = useState<string | null>(null);
    const [selectedFile, setSelectedFile] = useState<string | null>(null);
    const [fileContent, setFileContent] = useState<string>('');
    const [fileLoading, setFileLoading] = useState(false);
    const [mdPreview, setMdPreview] = useState(true);
    const [editMode, setEditMode] = useState(false);
    const [editContent, setEditContent] = useState('');
    const [editMessage, setEditMessage] = useState('');
    const [editSha, setEditSha] = useState('');
    const [editSaving, setEditSaving] = useState(false);

    // Commit detail
    const [selectedCommit, setSelectedCommit] = useState<CommitDetail | null>(null);
    const [commitLoading, setCommitLoading] = useState(false);
    const [expandedDiffs, setExpandedDiffs] = useState<Set<string>>(new Set());

    // Monaco Config
    const editorRef = React.useRef<any>(null);
    const handleEditorMount: OnMount = (editor, monacoInstance) => {
        editorRef.current = editor;
        // Define Obsidian Theme (Vibrant)
        monacoInstance.editor.defineTheme('obsidian', {
            base: 'vs-dark',
            inherit: true,
            rules: [
                { token: 'comment', foreground: '546e7a', fontStyle: 'italic' },
                { token: 'keyword', foreground: 'c792ea', fontStyle: 'bold' },
                { token: 'string', foreground: 'c3e88d' },
                { token: 'number', foreground: 'f78c6c' },
                { token: 'operator', foreground: '89ddff' },
                { token: 'identifier', foreground: 'eeffff' },
                { token: 'function', foreground: '82aaff' },
                { token: 'class.identifier', foreground: 'ffcb6b' },
                { token: 'type.identifier', foreground: 'ffcb6b' },
                { token: 'delimiter', foreground: 'eeffff' },
            ],
            colors: {
                'editor.background': '#111113',
                'editor.foreground': '#eeffff',
                'editor.selectionBackground': '#2d4264',
                'editor.lineHighlightBackground': '#1a1a1e',
                'editorCursor.foreground': '#818cf8',
                'editorWhitespace.foreground': '#2a2a30',
                'editorIndentGuide.background1': '#2a2a30',
                'editorLineNumber.foreground': '#3a3a4a',
                'editorLineNumber.activeForeground': '#6b7280',
            }
        });
        monacoInstance.editor.setTheme('obsidian');
    };

    // Pull requests
    const [pulls, setPulls] = useState<PullRequest[]>([]);
    const [prState, setPrState] = useState<'open' | 'closed' | 'all'>('all');
    const [pullsPage, setPullsPage] = useState(1);
    const [pullsHasMore, setPullsHasMore] = useState(false);
    const [pullsLoadingMore, setPullsLoadingMore] = useState(false);
    const [showCreatePR, setShowCreatePR] = useState(false);
    const [selectedPR, setSelectedPR] = useState<number | null>(null);
    const [prDetail, setPrDetail] = useState<{ pull: PullRequest; files: { filename: string; status: string; additions: number; deletions: number; patch?: string }[] } | null>(null);
    const [prDetailLoading, setPrDetailLoading] = useState(false);
    const [prTitle, setPrTitle] = useState('');
    const [prBody, setPrBody] = useState('');
    const [prHead, setPrHead] = useState('');
    const [prBase, setPrBase] = useState('');
    const [prChecksState, setPrChecksState] = useState<string>('unknown');
    const [prChecks, setPrChecks] = useState<CommitStatusItem[]>([]);
    const [prChecksLoading, setPrChecksLoading] = useState(false);
    const [prComments, setPrComments] = useState<PullComment[]>([]);
    const [prReviews, setPrReviews] = useState<PullReview[]>([]);
    const [prInlineComments, setPrInlineComments] = useState<PullInlineComment[]>([]);
    const [prActivityLoading, setPrActivityLoading] = useState(false);
    const [prConflictHint, setPrConflictHint] = useState<{ prNumber: number; message: string } | null>(null);
    const [prUpdateLoading, setPrUpdateLoading] = useState(false);
    const [inlineDraft, setInlineDraft] = useState<{ path: string; newPosition: number; oldPosition: number; body: string } | null>(null);
    const [inlineSubmitting, setInlineSubmitting] = useState(false);

    // Security / branch protection
    const [branchProtectionLoading, setBranchProtectionLoading] = useState(false);
    const [branchProtectionSaving, setBranchProtectionSaving] = useState(false);
    const [branchProtectionExists, setBranchProtectionExists] = useState(false);
    const [branchProtectionError, setBranchProtectionError] = useState<string | null>(null);
    const [branchProtection, setBranchProtection] = useState<BranchProtectionRule>({
        enable_status_check: true,
        status_check_contexts: [],
        required_approvals: 1,
        dismiss_stale_approvals: true,
        block_on_rejected_reviews: true,
        block_on_outdated_branch: true,
        block_on_official_review_requests: false,
        enable_force_push: false,
        require_signed_commits: false,
    });
    const [statusCheckContextsInput, setStatusCheckContextsInput] = useState('');

    // Issues / Releases
    const [issues, setIssues] = useState<IssueItem[]>([]);
    const [issuesState, setIssuesState] = useState<'open' | 'closed' | 'all'>('open');
    const [issuesPage, setIssuesPage] = useState(1);
    const [issuesHasMore, setIssuesHasMore] = useState(false);
    const [issuesLoadingMore, setIssuesLoadingMore] = useState(false);
    const [releases, setReleases] = useState<ReleaseItem[]>([]);
    const [releasesPage, setReleasesPage] = useState(1);
    const [releasesHasMore, setReleasesHasMore] = useState(false);
    const [releasesLoadingMore, setReleasesLoadingMore] = useState(false);

    // Branch create
    const [showCreateBranch, setShowCreateBranch] = useState(false);
    const [newBranchName, setNewBranchName] = useState('');
    const [baseBranch, setBaseBranch] = useState('');
    const [deleteBranchTarget, setDeleteBranchTarget] = useState<string | null>(null);
    const [deletingBranch, setDeletingBranch] = useState<string | null>(null);
    const [uiNotice, setUiNotice] = useState<{ tone: UiNoticeTone; message: string } | null>(null);

    // About sidebar
    const [repoLanguages, setRepoLanguages] = useState<Record<string, number>>({});
    const [repoTopics, setRepoTopics] = useState<string[]>([]);
    const [showCloneDropdown, setShowCloneDropdown] = useState(false);
    const [cloneCopied, setCloneCopied] = useState<string | null>(null);
    const fileRequestIdRef = useRef(0);
    const prRequestIdRef = useRef(0);

    useEffect(() => {
        if (!uiNotice) return;
        const timer = window.setTimeout(() => setUiNotice(null), 3500);
        return () => window.clearTimeout(timer);
    }, [uiNotice]);

    // ─── Health Check ───
    const checkHealth = useCallback(async () => {
        setHealth('checking');
        try {
            const data = await fetchJsonWithTimeout('/api/gitea/health', 8000);
            if (data.status === 'online') {
                setHealth('online');
                setGiteaVersion(data.version || '');
            } else { setHealth('offline'); }
        } catch (err) {
            console.error('[CICD] health check failed:', err);
            setHealth('offline');
        }
    }, []);

    // ─── Fetch Repos ───
    const fetchRepos = useCallback(async () => {
        setLoading(true);
        try {
            const data = await fetchJsonWithTimeout('/api/gitea/repos?limit=50', 10000);
            setRepos(data.repos || []);
            setError(null);
        } catch (err: unknown) {
            setError(err instanceof Error ? err.message : 'Failed to fetch repos');
        } finally { setLoading(false); }
    }, []);

    // ─── Detail Fetch Functions ───
    const fetchBranches = useCallback(async (owner: string, repo: string) => {
        try {
            const res = await fetch(`/api/gitea/repos/${owner}/${repo}/branches`);
            const data = await res.json();
            setBranches(data.branches || []);
        } catch (err) {
            console.error('[CICD] fetchBranches failed:', err);
            setBranches([]);
        }
    }, []);

    const fetchTags = useCallback(async (
        owner: string,
        repo: string,
        opts?: { page?: number; append?: boolean }
    ) => {
        const page = opts?.page ?? 1;
        const append = Boolean(opts?.append);
        const limit = 20;
        if (append) setTagsLoadingMore(true);
        try {
            const res = await fetch(`/api/gitea/repos/${owner}/${repo}/tags?limit=${limit}&page=${page}`);
            const data = await res.json();
            const list = Array.isArray(data) ? data : (data.tags || []);
            setTags(prev => {
                if (!append) return list;
                const seen = new Set(prev.map(t => t.name));
                const merged = [...prev];
                for (const tag of list) {
                    if (!seen.has(tag.name)) merged.push(tag);
                }
                return merged;
            });
            setTagsPage(page);
            setTagsHasMore(Array.isArray(list) && list.length === limit);
        } catch (err) {
            console.error('[CICD] fetchTags failed:', err);
            if (!append) {
                setTags([]);
                setTagsHasMore(false);
            }
        } finally {
            if (append) setTagsLoadingMore(false);
        }
    }, []);

    const fetchCommits = useCallback(async (
        owner: string,
        repo: string,
        ref?: string,
        opts?: { page?: number; append?: boolean }
    ) => {
        const page = opts?.page ?? 1;
        const append = Boolean(opts?.append);
        const limit = 30;
        if (append) setCommitsLoadingMore(true);
        try {
            const sha = ref ? `&sha=${encodeURIComponent(ref)}` : '';
            const res = await fetch(`/api/gitea/repos/${owner}/${repo}/commits?limit=${limit}&page=${page}${sha}`);
            const data = await res.json();
            const list = data.commits || [];
            setCommits(prev => {
                if (!append) return list;
                const seen = new Set(prev.map(c => c.sha));
                const merged = [...prev];
                for (const commit of list) {
                    if (!seen.has(commit.sha)) merged.push(commit);
                }
                return merged;
            });
            setCommitsPage(page);
            setCommitsHasMore(Array.isArray(list) && list.length === limit);
        } catch (err) {
            console.error('[CICD] fetchCommits failed:', err);
            if (!append) {
                setCommits([]);
                setCommitsHasMore(false);
            }
        } finally {
            if (append) setCommitsLoadingMore(false);
        }
    }, []);

    const fetchIssues = useCallback(async (
        owner: string,
        repo: string,
        state: string = 'open',
        opts?: { page?: number; append?: boolean }
    ) => {
        const page = opts?.page ?? 1;
        const append = Boolean(opts?.append);
        const limit = 20;
        if (append) setIssuesLoadingMore(true);
        try {
            const res = await fetch(`/api/gitea/repos/${owner}/${repo}/issues?state=${state}&limit=${limit}&page=${page}`);
            const data = await res.json();
            const list = data.issues || [];
            setIssues(prev => {
                if (!append) return list;
                const seen = new Set(prev.map(i => i.number));
                const merged = [...prev];
                for (const issue of list) {
                    if (!seen.has(issue.number)) merged.push(issue);
                }
                return merged;
            });
            setIssuesPage(page);
            setIssuesHasMore(Array.isArray(list) && list.length === limit);
        } catch (err) {
            console.error('[CICD] fetchIssues failed:', err);
            if (!append) {
                setIssues([]);
                setIssuesHasMore(false);
            }
        } finally {
            if (append) setIssuesLoadingMore(false);
        }
    }, []);

    const fetchReleases = useCallback(async (
        owner: string,
        repo: string,
        opts?: { page?: number; append?: boolean }
    ) => {
        const page = opts?.page ?? 1;
        const append = Boolean(opts?.append);
        const limit = 20;
        if (append) setReleasesLoadingMore(true);
        try {
            const res = await fetch(`/api/gitea/repos/${owner}/${repo}/releases?limit=${limit}&page=${page}`);
            const data = await res.json();
            const list = data.releases || [];
            setReleases(prev => {
                if (!append) return list;
                const seen = new Set(prev.map(r => r.id));
                const merged = [...prev];
                for (const release of list) {
                    if (!seen.has(release.id)) merged.push(release);
                }
                return merged;
            });
            setReleasesPage(page);
            setReleasesHasMore(Array.isArray(list) && list.length === limit);
        } catch (err) {
            console.error('[CICD] fetchReleases failed:', err);
            if (!append) {
                setReleases([]);
                setReleasesHasMore(false);
            }
        } finally {
            if (append) setReleasesLoadingMore(false);
        }
    }, []);

    const fetchActions = useCallback(async (
        owner: string,
        repo: string,
        opts?: { page?: number; append?: boolean }
    ) => {
        const page = opts?.page ?? 1;
        const append = Boolean(opts?.append);
        const limit = 20;
        if (append) setActionsLoadingMore(true);
        try {
            const res = await fetch(`/api/gitea/repos/${owner}/${repo}/actions/runs?limit=${limit}&page=${page}`);
            const data = await res.json();
            const runs = data.workflow_runs || data.tasks || data.task_list || [];
            const normalizedRuns = Array.isArray(runs) ? runs : [];
            setActionRuns(prev => {
                if (!append) return normalizedRuns;
                const seen = new Set(prev.map(r => r.id));
                const merged = [...prev];
                for (const run of normalizedRuns) {
                    if (!seen.has(run.id)) merged.push(run);
                }
                return merged;
            });
            setActionsPage(page);
            setActionsHasMore(normalizedRuns.length === limit);
            if (!append) {
                setSelectedActionRun(null);
                setActionJobs([]);
            }
        } catch (err) {
            console.error('[CICD] fetchActions failed:', err);
            if (!append) {
                setActionRuns([]);
                setActionsHasMore(false);
            }
        } finally {
            if (append) setActionsLoadingMore(false);
        }
    }, []);

    const fetchActionRunJobs = useCallback(async (owner: string, repo: string, runId: number) => {
        setActionJobsLoading(true);
        try {
            const res = await fetch(`/api/gitea/repos/${owner}/${repo}/actions/runs/${runId}/jobs`);
            const data = await res.json();
            const jobs = Array.isArray(data?.jobs)
                ? data.jobs
                : Array.isArray(data)
                    ? data
                    : [data];
            setActionJobs(jobs.filter(Boolean));
        } catch (err) {
            console.error('[CICD] fetchActionRunJobs failed:', err);
            setActionJobs([]);
        } finally {
            setActionJobsLoading(false);
        }
    }, []);

    const fetchPulls = useCallback(async (
        owner: string,
        repo: string,
        state: string = 'all',
        opts?: { page?: number; append?: boolean }
    ) => {
        const page = opts?.page ?? 1;
        const append = Boolean(opts?.append);
        const limit = 20;
        if (append) setPullsLoadingMore(true);
        try {
            const res = await fetch(`/api/gitea/repos/${owner}/${repo}/pulls?state=${state}&limit=${limit}&page=${page}`);
            const data = await res.json();
            const list = data.pulls || [];
            setPulls(prev => {
                if (!append) return list;
                const seen = new Set(prev.map(pr => pr.number));
                const merged = [...prev];
                for (const pr of list) {
                    if (!seen.has(pr.number)) merged.push(pr);
                }
                return merged;
            });
            setPullsPage(page);
            setPullsHasMore(Array.isArray(list) && list.length === limit);
        } catch (err) {
            console.error('[CICD] fetchPulls failed:', err);
            if (!append) {
                setPulls([]);
                setPullsHasMore(false);
            }
        } finally {
            if (append) setPullsLoadingMore(false);
        }
    }, []);

    const fetchPRChecks = useCallback(async (owner: string, repo: string, ref: string) => {
        if (!ref) {
            setPrChecksState('unknown');
            setPrChecks([]);
            return;
        }
        setPrChecksLoading(true);
        try {
            const res = await fetch(`/api/gitea/repos/${owner}/${repo}/commits/${encodeURIComponent(ref)}/status`);
            const data = await res.json();
            setPrChecksState(normalizeCheckState(data?.state));
            setPrChecks(Array.isArray(data?.statuses) ? data.statuses : []);
        } catch (err) {
            console.error('[CICD] fetchPRChecks failed:', err);
            setPrChecksState('unknown');
            setPrChecks([]);
        } finally {
            setPrChecksLoading(false);
        }
    }, []);

    const fetchPRDetail = useCallback(async (owner: string, repo: string, prNumber: number) => {
        const requestId = ++prRequestIdRef.current;
        setPrDetailLoading(true);
        try {
            const res = await fetch(`/api/gitea/repos/${owner}/${repo}/pulls/${prNumber}`);
            const data = await res.json();
            if (requestId !== prRequestIdRef.current) return;
            setPrDetail(data);
            if (data?.pull?.mergeable === false) {
                setPrConflictHint({ prNumber, message: 'PR is not mergeable with current base. Update branch from base and retry.' });
            } else {
                setPrConflictHint((prev) => (prev?.prNumber === prNumber ? null : prev));
            }
            const headRef = data?.pull?.head?.sha || data?.pull?.head?.ref || '';
            fetchPRChecks(owner, repo, headRef);
        } catch (err) {
            console.error('[CICD] fetchPRDetail failed:', err);
            if (requestId !== prRequestIdRef.current) return;
            setPrDetail(null);
            setPrChecksState('unknown');
            setPrChecks([]);
        } finally {
            if (requestId === prRequestIdRef.current) setPrDetailLoading(false);
        }
    }, [fetchPRChecks]);

    const fetchPRActivity = useCallback(async (owner: string, repo: string, prNumber: number) => {
        setPrActivityLoading(true);
        try {
            const res = await fetch(`/api/gitea/repos/${owner}/${repo}/pulls/${prNumber}/activity`);
            const data = await res.json();
            setPrComments(Array.isArray(data.comments) ? data.comments : []);
            setPrReviews(Array.isArray(data.reviews) ? data.reviews : []);
            setPrInlineComments(Array.isArray(data.inline_comments) ? data.inline_comments : []);
        } catch (err) {
            console.error('[CICD] fetchPRActivity failed:', err);
            setPrComments([]);
            setPrReviews([]);
            setPrInlineComments([]);
        } finally {
            setPrActivityLoading(false);
        }
    }, []);

    const fetchTree = useCallback(async (owner: string, repo: string, path: string = '', ref?: string) => {
        try {
            const refParam = ref ? `&ref=${encodeURIComponent(ref)}` : '';
            const res = await fetch(`/api/gitea/repos/${owner}/${repo}/tree?path=${encodeURIComponent(path)}${refParam}`);
            const data = await res.json();
            setTreeEntries(prev => ({ ...prev, [path || '/']: data.entries || [] }));
        } catch (err) {
            console.error('[CICD] fetchTree failed:', err);
            setTreeEntries(prev => ({ ...prev, [path || '/']: [] }));
        }
    }, []);

    const fetchFileContent = useCallback(async (owner: string, repo: string, path: string, ref?: string) => {
        const requestId = ++fileRequestIdRef.current;
        setFileLoading(true);
        setSelectedFile(path);
        setEditMode(false);
        try {
            const refParam = ref ? `&ref=${encodeURIComponent(ref)}` : '';
            const res = await fetch(`/api/gitea/repos/${owner}/${repo}/file?path=${encodeURIComponent(path)}${refParam}`);
            if (!res.ok) {
                const errData = await res.json().catch(() => ({}));
                if (requestId !== fileRequestIdRef.current) return;
                console.error('[CICD] fetchFileContent error:', res.status, errData);
                setFileContent(`// Error ${res.status}: Could not load file "${path}"\n// ${errData?.detail || 'Unknown error'}`);
                setEditSha('');
                return;
            }
            const data = await res.json();
            if (requestId !== fileRequestIdRef.current) return;
            const content = data.content ?? '';
            console.log('[CICD] File loaded:', path, 'length:', content.length);
            setFileContent(content);
            setEditSha(data.sha || '');
        } catch (err) {
            if (requestId !== fileRequestIdRef.current) return;
            console.error('[CICD] fetchFileContent exception:', err);
            setFileContent(`// Error loading file "${path}"`);
        }
        finally {
            if (requestId === fileRequestIdRef.current) setFileLoading(false);
        }
    }, []);

    const fetchCommitDetail = useCallback(async (owner: string, repo: string, sha: string) => {
        setCommitLoading(true);
        try {
            const res = await fetch(`/api/gitea/repos/${owner}/${repo}/commits/${sha}`);
            const data = await res.json();
            setSelectedCommit(data);
            setExpandedDiffs(new Set());
        } catch (err) {
            console.error('[CICD] fetchCommitDetail failed:', err);
            setSelectedCommit(null);
        }
        finally { setCommitLoading(false); }
    }, []);

    // ─── Fetch Languages & Topics ───
    const fetchLanguages = useCallback(async (owner: string, repo: string) => {
        try {
            const res = await fetch(`/api/gitea/repos/${owner}/${repo}/languages`);
            if (res.ok) { const d = await res.json(); setRepoLanguages(d || {}); }
        } catch (err) {
            console.error('[CICD] fetchLanguages failed:', err);
            setRepoLanguages({});
        }
    }, []);

    const fetchTopics = useCallback(async (owner: string, repo: string) => {
        try {
            const res = await fetch(`/api/gitea/repos/${owner}/${repo}/topics`);
            if (res.ok) { const d = await res.json(); setRepoTopics(d?.topics || []); }
        } catch (err) {
            console.error('[CICD] fetchTopics failed:', err);
            setRepoTopics([]);
        }
    }, []);

    // ─── Select Repo ───
    const selectRepo = useCallback(async (repo: GiteaRepo) => {
        const defaultBranch = repo.default_branch || 'main';
        setSelectedRepo(repo);
        setDetailTab('files');
        setDetailLoading(true);
        setCurrentPath('');
        setSelectedFile(null);
        setFileContent('');
        setSelectedCommit(null);
        setTreeEntries({});
        setRepoLanguages({});
        setRepoTopics([]);

        setActiveBranch(defaultBranch);
        setShowBranchDropdown(false);
        setSelectedPR(null);
        setPrDetail(null);
        setPrComments([]);
        setPrReviews([]);
        setPrInlineComments([]);
        setInlineDraft(null);
        setPrConflictHint(null);
        setPrChecksState('unknown');
        setPrChecks([]);
        setIssuesState('open');
        setIssues([]);
        setReleases([]);
        setCommitsPage(1);
        setTagsPage(1);
        setIssuesPage(1);
        setReleasesPage(1);
        setPullsPage(1);
        setActionsPage(1);
        setCommitsHasMore(false);
        setTagsHasMore(false);
        setIssuesHasMore(false);
        setReleasesHasMore(false);
        setPullsHasMore(false);
        setActionsHasMore(false);
        setSelectedActionRun(null);
        setActionJobs([]);
        const [owner, name] = repo.full_name.split('/');
        try {
            await Promise.all([
                fetchTree(owner, name, '', defaultBranch),
                fetchBranches(owner, name),
                fetchCommits(owner, name, defaultBranch, { page: 1 }),
                fetchPulls(owner, name, 'all', { page: 1 }),
                fetchActions(owner, name, { page: 1 }),
                fetchTags(owner, name, { page: 1 }),
                fetchIssues(owner, name, 'open', { page: 1 }),
                fetchReleases(owner, name, { page: 1 }),
                fetchLanguages(owner, name),
                fetchTopics(owner, name),
            ]);
        } finally {
            setDetailLoading(false);
        }
    }, [fetchTree, fetchBranches, fetchCommits, fetchPulls, fetchActions, fetchTags, fetchIssues, fetchReleases, fetchLanguages, fetchTopics]);

    // ─── Switch Branch ───
    const switchBranch = useCallback(async (branchName: string) => {
        if (!selectedRepo) return;
        const [owner, name] = selectedRepo.full_name.split('/');
        setActiveBranch(branchName);
        setShowBranchDropdown(false);
        setCurrentPath('');
        setSelectedFile(null);
        setFileContent('');
        setTreeEntries({});
        setCommitsPage(1);
        setCommitsHasMore(false);
        setSelectedCommit(null);

        await Promise.all([
            fetchTree(owner, name, '', branchName),
            fetchCommits(owner, name, branchName, { page: 1 }),
        ]);
    }, [selectedRepo, fetchTree, fetchCommits]);

    // ─── Delete Branch ───
    const handleDeleteBranch = useCallback(async (branchName: string) => {
        if (!selectedRepo) return;
        const [owner, name] = selectedRepo.full_name.split('/');
        console.log('Deleting branch:', owner, name, branchName);
        setDeletingBranch(branchName);
        try {
            const res = await fetch(
                `/api/gitea/repos/${owner}/${name}/branches?branch=${encodeURIComponent(branchName)}`,
                { method: 'DELETE' }
            );
            if (!res.ok) {
                const err = await res.json().catch(() => ({ detail: res.statusText }));
                setUiNotice({ tone: 'error', message: `Failed to delete branch: ${err.detail || res.statusText}` });
                return;
            }
            if (activeBranch === branchName) await switchBranch(selectedRepo.default_branch || 'main');
            fetchBranches(owner, name);
            setUiNotice({ tone: 'success', message: `Branch "${branchName}" deleted` });
        } catch (err) {
            console.error('[CICD] handleDeleteBranch failed:', err);
            setUiNotice({ tone: 'error', message: `Failed to delete branch: ${err instanceof Error ? err.message : 'Unknown error'}` });
        } finally {
            setDeletingBranch((current) => (current === branchName ? null : current));
        }
    }, [selectedRepo, activeBranch, switchBranch, fetchBranches]);

    // ─── Save File (inline edit) ───
    const handleSaveFile = useCallback(async () => {
        if (!selectedRepo || !selectedFile || !editMessage) return;
        const [owner, name] = selectedRepo.full_name.split('/');
        setEditSaving(true);
        try {
            const res = await fetch(`/api/gitea/repos/${owner}/${name}/file`, {
                method: 'PUT',
                headers: { 'Content-Type': 'application/json' },
                body: JSON.stringify({
                    path: selectedFile,
                    content: encodeBase64Utf8(editContent),
                    message: editMessage,
                    sha: editSha,
                    branch: activeBranch,
                }),
            });
            if (!res.ok) {
                const err = await res.json().catch(() => ({ detail: res.statusText }));
                alert(`Failed to save: ${err.detail || res.statusText}`);
                return;
            }
            setEditMode(false);
            setEditMessage('');
            await fetchFileContent(owner, name, selectedFile, activeBranch);
            await fetchCommits(owner, name, activeBranch);
        } catch (e) {
            alert(`Failed to save: ${e instanceof Error ? e.message : 'Unknown error'}`);
        } finally { setEditSaving(false); }
    }, [selectedRepo, selectedFile, editContent, editMessage, editSha, activeBranch, fetchFileContent, fetchCommits]);

    // ─── Delete File ───
    const handleDeleteFile = useCallback(async () => {
        if (!selectedRepo || !selectedFile) return;
        if (!confirm(`Delete file "${selectedFile}"? This cannot be undone.`)) return;
        const [owner, name] = selectedRepo.full_name.split('/');
        setFileLoading(true);
        try {
            const res = await fetch(`/api/gitea/repos/${owner}/${name}/file`, {
                method: 'DELETE',
                headers: { 'Content-Type': 'application/json' },
                body: JSON.stringify({
                    path: selectedFile,
                    message: `Delete ${selectedFile}`,
                    sha: editSha,
                    branch: activeBranch,
                }),
            });
            if (!res.ok) {
                const err = await res.json().catch(() => ({ detail: res.statusText }));
                alert(`Failed to delete file: ${err.detail || res.statusText}`);
                return;
            }
            setSelectedFile(null);
            setFileContent('');
            await fetchTree(owner, name, '', activeBranch);
            await fetchCommits(owner, name, activeBranch);
        } catch (e) {
            alert(`Failed to delete file: ${e instanceof Error ? e.message : 'Unknown error'}`);
        } finally {
            setFileLoading(false);
        }
    }, [selectedRepo, selectedFile, editSha, activeBranch, fetchTree, fetchCommits]);

    // ─── Effects ───
    useEffect(() => { checkHealth(); fetchRepos(); }, [checkHealth, fetchRepos]);

    // Failsafe: never keep the page in infinite "checking/loading" state.
    useEffect(() => {
        if (!loading && health !== 'checking') return;
        const t = window.setTimeout(() => {
            setLoading(false);
            setHealth(prev => (prev === 'checking' ? 'offline' : prev));
            setError(prev => prev ?? 'Request timeout while loading repositories');
        }, 12000);
        return () => window.clearTimeout(t);
    }, [loading, health]);

    useEffect(() => {
        const handler = (e: Event) => {
            const detail = (e as CustomEvent).detail;
            if (detail?.repo) {
                const found = repos.find(r => r.full_name === detail.repo);
                if (found) selectRepo(found);
            }
        };
        window.addEventListener('openclaw:select-repo', handler);
        return () => window.removeEventListener('openclaw:select-repo', handler);
    }, [repos, selectRepo]);

    useEffect(() => {
        if (!selectedRepo) return;
        const [owner, name] = selectedRepo.full_name.split('/');
        const interval = setInterval(() => fetchActions(owner, name, { page: 1 }), 15000);
        return () => clearInterval(interval);
    }, [selectedRepo, fetchActions]);

    // ─── Handlers ───
    const handleDirClick = async (path: string) => {
        if (!selectedRepo) return;
        setCurrentPath(path);
        setSelectedFile(null);
        if (!treeEntries[path]) {
            const [owner, name] = selectedRepo.full_name.split('/');
            await fetchTree(owner, name, path, activeBranch);
        }
    };

    const handleFileClick = (path: string) => {
        if (!selectedRepo) return;
        const [owner, name] = selectedRepo.full_name.split('/');
        fetchFileContent(owner, name, path, activeBranch);
    };

    const handleCommitClick = (sha: string) => {
        if (!selectedRepo) return;
        const [owner, name] = selectedRepo.full_name.split('/');
        fetchCommitDetail(owner, name, sha);
    };

    const handlePRClick = (prNumber: number) => {
        if (!selectedRepo) return;
        const [owner, name] = selectedRepo.full_name.split('/');
        const newSelected = selectedPR === prNumber ? null : prNumber;
        setSelectedPR(newSelected);
        setPrDetail(null);
        setPrComments([]);
        setPrReviews([]);
        setPrChecksState('unknown');
        setPrChecks([]);
        if (newSelected !== null) {
            fetchPRDetail(owner, name, newSelected);
            fetchPRActivity(owner, name, newSelected);
        }
    };

    const handleCreateBranch = async () => {
        if (!selectedRepo || !newBranchName) return;
        const [owner, name] = selectedRepo.full_name.split('/');
        try {
            const res = await fetch(`/api/gitea/repos/${owner}/${name}/branches`, {
                method: 'POST',
                headers: { 'Content-Type': 'application/json' },
                body: JSON.stringify({
                    new_branch_name: newBranchName,
                    old_branch_name: baseBranch || selectedRepo.default_branch,
                }),
            });
            if (!res.ok) {
                const err = await res.json().catch(() => ({ detail: res.statusText }));
                alert(`Failed to create branch: ${err.detail || res.statusText}`);
                return;
            }
            setNewBranchName('');
            setShowCreateBranch(false);
            fetchBranches(owner, name);
        } catch (e) {
            alert(`Failed to create branch: ${e instanceof Error ? e.message : 'Unknown error'}`);
        }
    };

    const handleCreatePR = async () => {
        if (!selectedRepo || !prTitle || !prHead || !prBase) return;
        const [owner, name] = selectedRepo.full_name.split('/');
        try {
            const res = await fetch(`/api/gitea/repos/${owner}/${name}/pulls`, {
                method: 'POST',
                headers: { 'Content-Type': 'application/json' },
                body: JSON.stringify({ title: prTitle, body: prBody, head: prHead, base: prBase }),
            });
            if (!res.ok) {
                const err = await res.json().catch(() => ({ detail: res.statusText }));
                alert(`Failed to create PR: ${err.detail || res.statusText}`);
                return;
            }
            setPrTitle(''); setPrBody(''); setPrHead(''); setPrBase('');
            setShowCreatePR(false);
            fetchPulls(owner, name, prState, { page: 1 });
        } catch (e) {
            alert(`Failed to create PR: ${e instanceof Error ? e.message : 'Unknown error'}`);
        }
    };

    const handleMergePR = async (prNumber: number, style: string = 'merge') => {
        if (!selectedRepo) return;
        const [owner, name] = selectedRepo.full_name.split('/');
        const styleLabel = style.charAt(0).toUpperCase() + style.slice(1);
        if (!window.confirm(`Merge PR #${prNumber} using "${styleLabel}" strategy?`)) return;
        try {
            const res = await fetch(`/api/gitea/repos/${owner}/${name}/pulls/${prNumber}/merge`, {
                method: 'POST',
                headers: { 'Content-Type': 'application/json' },
                body: JSON.stringify({ merge_message_field_style: style, delete_branch_after_merge: false }),
            });
            if (!res.ok) {
                const err = await res.json().catch(() => ({ detail: res.statusText }));
                const detail = String(err.detail || res.statusText || 'merge failed');
                const lower = detail.toLowerCase();
                if (res.status === 409 || lower.includes('conflict') || lower.includes('not mergeable')) {
                    setPrConflictHint({ prNumber, message: detail });
                }
                alert(`Failed to merge PR: ${detail}`);
                return;
            }
            setPrConflictHint(null);
            alert(`PR #${prNumber} merged successfully!`);
            fetchPRDetail(owner, name, prNumber);
            fetchPRActivity(owner, name, prNumber);
            fetchPulls(owner, name, prState, { page: 1 });
        } catch (e) {
            alert(`Failed to merge PR: ${e instanceof Error ? e.message : 'Unknown error'}`);
        }
    };

    const handleClosePR = async (prNumber: number) => {
        if (!selectedRepo) return;
        const [owner, name] = selectedRepo.full_name.split('/');
        if (!window.confirm(`Close PR #${prNumber}?`)) return;
        try {
            const res = await fetch(`/api/gitea/repos/${owner}/${name}/pulls/${prNumber}/close`, {
                method: 'PATCH',
            });
            if (!res.ok) {
                const err = await res.json().catch(() => ({ detail: res.statusText }));
                alert(`Failed to close PR: ${err.detail || res.statusText}`);
                return;
            }
            const data = await res.json();
            alert(`PR #${prNumber} Closed!`);
            setPrDetail(prev => prev ? { ...prev, pull: data.pull } : null);
            fetchPRActivity(owner, name, prNumber);
            fetchPulls(owner, name, prState, { page: 1 });
        } catch (e) {
            alert(`Failed to close PR: ${e instanceof Error ? e.message : 'Unknown error'}`);
        }
    };

    const submitPRReview = async (prNumber: number, event: 'APPROVED' | 'REQUEST_CHANGES' | 'COMMENT', body: string) => {
        if (!selectedRepo) return;
        const [owner, name] = selectedRepo.full_name.split('/');
        try {
            const res = await fetch(`/api/gitea/repos/${owner}/${name}/pulls/${prNumber}/reviews`, {
                method: 'POST',
                headers: { 'Content-Type': 'application/json' },
                body: JSON.stringify({ event, body }),
            });
            if (!res.ok) {
                const err = await res.json().catch(() => ({ detail: res.statusText || 'unknown error' }));
                alert(`Failed to submit review: ${err.detail || res.statusText}`);
                return;
            }
            const actionLabel = event === 'APPROVED' ? 'approved' : event === 'REQUEST_CHANGES' ? 'requested changes on' : 'commented on';
            alert(`PR #${prNumber} ${actionLabel} ✅`);
            fetchPRDetail(owner, name, prNumber);
            fetchPRActivity(owner, name, prNumber);
        } catch (e) {
            alert(`Failed to submit review: ${e instanceof Error ? e.message : 'Unknown error'}`);
        }
    };

    const handleUpdatePRBranch = async (prNumber: number, style: 'merge' | 'rebase') => {
        if (!selectedRepo) return;
        const [owner, name] = selectedRepo.full_name.split('/');
        setPrUpdateLoading(true);
        try {
            const res = await fetch(`/api/gitea/repos/${owner}/${name}/pulls/${prNumber}/update?style=${style}`, {
                method: 'POST',
            });
            if (!res.ok) {
                const err = await res.json().catch(() => ({ detail: res.statusText }));
                alert(`Failed to update PR branch: ${err.detail || res.statusText}`);
                return;
            }
            setPrConflictHint(null);
            setUiNotice({ tone: 'success', message: `PR #${prNumber} branch updated with ${style}` });
            fetchPRDetail(owner, name, prNumber);
            fetchPRActivity(owner, name, prNumber);
            fetchPulls(owner, name, prState, { page: 1 });
        } catch (e) {
            alert(`Failed to update PR branch: ${e instanceof Error ? e.message : 'Unknown error'}`);
        } finally {
            setPrUpdateLoading(false);
        }
    };

    const handleSubmitInlineComment = async (prNumber: number) => {
        if (!selectedRepo || !inlineDraft) return;
        const [owner, name] = selectedRepo.full_name.split('/');
        const message = inlineDraft.body.trim();
        if (!message) return;
        setInlineSubmitting(true);
        try {
            const res = await fetch(`/api/gitea/repos/${owner}/${name}/pulls/${prNumber}/inline-comments`, {
                method: 'POST',
                headers: { 'Content-Type': 'application/json' },
                body: JSON.stringify({
                    path: inlineDraft.path,
                    new_position: inlineDraft.newPosition,
                    old_position: inlineDraft.oldPosition,
                    body: message,
                    commit_id: prDetail?.pull?.head?.sha || '',
                }),
            });
            if (!res.ok) {
                const err = await res.json().catch(() => ({ detail: res.statusText }));
                alert(`Failed to add inline comment: ${err.detail || res.statusText}`);
                return;
            }
            setUiNotice({ tone: 'success', message: 'Inline comment added' });
            setInlineDraft(null);
            fetchPRActivity(owner, name, prNumber);
        } catch (e) {
            alert(`Failed to add inline comment: ${e instanceof Error ? e.message : 'Unknown error'}`);
        } finally {
            setInlineSubmitting(false);
        }
    };

    const handleActionRunCommand = async (runId: number, action: 'rerun' | 'cancel') => {
        if (!selectedRepo) return;
        const [owner, name] = selectedRepo.full_name.split('/');
        setActionRunCommand({ runId, action });
        try {
            const res = await fetch(`/api/gitea/repos/${owner}/${name}/actions/runs/${runId}/${action}`, {
                method: 'POST',
            });
            if (!res.ok) {
                const err = await res.json().catch(() => ({ detail: res.statusText }));
                alert(`Failed to ${action} run #${runId}: ${err.detail || res.statusText}`);
                return;
            }
            setUiNotice({ tone: 'success', message: action === 'rerun' ? `Run #${runId} rerun dispatched` : `Run #${runId} cancelled` });
            fetchActions(owner, name, { page: 1 });
        } catch (e) {
            alert(`Failed to ${action} run #${runId}: ${e instanceof Error ? e.message : 'Unknown error'}`);
        } finally {
            setActionRunCommand(null);
        }
    };

    const handleApprovePR = async (prNumber: number) => {
        await submitPRReview(prNumber, 'APPROVED', 'LGTM ✅');
    };

    const handleRequestChangesPR = async (prNumber: number) => {
        const reason = window.prompt('Why are you requesting changes? (optional)', 'Please address review feedback.');
        if (reason === null) return;
        await submitPRReview(prNumber, 'REQUEST_CHANGES', reason);
    };

    const handleCommentPR = async (prNumber: number) => {
        const comment = window.prompt('Add review comment');
        if (!comment) return;
        await submitPRReview(prNumber, 'COMMENT', comment);
    };

    const loadBranchProtection = useCallback(async (owner: string, repo: string, branchName: string) => {
        setBranchProtectionLoading(true);
        setBranchProtectionError(null);
        try {
            const res = await fetch(`/api/gitea/repos/${owner}/${repo}/branch-protections`);
            const data = await res.json();
            const list = Array.isArray(data.protections) ? data.protections : [];
            const rule = list.find((item: BranchProtectionRule) =>
                item.rule_name === branchName || item.branch_name === branchName
            ) as BranchProtectionRule | undefined;
            if (!rule) {
                setBranchProtectionExists(false);
                setBranchProtection({
                    branch_name: branchName,
                    rule_name: branchName,
                    enable_status_check: true,
                    status_check_contexts: [],
                    required_approvals: 1,
                    dismiss_stale_approvals: true,
                    block_on_rejected_reviews: true,
                    block_on_outdated_branch: true,
                    block_on_official_review_requests: false,
                    enable_force_push: false,
                    require_signed_commits: false,
                });
                setStatusCheckContextsInput('');
                return;
            }
            setBranchProtectionExists(true);
            setBranchProtection({
                branch_name: branchName,
                rule_name: rule.rule_name || rule.branch_name || branchName,
                enable_status_check: Boolean(rule.enable_status_check),
                status_check_contexts: Array.isArray(rule.status_check_contexts) ? rule.status_check_contexts : [],
                required_approvals: Number(rule.required_approvals ?? 1),
                dismiss_stale_approvals: Boolean(rule.dismiss_stale_approvals),
                block_on_rejected_reviews: Boolean(rule.block_on_rejected_reviews),
                block_on_outdated_branch: Boolean(rule.block_on_outdated_branch),
                block_on_official_review_requests: Boolean(rule.block_on_official_review_requests),
                enable_force_push: Boolean(rule.enable_force_push),
                require_signed_commits: Boolean(rule.require_signed_commits),
            });
            setStatusCheckContextsInput((Array.isArray(rule.status_check_contexts) ? rule.status_check_contexts : []).join(', '));
        } catch (err) {
            setBranchProtectionError(err instanceof Error ? err.message : 'Failed to load branch protection');
        } finally {
            setBranchProtectionLoading(false);
        }
    }, []);

    const saveBranchProtection = async () => {
        if (!selectedRepo) return;
        const [owner, name] = selectedRepo.full_name.split('/');
        const branchName = selectedRepo.default_branch || 'main';
        const contexts = statusCheckContextsInput
            .split(',')
            .map((s) => s.trim())
            .filter(Boolean);
        setBranchProtectionSaving(true);
        setBranchProtectionError(null);
        try {
            const payload = {
                branch_name: branchName,
                rule_name: branchProtection.rule_name || branchName,
                enable_status_check: Boolean(branchProtection.enable_status_check),
                status_check_contexts: contexts,
                required_approvals: Math.max(0, Number(branchProtection.required_approvals ?? 0)),
                dismiss_stale_approvals: Boolean(branchProtection.dismiss_stale_approvals),
                block_on_rejected_reviews: Boolean(branchProtection.block_on_rejected_reviews),
                block_on_outdated_branch: Boolean(branchProtection.block_on_outdated_branch),
                block_on_official_review_requests: Boolean(branchProtection.block_on_official_review_requests),
                enable_force_push: Boolean(branchProtection.enable_force_push),
                require_signed_commits: Boolean(branchProtection.require_signed_commits),
            };
            const res = await fetch(`/api/gitea/repos/${owner}/${name}/branch-protections/${encodeURIComponent(branchName)}`, {
                method: 'PUT',
                headers: { 'Content-Type': 'application/json' },
                body: JSON.stringify(payload),
            });
            if (!res.ok) {
                const err = await res.json().catch(() => ({ detail: res.statusText }));
                setBranchProtectionError(err.detail || res.statusText);
                return;
            }
            setBranchProtectionExists(true);
            setUiNotice({ tone: 'success', message: `Branch protection saved for ${branchName}` });
            await loadBranchProtection(owner, name, branchName);
        } catch (err) {
            setBranchProtectionError(err instanceof Error ? err.message : 'Failed to save branch protection');
        } finally {
            setBranchProtectionSaving(false);
        }
    };

    useEffect(() => {
        if (!selectedRepo || detailTab !== 'security') return;
        const [owner, name] = selectedRepo.full_name.split('/');
        const branchName = selectedRepo.default_branch || 'main';
        loadBranchProtection(owner, name, branchName);
    }, [selectedRepo, detailTab, loadBranchProtection]);


    // ─── Filter Repos ───
    const filteredRepos = repos.filter(r =>
        !search || r.full_name.toLowerCase().includes(search.toLowerCase()) ||
        (r.description || '').toLowerCase().includes(search.toLowerCase())
    );

    // ─── Fetch README ───
    useEffect(() => {
        const loadReadme = async () => {
            if (!selectedRepo) return;
            const entries = treeEntries[currentPath || '/'] || [];
            const readmeEntry = entries.find(e => e.name.toLowerCase() === 'readme.md' && e.type === 'file');
                if (readmeEntry) {
                    const [owner, name] = selectedRepo.full_name.split('/');
                    const refParam = activeBranch ? `&ref=${encodeURIComponent(activeBranch)}` : '';
                    try {
                        const res = await fetch(`/api/gitea/repos/${owner}/${name}/file?path=${encodeURIComponent(readmeEntry.path)}${refParam}`);
                        const data = await res.json();
                        setReadmeContent(data.content || '');
                    } catch (err) {
                        console.error('[CICD] README load failed:', err);
                        setReadmeContent(null);
                    }
                } else {
                    setReadmeContent(null);
                }
        };
        loadReadme();
    }, [treeEntries, currentPath, selectedRepo, activeBranch]);



    // ─── Tabs Config ───
    const tabs: { key: DetailTab; label: string; icon: typeof GitBranch; count?: number }[] = [
        { key: 'files', label: 'Files', icon: Folder },
        { key: 'commits', label: 'Commits', icon: GitCommit, count: commits.length },
        { key: 'pulls', label: 'PRs', icon: GitPullRequest, count: pulls.length },
        { key: 'issues', label: 'Issues', icon: AlertCircle, count: issues.length },
        { key: 'branches', label: 'Branches', icon: GitBranch, count: branches.length },
        { key: 'tags', label: 'Tags', icon: Star, count: tags.length },
        { key: 'releases', label: 'Releases', icon: PackageOpen, count: releases.length },
        { key: 'actions', label: 'Actions', icon: Play, count: actionRuns.length },
        { key: 'security', label: 'Security', icon: Scale },
        { key: 'insights', label: 'Insights', icon: BookOpen },
    ];

    // ─── Create File ───
    const [showCreateFile, setShowCreateFile] = useState(false);
    const [newFilePath, setNewFilePath] = useState('');

    const handleCreateFile = async () => {
        if (!selectedRepo || !newFilePath) return;
        const [owner, name] = selectedRepo.full_name.split('/');
        try {
            // Use POST for creation
            const res = await fetch(`/api/gitea/repos/${owner}/${name}/file`, {
                method: 'POST',
                headers: { 'Content-Type': 'application/json' },
                body: JSON.stringify({
                    path: newFilePath,
                    content: encodeBase64Utf8(''),
                    message: `Create ${newFilePath}`,
                    sha: '',
                    branch: activeBranch,
                }),
            });
            if (!res.ok) {
                const err = await res.json().catch(() => ({ detail: res.statusText }));
                alert(`Failed to create file: ${err.detail || res.statusText}`);
                return;
            }
            alert(`File "${newFilePath}" created!`);
            setShowCreateFile(false);
            setNewFilePath('');
            // Refresh tree and select the new file
            await fetchTree(owner, name, '', activeBranch);
            // Optionally select it for editing immediately
            // handleFileClick(newFilePath); 
        } catch (e) {
            alert(`Failed to create file: ${e instanceof Error ? e.message : 'Unknown error'}`);
        }
    };


    return (
        <div className="flex h-screen bg-[#09090b] text-foreground font-sans overflow-hidden relative">
            {/* Ambient Lighting */}
            <div className="absolute top-0 left-0 w-[800px] h-[800px] bg-obsidian-purple/[0.04] rounded-full blur-[120px] pointer-events-none -translate-x-1/4 -translate-y-1/4" />
            <div className="absolute bottom-0 right-0 w-[600px] h-[600px] bg-obsidian-info/[0.03] rounded-full blur-[100px] pointer-events-none translate-x-1/4 translate-y-1/4" />

            {/* Sidebar */}
            <div className="relative z-10 shrink-0">
                {!isFullscreen && <Sidebar />}
            </div>

            <main className="flex-1 flex flex-col min-w-0 overflow-hidden relative z-10">
                {/* ─── Top Bar ─── */}
                <div className="flex items-center px-4 justify-between shrink-0 h-10 bg-black/40 backdrop-blur-md border-b border-white/5 z-10 w-full relative">
                    {/* Top Edge Gradient Branding */}
                    <div className="absolute top-0 left-0 right-0 h-[1px] bg-gradient-to-r from-transparent via-obsidian-info/30 to-transparent opacity-50"></div>

                    <div className="flex items-center gap-3 text-[12px]">
                        <button
                            onClick={() => window.dispatchEvent(new CustomEvent('openclaw:toggle-sidebar'))}
                            className="p-1.5 hover:bg-white/10 rounded-md text-obsidian-muted hover:text-white transition-all active:scale-95 border border-transparent hover:border-obsidian-border/50"
                            title="Toggle Explorer"
                        >
                            <LayoutPanelLeft className="w-4 h-4 drop-shadow-[0_0_8px_rgba(255,255,255,0.2)]" />
                        </button>
                        <div className="w-[1px] h-4 bg-obsidian-border/50"></div>
                        <GitPullRequest className="w-3.5 h-3.5" style={{ color: '#a78bfa' }} />
                        <span className="text-foreground font-medium">Source Control</span>
                        <span className="text-obsidian-muted">·</span>
                        <span className="text-obsidian-muted">Gitea {giteaVersion && `v${giteaVersion}`}</span>
                        <span className="text-obsidian-muted">·</span>
                        <HealthDot status={health} />
                        <span className="text-obsidian-muted">·</span>
                        <span className="text-[10px] text-obsidian-muted">{repos.length} repos</span>
                    </div>
                    <div className="flex items-center gap-1.5">
                        <button onClick={() => { checkHealth(); fetchRepos(); }} className="p-1 hover:bg-obsidian-border-active rounded text-obsidian-muted hover:text-foreground transition-colors" title="Refresh">
                            <RefreshCw className={clsx("w-3.5 h-3.5", loading && "animate-spin")} />
                        </button>
                        <button onClick={() => setIsFullscreen(!isFullscreen)} className="p-1 hover:bg-obsidian-border-active rounded text-obsidian-muted hover:text-foreground transition-colors">
                            {isFullscreen ? <Minimize2 className="w-3.5 h-3.5" /> : <Maximize2 className="w-3.5 h-3.5" />}
                        </button>
                        <a href={GITEA_URL} target="_blank" rel="noopener noreferrer" className="p-1 hover:bg-obsidian-border-active rounded text-obsidian-muted hover:text-foreground transition-colors" title="Open Gitea">
                            <ExternalLink className="w-3.5 h-3.5" />
                        </a>
                    </div>
                </div>

                {/* ─── Main Content ─── */}
                <div className="flex-1 flex overflow-hidden">

                    {/* ─── Welcome State (no repo selected) ─── */}
                    {!selectedRepo ? (
                        <div className="flex-1 flex flex-col items-center justify-center gap-4 text-obsidian-muted">
                            {health === 'offline' ? (
                                <div className="flex flex-col items-center gap-3">
                                    <GitPullRequest className="w-12 h-12 opacity-40" style={{ color: '#a78bfa' }} />
                                    <p className="text-[13px] text-obsidian-muted">Gitea is not running</p>
                                    <code className="text-[10px] text-obsidian-danger font-mono bg-obsidian-panel rounded px-3 py-1.5 transition-all active:scale-95">docker compose up -d gitea</code>
                                    <button onClick={() => { checkHealth(); fetchRepos(); }} className="px-4 py-1.5 bg-obsidian-danger/15 text-obsidian-danger rounded text-[11px] font-medium hover:bg-obsidian-danger/25 transition-all active:scale-95">Retry</button>
                                </div>
                            ) : loading ? (
                                <Loader2 className="w-6 h-6 text-obsidian-danger animate-spin" />
                            ) : (
                                <>
                                    <GitPullRequest className="w-12 h-12 opacity-20" />
                                    <p className="text-[14px] text-obsidian-muted font-medium">Select a repository</p>
                                    <p className="text-[11px] max-w-[280px] text-center leading-relaxed">
                                        Choose a repository from the sidebar to browse files, view commits, manage branches, and track CI/CD runs.
                                    </p>
                                </>
                            )}
                        </div>
                    ) : (
                        /* ─── Detail Area ─── */
                        <div className="flex-1 flex flex-col min-w-0 bg-transparent">
                            {/* GitHub-style Repo Header */}
                            <div className="bg-black/20 backdrop-blur-md border-b border-white/5 shrink-0 flex flex-col pt-4">
                                {/* Top Row - Title & Actions */}
                                <div className="flex items-start justify-between px-6 mb-4">
                                    <div className="flex items-center gap-3 min-w-0">
                                        <GitPullRequest className="w-5 h-5 shrink-0" style={{ color: '#a78bfa' }} />
                                        <div className="flex items-center gap-1.5 text-[18px] text-foreground truncate">
                                            <span className="text-obsidian-info hover:underline cursor-pointer">{selectedRepo.owner.login}</span>
                                            <span className="text-obsidian-muted">/</span>
                                            <span className="font-bold text-obsidian-info hover:underline cursor-pointer">{selectedRepo.name}</span>
                                        </div>
                                        <span className="border border-obsidian-border text-obsidian-muted text-[10px] px-2 py-0.5 rounded-full font-medium ml-2">
                                            Public
                                        </span>
                                    </div>

                                    {/* Action Buttons */}
                                    <div className="flex items-center gap-2">
                                        {/* Utility Buttons */}
                                        <div className="flex items-center gap-1">
                                            <button
                                                onClick={() => { navigator.clipboard.writeText(selectedRepo.html_url + '.git'); }}
                                                className="p-1.5 hover:bg-obsidian-border-active rounded-md text-obsidian-muted hover:text-foreground transition-colors"
                                                title="Copy clone URL"
                                            >
                                                <Code2 className="w-3.5 h-3.5" />
                                            </button>
                                            <a href={selectedRepo.html_url} target="_blank" rel="noopener noreferrer"
                                                className="p-1.5 hover:bg-obsidian-border-active rounded-md text-obsidian-muted hover:text-foreground transition-colors">
                                                <ExternalLink className="w-3.5 h-3.5" />
                                            </a>
                                            <button onClick={() => { setSelectedRepo(null); setSelectedFile(null); setFileContent(''); setSelectedCommit(null); }}
                                                className="p-1.5 hover:bg-obsidian-danger/20 rounded-md text-obsidian-muted hover:text-obsidian-danger transition-colors ml-1">
                                                <X className="w-4 h-4" />
                                            </button>
                                        </div>
                                    </div>
                                </div>

                                {/* Tabs Row */}
                                <div className="flex px-4 overflow-x-auto no-scrollbar">
                                    {tabs.map(tab => (
                                        <button key={tab.key} onClick={() => setDetailTab(tab.key)}
                                            className={clsx("flex items-center gap-1.5 px-4 py-2.5 text-[12px] font-medium transition-colors border-b-2 whitespace-nowrap",
                                                detailTab === tab.key
                                                    ? "text-foreground border-obsidian-info bg-white/[0.04]"
                                                    : "text-obsidian-muted border-transparent hover:text-foreground hover:bg-white/[0.02] hover:border-white/5"
                                            )}>
                                            <tab.icon className="w-4 h-4" />
                                            {tab.label}
                                            {tab.count !== undefined && tab.count > 0 && (
                                                <span className="text-[10px] bg-white/5 rounded-full px-2 py-0.5 text-obsidian-muted font-semibold transition-all active:scale-95">{tab.count}</span>
                                            )}
                                        </button>
                                    ))}
                                </div>
                            </div>

                            {/* Tab Content */}
                            <div className="flex-1 flex overflow-hidden">
                                {detailLoading ? (
                                    <div className="flex-1 flex items-center justify-center"><Loader2 className="w-6 h-6 text-obsidian-info animate-spin" /></div>
                                ) : detailTab === 'files' ? (
                                    /* ─── Files Tab ─── */
                                    <div className="flex-1 flex flex-col overflow-auto items-center py-6 backdrop-blur-sm">
                                        <div className="w-full max-w-[1240px] px-6">
                                            {!selectedFile ? (
                                                /* File Explorer */
                                                <div className="flex gap-8">
                                                    {/* ─── Left Column: File Table ─── */}
                                                    <div className="flex-1 min-w-0 flex flex-col gap-4">
                                                        {/* Top Action Bar */}
                                                        <div className="flex items-center justify-between">
                                                            <div className="flex items-center gap-3">
                                                                {/* Breadcrumbs based on currentPath */}
                                                                <div className="flex items-center text-[14px] font-bold text-obsidian-info">
                                                                    <span className="hover:underline cursor-pointer" onClick={() => handleDirClick('')}>
                                                                        {selectedRepo.name}
                                                                    </span>
                                                                    {currentPath.split('/').filter(Boolean).map((part, i, arr) => {
                                                                        const path = arr.slice(0, i + 1).join('/');
                                                                        return (
                                                                            <React.Fragment key={path}>
                                                                                <span className="text-obsidian-muted px-1.5 font-normal">/</span>
                                                                                <span className="hover:underline cursor-pointer" onClick={() => handleDirClick(path)}>
                                                                                    {part}
                                                                                </span>
                                                                            </React.Fragment>
                                                                        );
                                                                    })}
                                                                </div>
                                                            </div>
                                                            <div className="flex items-center gap-3">
                                                                {/* Branch Selector */}
                                                                <div className="relative flex items-center bg-white/[0.02] border border-white/10 rounded-md px-3 py-1.5 transition-colors shadow-sm hover:border-obsidian-info/50 group">
                                                                    <GitBranch className="w-3.5 h-3.5 text-obsidian-muted mr-2 group-hover:text-obsidian-info transition-colors" />
                                                                    <select
                                                                        className="bg-transparent text-[11px] font-medium text-foreground outline-none cursor-pointer appearance-none pr-5 min-w-[70px]"
                                                                        value={activeBranch || ''}
                                                                        onChange={e => switchBranch(e.target.value)}
                                                                    >
                                                                        {branches.map(b => <option key={b.name} value={b.name} className="bg-obsidian-panel">{b.name}</option>)}
                                                                    </select>
                                                                    <ChevronDown className="w-3 h-3 text-obsidian-muted absolute right-2.5 pointer-events-none group-hover:text-obsidian-info transition-colors" />
                                                                </div>

                                                                <div className="relative">
                                                                    <button
                                                                        onClick={() => setShowCreateFile(!showCreateFile)}
                                                                        className="px-3 py-1.5 text-[11px] font-medium text-foreground bg-white/[0.02] border border-white/10 rounded-md hover:bg-white/[0.05] transition-all active:scale-95 flex items-center gap-1.5 shadow-sm"
                                                                    >
                                                                        Add file <ChevronDown className="w-3 h-3 opacity-60 ml-0.5" />
                                                                    </button>
                                                                    {showCreateFile && (
                                                                        <div className="absolute right-0 top-full mt-1 w-[240px] bg-[#1a1b1e]/90 backdrop-blur-xl border border-white/10 shadow-2xl rounded-md z-20 p-2">
                                                                            <div className="flex items-center gap-1 bg-black/20 border border-obsidian-info/50 rounded px-1.5 py-1 mb-2">
                                                                                <FileIcon className="w-3.5 h-3.5 text-obsidian-muted shrink-0" />
                                                                                <input
                                                                                    autoFocus
                                                                                    type="text"
                                                                                    placeholder="name.txt"
                                                                                    className="w-full bg-transparent text-[11px] outline-none text-foreground"
                                                                                    value={newFilePath}
                                                                                    onChange={e => setNewFilePath(e.target.value)}
                                                                                    onKeyDown={e => {
                                                                                        if (e.key === 'Enter') handleCreateFile();
                                                                                        if (e.key === 'Escape') setShowCreateFile(false);
                                                                                    }}
                                                                                />
                                                                            </div>
                                                                            <div className="flex justify-end gap-1">
                                                                                <button onClick={() => setShowCreateFile(false)} className="text-[10px] px-2 py-1 text-obsidian-muted hover:text-foreground rounded hover:bg-obsidian-border-active transition-colors">Cancel</button>
                                                                                <button onClick={handleCreateFile} disabled={!newFilePath} className="text-[10px] px-2 py-1 bg-obsidian-info/20 text-obsidian-info rounded hover:bg-obsidian-info/30 disabled:opacity-50 transition-colors">Create</button>
                                                                            </div>
                                                                        </div>
                                                                    )}
                                                                </div>
                                                                <div className="relative">
                                                                    <button
                                                                        onClick={() => setShowCloneDropdown(!showCloneDropdown)}
                                                                        className="px-3 py-1.5 bg-obsidian-success text-white rounded-md text-[11px] font-medium hover:bg-obsidian-success/90 transition-all active:scale-95 flex items-center gap-1.5"
                                                                    >
                                                                        <Code2 className="w-3.5 h-3.5" />
                                                                        Code
                                                                        <ChevronDown className="w-3 h-3 opacity-60 ml-0.5" />
                                                                    </button>
                                                                    {showCloneDropdown && (
                                                                        <div className="absolute right-0 top-full mt-2 w-[340px] bg-[#1a1b1e]/90 backdrop-blur-xl border border-white/10 rounded-lg shadow-2xl z-50 overflow-hidden">
                                                                            <div className="px-3 py-2.5 border-b border-white/5">
                                                                                <span className="text-[12px] font-semibold text-foreground">Clone</span>
                                                                            </div>
                                                                            {(['HTTPS', 'SSH'] as const).map(protocol => {
                                                                                const url = protocol === 'HTTPS'
                                                                                    ? `${GITEA_URL}/${selectedRepo.full_name}.git`
                                                                                    : (
                                                                                        GITEA_SSH_PORT === 22
                                                                                            ? `git@${GITEA_SSH_HOST}:${selectedRepo.full_name}.git`
                                                                                            : `ssh://git@${GITEA_SSH_HOST}:${GITEA_SSH_PORT}/${selectedRepo.full_name}.git`
                                                                                    );
                                                                                return (
                                                                                    <div key={protocol} className="px-3 py-2 border-b border-white/5 last:border-b-0">
                                                                                        <div className="text-[11px] font-medium text-obsidian-muted mb-1.5">{protocol}</div>
                                                                                        <div className="flex items-center gap-1.5">
                                                                                            <input readOnly value={url} className="flex-1 bg-black/20 border border-white/10 rounded px-2 py-1.5 text-[11px] font-mono text-obsidian-muted outline-none select-all" />
                                                                                            <button
                                                                                                onClick={() => { navigator.clipboard.writeText(url); setCloneCopied(protocol); setTimeout(() => setCloneCopied(null), 2000); }}
                                                                                                className="p-1.5 border border-white/10 rounded hover:bg-white/[0.05] transition-colors"
                                                                                            >
                                                                                                {cloneCopied === protocol ? <Check className="w-3.5 h-3.5 text-obsidian-success" /> : <Clipboard className="w-3.5 h-3.5 text-obsidian-muted" />}
                                                                                            </button>
                                                                                        </div>
                                                                                    </div>
                                                                                );
                                                                            })}
                                                                        </div>
                                                                    )}
                                                                </div>
                                                            </div>
                                                        </div>

                                                        {/* Commit Header + Directory Table — single container */}
                                                        <div className="border border-white/5 rounded-lg bg-black/20 backdrop-blur-md overflow-hidden mt-4 shadow-sm">
                                                            {/* Latest Commit Header */}
                                                            <div className="bg-black/20 px-4 py-2.5 flex items-center justify-between border-b border-white/5">
                                                                <div className="flex items-center gap-2.5 min-w-0">
                                                                    <img
                                                                        src={commits[0]?.author?.avatar_url || `https://ui-avatars.com/api/?name=${commits[0]?.commit?.author?.name || 'U'}&background=1a1a2e&color=58a6ff&size=24&font-size=0.45&bold=true`}
                                                                        alt=""
                                                                        className="w-5 h-5 rounded-full border border-white/10 shrink-0"
                                                                    />
                                                                    <span className="text-[12px] font-semibold text-foreground shrink-0">{commits[0]?.commit?.author?.name || 'admin'}</span>
                                                                    <span className="text-[12px] text-obsidian-muted truncate hover:text-obsidian-info hover:underline transition-colors cursor-pointer">{commits[0]?.commit?.message?.split('\n')[0] || 'Initial commit'}</span>
                                                                </div>
                                                                <div className="flex items-center gap-3 text-[12px] text-obsidian-muted shrink-0 ml-4">
                                                                    <code className="text-[11px] font-mono font-medium text-obsidian-info hover:underline cursor-pointer bg-obsidian-info/10 px-1.5 py-0.5 rounded">{commits[0]?.sha?.slice(0, 7) || '0000000'}</code>
                                                                    <span className="flex items-center gap-1.5"><Clock className="w-3 h-3 opacity-60" />{timeAgo(commits[0]?.commit?.author?.date)}</span>
                                                                    <span className="flex items-center gap-1.5 text-obsidian-muted border border-white/5 rounded-md px-2 py-0.5 text-[11px] font-medium hover:text-foreground hover:border-white/20 cursor-pointer transition-colors">
                                                                        <GitCommit className="w-3 h-3" />
                                                                        {commits.length} commits
                                                                    </span>
                                                                </div>
                                                            </div>

                                                            {/* Directory Table */}
                                                            <table className="w-full text-left border-collapse">
                                                                <tbody>
                                                                    {currentPath !== '' && (
                                                                        <tr className="border-t border-white/5 hover:bg-white/[0.02] transition-colors group">
                                                                            <td colSpan={3} className="px-4 py-2.5">
                                                                                <div
                                                                                    className="text-[12px] text-obsidian-info font-bold cursor-pointer hover:text-white transition-colors flex items-center gap-2 w-fit px-2 py-0.5 rounded-md hover:bg-obsidian-info/20"
                                                                                    onClick={() => {
                                                                                        const parts = currentPath.split('/');
                                                                                        parts.pop();
                                                                                        handleDirClick(parts.join('/'));
                                                                                    }}
                                                                                >
                                                                                    <ChevronRight className="w-4 h-4 rotate-180" />
                                                                                    <span className="tracking-widest">..</span>
                                                                                </div>
                                                                            </td>
                                                                        </tr>
                                                                    )}
                                                                    {(treeEntries[currentPath || '/'] || [])
                                                                        .sort((a, b) => {
                                                                            if (a.type !== b.type) return a.type === 'dir' ? -1 : 1;
                                                                            return a.name.localeCompare(b.name);
                                                                        })
                                                                        .map(entry => {
                                                                            const isDir = entry.type === 'dir';
                                                                            const Icon = isDir ? Folder : fileIcon(entry.name);
                                                                            const color = isDir ? '#6895a8' : fileColor(entry.name);
                                                                            return (
                                                                                <tr key={entry.path} className="border-t border-white/5 hover:bg-white/[0.02] transition-colors group cursor-pointer" onClick={() => isDir ? handleDirClick(entry.path) : handleFileClick(entry.path)}>
                                                                                    <td className="px-4 py-3 w-8">
                                                                                        <div className="w-6 h-6 rounded flex items-center justify-center group-hover:bg-white/5 transition-colors border border-transparent group-hover:border-white/10">
                                                                                            <Icon className="w-4 h-4" style={{ color }} />
                                                                                        </div>
                                                                                    </td>
                                                                                    <td className="px-1 py-3">
                                                                                        <span
                                                                                            className={clsx("text-[13px] transition-colors", isDir ? "font-medium text-foreground group-hover:text-obsidian-info" : "text-obsidian-muted group-hover:text-foreground")}
                                                                                        >
                                                                                            {entry.name}
                                                                                        </span>
                                                                                    </td>
                                                                                    <td className="px-5 py-3 text-right w-24">
                                                                                        {!isDir && entry.size > 0 && <span className="text-[11px] text-obsidian-muted font-mono">{formatBytes(entry.size)}</span>}
                                                                                    </td>
                                                                                </tr>
                                                                            );
                                                                        })}
                                                                    {(!treeEntries[currentPath || '/'] || treeEntries[currentPath || '/'].length === 0) && (
                                                                        <tr className="border-t border-white/5">
                                                                            <td colSpan={3} className="px-4 py-12 text-center flex flex-col items-center justify-center gap-3">
                                                                                <FolderOpen className="w-10 h-10 text-obsidian-muted opacity-30" />
                                                                                <span className="text-[13px] text-obsidian-muted font-medium">This directory is empty</span>
                                                                            </td>
                                                                        </tr>
                                                                    )}
                                                                </tbody>
                                                            </table>
                                                        </div>

                                                        {/* README.md Rendering */}
                                                        {readmeContent && (
                                                            <div className="border border-white/5 rounded-lg bg-black/20 backdrop-blur-md mt-6 overflow-hidden shadow-sm">
                                                                <div className="bg-black/20 border-b border-white/5 px-4 py-3 flex items-center gap-2">
                                                                    <FileText className="w-4 h-4 text-obsidian-muted" />
                                                                    <span className="text-[13px] font-semibold text-foreground">README.md</span>
                                                                </div>
                                                                <div className="p-8 prose prose-invert prose-obsidian max-w-none prose-a:text-obsidian-info hover:prose-a:underline prose-headings:border-b prose-headings:border-white/10 prose-headings:pb-2 prose-img:inline prose-img:m-0.5 prose-p:leading-relaxed [&_p:has(img)]:text-center [&_p:has(img)]:leading-loose">
                                                                    <ReactMarkdown
                                                                        remarkPlugins={[remarkGfm]}
                                                                        components={{
                                                                            code({ node, inline, className, children, ...props }: any) {
                                                                                const match = /language-(\w+)/.exec(className || '');
                                                                                return !inline && match ? (
                                                                                    <div className="not-prose rounded-md overflow-hidden my-6 border border-white/10 shadow-[0_4px_24px_rgba(0,0,0,0.2)]">
                                                                                        <div className="bg-black/40 px-4 py-2 text-[11px] font-mono text-obsidian-muted border-b border-white/5 flex items-center justify-between">
                                                                                            <span>{match[1]}</span>
                                                                                            <button className="hover:text-foreground transition-colors" onClick={() => navigator.clipboard.writeText(String(children).replace(/\n$/, ''))}><Copy className="w-3.5 h-3.5" /></button>
                                                                                        </div>
                                                                                        <SyntaxHighlighter
                                                                                            {...props}
                                                                                            style={vscDarkPlus as any}
                                                                                            language={match[1]}
                                                                                            PreTag="div"
                                                                                            className="!m-0 !bg-obsidian-bg !text-[13px] !p-4 !overflow-x-auto"
                                                                                        >
                                                                                            {String(children).replace(/\n$/, '')}
                                                                                        </SyntaxHighlighter>
                                                                                    </div>
                                                                                ) : (
                                                                                    <code {...props} className={clsx(className, "bg-obsidian-panel px-1.5 py-0.5 rounded text-[13px] font-mono border border-obsidian-border text-obsidian-info before:content-hidden after:content-hidden")}>
                                                                                        {children}
                                                                                    </code>
                                                                                );
                                                                            }
                                                                        }}
                                                                    >
                                                                        {readmeContent}
                                                                    </ReactMarkdown>
                                                                </div>
                                                            </div>
                                                        )}
                                                    </div>

                                                    {/* ─── Right Column: About Sidebar ─── */}
                                                    <div className="w-[280px] shrink-0 hidden xl:block">
                                                        <div className="sticky top-6 flex flex-col gap-5 bg-black/20 backdrop-blur-md border border-white/5 rounded-lg p-5">
                                                            {/* About */}
                                                            <div>
                                                                <h3 className="text-[12px] font-semibold text-foreground mb-3">About</h3>
                                                                {selectedRepo.description ? (
                                                                    <p className="text-[13px] text-obsidian-muted leading-relaxed mb-3">{selectedRepo.description}</p>
                                                                ) : (
                                                                    <p className="text-[12px] text-obsidian-muted/50 italic mb-3">No description provided.</p>
                                                                )}
                                                                {repoTopics.length > 0 && (
                                                                    <div className="flex flex-wrap gap-1.5 mb-3">
                                                                        {repoTopics.map(topic => (
                                                                            <span key={topic} className="text-[11px] font-medium text-obsidian-info bg-obsidian-info/15 px-2.5 py-0.5 rounded-full hover:bg-obsidian-info/25 cursor-pointer transition-colors">{topic}</span>
                                                                        ))}
                                                                    </div>
                                                                )}
                                                                <div className="flex flex-col gap-2 text-[12px] text-obsidian-muted">
                                                                    <div className="flex items-center gap-2"><Star className="w-3.5 h-3.5" /> {selectedRepo.stars_count} stars</div>
                                                                    <div className="flex items-center gap-2"><Eye className="w-3.5 h-3.5" /> {selectedRepo.forks_count} forks</div>
                                                                    <div className="flex items-center gap-2"><GitBranch className="w-3.5 h-3.5" /> {branches.length} branches</div>
                                                                    <div className="flex items-center gap-2"><Tag className="w-3.5 h-3.5" /> {tags.length} tags</div>
                                                                </div>
                                                            </div>

                                                            <div className="border-t border-white/5" />

                                                            {/* Languages */}
                                                            {Object.keys(repoLanguages).length > 0 && (() => {
                                                                const total = Object.values(repoLanguages).reduce((a, b) => a + b, 0);
                                                                const langs = Object.entries(repoLanguages)
                                                                    .sort((a, b) => b[1] - a[1])
                                                                    .map(([name, bytes]) => ({ name, bytes, pct: (bytes / total) * 100, color: LANG_COLORS[name] || '#6c707e' }));
                                                                return (
                                                                    <div>
                                                                        <h3 className="text-[12px] font-semibold text-foreground mb-3">Languages</h3>
                                                                        <div className="flex h-2 rounded-full overflow-hidden mb-3">
                                                                            {langs.map(l => (
                                                                                <div key={l.name} style={{ width: `${l.pct}%`, backgroundColor: l.color }} title={`${l.name} ${l.pct.toFixed(1)}%`} />
                                                                            ))}
                                                                        </div>
                                                                        <div className="flex flex-wrap gap-x-4 gap-y-1.5">
                                                                            {langs.map(l => (
                                                                                <div key={l.name} className="flex items-center gap-1.5 text-[11px]">
                                                                                    <span className="w-2.5 h-2.5 rounded-full shrink-0" style={{ backgroundColor: l.color }} />
                                                                                    <span className="font-medium text-foreground">{l.name}</span>
                                                                                    <span className="text-obsidian-muted">{l.pct.toFixed(1)}%</span>
                                                                                </div>
                                                                            ))}
                                                                        </div>
                                                                    </div>
                                                                );
                                                            })()}
                                                        </div>
                                                    </div>
                                                </div>
                                            ) : (
                                                /* File Content View */
                                                <div className="flex flex-col border border-white/5 shadow-sm rounded-lg bg-black/20 backdrop-blur-md overflow-hidden" style={{ minHeight: 'calc(100vh - 120px)' }}>
                                                    {fileLoading ? (
                                                        <div className="flex-1 flex items-center justify-center"><Loader2 className="w-6 h-6 text-obsidian-info animate-spin" /></div>
                                                    ) : (
                                                        <>
                                                            {/* File header (GitHub style meta header) */}
                                                            <div className="bg-black/20 border-b border-white/5 flex items-center px-4 py-2.5 justify-between shrink-0">
                                                                <div className="flex items-center gap-3">
                                                                    <button className="flex items-center gap-1.5 border border-white/10 shadow-sm rounded-md bg-white/[0.02] hover:bg-white/[0.05] px-2.5 py-1 text-[11px] font-medium text-obsidian-muted hover:text-foreground transition-colors" onClick={() => setSelectedFile(null)}>
                                                                        <ChevronRight className="w-3.5 h-3.5 rotate-180" /> Back
                                                                    </button>
                                                                    <div className="w-px h-4 bg-white/10 mx-1" />
                                                                    <FileCode className="w-4 h-4 ml-1" style={{ color: fileColor(selectedFile) }} />
                                                                    <div className="text-[13px] font-mono font-medium text-foreground">{selectedFile.split('/').pop()}</div>
                                                                    <span className="text-[9px] text-obsidian-muted font-bold font-mono uppercase bg-white/5 px-1.5 py-0.5 rounded ml-2 border border-white/5">{extToLang(selectedFile)}</span>
                                                                </div>
                                                                <div className="flex items-center gap-2">
                                                                    {isMarkdown(selectedFile) && !editMode && (
                                                                        <div className="flex rounded-md overflow-hidden bg-black/20 border border-white/10 text-[11px] font-medium transition-colors mr-2">
                                                                            <button
                                                                                onClick={() => setMdPreview(true)}
                                                                                className={clsx("px-3 py-1.5 transition-colors", mdPreview ? "bg-white/10 text-foreground" : "text-obsidian-muted hover:text-foreground")}
                                                                            >
                                                                                Preview
                                                                            </button>
                                                                            <div className="w-px bg-white/10" />
                                                                            <button
                                                                                onClick={() => setMdPreview(false)}
                                                                                className={clsx("px-3 py-1.5 transition-colors flex items-center gap-1", !mdPreview ? "bg-white/10 text-foreground" : "text-obsidian-muted hover:text-foreground")}
                                                                            >
                                                                                <Code2 className="w-3.5 h-3.5" /> Source
                                                                            </button>
                                                                        </div>
                                                                    )}

                                                                    {activeBranch === (selectedRepo?.default_branch || 'main') ? (
                                                                        <span className="text-[10px] text-obsidian-muted bg-white/[0.02] border border-white/10 px-2.5 py-1.5 rounded-md font-medium">🔒 Protected Branch</span>
                                                                    ) : !editMode ? (
                                                                        <div className="flex items-center gap-2">
                                                                            <button
                                                                                onClick={() => { setEditContent(fileContent); setEditMode(true); }}
                                                                                className="flex items-center gap-2 px-3 py-1.5 text-[11px] font-medium border border-white/10 bg-white/[0.02] text-foreground rounded-md shadow-sm hover:bg-white/[0.05] transition-colors active:scale-95"
                                                                            >
                                                                                ✏️ Edit
                                                                            </button>
                                                                            <button
                                                                                onClick={handleDeleteFile}
                                                                                className="flex items-center gap-1 p-1.5 border border-white/10 text-[11px] font-medium bg-white/[0.02] text-obsidian-danger rounded-md shadow-sm hover:bg-obsidian-danger/20 transition-colors active:scale-95"
                                                                                title="Delete file"
                                                                            >
                                                                                <Trash2 className="w-3.5 h-3.5" />
                                                                            </button>
                                                                        </div>
                                                                    ) : (
                                                                        <div className="flex items-center gap-2">
                                                                            <span className="text-[11px] mr-2 text-obsidian-warning font-medium flex items-center gap-1">
                                                                                <span className="w-2 h-2 rounded-full bg-obsidian-warning animate-pulse" />
                                                                                Editing
                                                                            </span>
                                                                            <button onClick={() => setEditMode(false)} className="text-[11px] font-medium border border-obsidian-border bg-obsidian-bg text-obsidian-muted hover:text-foreground px-3 py-1.5 rounded-md shadow-sm hover:bg-obsidian-border-active transition-colors active:scale-95">Cancel</button>
                                                                        </div>
                                                                    )}
                                                                </div>
                                                            </div>
                                                            {/* File body */}
                                                            {editMode ? (
                                                                <div className="flex flex-col flex-1">
                                                                    <div style={{ height: 'calc(100vh - 260px)', minHeight: '400px' }}>
                                                                        <Editor
                                                                            height="100%"
                                                                            width="100%"
                                                                            language={extToLang(selectedFile)}
                                                                            value={editContent}
                                                                            theme="obsidian"
                                                                            onChange={(value) => setEditContent(value || '')}
                                                                            onMount={handleEditorMount}
                                                                            options={{
                                                                                readOnly: false,
                                                                                domReadOnly: false,
                                                                                minimap: { enabled: false },
                                                                                fontSize: 14,
                                                                                fontFamily: 'JetBrains Mono, Fira Code, DM Mono, Menlo, monospace',
                                                                                fontLigatures: true,
                                                                                lineHeight: 24,
                                                                                scrollBeyondLastLine: false,
                                                                                automaticLayout: true,
                                                                                padding: { top: 16, bottom: 16 },
                                                                                renderLineHighlight: 'all',
                                                                            }}
                                                                        />
                                                                    </div>
                                                                    <div className="border-t border-white/5 bg-black/40 p-3.5 flex items-center gap-3 shrink-0">
                                                                        <input
                                                                            type="text"
                                                                            placeholder="Commit message..."
                                                                            value={editMessage}
                                                                            onChange={e => setEditMessage(e.target.value)}
                                                                            className="flex-1 bg-black/20 border border-white/10 focus:border-obsidian-info shadow-sm rounded-md px-3 py-2 text-[12px] text-foreground placeholder-obsidian-muted outline-none transition-all"
                                                                        />
                                                                        <button
                                                                            onClick={handleSaveFile}
                                                                            disabled={!editMessage || editSaving}
                                                                            className="px-4 py-2 bg-[#238636] border border-[rgba(240,246,252,0.1)] text-white rounded-md text-[12px] font-bold hover:bg-[#2ea043] shadow-sm disabled:opacity-40 flex items-center gap-1.5 transition-all active:scale-95"
                                                                        >
                                                                            {editSaving && <Loader2 className="w-3.5 h-3.5 animate-spin" />}
                                                                            Commit changes
                                                                        </button>
                                                                    </div>
                                                                </div>
                                                            ) : (
                                                                <div className="flex-1 flex flex-col overflow-auto bg-transparent">
                                                                    {isMarkdown(selectedFile) && mdPreview ? (
                                                                        /* Markdown Preview */
                                                                        <div className="px-10 py-10 max-w-[900px] mx-auto prose prose-invert max-w-none prose-a:text-obsidian-info hover:prose-a:underline prose-headings:border-b prose-headings:border-obsidian-border prose-headings:pb-2">
                                                                            <ReactMarkdown
                                                                                remarkPlugins={[remarkGfm]}
                                                                                components={{
                                                                                    code({ node, inline, className, children, ...props }: any) {
                                                                                        const match = /language-(\w+)/.exec(className || '');
                                                                                        return !inline && match ? (
                                                                                            <div className="not-prose rounded-md overflow-hidden my-6 border border-white/10 shadow-[0_4px_24px_rgba(0,0,0,0.2)]">
                                                                                                <div className="bg-black/40 px-4 py-2 text-[11px] font-mono text-obsidian-muted border-b border-white/5 flex items-center justify-between">
                                                                                                    <span>{match[1]}</span>
                                                                                                    <button className="hover:text-foreground transition-colors" onClick={() => navigator.clipboard.writeText(String(children).replace(/\n$/, ''))}><Copy className="w-3.5 h-3.5" /></button>
                                                                                                </div>
                                                                                                <SyntaxHighlighter
                                                                                                    {...props}
                                                                                                    style={vscDarkPlus as any}
                                                                                                    language={match[1]}
                                                                                                    PreTag="div"
                                                                                                    className="!m-0 !bg-[#0b0c0f] !text-[13px] !p-4 !overflow-x-auto"
                                                                                                >
                                                                                                    {String(children).replace(/\n$/, '')}
                                                                                                </SyntaxHighlighter>
                                                                                            </div>
                                                                                        ) : (
                                                                                            <code {...props} className={clsx(className, "bg-white/[0.04] px-1.5 py-0.5 rounded text-[13px] font-mono border border-white/5 text-obsidian-info before:content-hidden after:content-hidden")}>
                                                                                                {children}
                                                                                            </code>
                                                                                        );
                                                                                    }
                                                                                }}
                                                                            >
                                                                                {fileContent}
                                                                            </ReactMarkdown>
                                                                        </div>
                                                                    ) : isImage(selectedFile) ? (
                                                                        /* Image Preview */
                                                                        <div className="flex items-center justify-center h-full p-8 bg-[#0d1117]">
                                                                            <div className="text-center">
                                                                                <div className="text-[12px] text-obsidian-muted mb-3">Image preview not available via API</div>
                                                                            </div>
                                                                        </div>
                                                                    ) : (
                                                                        /* Syntax Highlighted Code */
                                                                        <div style={{ height: 'calc(100vh - 200px)', minHeight: '500px' }}>
                                                                            <Editor
                                                                                height="100%"
                                                                                width="100%"
                                                                                language={extToLang(selectedFile)}
                                                                                value={fileContent}
                                                                                theme="obsidian"
                                                                                onMount={handleEditorMount}
                                                                                options={{
                                                                                    readOnly: true,
                                                                                    domReadOnly: true,
                                                                                    minimap: { enabled: false },
                                                                                    fontSize: 14,
                                                                                    fontFamily: 'JetBrains Mono, Fira Code, DM Mono, Menlo, monospace',
                                                                                    fontLigatures: true,
                                                                                    lineHeight: 24,
                                                                                    scrollBeyondLastLine: false,
                                                                                    automaticLayout: true,
                                                                                    padding: { top: 16, bottom: 16 },
                                                                                    renderLineHighlight: 'none',
                                                                                }}
                                                                            />
                                                                        </div>
                                                                    )}
                                                                </div>
                                                            )}
                                                        </>
                                                    )}
                                                </div>
                                            )}
                                        </div>
                                    </div>

                                ) : detailTab === 'commits' ? (
                                    /* ─── Commits Tab ─── */
                                    <div className="flex-1 flex overflow-hidden">
                                        {/* Commit List */}
                                        <div className={clsx("overflow-auto shrink-0 py-1", selectedCommit ? "w-[300px] border-r border-white/5" : "flex-1")}>
                                            {commits.length === 0 ? (
                                                <div className="text-center py-8 text-obsidian-muted text-[11px]">No commits</div>
                                            ) : commits.map(commit => (
                                                <div key={commit.sha} onClick={() => handleCommitClick(commit.sha)}
                                                    className={clsx("px-4 py-2.5 cursor-pointer transition-colors border-b border-white/5",
                                                        selectedCommit?.sha === commit.sha ? "bg-white/[0.04]" : "hover:bg-white/[0.02]"
                                                    )}>
                                                    <div className="flex items-start gap-2">
                                                        <GitCommit className="w-3.5 h-3.5 text-obsidian-info shrink-0 mt-0.5" />
                                                        <div className="flex-1 min-w-0">
                                                            <p className="text-[11px] text-foreground truncate leading-relaxed">{commit.commit?.message?.split('\n')[0]}</p>
                                                            <div className="flex items-center gap-2 mt-0.5 text-[10px] text-obsidian-muted">
                                                                <code className="text-obsidian-danger font-mono">{commit.sha?.slice(0, 7)}</code>
                                                                <span>{commit.commit?.author?.name}</span>
                                                                <span className="ml-auto">{timeAgo(commit.commit?.author?.date)}</span>
                                                            </div>
                                                        </div>
                                                    </div>
                                                </div>
                                            ))}
                                            {commitsHasMore && (
                                                <div className="px-4 py-3 flex justify-center border-b border-white/5">
                                                    <button
                                                        onClick={() => {
                                                            if (!selectedRepo || commitsLoadingMore) return;
                                                            const [owner, repo] = selectedRepo.full_name.split('/');
                                                            fetchCommits(owner, repo, activeBranch, { page: commitsPage + 1, append: true });
                                                        }}
                                                        disabled={commitsLoadingMore}
                                                        className="px-3 py-1.5 rounded-md text-[11px] font-medium border border-white/10 bg-white/[0.02] hover:bg-white/[0.05] disabled:opacity-50 flex items-center gap-1.5"
                                                    >
                                                        {commitsLoadingMore && <Loader2 className="w-3.5 h-3.5 animate-spin" />}
                                                        Load more commits
                                                    </button>
                                                </div>
                                            )}
                                        </div>
                                        {/* Commit Detail */}
                                        {selectedCommit && (
                                            <div className="flex-1 overflow-auto bg-transparent">
                                                {commitLoading ? (
                                                    <div className="flex items-center justify-center h-full"><Loader2 className="w-5 h-5 text-obsidian-info animate-spin" /></div>
                                                ) : (
                                                    <div>
                                                        {/* Commit header */}
                                                        <div className="px-4 py-3 border-b border-white/5 bg-black/20 backdrop-blur-md transition-all active:scale-95">
                                                            <p className="text-[13px] text-foreground font-medium mb-2">{selectedCommit.commit?.message?.split('\n')[0]}</p>
                                                            {selectedCommit.commit?.message?.split('\n').slice(1).join('\n').trim() && (
                                                                <p className="text-[11px] text-obsidian-muted mb-2 whitespace-pre-wrap">{selectedCommit.commit.message.split('\n').slice(1).join('\n').trim()}</p>
                                                            )}
                                                            <div className="flex items-center gap-3 text-[10px] text-obsidian-muted">
                                                                <code className="text-obsidian-danger font-mono">{selectedCommit.sha?.slice(0, 7)}</code>
                                                                <span>{selectedCommit.commit?.author?.name}</span>
                                                                <span>{timeAgo(selectedCommit.commit?.author?.date)}</span>
                                                            </div>
                                                            {selectedCommit.stats && (
                                                                <div className="flex items-center gap-3 mt-2 text-[10px]">
                                                                    <span className="text-obsidian-success">+{selectedCommit.stats.additions}</span>
                                                                    <span className="text-obsidian-danger">-{selectedCommit.stats.deletions}</span>
                                                                    <span className="text-obsidian-muted">{selectedCommit.files?.length || 0} files changed</span>
                                                                </div>
                                                            )}
                                                        </div>
                                                        {/* Files with diffs */}
                                                        {selectedCommit.files?.map(file => (
                                                            <div key={file.filename} className="border-b border-white/5">
                                                                <div className="flex items-center gap-2 px-4 py-2 bg-white/[0.01] cursor-pointer hover:bg-white/[0.03] transition-all active:scale-95"
                                                                    onClick={() => {
                                                                        const next = new Set(expandedDiffs);
                                                                        next.has(file.filename) ? next.delete(file.filename) : next.add(file.filename);
                                                                        setExpandedDiffs(next);
                                                                    }}>
                                                                    <ChevronRight className={clsx("w-3 h-3 text-obsidian-muted transition-transform", expandedDiffs.has(file.filename) && "rotate-90")} />
                                                                    <span className={clsx("text-[9px] px-1 py-0.5 rounded font-semibold uppercase",
                                                                        file.status === 'added' ? "bg-obsidian-success/15 text-obsidian-success" :
                                                                            file.status === 'removed' ? "bg-obsidian-danger/15 text-obsidian-danger" :
                                                                                "bg-obsidian-warning/15 text-obsidian-warning"
                                                                    )}>{file.status?.[0] || 'M'}</span>
                                                                    <span className="text-[11px] text-foreground font-mono truncate">{file.filename}</span>
                                                                    <div className="ml-auto flex items-center gap-1 text-[10px] shrink-0">
                                                                        {file.additions > 0 && <span className="text-obsidian-success">+{file.additions}</span>}
                                                                        {file.deletions > 0 && <span className="text-obsidian-danger">-{file.deletions}</span>}
                                                                    </div>
                                                                </div>
                                                                {expandedDiffs.has(file.filename) && (
                                                                    <DiffBlock patch={file.patch || ''} filename={file.filename} />
                                                                )}
                                                            </div>
                                                        ))}
                                                    </div>
                                                )}
                                            </div>
                                        )}
                                    </div>

                                ) : detailTab === 'pulls' ? (
                                    /* ─── Pull Requests Tab ─── */
                                    <div className="flex-1 overflow-auto">
                                        {/* PR Controls */}
                                        <div className="flex items-center gap-2 px-4 py-2 border-b border-white/5 bg-black/20 backdrop-blur-md transition-all active:scale-95">
                                            {(['all', 'open', 'closed'] as const).map(s => (
                                                <button key={s} onClick={() => {
                                                    setPrState(s);
                                                    setSelectedPR(null);
                                                    setPrDetail(null);
                                                    setPrComments([]);
                                                    setPrReviews([]);
                                                    setPrChecksState('unknown');
                                                    setPrChecks([]);
                                                    if (selectedRepo) {
                                                        const [o, n] = selectedRepo.full_name.split('/');
                                                        fetchPulls(o, n, s, { page: 1 });
                                                    }
                                                }}
                                                    className={clsx("px-2 py-1 rounded text-[10px] font-medium transition-all",
                                                        prState === s ? "bg-obsidian-info/20 text-obsidian-info" : "text-obsidian-muted hover:text-foreground"
                                                    )}>{s.charAt(0).toUpperCase() + s.slice(1)}</button>
                                            ))}
                                            <button onClick={() => setShowCreatePR(!showCreatePR)}
                                                className="ml-auto flex items-center gap-1 px-2 py-1 bg-obsidian-success/15 text-obsidian-success rounded text-[10px] font-medium hover:bg-obsidian-success/25 transition-all active:scale-95">
                                                <Plus className="w-3.5 h-3.5" /> New PR
                                            </button>
                                        </div>

                                        {/* Create PR Form */}
                                        {showCreatePR && (
                                            <div className="px-4 py-3 border-b border-white/5 bg-black/40 backdrop-blur-md space-y-2 transition-all active:scale-95">
                                                <div className="flex gap-2">
                                                    <select value={prHead} onChange={e => setPrHead(e.target.value)}
                                                        className="flex-1 bg-black/20 border border-white/10 rounded px-2 py-1 text-[11px] text-foreground outline-none transition-all active:scale-95">
                                                        <option value="">Head branch...</option>
                                                        {branches.map(b => <option key={b.name} value={b.name}>{b.name}</option>)}
                                                    </select>
                                                    <ArrowUpRight className="w-4 h-4 text-obsidian-muted shrink-0 self-center" />
                                                    <select value={prBase} onChange={e => setPrBase(e.target.value)}
                                                        className="flex-1 bg-black/20 border border-white/10 rounded px-2 py-1 text-[11px] text-foreground outline-none transition-all active:scale-95">
                                                        <option value="">Base branch...</option>
                                                        {branches.map(b => <option key={b.name} value={b.name}>{b.name}</option>)}
                                                    </select>
                                                </div>
                                                <input type="text" placeholder="PR title..." value={prTitle} onChange={e => setPrTitle(e.target.value)}
                                                    className="w-full bg-black/20 border border-white/10 rounded px-2 py-1.5 text-[11px] text-foreground placeholder-obsidian-muted outline-none transition-all active:scale-95" />
                                                <textarea placeholder="Description (optional)..." value={prBody} onChange={e => setPrBody(e.target.value)} rows={3}
                                                    className="w-full bg-black/20 border border-white/10 rounded px-2 py-1.5 text-[11px] text-foreground placeholder-obsidian-muted outline-none resize-none transition-all active:scale-95" />
                                                <div className="flex gap-2">
                                                    <button onClick={handleCreatePR} disabled={!prTitle || !prHead || !prBase}
                                                        className="px-3 py-1 bg-obsidian-success text-white rounded text-[10px] font-medium hover:bg-obsidian-success disabled:opacity-40 transition-all active:scale-95">Create</button>
                                                    <button onClick={() => setShowCreatePR(false)} className="px-3 py-1 text-obsidian-muted text-[10px] hover:text-foreground transition-all active:scale-95">Cancel</button>
                                                </div>
                                            </div>
                                        )}

                                        {/* PR List */}
                                        {pulls.length === 0 ? (
                                            <div className="flex flex-col items-center justify-center py-16 text-obsidian-muted gap-2">
                                                <GitPullRequest className="w-8 h-8 opacity-30" />
                                                <span className="text-[12px]">No pull requests</span>
                                                <span className="text-[10px]">Create a PR to merge changes between branches</span>
                                            </div>
                                        ) : pulls.map(pr => (
                                            <div key={pr.id} className="border-b border-white/5">
                                                {/* PR Row */}
                                                <div
                                                    onClick={() => handlePRClick(pr.number)}
                                                    className={clsx("px-4 py-3 cursor-pointer transition-colors",
                                                        selectedPR === pr.number ? "bg-white/[0.04]" : "hover:bg-white/[0.02]"
                                                    )}
                                                >
                                                    <div className="flex items-start gap-2">
                                                        <PRBadge pr={pr} />
                                                        <div className="flex-1 min-w-0">
                                                            <p className="text-[12px] text-foreground font-medium truncate">{pr.title}</p>
                                                            <div className="flex items-center gap-2 mt-1 text-[10px] text-obsidian-muted">
                                                                <span>#{pr.number}</span>
                                                                <span>{pr.user?.login}</span>
                                                                <span className="font-mono text-obsidian-info">{pr.head?.ref}</span>
                                                                <ArrowUpRight className="w-2.5 h-2.5" />
                                                                <span className="font-mono text-obsidian-success">{pr.base?.ref}</span>
                                                                <span className="ml-auto">{timeAgo(pr.updated_at)}</span>
                                                            </div>
                                                        </div>
                                                        {pr.comments > 0 && (
                                                            <div className="flex items-center gap-0.5 text-[10px] text-obsidian-muted shrink-0">
                                                                <MessageSquare className="w-3.5 h-3.5" />{pr.comments}
                                                            </div>
                                                        )}
                                                    </div>
                                                </div>
                                                {/* PR Actions Panel (expands on click) */}
                                                {selectedPR === pr.number && (
                                                    <div className="px-4 pb-3 pt-2 bg-white/[0.01] flex items-center gap-2 flex-wrap border-b border-white/5">
                                                        <div className="w-full mb-1">
                                                            <div className="flex items-center gap-2 text-[10px]">
                                                                <span className="text-obsidian-muted font-medium">Checks</span>
                                                                <span className={clsx("px-2 py-0.5 rounded border font-medium", checkStateStyle(prChecksState).bg, checkStateStyle(prChecksState).text, checkStateStyle(prChecksState).border)}>
                                                                    {prChecksLoading ? 'loading...' : prChecksState}
                                                                </span>
                                                                {!prChecksLoading && prChecks.length > 0 && (
                                                                    <span className="text-obsidian-muted">{prChecks.length} context{prChecks.length !== 1 ? 's' : ''}</span>
                                                                )}
                                                                {isBlockingCheckState(prChecksState) && (
                                                                    <span className="text-obsidian-danger ml-auto">Merge is blocked by failing checks</span>
                                                                )}
                                                            </div>
                                                            {!prChecksLoading && prChecks.length > 0 && (
                                                                <div className="mt-2 flex flex-wrap gap-1.5">
                                                                    {prChecks.slice(0, 8).map((check, idx) => {
                                                                        const style = checkStateStyle(check.state);
                                                                        const label = check.context || `check-${idx + 1}`;
                                                                        const badge = (
                                                                            <span className={clsx("px-2 py-0.5 rounded border text-[9px] font-medium", style.bg, style.text, style.border)}>
                                                                                {label}: {normalizeCheckState(check.state)}
                                                                            </span>
                                                                        );
                                                                        if (check.target_url) {
                                                                            return (
                                                                                <a
                                                                                    key={`${label}-${idx}`}
                                                                                    href={check.target_url}
                                                                                    target="_blank"
                                                                                    rel="noopener noreferrer"
                                                                                    onClick={e => e.stopPropagation()}
                                                                                >
                                                                                    {badge}
                                                                                </a>
                                                                            );
                                                                        }
                                                                        return <span key={`${label}-${idx}`}>{badge}</span>;
                                                                    })}
                                                                </div>
                                                            )}
                                                        </div>
                                                        {prConflictHint?.prNumber === pr.number && (
                                                            <div className="w-full mb-2 rounded-md border border-obsidian-warning/30 bg-obsidian-warning/10 px-3 py-2 text-[10px] text-obsidian-warning">
                                                                <div className="font-semibold mb-1">Merge conflict detected</div>
                                                                <div className="text-obsidian-muted mb-2">{prConflictHint.message}</div>
                                                                <div className="flex items-center gap-2">
                                                                    <button
                                                                        onClick={(e) => { e.stopPropagation(); handleUpdatePRBranch(pr.number, 'rebase'); }}
                                                                        disabled={prUpdateLoading}
                                                                        className="px-2 py-1 rounded border border-obsidian-info/30 bg-obsidian-info/10 text-obsidian-info hover:bg-obsidian-info/20 disabled:opacity-50"
                                                                    >
                                                                        {prUpdateLoading ? 'Updating...' : 'Update Branch (Rebase)'}
                                                                    </button>
                                                                    <button
                                                                        onClick={(e) => { e.stopPropagation(); handleUpdatePRBranch(pr.number, 'merge'); }}
                                                                        disabled={prUpdateLoading}
                                                                        className="px-2 py-1 rounded border border-white/20 bg-white/[0.05] text-foreground hover:bg-white/[0.1] disabled:opacity-50"
                                                                    >
                                                                        Update Branch (Merge)
                                                                    </button>
                                                                </div>
                                                            </div>
                                                        )}
                                                        {!pr.merged && pr.state === 'open' ? (
                                                            <>
                                                                <button
                                                                    onClick={(e) => { e.stopPropagation(); handleApprovePR(pr.number); }}
                                                                    className="flex items-center gap-1.5 px-3 py-1.5 bg-obsidian-success/10 text-obsidian-success border border-obsidian-success/20 rounded-md text-[11px] font-medium hover:bg-obsidian-success/20 transition-colors active:scale-95"
                                                                >
                                                                    <CheckCircle2 className="w-3.5 h-3.5" /> Approve
                                                                </button>
                                                                <button
                                                                    onClick={(e) => { e.stopPropagation(); handleRequestChangesPR(pr.number); }}
                                                                    className="flex items-center gap-1.5 px-3 py-1.5 bg-obsidian-warning/10 text-obsidian-warning border border-obsidian-warning/20 rounded-md text-[11px] font-medium hover:bg-obsidian-warning/20 transition-colors active:scale-95"
                                                                >
                                                                    <AlertCircle className="w-3.5 h-3.5" /> Request changes
                                                                </button>
                                                                <button
                                                                    onClick={(e) => { e.stopPropagation(); handleCommentPR(pr.number); }}
                                                                    className="flex items-center gap-1.5 px-3 py-1.5 bg-white/[0.02] text-foreground border border-white/10 rounded-md text-[11px] font-medium hover:bg-white/[0.06] transition-colors active:scale-95"
                                                                >
                                                                    <MessageSquare className="w-3.5 h-3.5" /> Comment
                                                                </button>
                                                                <div className="w-px h-4 bg-obsidian-border-active/50 mx-1"></div>
                                                                <button
                                                                    onClick={(e) => { e.stopPropagation(); handleMergePR(pr.number, 'merge'); }}
                                                                    disabled={isBlockingCheckState(prChecksState)}
                                                                    className="flex items-center gap-1.5 px-3 py-1.5 bg-obsidian-info/10 text-obsidian-info border border-obsidian-info/20 rounded-md text-[11px] font-medium hover:bg-obsidian-info/20 transition-colors active:scale-95 disabled:opacity-40 disabled:cursor-not-allowed"
                                                                >
                                                                    <GitMerge className="w-3.5 h-3.5" /> Merge
                                                                </button>
                                                                <button
                                                                    onClick={(e) => { e.stopPropagation(); handleMergePR(pr.number, 'squash'); }}
                                                                    disabled={isBlockingCheckState(prChecksState)}
                                                                    className="flex items-center gap-1.5 px-3 py-1.5 bg-obsidian-warning/10 text-obsidian-warning border border-obsidian-warning/20 rounded-md text-[11px] font-medium hover:bg-obsidian-warning/20 transition-colors active:scale-95 disabled:opacity-40 disabled:cursor-not-allowed"
                                                                >
                                                                    <Minimize2 className="w-3.5 h-3.5" /> Squash
                                                                </button>
                                                                <button
                                                                    onClick={(e) => { e.stopPropagation(); handleMergePR(pr.number, 'rebase'); }}
                                                                    disabled={isBlockingCheckState(prChecksState)}
                                                                    title="Rebase and Merge"
                                                                    className="flex items-center gap-1.5 px-3 py-1.5 bg-obsidian-warning/10 text-obsidian-warning border border-obsidian-warning/20 rounded-md text-[11px] font-medium hover:bg-obsidian-warning/20 transition-colors active:scale-95 disabled:opacity-40 disabled:cursor-not-allowed"
                                                                >
                                                                    <RefreshCw className="w-3.5 h-3.5" /> Rebase
                                                                </button>
                                                                <button
                                                                    onClick={(e) => { e.stopPropagation(); handleClosePR(pr.number); }}
                                                                    className="flex items-center gap-1.5 px-3 py-1.5 bg-obsidian-danger/10 text-obsidian-danger border border-obsidian-danger/20 rounded-md text-[11px] font-medium hover:bg-obsidian-danger/20 transition-colors ml-auto active:scale-95"
                                                                >
                                                                    <XCircle className="w-3.5 h-3.5" /> Close
                                                                </button>
                                                            </>
                                                        ) : (
                                                            <div className={clsx(
                                                                "flex items-center gap-2 px-3 py-1.5 rounded-md border text-[11px] font-medium",
                                                                pr.merged
                                                                    ? "bg-obsidian-primary/10 text-obsidian-primary border-obsidian-primary/20"
                                                                    : "bg-obsidian-border-active/30 text-obsidian-muted border-obsidian-border/50"
                                                            )}>
                                                                {pr.merged ? <GitMerge className="w-3.5 h-3.5" /> : <XCircle className="w-3.5 h-3.5" />}
                                                                {pr.merged ? 'Merged' : 'Closed'}
                                                            </div>
                                                        )}
                                                    </div>
                                                )}
                                                {/* PR Diff Viewer */}
                                                {selectedPR === pr.number && prDetailLoading && (
                                                    <div className="px-4 py-3 flex items-center gap-2 text-obsidian-muted text-[11px]">
                                                        <Loader2 className="w-3.5 h-3.5 animate-spin text-obsidian-info" />
                                                        Loading changed files...
                                                    </div>
                                                )}
                                                {selectedPR === pr.number && prDetail && prDetail.files && prDetail.files.length > 0 && (
                                                    <div className="border-t border-obsidian-border/50">
                                                        <div className="px-4 py-2 text-[10px] text-obsidian-muted font-medium border-b border-obsidian-border/30 flex items-center gap-2 bg-obsidian-panel/40 transition-all active:scale-95">
                                                            <FileCode className="w-3.5 h-3.5" />
                                                            {prDetail.files.length} changed file{prDetail.files.length !== 1 ? 's' : ''}
                                                            <span className="text-obsidian-success ml-auto">+{prDetail.files.reduce((s, f) => s + (f.additions || 0), 0)}</span>
                                                            <span className="text-obsidian-danger ml-1">-{prDetail.files.reduce((s, f) => s + (f.deletions || 0), 0)}</span>
                                                        </div>
                                                        {prDetail.files.map((file, fi) => {
                                                            const fileInlineComments = prInlineComments.filter((c) => c.path === file.filename);
                                                            return (
                                                                <div key={fi} className="border-b border-obsidian-border/20">
                                                                    <div
                                                                        className="px-4 py-1.5 flex items-center gap-2 cursor-pointer hover:bg-obsidian-panel/60 bg-obsidian-bg/30 transition-all active:scale-95"
                                                                        onClick={e => { e.stopPropagation(); setExpandedDiffs(prev => { const nx = new Set(prev); nx.has(file.filename) ? nx.delete(file.filename) : nx.add(file.filename); return nx; }); }}
                                                                    >
                                                                        <ChevronRight className={"w-3 h-3 text-obsidian-muted shrink-0 transition-transform" + (expandedDiffs.has(file.filename) ? " rotate-90" : "")} />
                                                                        <span className={"text-[9px] px-1 rounded font-mono shrink-0 " + (file.status === 'added' ? "bg-obsidian-success/15 text-obsidian-success" : file.status === 'removed' ? "bg-obsidian-danger/15 text-obsidian-danger" : "bg-obsidian-warning/15 text-obsidian-warning")}>{(file.status || 'M').slice(0, 1).toUpperCase()}</span>
                                                                        <span className="font-mono text-[11px] text-foreground truncate flex-1">{file.filename}</span>
                                                                        {fileInlineComments.length > 0 && (
                                                                            <span className="text-[9px] text-obsidian-info shrink-0">{fileInlineComments.length} inline comment{fileInlineComments.length === 1 ? '' : 's'}</span>
                                                                        )}
                                                                        <span className="text-[9px] text-obsidian-success shrink-0">+{file.additions || 0}</span>
                                                                        <span className="text-[9px] text-obsidian-danger shrink-0 ml-1">-{file.deletions || 0}</span>
                                                                    </div>
                                                                    {expandedDiffs.has(file.filename) && file.patch && (
                                                                        <div className="overflow-x-auto text-[11px] font-mono leading-5" style={{ fontFamily: 'JetBrains Mono,Menlo,Monaco,Consolas,monospace' }}>
                                                                            {parsePatchLines(file.patch).map((line, li) => {
                                                                                let bg = 'transparent', color = '#bcbec4';
                                                                                if (line.kind === 'add') { bg = '#1a3a2a'; color = '#6aab73'; }
                                                                                else if (line.kind === 'del') { bg = '#3a1a1a'; color = '#ff5261'; }
                                                                                else if (line.kind === 'hunk') { bg = '#1a2a3a'; color = '#3574f0'; }

                                                                                const lineComments = fileInlineComments.filter((c) =>
                                                                                    (line.newPosition > 0 && c.new_position === line.newPosition) ||
                                                                                    (line.oldPosition > 0 && c.old_position === line.oldPosition)
                                                                                );

                                                                                return (
                                                                                    <div key={`${file.filename}-${li}`}>
                                                                                        <div className="px-2 py-0 whitespace-pre flex items-center gap-2" style={{ backgroundColor: bg, color }}>
                                                                                            <span className="w-10 text-right text-[9px] text-obsidian-muted/80">{line.oldPosition > 0 ? line.oldPosition : ''}</span>
                                                                                            <span className="w-10 text-right text-[9px] text-obsidian-muted/80">{line.newPosition > 0 ? line.newPosition : ''}</span>
                                                                                            <span className="flex-1">{line.text}</span>
                                                                                            {(line.newPosition > 0 || line.oldPosition > 0) && (
                                                                                                <button
                                                                                                    onClick={(e) => {
                                                                                                        e.stopPropagation();
                                                                                                        setInlineDraft({
                                                                                                            path: file.filename,
                                                                                                            newPosition: line.newPosition,
                                                                                                            oldPosition: line.oldPosition,
                                                                                                            body: '',
                                                                                                        });
                                                                                                    }}
                                                                                                    className="px-1.5 py-0.5 rounded border border-obsidian-info/30 bg-obsidian-info/10 text-obsidian-info text-[9px] hover:bg-obsidian-info/20"
                                                                                                >
                                                                                                    Comment
                                                                                                </button>
                                                                                            )}
                                                                                        </div>
                                                                                        {lineComments.length > 0 && (
                                                                                            <div className="px-4 py-1.5 bg-black/20 border-t border-white/5 space-y-1">
                                                                                                {lineComments.map((comment) => (
                                                                                                    <div key={`${comment.id}-${comment.review_id || 0}`} className="text-[10px] text-foreground/90">
                                                                                                        <span className="text-obsidian-info">{comment.user?.login || 'unknown'}</span>
                                                                                                        <span className="text-obsidian-muted"> · line {comment.new_position || comment.old_position || '-'}</span>
                                                                                                        <div className="text-[11px] mt-0.5 whitespace-pre-wrap">{comment.body || ''}</div>
                                                                                                    </div>
                                                                                                ))}
                                                                                            </div>
                                                                                        )}
                                                                                    </div>
                                                                                );
                                                                            })}
                                                                        </div>
                                                                    )}
                                                                    {expandedDiffs.has(file.filename) && inlineDraft?.path === file.filename && (
                                                                        <div className="px-4 py-3 bg-black/20 border-t border-white/10 space-y-2">
                                                                            <div className="text-[10px] text-obsidian-muted">
                                                                                Inline comment on <span className="font-mono text-foreground">{inlineDraft.path}</span>
                                                                                {' '}line <span className="text-obsidian-info">{inlineDraft.newPosition || inlineDraft.oldPosition}</span>
                                                                            </div>
                                                                            <textarea
                                                                                rows={3}
                                                                                value={inlineDraft.body}
                                                                                onChange={(e) => setInlineDraft(prev => prev ? { ...prev, body: e.target.value } : prev)}
                                                                                placeholder="Write inline review comment..."
                                                                                className="w-full bg-black/30 border border-white/10 rounded px-2 py-1.5 text-[11px] text-foreground placeholder-obsidian-muted outline-none resize-y"
                                                                            />
                                                                            <div className="flex items-center justify-end gap-2">
                                                                                <button
                                                                                    onClick={() => setInlineDraft(null)}
                                                                                    disabled={inlineSubmitting}
                                                                                    className="px-3 py-1.5 rounded text-[10px] border border-white/10 text-obsidian-muted hover:text-foreground"
                                                                                >
                                                                                    Cancel
                                                                                </button>
                                                                                <button
                                                                                    onClick={() => handleSubmitInlineComment(pr.number)}
                                                                                    disabled={inlineSubmitting || !inlineDraft.body.trim()}
                                                                                    className="px-3 py-1.5 rounded text-[10px] border border-obsidian-info/30 bg-obsidian-info/20 text-obsidian-info hover:bg-obsidian-info/30 disabled:opacity-50 inline-flex items-center gap-1.5"
                                                                                >
                                                                                    {inlineSubmitting && <Loader2 className="w-3 h-3 animate-spin" />}
                                                                                    Submit Comment
                                                                                </button>
                                                                            </div>
                                                                        </div>
                                                                    )}
                                                                </div>
                                                            );
                                                        })}
                                                    </div>
                                                )}
                                                {selectedPR === pr.number && (
                                                    <div className="border-t border-white/5 bg-black/10 px-4 py-3">
                                                        <div className="text-[11px] font-semibold text-foreground mb-2">Conversation</div>
                                                        {prActivityLoading ? (
                                                            <div className="flex items-center gap-2 text-[11px] text-obsidian-muted">
                                                                <Loader2 className="w-3.5 h-3.5 animate-spin text-obsidian-info" />
                                                                Loading comments and reviews...
                                                            </div>
                                                        ) : (prComments.length === 0 && prReviews.length === 0 && prInlineComments.length === 0) ? (
                                                            <div className="text-[11px] text-obsidian-muted">No comments or reviews yet.</div>
                                                        ) : (
                                                            <div className="space-y-2">
                                                                {[...prComments.map(c => ({ kind: 'comment' as const, at: c.created_at || '', by: c.user?.login || 'unknown', body: c.body || '' })),
                                                                ...prReviews.map(r => ({ kind: 'review' as const, at: r.submitted_at || '', by: r.user?.login || 'unknown', body: r.body || '', state: r.state || 'REVIEWED' })),
                                                                ...prInlineComments.map(ic => ({
                                                                    kind: 'inline' as const,
                                                                    at: ic.created_at || '',
                                                                    by: ic.user?.login || 'unknown',
                                                                    body: ic.body || '',
                                                                    state: `INLINE ${ic.path || ''}:${ic.new_position || ic.old_position || '-'}`.trim(),
                                                                }))]
                                                                    .sort((a, b) => new Date(a.at).getTime() - new Date(b.at).getTime())
                                                                    .map((item, idx) => (
                                                                        <div key={`${item.kind}-${idx}`} className="rounded-md border border-white/10 bg-white/[0.02] p-2.5">
                                                                            <div className="flex items-center gap-2 text-[10px] text-obsidian-muted mb-1">
                                                                                <span className="font-medium text-foreground">{item.by}</span>
                                                                                <span>{item.kind === 'review' ? (item.state || 'REVIEWED') : item.kind === 'inline' ? (item.state || 'INLINE') : 'COMMENTED'}</span>
                                                                                <span className="ml-auto">{timeAgo(item.at)}</span>
                                                                            </div>
                                                                            {item.body ? (
                                                                                <div className="text-[11px] text-foreground/90 whitespace-pre-wrap">{item.body}</div>
                                                                            ) : (
                                                                                <div className="text-[11px] text-obsidian-muted italic">No message</div>
                                                                            )}
                                                                        </div>
                                                                    ))}
                                                            </div>
                                                        )}
                                                    </div>
                                                )}
                                            </div>
                                        ))}
                                        {pullsHasMore && (
                                            <div className="px-4 py-3 flex justify-center border-b border-white/5">
                                                <button
                                                    onClick={() => {
                                                        if (!selectedRepo || pullsLoadingMore) return;
                                                        const [owner, repo] = selectedRepo.full_name.split('/');
                                                        fetchPulls(owner, repo, prState, { page: pullsPage + 1, append: true });
                                                    }}
                                                    disabled={pullsLoadingMore}
                                                    className="px-3 py-1.5 rounded-md text-[11px] font-medium border border-white/10 bg-white/[0.02] hover:bg-white/[0.05] disabled:opacity-50 flex items-center gap-1.5"
                                                >
                                                    {pullsLoadingMore && <Loader2 className="w-3.5 h-3.5 animate-spin" />}
                                                    Load more PRs
                                                </button>
                                            </div>
                                        )}
                                    </div>

                                ) : detailTab === 'issues' ? (
                                    /* ─── Issues Tab ─── */
                                    <div className="flex-1 overflow-auto">
                                        <div className="flex items-center gap-2 px-4 py-2 border-b border-white/5 bg-black/20 backdrop-blur-md">
                                            {(['open', 'closed', 'all'] as const).map(s => (
                                                <button
                                                    key={s}
                                                    onClick={() => {
                                                        setIssuesState(s);
                                                        if (!selectedRepo) return;
                                                        const [owner, repo] = selectedRepo.full_name.split('/');
                                                        fetchIssues(owner, repo, s, { page: 1 });
                                                    }}
                                                    className={clsx(
                                                        "px-2 py-1 rounded text-[10px] font-medium transition-all",
                                                        issuesState === s ? "bg-obsidian-info/20 text-obsidian-info" : "text-obsidian-muted hover:text-foreground"
                                                    )}
                                                >
                                                    {s.charAt(0).toUpperCase() + s.slice(1)}
                                                </button>
                                            ))}
                                            <span className="ml-auto text-[11px] text-obsidian-muted">{issues.length} issues</span>
                                        </div>
                                        {issues.length === 0 ? (
                                            <div className="text-center py-10 text-obsidian-muted text-[11px]">No issues</div>
                                        ) : (
                                            <>
                                                {issues.map(issue => (
                                                    <div key={issue.id} className="px-4 py-3 border-b border-white/5 hover:bg-white/[0.02] transition-colors">
                                                        <div className="flex items-start gap-2">
                                                            <AlertCircle className={clsx("w-4 h-4 mt-0.5", issue.state === 'open' ? "text-obsidian-success" : "text-obsidian-muted")} />
                                                            <div className="flex-1 min-w-0">
                                                                <div className="text-[12px] font-medium text-foreground truncate">#{issue.number} {issue.title}</div>
                                                                <div className="flex items-center gap-2 mt-1 text-[10px] text-obsidian-muted">
                                                                    <span>{issue.state}</span>
                                                                    <span>{issue.user?.login || 'unknown'}</span>
                                                                    <span>{timeAgo(issue.updated_at || issue.created_at)}</span>
                                                                    {(issue.comments || 0) > 0 && (
                                                                        <span className="ml-auto flex items-center gap-1"><MessageSquare className="w-3 h-3" />{issue.comments}</span>
                                                                    )}
                                                                </div>
                                                                {Array.isArray(issue.labels) && issue.labels.length > 0 && (
                                                                    <div className="flex flex-wrap gap-1 mt-2">
                                                                        {issue.labels.slice(0, 4).map(label => (
                                                                            <span
                                                                                key={label.id}
                                                                                className="text-[9px] px-1.5 py-0.5 rounded font-medium"
                                                                                style={{ backgroundColor: `#${label.color || '6c707e'}33`, color: `#${label.color || '6c707e'}` }}
                                                                            >
                                                                                {label.name}
                                                                            </span>
                                                                        ))}
                                                                    </div>
                                                                )}
                                                            </div>
                                                        </div>
                                                    </div>
                                                ))}
                                                {issuesHasMore && (
                                                    <div className="px-4 py-3 flex justify-center">
                                                        <button
                                                            onClick={() => {
                                                                if (!selectedRepo || issuesLoadingMore) return;
                                                                const [owner, repo] = selectedRepo.full_name.split('/');
                                                                fetchIssues(owner, repo, issuesState, { page: issuesPage + 1, append: true });
                                                            }}
                                                            disabled={issuesLoadingMore}
                                                            className="px-3 py-1.5 rounded-md text-[11px] font-medium border border-white/10 bg-white/[0.02] hover:bg-white/[0.05] disabled:opacity-50 flex items-center gap-1.5"
                                                        >
                                                            {issuesLoadingMore && <Loader2 className="w-3.5 h-3.5 animate-spin" />}
                                                            Load more issues
                                                        </button>
                                                    </div>
                                                )}
                                            </>
                                        )}
                                    </div>

                                ) : detailTab === 'releases' ? (
                                    /* ─── Releases Tab ─── */
                                    <div className="flex-1 overflow-auto">
                                        <div className="px-4 py-2 border-b border-white/5 bg-black/20 backdrop-blur-md text-[11px] text-obsidian-muted">
                                            {releases.length} release{releases.length !== 1 ? 's' : ''}
                                        </div>
                                        {releases.length === 0 ? (
                                            <div className="text-center py-10 text-obsidian-muted text-[11px]">No releases</div>
                                        ) : (
                                            <>
                                                {releases.map(release => (
                                                    <div key={release.id} className="px-4 py-3 border-b border-white/5 hover:bg-white/[0.02] transition-colors">
                                                        <div className="flex items-center gap-2">
                                                            <PackageOpen className="w-4 h-4 text-obsidian-info" />
                                                            <span className="text-[12px] font-medium text-foreground">{release.name || release.tag_name}</span>
                                                            <span className="text-[10px] text-obsidian-muted font-mono">{release.tag_name}</span>
                                                            {release.prerelease && <span className="text-[9px] px-1.5 py-0.5 rounded bg-obsidian-warning/15 text-obsidian-warning">prerelease</span>}
                                                            {release.draft && <span className="text-[9px] px-1.5 py-0.5 rounded bg-obsidian-border-active/30 text-obsidian-muted">draft</span>}
                                                            <span className="ml-auto text-[10px] text-obsidian-muted">{timeAgo(release.published_at || release.created_at || null)}</span>
                                                        </div>
                                                        {release.body && (
                                                            <p className="mt-2 text-[11px] text-obsidian-muted whitespace-pre-wrap line-clamp-3">{release.body}</p>
                                                        )}
                                                    </div>
                                                ))}
                                                {releasesHasMore && (
                                                    <div className="px-4 py-3 flex justify-center">
                                                        <button
                                                            onClick={() => {
                                                                if (!selectedRepo || releasesLoadingMore) return;
                                                                const [owner, repo] = selectedRepo.full_name.split('/');
                                                                fetchReleases(owner, repo, { page: releasesPage + 1, append: true });
                                                            }}
                                                            disabled={releasesLoadingMore}
                                                            className="px-3 py-1.5 rounded-md text-[11px] font-medium border border-white/10 bg-white/[0.02] hover:bg-white/[0.05] disabled:opacity-50 flex items-center gap-1.5"
                                                        >
                                                            {releasesLoadingMore && <Loader2 className="w-3.5 h-3.5 animate-spin" />}
                                                            Load more releases
                                                        </button>
                                                    </div>
                                                )}
                                            </>
                                        )}
                                    </div>

                                ) : detailTab === 'branches' ? (
                                    /* ─── Branches Tab ─── */
                                    <div className="flex-1 overflow-auto">
                                        {/* Create Branch */}
                                        <div className="flex items-center gap-2 px-4 py-2 border-b border-white/5 bg-black/20 backdrop-blur-md transition-all active:scale-95">
                                            <span className="text-[11px] text-obsidian-muted">{branches.length} branches</span>
                                            <button onClick={() => setShowCreateBranch(!showCreateBranch)}
                                                className="ml-auto flex items-center gap-1 px-2 py-1 bg-obsidian-info/15 text-obsidian-info rounded text-[10px] font-medium hover:bg-obsidian-info/25 transition-all active:scale-95">
                                                <Plus className="w-3.5 h-3.5" /> New Branch
                                            </button>
                                        </div>

                                        {showCreateBranch && (
                                            <div className="px-4 py-3 border-b border-white/5 bg-black/40 backdrop-blur-md space-y-2 transition-all active:scale-95">
                                                <input type="text" placeholder="Branch name..." value={newBranchName} onChange={e => setNewBranchName(e.target.value)}
                                                    className="w-full bg-black/20 border border-white/10 rounded px-2 py-1.5 text-[11px] text-foreground placeholder-obsidian-muted outline-none transition-all active:scale-95" />
                                                <div className="flex items-center gap-2">
                                                    <span className="text-[10px] text-obsidian-muted">from</span>
                                                    <select value={baseBranch} onChange={e => setBaseBranch(e.target.value)}
                                                        className="flex-1 bg-black/20 border border-white/10 rounded px-2 py-1 text-[11px] text-foreground outline-none transition-all active:scale-95">
                                                        <option value="">{selectedRepo?.default_branch || 'main'}</option>
                                                        {branches.map(b => <option key={b.name} value={b.name}>{b.name}</option>)}
                                                    </select>
                                                    <button onClick={handleCreateBranch} disabled={!newBranchName}
                                                        className="px-3 py-1 bg-obsidian-info text-white rounded text-[10px] font-medium hover:bg-obsidian-info disabled:opacity-40 transition-all active:scale-95">Create</button>
                                                </div>
                                            </div>
                                        )}

                                        {branches.length === 0 ? (
                                            <div className="text-center py-8 text-obsidian-muted text-[11px]">No branches</div>
                                        ) : branches.map(branch => (
                                            <div key={branch.name} className="px-4 py-3 border-b border-white/5 hover:bg-white/[0.02] transition-all active:scale-95">
                                                <div className="flex items-center gap-2">
                                                    <GitBranch className="w-3.5 h-3.5 text-obsidian-success shrink-0" />
                                                    <span className="text-[12px] text-foreground font-mono">{branch.name}</span>
                                                    {branch.name === selectedRepo?.default_branch && (
                                                        <span className="text-[8px] px-1.5 py-0.5 rounded bg-obsidian-info/15 text-obsidian-info font-semibold uppercase transition-all active:scale-95">default</span>
                                                    )}
                                                    {branch.protected && (
                                                        <span className="text-[8px] px-1.5 py-0.5 rounded bg-obsidian-warning/15 text-obsidian-warning font-semibold uppercase transition-all active:scale-95">protected</span>
                                                    )}
                                                </div>
                                                <div className="mt-1 text-[10px] text-obsidian-muted pl-5 truncate">
                                                    {branch.commit?.id?.slice(0, 7)} — {branch.commit?.message?.split('\n')[0]}
                                                </div>
                                                <div className="mt-1.5 flex items-center gap-1.5 pl-5">
                                                    <button
                                                        onClick={() => switchBranch(branch.name)}
                                                        disabled={activeBranch === branch.name}
                                                        className="flex items-center gap-1 px-2 py-0.5 text-[9px] bg-obsidian-border-active text-foreground rounded hover:bg-obsidian-info/20 hover:text-obsidian-info disabled:opacity-40 disabled:cursor-default transition-colors active:scale-95"
                                                    >
                                                        {activeBranch === branch.name ? '✓ Active' : '⇄ Switch'}
                                                    </button>
                                                    {branch.name !== selectedRepo?.default_branch && !branch.protected && (
                                                        <button
                                                            onClick={() => setDeleteBranchTarget(branch.name)}
                                                            className="flex items-center gap-1 px-2 py-0.5 text-[9px] bg-obsidian-danger/10 text-obsidian-danger rounded hover:bg-obsidian-danger/20 transition-colors active:scale-95"
                                                        >
                                                            🗑 Delete
                                                        </button>
                                                    )}
                                                </div>
                                            </div>
                                        ))}
                                    </div>


                                ) : detailTab === 'tags' ? (
                                    /* ─── Tags Tab ─── */
                                    <div className="flex-1 overflow-auto">
                                        <div className="px-4 py-2 border-b border-white/5 bg-black/20 backdrop-blur-md text-[11px] text-obsidian-muted transition-all active:scale-95">
                                            {tags.length} tag{tags.length !== 1 ? 's' : ''}
                                        </div>
                                        {tags.length === 0 ? (
                                            <div className="text-center py-8 text-obsidian-muted text-[11px]">No tags</div>
                                        ) : tags.map(tag => (
                                            <div key={tag.name} className="px-4 py-3 border-b border-white/5 hover:bg-white/[0.02] flex items-center gap-2 transition-all active:scale-95">
                                                <Star className="w-3.5 h-3.5 text-obsidian-warning shrink-0" />
                                                <span className="text-[12px] font-mono text-foreground">{tag.name}</span>
                                                <span className="ml-2 text-[10px] text-obsidian-muted font-mono">{tag.commit?.sha?.slice(0, 7)}</span>
                                                {tag.tarball_url && (
                                                    <a href={tag.tarball_url} className="ml-auto text-[9px] text-obsidian-info hover:underline" download>↓ tarball</a>
                                                )}
                                            </div>
                                        ))}
                                        {tagsHasMore && (
                                            <div className="px-4 py-3 flex justify-center border-b border-white/5">
                                                <button
                                                    onClick={() => {
                                                        if (!selectedRepo || tagsLoadingMore) return;
                                                        const [owner, repo] = selectedRepo.full_name.split('/');
                                                        fetchTags(owner, repo, { page: tagsPage + 1, append: true });
                                                    }}
                                                    disabled={tagsLoadingMore}
                                                    className="px-3 py-1.5 rounded-md text-[11px] font-medium border border-white/10 bg-white/[0.02] hover:bg-white/[0.05] disabled:opacity-50 flex items-center gap-1.5"
                                                >
                                                    {tagsLoadingMore && <Loader2 className="w-3.5 h-3.5 animate-spin" />}
                                                    Load more tags
                                                </button>
                                            </div>
                                        )}
                                    </div>

                                ) : detailTab === 'actions' ? (
                                    /* ─── Actions Tab ─── */
                                    <div className="flex-1 overflow-auto">
                                        {actionRuns.length === 0 ? (
                                            <div className="flex flex-col items-center justify-center py-16 text-obsidian-muted gap-2">
                                                <Play className="w-8 h-8 opacity-30" />
                                                <span className="text-[12px]">No workflow runs</span>
                                            </div>
                                        ) : actionRuns.map((run) => (
                                            <div key={run.id} className="border-b border-white/5">
                                                <div
                                                    className={clsx(
                                                        "px-4 py-2.5 hover:bg-white/[0.02] transition-all active:scale-95 cursor-pointer",
                                                        selectedActionRun === run.id && "bg-white/[0.03]"
                                                    )}
                                                    onClick={() => {
                                                        if (!selectedRepo) return;
                                                        const [owner, repo] = selectedRepo.full_name.split('/');
                                                        if (selectedActionRun === run.id) {
                                                            setSelectedActionRun(null);
                                                            setActionJobs([]);
                                                            return;
                                                        }
                                                        setSelectedActionRun(run.id);
                                                        fetchActionRunJobs(owner, repo, run.id);
                                                    }}
                                                >
                                                    <div className="flex items-center justify-between mb-1">
                                                        <div className="flex items-center gap-2 min-w-0">
                                                            <ActionBadge status={run.status || 'unknown'} conclusion={run.conclusion} />
                                                            <span className="text-[11px] text-foreground font-medium truncate">{run.name || `Run #${run.id}`}</span>
                                                        </div>
                                                        <span className="text-[10px] text-obsidian-muted shrink-0 ml-2">{timeAgo(run.updated_at || run.created_at)}</span>
                                                    </div>
                                                    <div className="flex items-center gap-2 text-[10px] text-obsidian-muted pl-1">
                                                        {run.head_branch && (
                                                            <span className="flex items-center gap-0.5"><GitBranch className="w-2.5 h-2.5" />{run.head_branch}</span>
                                                        )}
                                                        {run.run_number && <span>#{run.run_number}</span>}
                                                        <div className="ml-auto flex items-center gap-1.5">
                                                            <button
                                                                onClick={(e) => {
                                                                    e.stopPropagation();
                                                                    handleActionRunCommand(run.id, 'rerun');
                                                                }}
                                                                disabled={Boolean(actionRunCommand && actionRunCommand.runId === run.id)}
                                                                className="px-2 py-0.5 rounded border border-obsidian-info/30 bg-obsidian-info/10 text-obsidian-info hover:bg-obsidian-info/20 disabled:opacity-50 text-[9px]"
                                                            >
                                                                {actionRunCommand?.runId === run.id && actionRunCommand.action === 'rerun' ? 'Running...' : 'Re-run'}
                                                            </button>
                                                            <button
                                                                onClick={(e) => {
                                                                    e.stopPropagation();
                                                                    handleActionRunCommand(run.id, 'cancel');
                                                                }}
                                                                disabled={Boolean(actionRunCommand && actionRunCommand.runId === run.id)}
                                                                className="px-2 py-0.5 rounded border border-obsidian-danger/30 bg-obsidian-danger/10 text-obsidian-danger hover:bg-obsidian-danger/20 disabled:opacity-50 text-[9px]"
                                                            >
                                                                {actionRunCommand?.runId === run.id && actionRunCommand.action === 'cancel' ? 'Cancelling...' : 'Cancel'}
                                                            </button>
                                                            <span className="text-[9px] text-obsidian-info">
                                                            {selectedActionRun === run.id ? 'Hide details' : 'View details'}
                                                            </span>
                                                        </div>
                                                    </div>
                                                </div>

                                                {selectedActionRun === run.id && (
                                                    <div className="px-4 pb-3 bg-white/[0.01] border-t border-white/5">
                                                        {actionJobsLoading ? (
                                                            <div className="flex items-center gap-2 text-[11px] text-obsidian-muted py-2">
                                                                <Loader2 className="w-3.5 h-3.5 animate-spin text-obsidian-info" />
                                                                Loading run details...
                                                            </div>
                                                        ) : actionJobs.length === 0 ? (
                                                            <div className="text-[11px] text-obsidian-muted py-2">No job details returned by Gitea for this run.</div>
                                                        ) : (
                                                            <div className="space-y-2 pt-2">
                                                                {actionJobs.map((job, idx) => (
                                                                    <div key={`${job.id || idx}`} className="rounded-md border border-white/10 bg-black/20 p-2.5">
                                                                        <div className="flex items-center gap-2 text-[11px]">
                                                                            <span className="font-medium text-foreground">{job.name || `Job ${idx + 1}`}</span>
                                                                            <ActionBadge status={job.status || 'unknown'} conclusion={job.conclusion} />
                                                                            {job.runner_name && <span className="ml-auto text-[10px] text-obsidian-muted">{job.runner_name}</span>}
                                                                        </div>
                                                                        {Array.isArray(job.steps) && job.steps.length > 0 && (
                                                                            <div className="mt-2 space-y-1">
                                                                                {job.steps.map((step, si) => (
                                                                                    <div key={`${step.number || si}`} className="flex items-center gap-2 text-[10px] text-obsidian-muted">
                                                                                        <span className="w-4 text-right">{step.number ?? si + 1}</span>
                                                                                        <span className="text-foreground/90">{step.name || `Step ${si + 1}`}</span>
                                                                                        <span className="ml-auto">{step.conclusion || step.status || 'unknown'}</span>
                                                                                    </div>
                                                                                ))}
                                                                            </div>
                                                                        )}
                                                                    </div>
                                                                ))}
                                                            </div>
                                                        )}
                                                    </div>
                                                )}
                                            </div>
                                        ))}
                                        {actionsHasMore && (
                                            <div className="px-4 py-3 flex justify-center">
                                                <button
                                                    onClick={() => {
                                                        if (!selectedRepo || actionsLoadingMore) return;
                                                        const [owner, repo] = selectedRepo.full_name.split('/');
                                                        fetchActions(owner, repo, { page: actionsPage + 1, append: true });
                                                    }}
                                                    disabled={actionsLoadingMore}
                                                    className="px-3 py-1.5 rounded-md text-[11px] font-medium border border-white/10 bg-white/[0.02] hover:bg-white/[0.05] disabled:opacity-50 flex items-center gap-1.5"
                                                >
                                                    {actionsLoadingMore && <Loader2 className="w-3.5 h-3.5 animate-spin" />}
                                                    Load more runs
                                                </button>
                                            </div>
                                        )}
                                    </div>
                                ) : detailTab === 'security' ? (
                                    /* ─── Security Tab ─── */
                                    <div className="flex-1 overflow-auto">
                                        <div className="px-4 py-3 border-b border-white/5 bg-black/20 backdrop-blur-md">
                                            <div className="text-[12px] font-semibold text-foreground flex items-center gap-2">
                                                <Scale className="w-4 h-4 text-obsidian-warning" />
                                                Branch Protection
                                            </div>
                                        </div>
                                        <div className="p-6 space-y-4">
                                            <div className="rounded-lg border border-white/10 bg-black/20 p-4">
                                                <div className="text-[12px] text-foreground font-medium mb-1">Target Branch</div>
                                                <div className="text-[11px] text-obsidian-muted">
                                                    Default branch:
                                                    <span className="font-mono text-foreground ml-1">{selectedRepo?.default_branch || 'main'}</span>
                                                    <span className="ml-2 text-obsidian-info">{branchProtectionExists ? '(existing rule)' : '(new rule)'}</span>
                                                </div>
                                            </div>

                                            {branchProtectionLoading ? (
                                                <div className="rounded-lg border border-white/10 bg-black/20 p-4 text-[11px] text-obsidian-muted flex items-center gap-2">
                                                    <Loader2 className="w-3.5 h-3.5 animate-spin text-obsidian-info" />
                                                    Loading branch protection...
                                                </div>
                                            ) : (
                                                <div className="rounded-lg border border-white/10 bg-black/20 p-4 space-y-3">
                                                    <label className="flex items-center gap-2 text-[11px] text-foreground">
                                                        <input
                                                            type="checkbox"
                                                            checked={Boolean(branchProtection.enable_status_check)}
                                                            onChange={(e) => setBranchProtection(prev => ({ ...prev, enable_status_check: e.target.checked }))}
                                                        />
                                                        Enable required status checks
                                                    </label>

                                                    <div className="space-y-1">
                                                        <div className="text-[10px] text-obsidian-muted">Status check contexts (comma separated)</div>
                                                        <input
                                                            value={statusCheckContextsInput}
                                                            onChange={(e) => setStatusCheckContextsInput(e.target.value)}
                                                            placeholder="build, test, lint"
                                                            className="w-full bg-black/30 border border-white/10 rounded px-2 py-1.5 text-[11px] text-foreground outline-none"
                                                        />
                                                    </div>

                                                    <div className="grid grid-cols-1 md:grid-cols-2 gap-3">
                                                        <label className="space-y-1">
                                                            <div className="text-[10px] text-obsidian-muted">Required approvals</div>
                                                            <input
                                                                type="number"
                                                                min={0}
                                                                value={branchProtection.required_approvals ?? 1}
                                                                onChange={(e) => setBranchProtection(prev => ({ ...prev, required_approvals: Number(e.target.value || 0) }))}
                                                                className="w-full bg-black/30 border border-white/10 rounded px-2 py-1.5 text-[11px] text-foreground outline-none"
                                                            />
                                                        </label>
                                                        <label className="flex items-center gap-2 text-[11px] text-foreground mt-5">
                                                            <input
                                                                type="checkbox"
                                                                checked={Boolean(branchProtection.dismiss_stale_approvals)}
                                                                onChange={(e) => setBranchProtection(prev => ({ ...prev, dismiss_stale_approvals: e.target.checked }))}
                                                            />
                                                            Dismiss stale approvals
                                                        </label>
                                                    </div>

                                                    <div className="grid grid-cols-1 md:grid-cols-2 gap-2">
                                                        <label className="flex items-center gap-2 text-[11px] text-foreground">
                                                            <input
                                                                type="checkbox"
                                                                checked={Boolean(branchProtection.block_on_rejected_reviews)}
                                                                onChange={(e) => setBranchProtection(prev => ({ ...prev, block_on_rejected_reviews: e.target.checked }))}
                                                            />
                                                            Block on rejected reviews
                                                        </label>
                                                        <label className="flex items-center gap-2 text-[11px] text-foreground">
                                                            <input
                                                                type="checkbox"
                                                                checked={Boolean(branchProtection.block_on_outdated_branch)}
                                                                onChange={(e) => setBranchProtection(prev => ({ ...prev, block_on_outdated_branch: e.target.checked }))}
                                                            />
                                                            Block on outdated branch
                                                        </label>
                                                        <label className="flex items-center gap-2 text-[11px] text-foreground">
                                                            <input
                                                                type="checkbox"
                                                                checked={Boolean(branchProtection.enable_force_push)}
                                                                onChange={(e) => setBranchProtection(prev => ({ ...prev, enable_force_push: e.target.checked }))}
                                                            />
                                                            Allow force push
                                                        </label>
                                                        <label className="flex items-center gap-2 text-[11px] text-foreground">
                                                            <input
                                                                type="checkbox"
                                                                checked={Boolean(branchProtection.require_signed_commits)}
                                                                onChange={(e) => setBranchProtection(prev => ({ ...prev, require_signed_commits: e.target.checked }))}
                                                            />
                                                            Require signed commits
                                                        </label>
                                                    </div>

                                                    {branchProtectionError && (
                                                        <div className="rounded border border-obsidian-danger/30 bg-obsidian-danger/10 px-2 py-1.5 text-[11px] text-obsidian-danger">
                                                            {branchProtectionError}
                                                        </div>
                                                    )}

                                                    <div className="flex items-center justify-end gap-2 pt-1">
                                                        <button
                                                            onClick={() => {
                                                                if (!selectedRepo) return;
                                                                const [owner, repo] = selectedRepo.full_name.split('/');
                                                                loadBranchProtection(owner, repo, selectedRepo.default_branch || 'main');
                                                            }}
                                                            disabled={branchProtectionLoading || branchProtectionSaving}
                                                            className="px-3 py-1.5 rounded-md text-[11px] border border-white/10 text-obsidian-muted hover:text-foreground hover:bg-white/[0.04] disabled:opacity-50"
                                                        >
                                                            Reload
                                                        </button>
                                                        <button
                                                            onClick={saveBranchProtection}
                                                            disabled={branchProtectionLoading || branchProtectionSaving}
                                                            className="px-3 py-1.5 rounded-md text-[11px] border border-obsidian-info/30 bg-obsidian-info/20 text-obsidian-info hover:bg-obsidian-info/30 disabled:opacity-50 inline-flex items-center gap-1.5"
                                                        >
                                                            {branchProtectionSaving && <Loader2 className="w-3.5 h-3.5 animate-spin" />}
                                                            Save Rules
                                                        </button>
                                                    </div>
                                                </div>
                                            )}

                                            <div className="rounded-lg border border-white/10 bg-black/20 p-4">
                                                <div className="text-[12px] text-foreground font-medium mb-1">PR Conflict Handling</div>
                                                <div className="text-[11px] text-obsidian-muted">
                                                    PR detayında merge conflict olursa "Update Branch (rebase/merge)" aksiyonları aktif.
                                                </div>
                                            </div>
                                        </div>
                                    </div>
                                ) : (
                                    /* ─── Insights Tab ─── */
                                    <div className="flex-1 overflow-auto">
                                        <div className="px-4 py-3 border-b border-white/5 bg-black/20 backdrop-blur-md">
                                            <div className="text-[12px] font-semibold text-foreground flex items-center gap-2">
                                                <BookOpen className="w-4 h-4 text-obsidian-info" />
                                                Repository Insights
                                            </div>
                                        </div>
                                        <div className="p-6 grid grid-cols-1 md:grid-cols-3 gap-4">
                                            <div className="rounded-lg border border-white/10 bg-black/20 p-4">
                                                <div className="text-[10px] text-obsidian-muted mb-1">COMMITS (LOADED)</div>
                                                <div className="text-[20px] font-semibold text-foreground">{commits.length}</div>
                                            </div>
                                            <div className="rounded-lg border border-white/10 bg-black/20 p-4">
                                                <div className="text-[10px] text-obsidian-muted mb-1">OPEN PRs</div>
                                                <div className="text-[20px] font-semibold text-foreground">{pulls.filter(p => p.state === 'open' && !p.merged).length}</div>
                                            </div>
                                            <div className="rounded-lg border border-white/10 bg-black/20 p-4">
                                                <div className="text-[10px] text-obsidian-muted mb-1">OPEN ISSUES</div>
                                                <div className="text-[20px] font-semibold text-foreground">{issues.filter(i => i.state === 'open').length}</div>
                                            </div>
                                        </div>
                                    </div>
                                )}
                            </div>
                        </div>
                    )}
                </div>
            </main>

            {deleteBranchTarget && (
                <div className="absolute inset-0 z-[120] bg-black/60 backdrop-blur-sm flex items-center justify-center p-4">
                    <div className="w-full max-w-md rounded-xl border border-white/10 bg-[#101114] shadow-2xl overflow-hidden">
                        <div className="px-4 py-3 border-b border-white/10">
                            <div className="text-[13px] font-semibold text-foreground">Delete branch?</div>
                            <div className="text-[11px] text-obsidian-muted mt-1">
                                Branch <span className="font-mono text-foreground">{deleteBranchTarget}</span> kalici olarak silinecek.
                            </div>
                        </div>
                        <div className="px-4 py-3 flex items-center justify-end gap-2">
                            <button
                                onClick={() => setDeleteBranchTarget(null)}
                                disabled={deletingBranch === deleteBranchTarget}
                                className="px-3 py-1.5 rounded-md text-[11px] border border-white/10 text-obsidian-muted hover:text-foreground hover:bg-white/[0.04] disabled:opacity-50"
                            >
                                Cancel
                            </button>
                            <button
                                onClick={async () => {
                                    const target = deleteBranchTarget;
                                    setDeleteBranchTarget(null);
                                    if (target) await handleDeleteBranch(target);
                                }}
                                disabled={deletingBranch === deleteBranchTarget}
                                className="px-3 py-1.5 rounded-md text-[11px] border border-obsidian-danger/40 bg-obsidian-danger/20 text-obsidian-danger hover:bg-obsidian-danger/30 disabled:opacity-50 inline-flex items-center gap-1.5"
                            >
                                {deletingBranch === deleteBranchTarget && <Loader2 className="w-3 h-3 animate-spin" />}
                                Delete Branch
                            </button>
                        </div>
                    </div>
                </div>
            )}

            {uiNotice && (
                <div className="absolute right-4 bottom-4 z-[130] max-w-md">
                    <div
                        className={clsx(
                            "flex items-start gap-2 rounded-lg border px-3 py-2 shadow-2xl backdrop-blur-md",
                            uiNotice.tone === 'success' && "border-obsidian-success/40 bg-obsidian-success/15 text-obsidian-success",
                            uiNotice.tone === 'error' && "border-obsidian-danger/40 bg-obsidian-danger/15 text-obsidian-danger",
                            uiNotice.tone === 'info' && "border-obsidian-info/40 bg-obsidian-info/15 text-obsidian-info",
                        )}
                    >
                        {uiNotice.tone === 'success' ? (
                            <CheckCircle2 className="w-4 h-4 mt-0.5 shrink-0" />
                        ) : uiNotice.tone === 'error' ? (
                            <AlertCircle className="w-4 h-4 mt-0.5 shrink-0" />
                        ) : (
                            <AlertCircle className="w-4 h-4 mt-0.5 shrink-0" />
                        )}
                        <div className="text-[11px] leading-5">{uiNotice.message}</div>
                        <button
                            onClick={() => setUiNotice(null)}
                            className="ml-1 text-current/80 hover:text-current"
                            aria-label="Dismiss notification"
                        >
                            <X className="w-3.5 h-3.5" />
                        </button>
                    </div>
                </div>
            )}
        </div>
    );
}
