
import React, { useState, useEffect, useCallback, useMemo } from 'react';
import {
    GitPullRequest, ExternalLink, RefreshCw, Loader2, AlertCircle,
    CheckCircle2, Maximize2, Minimize2, Search, Star, Clock,
    GitBranch, GitCommit, Play, XCircle, ChevronRight, X, PackageOpen,
    Folder, FileCode, FileText, File as FileIcon, FolderOpen,
    Plus, ArrowUpRight, GitMerge, MessageSquare, ChevronDown, Eye, Code2
} from 'lucide-react';
import clsx from 'clsx';
import { Sidebar } from '@/components/Sidebar';
import { Prism as SyntaxHighlighter } from 'react-syntax-highlighter';
import { vscDarkPlus } from 'react-syntax-highlighter/dist/esm/styles/prism';
import ReactMarkdown from 'react-markdown';
import remarkGfm from 'remark-gfm';
import rehypeRaw from 'rehype-raw';
import Editor, { OnMount } from "@monaco-editor/react";

const GITEA_URL = 'http://localhost:3030';

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
    head: { label: string; ref: string };
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

interface ActionRun {
    id: number;
    name: string;
    status: string;
    conclusion: string;
    head_branch: string;
    created_at: string;
    updated_at: string;
    run_number: number;
}

// ─── Utility ───

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
        <span className="inline-flex items-center gap-1.5 rounded-md px-2 py-0.5 text-[10px] font-medium border bg-obsidian-primary/10 text-obsidian-primary border-obsidian-primary/20">
            <GitMerge className="w-3.5 h-3.5" /> Merged
        </span>
    );
    if (pr.state === 'closed') return (
        <span className="inline-flex items-center gap-1.5 rounded-md px-2 py-0.5 text-[10px] font-medium border bg-obsidian-danger/10 text-obsidian-danger border-obsidian-danger/20">
            <XCircle className="w-3.5 h-3.5" /> Closed
        </span>
    );
    return (
        <span className="inline-flex items-center gap-1.5 rounded-md px-2 py-0.5 text-[10px] font-medium border bg-obsidian-success/10 text-obsidian-success border-obsidian-success/20">
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

type DetailTab = 'files' | 'commits' | 'pulls' | 'branches' | 'actions' | 'tags';

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
    const [actionRuns, setActionRuns] = useState<ActionRun[]>([]);
    const [detailLoading, setDetailLoading] = useState(false);
    const [activeBranch, setActiveBranch] = useState<string>('');
    const [showBranchDropdown, setShowBranchDropdown] = useState(false);

    // Tags
    const [tags, setTags] = useState<{ name: string; commit: { sha: string }; tarball_url: string }[]>([]);

    // File tree state
    const [treeEntries, setTreeEntries] = useState<Record<string, TreeEntry[]>>({});
    const [expandedDirs, setExpandedDirs] = useState<Set<string>>(new Set());
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
    const [showCreatePR, setShowCreatePR] = useState(false);
    const [selectedPR, setSelectedPR] = useState<number | null>(null);
    const [prDetail, setPrDetail] = useState<{ pull: PullRequest; files: { filename: string; status: string; additions: number; deletions: number; patch?: string }[] } | null>(null);
    const [prDetailLoading, setPrDetailLoading] = useState(false);
    const [prTitle, setPrTitle] = useState('');
    const [prBody, setPrBody] = useState('');
    const [prHead, setPrHead] = useState('');
    const [prBase, setPrBase] = useState('');

    // Branch create
    const [showCreateBranch, setShowCreateBranch] = useState(false);
    const [newBranchName, setNewBranchName] = useState('');
    const [baseBranch, setBaseBranch] = useState('');

    // ─── Health Check ───
    const checkHealth = useCallback(async () => {
        setHealth('checking');
        try {
            const data = await fetchJsonWithTimeout('/api/gitea/health', 8000);
            if (data.status === 'online') {
                setHealth('online');
                setGiteaVersion(data.version || '');
            } else { setHealth('offline'); }
        } catch { setHealth('offline'); }
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
        } catch { setBranches([]); }
    }, []);

    const fetchTags = useCallback(async (owner: string, repo: string) => {
        try {
            const res = await fetch(`/api/gitea/repos/${owner}/${repo}/tags`);
            const data = await res.json();
            setTags(Array.isArray(data) ? data : (data.tags || []));
        } catch { setTags([]); }
    }, []);

    const fetchCommits = useCallback(async (owner: string, repo: string, ref?: string) => {
        try {
            const sha = ref ? `&sha=${encodeURIComponent(ref)}` : '';
            const res = await fetch(`/api/gitea/repos/${owner}/${repo}/commits?limit=30${sha}`);
            const data = await res.json();
            setCommits(data.commits || []);
        } catch { setCommits([]); }
    }, []);

    const fetchActions = useCallback(async (owner: string, repo: string) => {
        try {
            const res = await fetch(`/api/gitea/repos/${owner}/${repo}/actions/runs?limit=20`);
            const data = await res.json();
            const runs = data.workflow_runs || data.tasks || data.task_list || [];
            setActionRuns(Array.isArray(runs) ? runs : []);
        } catch { setActionRuns([]); }
    }, []);

    const fetchPulls = useCallback(async (owner: string, repo: string, state: string = 'all') => {
        try {
            const res = await fetch(`/api/gitea/repos/${owner}/${repo}/pulls?state=${state}&limit=20`);
            const data = await res.json();
            setPulls(data.pulls || []);
        } catch { setPulls([]); }
    }, []);

    const fetchPRDetail = useCallback(async (owner: string, repo: string, prNumber: number) => {
        setPrDetailLoading(true);
        try {
            const res = await fetch(`/api/gitea/repos/${owner}/${repo}/pulls/${prNumber}`);
            const data = await res.json();
            setPrDetail(data);
        } catch { setPrDetail(null); }
        finally { setPrDetailLoading(false); }
    }, []);

    const fetchTree = useCallback(async (owner: string, repo: string, path: string = '', ref?: string) => {
        try {
            const refParam = ref ? `&ref=${encodeURIComponent(ref)}` : '';
            const res = await fetch(`/api/gitea/repos/${owner}/${repo}/tree?path=${encodeURIComponent(path)}${refParam}`);
            const data = await res.json();
            setTreeEntries(prev => ({ ...prev, [path || '/']: data.entries || [] }));
        } catch {
            setTreeEntries(prev => ({ ...prev, [path || '/']: [] }));
        }
    }, []);

    const fetchFileContent = useCallback(async (owner: string, repo: string, path: string, ref?: string) => {
        setFileLoading(true);
        setSelectedFile(path);
        setEditMode(false);
        try {
            const refParam = ref ? `&ref=${encodeURIComponent(ref)}` : '';
            const res = await fetch(`/api/gitea/repos/${owner}/${repo}/file?path=${encodeURIComponent(path)}${refParam}`);
            const data = await res.json();
            setFileContent(data.content || '');
            setEditSha(data.sha || '');
        } catch { setFileContent('[Error loading file]'); }
        finally { setFileLoading(false); }
    }, []);

    const fetchCommitDetail = useCallback(async (owner: string, repo: string, sha: string) => {
        setCommitLoading(true);
        try {
            const res = await fetch(`/api/gitea/repos/${owner}/${repo}/commits/${sha}`);
            const data = await res.json();
            setSelectedCommit(data);
            setExpandedDiffs(new Set());
        } catch { setSelectedCommit(null); }
        finally { setCommitLoading(false); }
    }, []);

    // ─── Select Repo ───
    const selectRepo = useCallback(async (repo: GiteaRepo) => {
        const defaultBranch = repo.default_branch || 'main';
        setSelectedRepo(repo);
        setDetailTab('files');
        setDetailLoading(true);
        setSelectedFile(null);
        setFileContent('');
        setSelectedCommit(null);
        setTreeEntries({});
        setExpandedDirs(new Set());
        setActiveBranch(defaultBranch);
        setShowBranchDropdown(false);
        setSelectedPR(null);
        setPrDetail(null);
        const [owner, name] = repo.full_name.split('/');
        await Promise.all([
            fetchTree(owner, name, '', defaultBranch),
            fetchBranches(owner, name),
            fetchCommits(owner, name, defaultBranch),
            fetchPulls(owner, name, 'all'),
            fetchActions(owner, name),
            fetchTags(owner, name),
        ]);
        setDetailLoading(false);
    }, [fetchTree, fetchBranches, fetchCommits, fetchPulls, fetchActions, fetchTags]);

    // ─── Switch Branch ───
    const switchBranch = useCallback(async (branchName: string) => {
        if (!selectedRepo) return;
        const [owner, name] = selectedRepo.full_name.split('/');
        setActiveBranch(branchName);
        setShowBranchDropdown(false);
        setSelectedFile(null);
        setFileContent('');
        setTreeEntries({});
        setExpandedDirs(new Set());
        await Promise.all([
            fetchTree(owner, name, '', branchName),
            fetchCommits(owner, name, branchName),
        ]);
    }, [selectedRepo, fetchTree, fetchCommits]);

    // ─── Delete Branch ───
    const handleDeleteBranch = useCallback(async (branchName: string) => {
        if (!selectedRepo) return;
        if (!confirm(`Delete branch "${branchName}"? This cannot be undone.`)) return;
        const [owner, name] = selectedRepo.full_name.split('/');
        try {
            const res = await fetch(`/api/gitea/repos/${owner}/${name}/branches/${branchName}`, { method: 'DELETE' });
            if (!res.ok) { alert('Failed to delete branch'); return; }
            if (activeBranch === branchName) await switchBranch(selectedRepo.default_branch || 'main');
            fetchBranches(owner, name);
        } catch { alert('Failed to delete branch'); }
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
                    content: btoa(unescape(encodeURIComponent(editContent))),
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
        const interval = setInterval(() => fetchActions(owner, name), 15000);
        return () => clearInterval(interval);
    }, [selectedRepo, fetchActions]);

    // ─── Handlers ───
    const toggleDir = async (path: string) => {
        if (!selectedRepo) return;
        const newExpanded = new Set(expandedDirs);
        if (newExpanded.has(path)) {
            newExpanded.delete(path);
        } else {
            newExpanded.add(path);
            if (!treeEntries[path]) {
                const [owner, name] = selectedRepo.full_name.split('/');
                await fetchTree(owner, name, path, activeBranch);
            }
        }
        setExpandedDirs(newExpanded);
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
        if (newSelected !== null) {
            fetchPRDetail(owner, name, newSelected);
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
            fetchPulls(owner, name, prState);
        } catch (e) {
            alert(`Failed to create PR: ${e instanceof Error ? e.message : 'Unknown error'}`);
        }
    };

    const handleMergePR = async (prNumber: number, style: string = 'merge') => {
        if (!selectedRepo) return;
        const [owner, name] = selectedRepo.full_name.split('/');
        // Removed native confirm for better UX
        try {
            const res = await fetch(`/api/gitea/repos/${owner}/${name}/pulls/${prNumber}/merge`, {
                method: 'POST',
                headers: { 'Content-Type': 'application/json' },
                body: JSON.stringify({ merge_message_field_style: style, delete_branch_after_merge: false }),
            });
            if (!res.ok) {
                const err = await res.json().catch(() => ({ detail: res.statusText }));
                alert(`Failed to merge PR: ${err.detail || res.statusText}`);
                return;
            }
            alert(`PR #${prNumber} merged successfully!`);
            fetchPRDetail(owner, name, prNumber);
            fetchPulls(owner, name, prState);
        } catch (e) {
            alert(`Failed to merge PR: ${e instanceof Error ? e.message : 'Unknown error'}`);
        }
    };

    const handleClosePR = async (prNumber: number) => {
        if (!selectedRepo) return;
        const [owner, name] = selectedRepo.full_name.split('/');
        // Removed native confirm for better UX
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
            fetchPulls(owner, name, prState);
        } catch (e) {
            alert(`Failed to close PR: ${e instanceof Error ? e.message : 'Unknown error'}`);
        }
    };

    const handleApprovePR = async (prNumber: number) => {
        if (!selectedRepo) return;
        const [owner, name] = selectedRepo.full_name.split('/');
        try {
            const res = await fetch(`/api/gitea/repos/${owner}/${name}/pulls/${prNumber}/reviews`, {
                method: 'POST',
                headers: { 'Content-Type': 'application/json' },
                body: JSON.stringify({}),
            });
            if (!res.ok) {
                const err = await res.json().catch(() => ({ detail: res.statusText }));
                alert(`Failed to approve PR: ${err.detail || res.statusText}`);
                return;
            }
            alert(`PR #${prNumber} approved! ✅`);
            fetchPRDetail(owner, name, prNumber);
        } catch (e) {
            alert(`Failed to approve PR: ${e instanceof Error ? e.message : 'Unknown error'}`);
        }
    };


    // ─── Filter Repos ───
    const filteredRepos = repos.filter(r =>
        !search || r.full_name.toLowerCase().includes(search.toLowerCase()) ||
        (r.description || '').toLowerCase().includes(search.toLowerCase())
    );

    // ─── Render File Tree ───
    const renderTree = (parentPath: string, level: number = 0) => {
        const entries = treeEntries[parentPath || '/'] || [];
        return entries.map(entry => {
            const isDir = entry.type === 'dir';
            const isExpanded = expandedDirs.has(entry.path);
            const Icon = isDir ? (isExpanded ? FolderOpen : Folder) : fileIcon(entry.name);
            const color = isDir ? '#6895a8' : fileColor(entry.name);
            const isSelected = selectedFile === entry.path;

            return (
                <div key={entry.path}>
                    <div
                        className={clsx(
                            "flex items-center gap-1.5 py-[3px] px-2 cursor-pointer transition-colors text-[12px]",
                            isSelected
                                ? "bg-obsidian-info/20 text-white"
                                : "text-foreground hover:bg-obsidian-border-active"
                        )}
                        style={{ paddingLeft: `${level * 16 + 8}px` }}
                        onClick={() => isDir ? toggleDir(entry.path) : handleFileClick(entry.path)}
                    >
                        {isDir && (
                            <ChevronRight className={clsx("w-3 h-3 text-obsidian-muted transition-transform shrink-0", isExpanded && "rotate-90")} />
                        )}
                        {!isDir && <div className="w-3" />}
                        <Icon className="w-4 h-4 shrink-0" style={{ color }} />
                        <span className="truncate">{entry.name}</span>
                        {!isDir && entry.size > 0 && (
                            <span className="ml-auto text-[9px] text-obsidian-muted">{formatBytes(entry.size)}</span>
                        )}
                    </div>
                    {isDir && isExpanded && renderTree(entry.path, level + 1)}
                </div>
            );
        });
    };

    // ─── Tabs Config ───
    const tabs: { key: DetailTab; label: string; icon: typeof GitBranch; count?: number }[] = [
        { key: 'files', label: 'Files', icon: Folder },
        { key: 'commits', label: 'Commits', icon: GitCommit, count: commits.length },
        { key: 'pulls', label: 'PRs', icon: GitPullRequest, count: pulls.length },
        { key: 'branches', label: 'Branches', icon: GitBranch, count: branches.length },
        { key: 'tags', label: 'Tags', icon: Star, count: tags.length },
        { key: 'actions', label: 'Actions', icon: Play, count: actionRuns.length },
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
                    content: btoa(' '), // Empty file (space) to ensure it's not empty string if API requires content
                    message: `Create ${newFilePath}`,
                    sha: '0000000000000000000000000000000000000000', // Dummy SHA to satisfy Pydantic model
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
        <div className="flex h-screen bg-obsidian-bg text-foreground font-sans overflow-hidden">
            {!isFullscreen && <Sidebar />}

            <div className="flex-1 flex flex-col min-w-0">
                {/* ─── Top Bar ─── */}
                <div className="h-9 bg-obsidian-panel border-b border-obsidian-border flex items-center px-4 justify-between shrink-0">
                    <div className="flex items-center gap-3 text-[12px]">
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
                                    <code className="text-[10px] text-obsidian-danger font-mono bg-obsidian-panel rounded px-3 py-1.5">docker compose up -d gitea</code>
                                    <button onClick={() => { checkHealth(); fetchRepos(); }} className="px-4 py-1.5 bg-obsidian-danger/15 text-obsidian-danger rounded text-[11px] font-medium hover:bg-obsidian-danger/25">Retry</button>
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
                        <div className="flex-1 flex flex-col min-w-0">
                            {/* Repo Header + Tabs */}
                            <div className="bg-obsidian-panel border-b border-obsidian-border shrink-0">
                                <div className="flex items-center justify-between px-4 h-10">
                                    <div className="flex items-center gap-2 min-w-0">
                                        <GitPullRequest className="w-3.5 h-3.5 shrink-0" style={{ color: '#a78bfa' }} />
                                        <span className="text-[12px] font-bold text-foreground truncate">{selectedRepo.full_name}</span>
                                        {/* Branch switcher */}
                                        <div className="relative">
                                            <button
                                                onClick={() => setShowBranchDropdown(!showBranchDropdown)}
                                                className="flex items-center gap-1 text-[10px] px-1.5 py-0.5 rounded bg-obsidian-info/15 text-obsidian-info font-mono hover:bg-obsidian-info/25 transition-colors"
                                            >
                                                <GitBranch className="w-3.5 h-3.5" />
                                                {activeBranch || selectedRepo.default_branch}
                                                <ChevronDown className="w-2.5 h-2.5" />
                                            </button>
                                            {showBranchDropdown && (
                                                <>
                                                    <div className="fixed inset-0 z-10" onClick={() => setShowBranchDropdown(false)} />
                                                    <div className="absolute top-full left-0 mt-1 min-w-[180px] bg-obsidian-panel border border-obsidian-border rounded-md shadow-2xl z-20 py-1 max-h-60 overflow-auto">
                                                        {branches.map(b => (
                                                            <button key={b.name} onClick={() => switchBranch(b.name)}
                                                                className={clsx("w-full text-left px-3 py-1.5 text-[11px] font-mono flex items-center gap-2 transition-colors",
                                                                    activeBranch === b.name ? "text-obsidian-info bg-obsidian-info/10" : "text-foreground hover:bg-obsidian-border-active"
                                                                )}>
                                                                <GitBranch className="w-3.5 h-3.5 shrink-0 opacity-60" />
                                                                <span className="truncate">{b.name}</span>
                                                                {b.protected && <span className="ml-auto text-[9px] text-obsidian-muted shrink-0">protected</span>}
                                                            </button>
                                                        ))}
                                                    </div>
                                                </>
                                            )}
                                        </div>
                                    </div>
                                    <div className="flex items-center gap-1">
                                        <button
                                            onClick={() => { navigator.clipboard.writeText(selectedRepo.html_url + '.git'); }}
                                            className="p-1 hover:bg-[#3c3f41] rounded text-obsidian-muted hover:text-foreground transition-colors"
                                            title="Copy clone URL"
                                        >
                                            <Code2 className="w-3.5 h-3.5" />
                                        </button>
                                        <a href={selectedRepo.html_url} target="_blank" rel="noopener noreferrer"
                                            className="p-1 hover:bg-[#3c3f41] rounded text-obsidian-muted hover:text-foreground transition-colors">
                                            <ExternalLink className="w-3.5 h-3.5" />
                                        </a>
                                        <button onClick={() => { setSelectedRepo(null); setSelectedFile(null); setFileContent(''); setSelectedCommit(null); }}
                                            className="p-1 hover:bg-[#3c3f41] rounded text-obsidian-muted hover:text-foreground transition-colors">
                                            <X className="w-3.5 h-3.5" />
                                        </button>
                                    </div>
                                </div>
                                {/* Tabs */}
                                <div className="flex">
                                    {tabs.map(tab => (
                                        <button key={tab.key} onClick={() => setDetailTab(tab.key)}
                                            className={clsx("flex items-center gap-1.5 px-4 py-2 text-[11px] font-medium transition-colors border-b-2",
                                                detailTab === tab.key
                                                    ? "text-foreground border-obsidian-info bg-obsidian-panel"
                                                    : "text-obsidian-muted border-transparent hover:text-foreground hover:bg-obsidian-border-active/30"
                                            )}>
                                            <tab.icon className="w-3.5 h-3.5" />
                                            {tab.label}
                                            {tab.count !== undefined && tab.count > 0 && (
                                                <span className="text-[9px] bg-obsidian-border-active rounded-full px-1.5 py-0.5 text-obsidian-muted">{tab.count}</span>
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
                                    <div className="flex-1 flex overflow-hidden">
                                        {/* File Tree */}
                                        <div className="w-[260px] border-r border-obsidian-border overflow-auto shrink-0 py-1 flex flex-col">
                                            {/* File Tree Actions */}
                                            <div className="px-2 py-1 flex items-center justify-between text-[10px] text-obsidian-muted border-b border-obsidian-border/30 mb-1">
                                                <span>Explorer</span>
                                                <button
                                                    onClick={() => setShowCreateFile(!showCreateFile)}
                                                    className="p-1 hover:bg-obsidian-border-active rounded hover:text-foreground transition-colors"
                                                    title="New File"
                                                >
                                                    <Plus className="w-3.5 h-3.5" />
                                                </button>
                                            </div>

                                            {/* Create File Input */}
                                            {showCreateFile && (
                                                <div className="px-2 py-1 mb-2">
                                                    <div className="flex items-center gap-1 bg-obsidian-bg border border-obsidian-info/50 rounded px-1.5 py-1">
                                                        <FileIcon className="w-3.5 h-3.5 text-obsidian-muted shrink-0" />
                                                        <input
                                                            autoFocus
                                                            type="text"
                                                            placeholder="path/to/file.txt"
                                                            className="w-full bg-transparent text-[11px] outline-none text-foreground placeholder-obsidian-muted/50"
                                                            value={newFilePath}
                                                            onChange={e => setNewFilePath(e.target.value)}
                                                            onKeyDown={e => {
                                                                if (e.key === 'Enter') handleCreateFile();
                                                                if (e.key === 'Escape') setShowCreateFile(false);
                                                            }}
                                                        />
                                                    </div>
                                                    <div className="flex justify-end gap-1 mt-1">
                                                        <button onClick={() => setShowCreateFile(false)} className="text-[9px] px-1.5 py-0.5 text-obsidian-muted hover:text-foreground rounded hover:bg-obsidian-border-active">Cancel</button>
                                                        <button onClick={handleCreateFile} disabled={!newFilePath} className="text-[9px] px-1.5 py-0.5 bg-obsidian-info/20 text-obsidian-info rounded hover:bg-obsidian-info/30 disabled:opacity-50">Create</button>
                                                    </div>
                                                </div>
                                            )}

                                            {renderTree('', 0)}
                                            {(treeEntries['/']?.length === 0) && (
                                                <div className="text-center py-8 text-obsidian-muted text-[11px]">Empty repository</div>
                                            )}
                                        </div>
                                        {/* File Content */}
                                        <div className="flex-1 overflow-auto bg-obsidian-bg">
                                            {fileLoading ? (
                                                <div className="flex items-center justify-center h-full"><Loader2 className="w-5 h-5 text-obsidian-info animate-spin" /></div>
                                            ) : selectedFile ? (
                                                <div className="flex flex-col h-full">
                                                    {/* File header */}
                                                    <div className="sticky top-0 h-8 bg-obsidian-panel border-b border-obsidian-border flex items-center px-4 gap-2 z-10 shrink-0">
                                                        <FileCode className="w-3.5 h-3.5" style={{ color: fileColor(selectedFile) }} />
                                                        <span className="text-[11px] text-foreground font-mono flex-1 truncate">{selectedFile}</span>
                                                        <span className="text-[9px] text-obsidian-muted font-mono uppercase">{extToLang(selectedFile)}</span>
                                                        {isMarkdown(selectedFile) && !editMode && (
                                                            <button
                                                                onClick={() => setMdPreview(!mdPreview)}
                                                                className={clsx("p-1 rounded transition-colors",
                                                                    mdPreview ? "bg-obsidian-info/20 text-obsidian-info" : "text-obsidian-muted hover:text-foreground"
                                                                )}
                                                                title={mdPreview ? "Show source" : "Preview markdown"}
                                                            >
                                                                {mdPreview ? <Code2 className="w-3.5 h-3.5" /> : <Eye className="w-3.5 h-3.5" />}
                                                            </button>
                                                        )}
                                                        {!editMode ? (
                                                            <button
                                                                onClick={() => { setEditContent(fileContent); setEditMode(true); }}
                                                                className="flex items-center gap-1 px-2 py-0.5 text-[9px] font-medium bg-obsidian-border-active text-foreground rounded hover:bg-obsidian-info/20 hover:text-obsidian-info transition-colors"
                                                                title="Edit file"
                                                            >
                                                                ✏️ Edit
                                                            </button>
                                                        ) : (
                                                            <div className="flex items-center gap-1">
                                                                <span className="text-[9px] text-obsidian-warning">editing</span>
                                                                <button onClick={() => setEditMode(false)} className="text-[9px] text-obsidian-muted hover:text-foreground px-1">cancel</button>
                                                            </div>
                                                        )}
                                                    </div>
                                                    {/* File body */}
                                                    {editMode ? (
                                                        <div className="flex flex-col h-full">
                                                            <div className="flex-1 relative">
                                                                <Editor
                                                                    height="100%"
                                                                    width="100%"
                                                                    language={extToLang(selectedFile)}
                                                                    value={editContent}
                                                                    theme="obsidian"
                                                                    onChange={(value) => setEditContent(value || '')}
                                                                    onMount={handleEditorMount}
                                                                    options={{
                                                                        minimap: { enabled: false },
                                                                        fontSize: 13,
                                                                        fontFamily: 'JetBrains Mono, Fira Code, DM Mono, Menlo, monospace',
                                                                        fontLigatures: true,
                                                                        lineHeight: 22,
                                                                        scrollBeyondLastLine: false,
                                                                        automaticLayout: true,
                                                                        padding: { top: 12, bottom: 12 },
                                                                        renderLineHighlight: 'all',
                                                                    }}
                                                                />
                                                            </div>
                                                            <div className="border-t border-obsidian-border bg-obsidian-panel p-3 flex items-center gap-2 shrink-0">
                                                                <input
                                                                    type="text"
                                                                    placeholder="Commit message..."
                                                                    value={editMessage}
                                                                    onChange={e => setEditMessage(e.target.value)}
                                                                    className="flex-1 bg-obsidian-bg border border-obsidian-border rounded px-2 py-1.5 text-[11px] text-foreground placeholder-obsidian-muted outline-none"
                                                                />
                                                                <button
                                                                    onClick={handleSaveFile}
                                                                    disabled={!editMessage || editSaving}
                                                                    className="px-3 py-1.5 bg-obsidian-success text-white rounded text-[10px] font-medium hover:bg-obsidian-success disabled:opacity-40 flex items-center gap-1"
                                                                >
                                                                    {editSaving ? <Loader2 className="w-3.5 h-3.5 animate-spin" /> : null}
                                                                    Commit
                                                                </button>
                                                                <button onClick={() => { setEditMode(false); setEditMessage(''); }} className="px-2 py-1.5 text-[10px] text-obsidian-muted hover:text-foreground">Cancel</button>
                                                            </div>
                                                        </div>
                                                    ) : (
                                                        <div className="flex-1 overflow-auto">
                                                            {isMarkdown(selectedFile) && mdPreview ? (
                                                                /* Markdown Preview */
                                                                <div className="px-8 py-6 max-w-[900px] mx-auto prose-gitea">
                                                                    <ReactMarkdown
                                                                        remarkPlugins={[remarkGfm]}
                                                                        rehypePlugins={[rehypeRaw]}
                                                                        components={{
                                                                            h1: ({ children }) => <h1 className="text-[24px] font-bold text-foreground border-b border-obsidian-border pb-2 mb-4 mt-6">{children}</h1>,
                                                                            h2: ({ children }) => <h2 className="text-[20px] font-semibold text-foreground border-b border-obsidian-border pb-1.5 mb-3 mt-5">{children}</h2>,
                                                                            h3: ({ children }) => <h3 className="text-[16px] font-semibold text-foreground mb-2 mt-4">{children}</h3>,
                                                                            h4: ({ children }) => <h4 className="text-[14px] font-semibold text-foreground mb-2 mt-3">{children}</h4>,
                                                                            p: ({ children }) => <p className="text-[14px] text-foreground leading-relaxed mb-3">{children}</p>,
                                                                            a: ({ href, children }) => <a href={href} className="text-obsidian-info hover:underline" target="_blank" rel="noopener noreferrer">{children}</a>,
                                                                            ul: ({ children }) => <ul className="list-disc pl-6 mb-3 text-[14px] text-foreground space-y-1">{children}</ul>,
                                                                            ol: ({ children }) => <ol className="list-decimal pl-6 mb-3 text-[14px] text-foreground space-y-1">{children}</ol>,
                                                                            li: ({ children }) => <li className="leading-relaxed">{children}</li>,
                                                                            blockquote: ({ children }) => <blockquote className="border-l-4 border-obsidian-info pl-4 py-1 my-3 text-obsidian-muted italic bg-obsidian-panel rounded-r">{children}</blockquote>,
                                                                            table: ({ children }) => <div className="overflow-x-auto mb-4"><table className="w-full border-collapse text-[13px]">{children}</table></div>,
                                                                            thead: ({ children }) => <thead className="bg-obsidian-panel">{children}</thead>,
                                                                            th: ({ children }) => <th className="border border-obsidian-border px-3 py-2 text-left text-foreground font-semibold">{children}</th>,
                                                                            td: ({ children }) => <td className="border border-obsidian-border px-3 py-2 text-foreground">{children}</td>,
                                                                            hr: () => <hr className="border-obsidian-border my-5" />,
                                                                            img: ({ src, alt }) => <img src={src} alt={alt || ''} className="max-w-full rounded-lg my-3 border border-obsidian-border" />,
                                                                            strong: ({ children }) => <strong className="text-foreground font-semibold">{children}</strong>,
                                                                            em: ({ children }) => <em className="text-foreground italic">{children}</em>,
                                                                            del: ({ children }) => <del className="text-obsidian-muted">{children}</del>,
                                                                            code: ({ className, children, ...props }) => {
                                                                                const match = /language-(\w+)/.exec(className || '');
                                                                                const inline = !match;
                                                                                if (inline) {
                                                                                    return <code className="bg-obsidian-panel text-obsidian-danger px-1.5 py-0.5 rounded text-[13px] font-mono" {...props}>{children}</code>;
                                                                                }
                                                                                return (
                                                                                    <SyntaxHighlighter
                                                                                        style={vscDarkPlus}
                                                                                        language={match[1]}
                                                                                        PreTag="div"
                                                                                        customStyle={{ margin: '12px 0', borderRadius: '8px', fontSize: '13px', border: '1px solid #393b40' }}
                                                                                    >
                                                                                        {String(children).replace(/\n$/, '')}
                                                                                    </SyntaxHighlighter>
                                                                                );
                                                                            },
                                                                        }}
                                                                    >
                                                                        {fileContent}
                                                                    </ReactMarkdown>
                                                                </div>
                                                            ) : isImage(selectedFile) ? (
                                                                /* Image Preview */
                                                                <div className="flex items-center justify-center h-full p-8">
                                                                    <div className="text-center">
                                                                        <div className="text-[11px] text-obsidian-muted mb-3">Image preview not available via API</div>
                                                                    </div>
                                                                </div>
                                                            ) : (
                                                                /* Syntax Highlighted Code */
                                                                <div className="flex-1 relative h-full">
                                                                    <Editor
                                                                        height="100%"
                                                                        width="100%"
                                                                        language={extToLang(selectedFile)}
                                                                        value={fileContent}
                                                                        theme="obsidian"
                                                                        onMount={handleEditorMount}
                                                                        options={{
                                                                            readOnly: true,
                                                                            minimap: { enabled: false },
                                                                            fontSize: 13,
                                                                            fontFamily: 'JetBrains Mono, Fira Code, DM Mono, Menlo, monospace',
                                                                            fontLigatures: true,
                                                                            lineHeight: 22,
                                                                            scrollBeyondLastLine: false,
                                                                            automaticLayout: true,
                                                                            padding: { top: 12, bottom: 12 },
                                                                            domReadOnly: true,
                                                                            renderLineHighlight: 'none',
                                                                        }}
                                                                    />
                                                                </div>
                                                            )}
                                                        </div>
                                                    )}
                                                </div>
                                            ) : (
                                                <div className="flex flex-col items-center justify-center h-full text-obsidian-muted gap-2">
                                                    <Folder className="w-10 h-10 opacity-30" />
                                                    <span className="text-[12px]">Select a file to view its contents</span>
                                                </div>
                                            )}
                                        </div>
                                    </div>

                                ) : detailTab === 'commits' ? (
                                    /* ─── Commits Tab ─── */
                                    <div className="flex-1 flex overflow-hidden">
                                        {/* Commit List */}
                                        <div className={clsx("overflow-auto shrink-0 py-1", selectedCommit ? "w-[300px] border-r border-obsidian-border" : "flex-1")}>
                                            {commits.length === 0 ? (
                                                <div className="text-center py-8 text-obsidian-muted text-[11px]">No commits</div>
                                            ) : commits.map(commit => (
                                                <div key={commit.sha} onClick={() => handleCommitClick(commit.sha)}
                                                    className={clsx("px-4 py-2.5 cursor-pointer transition-colors border-b border-obsidian-border/30",
                                                        selectedCommit?.sha === commit.sha ? "bg-obsidian-info/10" : "hover:bg-obsidian-panel/50"
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
                                        </div>
                                        {/* Commit Detail */}
                                        {selectedCommit && (
                                            <div className="flex-1 overflow-auto bg-obsidian-bg">
                                                {commitLoading ? (
                                                    <div className="flex items-center justify-center h-full"><Loader2 className="w-5 h-5 text-obsidian-info animate-spin" /></div>
                                                ) : (
                                                    <div>
                                                        {/* Commit header */}
                                                        <div className="px-4 py-3 border-b border-obsidian-border bg-obsidian-panel">
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
                                                            <div key={file.filename} className="border-b border-obsidian-border/50">
                                                                <div className="flex items-center gap-2 px-4 py-2 bg-obsidian-panel/50 cursor-pointer hover:bg-obsidian-panel"
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
                                        <div className="flex items-center gap-2 px-4 py-2 border-b border-obsidian-border bg-obsidian-panel">
                                            {(['all', 'open', 'closed'] as const).map(s => (
                                                <button key={s} onClick={() => { setPrState(s); if (selectedRepo) { const [o, n] = selectedRepo.full_name.split('/'); fetchPulls(o, n, s); } }}
                                                    className={clsx("px-2 py-1 rounded text-[10px] font-medium transition-all",
                                                        prState === s ? "bg-obsidian-info/20 text-obsidian-info" : "text-obsidian-muted hover:text-foreground"
                                                    )}>{s.charAt(0).toUpperCase() + s.slice(1)}</button>
                                            ))}
                                            <button onClick={() => setShowCreatePR(!showCreatePR)}
                                                className="ml-auto flex items-center gap-1 px-2 py-1 bg-obsidian-success/15 text-obsidian-success rounded text-[10px] font-medium hover:bg-obsidian-success/25">
                                                <Plus className="w-3.5 h-3.5" /> New PR
                                            </button>
                                        </div>

                                        {/* Create PR Form */}
                                        {showCreatePR && (
                                            <div className="px-4 py-3 border-b border-obsidian-border bg-obsidian-panel/50 space-y-2">
                                                <div className="flex gap-2">
                                                    <select value={prHead} onChange={e => setPrHead(e.target.value)}
                                                        className="flex-1 bg-obsidian-bg border border-obsidian-border rounded px-2 py-1 text-[11px] text-foreground outline-none">
                                                        <option value="">Head branch...</option>
                                                        {branches.map(b => <option key={b.name} value={b.name}>{b.name}</option>)}
                                                    </select>
                                                    <ArrowUpRight className="w-4 h-4 text-obsidian-muted shrink-0 self-center" />
                                                    <select value={prBase} onChange={e => setPrBase(e.target.value)}
                                                        className="flex-1 bg-obsidian-bg border border-obsidian-border rounded px-2 py-1 text-[11px] text-foreground outline-none">
                                                        <option value="">Base branch...</option>
                                                        {branches.map(b => <option key={b.name} value={b.name}>{b.name}</option>)}
                                                    </select>
                                                </div>
                                                <input type="text" placeholder="PR title..." value={prTitle} onChange={e => setPrTitle(e.target.value)}
                                                    className="w-full bg-obsidian-bg border border-obsidian-border rounded px-2 py-1.5 text-[11px] text-foreground placeholder-obsidian-muted outline-none" />
                                                <textarea placeholder="Description (optional)..." value={prBody} onChange={e => setPrBody(e.target.value)} rows={3}
                                                    className="w-full bg-obsidian-bg border border-obsidian-border rounded px-2 py-1.5 text-[11px] text-foreground placeholder-obsidian-muted outline-none resize-none" />
                                                <div className="flex gap-2">
                                                    <button onClick={handleCreatePR} disabled={!prTitle || !prHead || !prBase}
                                                        className="px-3 py-1 bg-obsidian-success text-white rounded text-[10px] font-medium hover:bg-obsidian-success disabled:opacity-40">Create</button>
                                                    <button onClick={() => setShowCreatePR(false)} className="px-3 py-1 text-obsidian-muted text-[10px] hover:text-foreground">Cancel</button>
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
                                            <div key={pr.id} className="border-b border-obsidian-border/30">
                                                {/* PR Row */}
                                                <div
                                                    onClick={() => handlePRClick(pr.number)}
                                                    className={clsx("px-4 py-3 cursor-pointer transition-colors",
                                                        selectedPR === pr.number ? "bg-obsidian-info/10" : "hover:bg-obsidian-panel/50"
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
                                                    <div className="px-4 pb-3 pt-2 bg-obsidian-panel/30 flex items-center gap-2 flex-wrap border-b border-obsidian-border/30">
                                                        {pr.state === 'open' ? (
                                                            <>
                                                                <button
                                                                    onClick={(e) => { e.stopPropagation(); handleApprovePR(pr.number); }}
                                                                    className="flex items-center gap-1.5 px-3 py-1.5 bg-obsidian-success/10 text-obsidian-success border border-obsidian-success/20 rounded-md text-[11px] font-medium hover:bg-obsidian-success/20 transition-colors"
                                                                >
                                                                    <CheckCircle2 className="w-3.5 h-3.5" /> Approve
                                                                </button>
                                                                <div className="w-px h-4 bg-obsidian-border-active/50 mx-1"></div>
                                                                <button
                                                                    onClick={(e) => { e.stopPropagation(); handleMergePR(pr.number, 'merge'); }}
                                                                    className="flex items-center gap-1.5 px-3 py-1.5 bg-obsidian-info/10 text-obsidian-info border border-obsidian-info/20 rounded-md text-[11px] font-medium hover:bg-obsidian-info/20 transition-colors"
                                                                >
                                                                    <GitMerge className="w-3.5 h-3.5" /> Merge
                                                                </button>
                                                                <button
                                                                    onClick={(e) => { e.stopPropagation(); handleMergePR(pr.number, 'squash'); }}
                                                                    className="flex items-center gap-1.5 px-3 py-1.5 bg-obsidian-warning/10 text-obsidian-warning border border-obsidian-warning/20 rounded-md text-[11px] font-medium hover:bg-obsidian-warning/20 transition-colors"
                                                                >
                                                                    <Minimize2 className="w-3.5 h-3.5" /> Squash
                                                                </button>
                                                                <button
                                                                    onClick={(e) => { e.stopPropagation(); handleMergePR(pr.number, 'rebase'); }}
                                                                    title="Rebase and Merge"
                                                                    className="flex items-center gap-1.5 px-3 py-1.5 bg-obsidian-warning/10 text-obsidian-warning border border-obsidian-warning/20 rounded-md text-[11px] font-medium hover:bg-obsidian-warning/20 transition-colors"
                                                                >
                                                                    <RefreshCw className="w-3.5 h-3.5" /> Rebase
                                                                </button>
                                                                <button
                                                                    onClick={(e) => { e.stopPropagation(); handleClosePR(pr.number); }}
                                                                    className="flex items-center gap-1.5 px-3 py-1.5 bg-obsidian-danger/10 text-obsidian-danger border border-obsidian-danger/20 rounded-md text-[11px] font-medium hover:bg-obsidian-danger/20 transition-colors ml-auto"
                                                                >
                                                                    <XCircle className="w-3.5 h-3.5" /> Close
                                                                </button>
                                                            </>
                                                        ) : (
                                                            <div className={clsx(
                                                                "flex items-center gap-2 px-3 py-1.5 rounded-md border text-[11px] font-medium",
                                                                pr.state === 'merged'
                                                                    ? "bg-obsidian-primary/10 text-obsidian-primary border-obsidian-primary/20"
                                                                    : "bg-obsidian-border-active/30 text-obsidian-muted border-obsidian-border/50"
                                                            )}>
                                                                {pr.state === 'merged' ? <GitMerge className="w-3.5 h-3.5" /> : <XCircle className="w-3.5 h-3.5" />}
                                                                {pr.state === 'merged' ? 'Merged' : 'Closed'}
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
                                                        <div className="px-4 py-2 text-[10px] text-obsidian-muted font-medium border-b border-obsidian-border/30 flex items-center gap-2 bg-obsidian-panel/40">
                                                            <FileCode className="w-3.5 h-3.5" />
                                                            {prDetail.files.length} changed file{prDetail.files.length !== 1 ? 's' : ''}
                                                            <span className="text-obsidian-success ml-auto">+{prDetail.files.reduce((s, f) => s + (f.additions || 0), 0)}</span>
                                                            <span className="text-obsidian-danger ml-1">-{prDetail.files.reduce((s, f) => s + (f.deletions || 0), 0)}</span>
                                                        </div>
                                                        {prDetail.files.map((file, fi) => (
                                                            <div key={fi} className="border-b border-obsidian-border/20">
                                                                <div
                                                                    className="px-4 py-1.5 flex items-center gap-2 cursor-pointer hover:bg-obsidian-panel/60 bg-obsidian-bg/30"
                                                                    onClick={e => { e.stopPropagation(); setExpandedDiffs(prev => { const nx = new Set(prev); nx.has(file.filename) ? nx.delete(file.filename) : nx.add(file.filename); return nx; }); }}
                                                                >
                                                                    <ChevronRight className={"w-3 h-3 text-obsidian-muted shrink-0 transition-transform" + (expandedDiffs.has(file.filename) ? " rotate-90" : "")} />
                                                                    <span className={"text-[9px] px-1 rounded font-mono shrink-0 " + (file.status === 'added' ? "bg-obsidian-success/15 text-obsidian-success" : file.status === 'removed' ? "bg-obsidian-danger/15 text-obsidian-danger" : "bg-obsidian-warning/15 text-obsidian-warning")}>{(file.status || 'M').slice(0, 1).toUpperCase()}</span>
                                                                    <span className="font-mono text-[11px] text-foreground truncate flex-1">{file.filename}</span>
                                                                    <span className="text-[9px] text-obsidian-success shrink-0">+{file.additions || 0}</span>
                                                                    <span className="text-[9px] text-obsidian-danger shrink-0 ml-1">-{file.deletions || 0}</span>
                                                                </div>
                                                                {expandedDiffs.has(file.filename) && file.patch && (
                                                                    <div className="overflow-x-auto text-[11px] font-mono leading-5" style={{ fontFamily: 'JetBrains Mono,Menlo,Monaco,Consolas,monospace' }}>
                                                                        {file.patch.split('\n').map((line, li) => {
                                                                            let bg = 'transparent', color = '#bcbec4';
                                                                            if (line.startsWith('+') && !line.startsWith('+++')) { bg = '#1a3a2a'; color = '#6aab73'; }
                                                                            else if (line.startsWith('-') && !line.startsWith('---')) { bg = '#3a1a1a'; color = 'obsidian-danger'; }
                                                                            else if (line.startsWith('@@')) { bg = '#1a2a3a'; color = '#3574f0'; }
                                                                            return <div key={li} className="px-4 py-0 whitespace-pre" style={{ backgroundColor: bg, color }}>{line}</div>;
                                                                        })}
                                                                    </div>
                                                                )}
                                                            </div>
                                                        ))}
                                                    </div>
                                                )}
                                            </div>
                                        ))}
                                    </div>

                                ) : detailTab === 'branches' ? (
                                    /* ─── Branches Tab ─── */
                                    <div className="flex-1 overflow-auto">
                                        {/* Create Branch */}
                                        <div className="flex items-center gap-2 px-4 py-2 border-b border-obsidian-border bg-obsidian-panel">
                                            <span className="text-[11px] text-obsidian-muted">{branches.length} branches</span>
                                            <button onClick={() => setShowCreateBranch(!showCreateBranch)}
                                                className="ml-auto flex items-center gap-1 px-2 py-1 bg-obsidian-info/15 text-obsidian-info rounded text-[10px] font-medium hover:bg-obsidian-info/25">
                                                <Plus className="w-3.5 h-3.5" /> New Branch
                                            </button>
                                        </div>

                                        {showCreateBranch && (
                                            <div className="px-4 py-3 border-b border-obsidian-border bg-obsidian-panel/50 space-y-2">
                                                <input type="text" placeholder="Branch name..." value={newBranchName} onChange={e => setNewBranchName(e.target.value)}
                                                    className="w-full bg-obsidian-bg border border-obsidian-border rounded px-2 py-1.5 text-[11px] text-foreground placeholder-obsidian-muted outline-none" />
                                                <div className="flex items-center gap-2">
                                                    <span className="text-[10px] text-obsidian-muted">from</span>
                                                    <select value={baseBranch} onChange={e => setBaseBranch(e.target.value)}
                                                        className="flex-1 bg-obsidian-bg border border-obsidian-border rounded px-2 py-1 text-[11px] text-foreground outline-none">
                                                        <option value="">{selectedRepo?.default_branch || 'main'}</option>
                                                        {branches.map(b => <option key={b.name} value={b.name}>{b.name}</option>)}
                                                    </select>
                                                    <button onClick={handleCreateBranch} disabled={!newBranchName}
                                                        className="px-3 py-1 bg-obsidian-info text-white rounded text-[10px] font-medium hover:bg-obsidian-info disabled:opacity-40">Create</button>
                                                </div>
                                            </div>
                                        )}

                                        {branches.length === 0 ? (
                                            <div className="text-center py-8 text-obsidian-muted text-[11px]">No branches</div>
                                        ) : branches.map(branch => (
                                            <div key={branch.name} className="px-4 py-3 border-b border-obsidian-border/30 hover:bg-obsidian-panel/50">
                                                <div className="flex items-center gap-2">
                                                    <GitBranch className="w-3.5 h-3.5 text-obsidian-success shrink-0" />
                                                    <span className="text-[12px] text-foreground font-mono">{branch.name}</span>
                                                    {branch.name === selectedRepo?.default_branch && (
                                                        <span className="text-[8px] px-1.5 py-0.5 rounded bg-obsidian-info/15 text-obsidian-info font-semibold uppercase">default</span>
                                                    )}
                                                    {branch.protected && (
                                                        <span className="text-[8px] px-1.5 py-0.5 rounded bg-obsidian-warning/15 text-obsidian-warning font-semibold uppercase">protected</span>
                                                    )}
                                                </div>
                                                <div className="mt-1 text-[10px] text-obsidian-muted pl-5 truncate">
                                                    {branch.commit?.id?.slice(0, 7)} — {branch.commit?.message?.split('\n')[0]}
                                                </div>
                                                <div className="mt-1.5 flex items-center gap-1.5 pl-5">
                                                    <button
                                                        onClick={() => switchBranch(branch.name)}
                                                        disabled={activeBranch === branch.name}
                                                        className="flex items-center gap-1 px-2 py-0.5 text-[9px] bg-obsidian-border-active text-foreground rounded hover:bg-obsidian-info/20 hover:text-obsidian-info disabled:opacity-40 disabled:cursor-default transition-colors"
                                                    >
                                                        {activeBranch === branch.name ? '✓ Active' : '⇄ Switch'}
                                                    </button>
                                                    {branch.name !== selectedRepo?.default_branch && !branch.protected && (
                                                        <button
                                                            onClick={() => handleDeleteBranch(branch.name)}
                                                            className="flex items-center gap-1 px-2 py-0.5 text-[9px] bg-obsidian-danger/10 text-obsidian-danger rounded hover:bg-obsidian-danger/20 transition-colors"
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
                                        <div className="px-4 py-2 border-b border-obsidian-border bg-obsidian-panel text-[11px] text-obsidian-muted">
                                            {tags.length} tag{tags.length !== 1 ? 's' : ''}
                                        </div>
                                        {tags.length === 0 ? (
                                            <div className="text-center py-8 text-obsidian-muted text-[11px]">No tags</div>
                                        ) : tags.map(tag => (
                                            <div key={tag.name} className="px-4 py-3 border-b border-obsidian-border/30 hover:bg-obsidian-panel/50 flex items-center gap-2">
                                                <Star className="w-3.5 h-3.5 text-obsidian-warning shrink-0" />
                                                <span className="text-[12px] font-mono text-foreground">{tag.name}</span>
                                                <span className="ml-2 text-[10px] text-obsidian-muted font-mono">{tag.commit?.sha?.slice(0, 7)}</span>
                                                {tag.tarball_url && (
                                                    <a href={tag.tarball_url} className="ml-auto text-[9px] text-obsidian-info hover:underline" download>↓ tarball</a>
                                                )}
                                            </div>
                                        ))}
                                    </div>

                                ) : (
                                    /* ─── Actions Tab ─── */
                                    <div className="flex-1 overflow-auto">
                                        {actionRuns.length === 0 ? (
                                            <div className="flex flex-col items-center justify-center py-16 text-obsidian-muted gap-2">
                                                <Play className="w-8 h-8 opacity-30" />
                                                <span className="text-[12px]">No workflow runs</span>
                                            </div>
                                        ) : actionRuns.map((run) => (
                                            <div key={run.id} className="px-4 py-2.5 border-b border-obsidian-border/30 hover:bg-obsidian-panel/50">
                                                <div className="flex items-center justify-between mb-1">
                                                    <div className="flex items-center gap-2">
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
                                                </div>
                                            </div>
                                        ))}
                                    </div>
                                )}
                            </div>
                        </div>
                    )}
                </div>
            </div>
        </div >
    );
}
