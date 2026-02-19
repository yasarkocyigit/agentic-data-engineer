
import React, { useState, useEffect, useCallback } from 'react';
import { Sidebar } from '@/components/Sidebar';
import DagGraph from '@/components/DagGraph';
import {
    Play, RefreshCw, Pause, Search, ChevronRight, ChevronDown,
    Clock, AlertCircle, CheckCircle2, XCircle,
    Loader2, Tag, User, Calendar, Zap,
    Activity, CircleStop, RotateCcw, Code2, FileText, Maximize2, Minimize2, GitBranch
} from 'lucide-react';
import clsx from 'clsx';
import { getStateColor } from '@/lib/airflow/types';
import type { PipelineDAG, PipelineRun, HealthStatus, AirflowTaskInstance } from '@/lib/airflow/types';

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

function formatDuration(seconds: number | null | undefined): string {
    if (seconds == null) return '—';
    if (seconds < 60) return `${seconds.toFixed(1)}s`;
    const mins = Math.floor(seconds / 60);
    const secs = Math.floor(seconds % 60);
    if (mins < 60) return `${mins}m ${secs}s`;
    const hrs = Math.floor(mins / 60);
    return `${hrs}h ${mins % 60}m`;
}

// ─── State badge ───

function StateBadge({ state, size = 'sm' }: { state: string | null | undefined; size?: 'sm' | 'md' }) {
    const colors = getStateColor(state);
    const Icon = state === 'success' ? CheckCircle2
        : state === 'failed' ? XCircle
            : state === 'running' ? Loader2
                : state === 'queued' ? Clock
                    : state === 'skipped' ? ChevronRight
                        : Activity;

    return (
        <span
            className={clsx(
                "inline-flex items-center gap-1 rounded font-semibold uppercase tracking-wider",
                size === 'sm' ? "px-1.5 py-0.5 text-[9px]" : "px-2 py-1 text-[10px]"
            )}
            style={{ backgroundColor: colors.bg, color: colors.text }}
        >
            <Icon className={clsx(
                "flex-shrink-0",
                size === 'sm' ? "w-2.5 h-2.5" : "w-3 h-3",
                state === 'running' && "animate-spin"
            )} />
            {colors.label}
        </span>
    );
}

// ─── Health Dot ───

function HealthDot({ label, status }: { label: string; status: string }) {
    const color = status === 'healthy' ? 'bg-obsidian-success' : status === 'unhealthy' ? 'bg-obsidian-danger' : 'bg-obsidian-warning';
    return (
        <div className="flex items-center gap-1.5 group relative">
            <div className={clsx("w-1.5 h-1.5 rounded-full", color, status === 'healthy' && "shadow-[0_0_4px_rgba(73,156,84,0.6)]")} />
            <span className="text-[10px] text-obsidian-muted group-hover:text-foreground transition-colors">{label}</span>
        </div>
    );
}

// ─── Python Syntax Highlighting ───

function highlightPython(line: string): string {
    // Escape HTML first
    let html = line.replace(/&/g, '&amp;').replace(/</g, '&lt;').replace(/>/g, '&gt;');

    // Detect comment portion FIRST (before inserting any span tags that contain # in colors)
    // Find the first # that is NOT inside a string
    let commentIdx = -1;
    let inStr: string | null = null;
    for (let c = 0; c < html.length; c++) {
        const ch = html[c];
        if (!inStr && (ch === '"' || ch === "'")) {
            inStr = ch;
        } else if (inStr && ch === inStr) {
            inStr = null;
        } else if (!inStr && ch === '#') {
            commentIdx = c;
            break;
        }
    }

    let codePart = html;
    let commentPart = '';
    if (commentIdx >= 0) {
        codePart = html.substring(0, commentIdx);
        commentPart = '<span style="color:#6c707e;font-style:italic">' + html.substring(commentIdx) + '</span>';
    }

    // Apply highlighting only to code part (not comment)
    // Triple-quoted strings
    codePart = codePart.replace(/((&amp;quot;){3}.*?(&amp;quot;){3}|(&quot;){3}.*?(&quot;){3}|&quot;&quot;&quot;.*?&quot;&quot;&quot;|&#39;&#39;&#39;.*?&#39;&#39;&#39;)/g, '<span style="color:#98c379">$1</span>');
    // Strings (single and double quotes)
    codePart = codePart.replace(/(f?&quot;[^&]*?&quot;|f?&#39;[^&]*?&#39;|f?"[^"]*"|f?'[^']*')/g, '<span style="color:#98c379">$1</span>');
    // Keywords
    const kw = '\\b(from|import|def|class|return|if|elif|else|for|while|with|as|try|except|finally|raise|yield|lambda|not|and|or|in|is|True|False|None|pass|break|continue|global|nonlocal|assert|del|async|await)\\b';
    codePart = codePart.replace(new RegExp(kw, 'g'), '<span style="color:#c678dd">$1</span>');
    // Decorators (@something)
    codePart = codePart.replace(/(@\w+)/g, '<span style="color:#e5c07b">$1</span>');
    // Numbers
    codePart = codePart.replace(/\b(\d+\.?\d*)\b/g, '<span style="color:#d19a66">$1</span>');

    return codePart + commentPart;
}

// ─── Main Page ───

export default function WorkflowsPage() {
    const [dags, setDags] = useState<PipelineDAG[]>([]);
    const [loading, setLoading] = useState(true);
    const [error, setError] = useState<string | null>(null);
    const [health, setHealth] = useState<HealthStatus | null>(null);
    const [search, setSearch] = useState('');
    const [filterTag, setFilterTag] = useState<string | null>(null);
    const [selectedDag, setSelectedDag] = useState<string | null>(null);
    const [runs, setRuns] = useState<PipelineRun[]>([]);
    const [runsLoading, setRunsLoading] = useState(false);
    const [actionLoading, setActionLoading] = useState<string | null>(null);
    const [actionError, setActionError] = useState<string | null>(null);
    // Sidebar tabs
    const [sidebarTab, setSidebarTab] = useState<'runs' | 'code' | 'graph'>('runs');
    const [dagSource, setDagSource] = useState<{ content: string; version: number } | null>(null);
    const [sourceLoading, setSourceLoading] = useState(false);
    // Expandable run details
    const [expandedRun, setExpandedRun] = useState<string | null>(null);
    const [taskInstances, setTaskInstances] = useState<AirflowTaskInstance[]>([]);
    const [tasksLoading, setTasksLoading] = useState(false);
    // Task-level code viewer
    const [taskCode, setTaskCode] = useState<{ task_id: string; filename: string; content: string } | null>(null);
    const [taskCodeLoading, setTaskCodeLoading] = useState(false);
    const [viewingTaskCode, setViewingTaskCode] = useState<string | null>(null);
    const [fullscreenCode, setFullscreenCode] = useState(false);
    const [fullscreenSidebar, setFullscreenSidebar] = useState(false);

    // ─── Fetch DAGs ───
    const fetchDags = useCallback(async () => {
        setLoading(true);
        try {
            const res = await fetch('/api/orchestrator/dags');
            const data = await res.json();
            if (data.error) throw new Error(data.error);
            setDags(data.dags || []);
            setError(null);
        } catch (err: unknown) {
            const message = err instanceof Error ? err.message : 'Failed to connect';
            setError(message);
        } finally {
            setLoading(false);
        }
    }, []);

    // ─── Fetch Health ───
    const fetchHealth = useCallback(async () => {
        try {
            const res = await fetch('/api/orchestrator/health');
            const data = await res.json();
            setHealth(data);
        } catch { /* silent */ }
    }, []);

    // ─── Fetch Runs for selected DAG ───
    const fetchRuns = useCallback(async (dagId: string) => {
        setRunsLoading(true);
        try {
            const res = await fetch(`/api/orchestrator/dags/${dagId}/runs?limit=10`);
            const data = await res.json();
            setRuns(data.runs || []);
        } catch {
            setRuns([]);
        } finally {
            setRunsLoading(false);
        }
    }, []);

    // ─── Fetch Task Instances for a run ───
    const fetchTaskInstances = useCallback(async (dagId: string, runId: string) => {
        setTasksLoading(true);
        try {
            const res = await fetch(`/api/orchestrator/dags/${dagId}/runs/${encodeURIComponent(runId)}/tasks`);
            const data = await res.json();
            setTaskInstances(data.task_instances || []);
        } catch {
            setTaskInstances([]);
        } finally {
            setTasksLoading(false);
        }
    }, []);

    // ─── Initial load + polling ───
    useEffect(() => {
        fetchDags();
        fetchHealth();
        const dagInterval = setInterval(fetchDags, 15000);
        const healthInterval = setInterval(fetchHealth, 30000);
        return () => { clearInterval(dagInterval); clearInterval(healthInterval); };
    }, [fetchDags, fetchHealth]);

    // ─── Select DAG → load runs ───
    useEffect(() => {
        if (selectedDag) {
            fetchRuns(selectedDag);
            setExpandedRun(null);
            setTaskInstances([]);
            setSidebarTab('graph');
            setDagSource(null);
        }
    }, [selectedDag, fetchRuns]);

    // ─── Fetch DAG Source Code ───
    const fetchDagSource = useCallback(async (dagId: string) => {
        setSourceLoading(true);
        try {
            const res = await fetch('/api/orchestrator/dags', {
                method: 'POST',
                headers: { 'Content-Type': 'application/json' },
                body: JSON.stringify({ dag_id: dagId, action: 'source' }),
            });
            const data = await res.json();
            if (data.error) throw new Error(data.error);
            setDagSource(data);
        } catch (err) {
            console.error('Failed to load source:', err);
            setDagSource({ content: '# Failed to load source code', version: 0 });
        } finally {
            setSourceLoading(false);
        }
    }, []);

    // Auto-fetch source when switching to code tab
    useEffect(() => {
        if (sidebarTab === 'code' && selectedDag && !dagSource) {
            fetchDagSource(selectedDag);
        }
    }, [sidebarTab, selectedDag, dagSource, fetchDagSource]);

    // ─── Actions ───
    const handleAction = async (dagId: string, action: 'trigger' | 'pause' | 'unpause' | 'cancel', runId?: string) => {
        setActionLoading(`${dagId}-${action}`);
        setActionError(null);
        try {
            const res = await fetch('/api/orchestrator/dags', {
                method: 'POST',
                headers: { 'Content-Type': 'application/json' },
                body: JSON.stringify({ dag_id: dagId, action, run_id: runId }),
            });
            const data = await res.json();
            if (data.error) throw new Error(data.error);
            // Refresh immediately
            await fetchDags();
            if (selectedDag === dagId) fetchRuns(dagId);
        } catch (err: unknown) {
            const message = err instanceof Error ? err.message : 'Action failed';
            setActionError(message);
            setTimeout(() => setActionError(null), 5000);
        } finally {
            setActionLoading(null);
        }
    };

    // ─── Expand a run to see task details ───
    const toggleRunExpand = (runId: string) => {
        if (expandedRun === runId) {
            setExpandedRun(null);
            setTaskInstances([]);
        } else {
            setExpandedRun(runId);
            if (selectedDag) fetchTaskInstances(selectedDag, runId);
        }
    };

    // ─── Filter DAGs ───
    const allTags = [...new Set(dags.flatMap(d => d.tags))];
    const filteredDags = dags.filter(dag => {
        if (search && !dag.dag_id.toLowerCase().includes(search.toLowerCase())) return false;
        if (filterTag && !dag.tags.includes(filterTag)) return false;
        return true;
    });

    const selectedDagData = selectedDag ? dags.find(d => d.dag_id === selectedDag) : null;

    return (
        <div className="flex h-screen bg-obsidian-bg text-foreground font-sans overflow-hidden">
            <Sidebar />
            <main className="flex-1 flex flex-col min-w-0 overflow-hidden">

                {/* ─── Top Toolbar ─── */}
                <header className="h-10 bg-obsidian-panel border-b border-obsidian-border flex items-center justify-between px-4 shrink-0">
                    <div className="flex items-center gap-3">
                        <div className="flex items-center gap-2">
                            <Activity className="w-4 h-4 text-obsidian-info" />
                            <span className="text-[12px] font-bold text-foreground tracking-wide">Orchestration</span>
                        </div>
                        <div className="flex items-center gap-3 ml-4 pl-4 border-l border-obsidian-border">
                            {health ? (
                                <>
                                    <HealthDot label="Scheduler" status={health.scheduler.status} />
                                    <HealthDot label="DB" status={health.database.status} />
                                    <HealthDot label="Processor" status={health.dag_processor.status} />
                                </>
                            ) : (
                                <span className="text-[10px] text-obsidian-muted animate-pulse">Connecting...</span>
                            )}
                        </div>
                    </div>
                    <div className="flex items-center gap-2">
                        <span className="text-[10px] text-obsidian-muted">{dags.length} pipelines</span>
                        <button
                            onClick={() => { fetchDags(); fetchHealth(); }}
                            className="p-1.5 hover:bg-obsidian-panel-hover rounded text-foreground transition-colors"
                        >
                            <RefreshCw className={clsx("w-3.5 h-3.5", loading && "animate-spin")} />
                        </button>
                    </div>
                </header>

                {/* ─── Filter Bar ─── */}
                <div className="h-9 bg-obsidian-panel border-b border-obsidian-border flex items-center px-4 gap-3 shrink-0">
                    <div className="flex items-center gap-1.5 bg-obsidian-bg rounded px-2 py-1 flex-1 max-w-xs">
                        <Search className="w-3 h-3 text-obsidian-muted" />
                        <input
                            type="text"
                            placeholder="Search pipelines..."
                            value={search}
                            onChange={(e) => setSearch(e.target.value)}
                            className="bg-transparent text-[11px] text-foreground placeholder-[#6c707e] outline-none w-full"
                        />
                    </div>
                    <div className="flex items-center gap-1.5">
                        <Tag className="w-3 h-3 text-obsidian-muted" />
                        <button
                            onClick={() => setFilterTag(null)}
                            className={clsx(
                                "px-2 py-0.5 rounded text-[10px] transition-colors",
                                !filterTag ? "bg-obsidian-info/20 text-obsidian-info" : "text-obsidian-muted hover:text-foreground"
                            )}
                        >
                            All
                        </button>
                        {allTags.map(tag => (
                            <button
                                key={tag}
                                onClick={() => setFilterTag(filterTag === tag ? null : tag)}
                                className={clsx(
                                    "px-2 py-0.5 rounded text-[10px] transition-colors",
                                    filterTag === tag ? "bg-obsidian-info/20 text-obsidian-info" : "text-obsidian-muted hover:text-foreground"
                                )}
                            >
                                {tag}
                            </button>
                        ))}
                    </div>
                </div>

                {/* ─── Error Toast ─── */}
                {actionError && (
                    <div className="mx-4 mt-1 px-3 py-2 bg-obsidian-danger/10 border border-obsidian-danger/30 rounded text-[11px] text-obsidian-danger-light flex items-center gap-2 shrink-0">
                        <AlertCircle className="w-3.5 h-3.5 text-obsidian-danger shrink-0" />
                        <span className="flex-1 truncate">{actionError}</span>
                        <button onClick={() => setActionError(null)} className="text-obsidian-danger hover:text-white text-[10px] font-bold">✕</button>
                    </div>
                )}

                {/* ─── Content Area ─── */}
                <div className="flex-1 flex overflow-hidden">

                    {/* DAG List / Graph View */}
                    <div className={clsx("flex-1 overflow-auto", selectedDag ? "border-r border-obsidian-border" : "")}>
                        {/* Show graph in main area when graph tab is active */}
                        {selectedDag && sidebarTab === 'graph' ? (
                            <DagGraph
                                dagId={selectedDag}
                                runId={expandedRun}
                                className="h-full"
                                onNodeClick={(taskId: string) => {
                                    console.log('Node clicked:', taskId);
                                }}
                            />
                        ) : error ? (
                            <div className="flex flex-col items-center justify-center h-full text-obsidian-danger gap-3">
                                <AlertCircle className="w-10 h-10 opacity-60" />
                                <span className="text-[13px] font-medium">Connection Error</span>
                                <span className="text-[11px] text-obsidian-muted max-w-sm text-center">{error}</span>
                                <button
                                    onClick={fetchDags}
                                    className="mt-2 px-3 py-1.5 bg-obsidian-info/20 text-obsidian-info rounded text-[11px] hover:bg-obsidian-info/30 transition-colors"
                                >
                                    Retry Connection
                                </button>
                            </div>
                        ) : (
                            <table className="w-full text-left border-collapse">
                                <thead className="sticky top-0 bg-obsidian-panel z-10 shadow-[0_1px_0_#393b40]">
                                    <tr>
                                        <th className="p-1.5 px-3 text-[10px] text-obsidian-muted font-medium uppercase tracking-wider w-8"></th>
                                        <th className="p-1.5 px-3 text-[10px] text-obsidian-muted font-medium uppercase tracking-wider text-left">Pipeline</th>
                                        <th className="p-1.5 px-3 text-[10px] text-obsidian-muted font-medium uppercase tracking-wider text-left w-24">Schedule</th>
                                        <th className="p-1.5 px-3 text-[10px] text-obsidian-muted font-medium uppercase tracking-wider text-left w-24">Last Run</th>
                                        <th className="p-1.5 px-3 text-[10px] text-obsidian-muted font-medium uppercase tracking-wider text-left w-20">State</th>
                                        <th className="p-1.5 px-3 text-[10px] text-obsidian-muted font-medium uppercase tracking-wider text-left w-20">Duration</th>
                                        <th className="p-1.5 px-3 text-[10px] text-obsidian-muted font-medium uppercase tracking-wider text-center w-44">Actions</th>
                                    </tr>
                                </thead>
                                <tbody className="text-[12px]">
                                    {filteredDags.map((dag) => {
                                        const isSelected = selectedDag === dag.dag_id;
                                        const isRunning = dag.last_run?.state === 'running' || dag.last_run?.state === 'queued';
                                        return (
                                            <tr
                                                key={dag.dag_id}
                                                onClick={() => setSelectedDag(isSelected ? null : dag.dag_id)}
                                                className={clsx(
                                                    "border-b border-obsidian-border/50 group cursor-pointer transition-colors",
                                                    isSelected ? "bg-[#214283]/30" : "hover:bg-obsidian-panel"
                                                )}
                                            >
                                                {/* Status indicator */}
                                                <td className="p-1.5 px-3 text-center">
                                                    <div className={clsx(
                                                        "w-2 h-2 rounded-full mx-auto transition-all",
                                                        dag.is_paused
                                                            ? "bg-obsidian-muted/50"
                                                            : "bg-obsidian-success shadow-[0_0_6px_rgba(73,156,84,0.5)]"
                                                    )} />
                                                </td>

                                                {/* Pipeline info */}
                                                <td className="p-1.5 px-3">
                                                    <div className="flex items-center gap-2">
                                                        <span className="text-foreground font-semibold">{dag.display_name}</span>
                                                        {dag.has_import_errors && (
                                                            <span title="Import errors"><AlertCircle className="w-3 h-3 text-obsidian-danger" /></span>
                                                        )}
                                                    </div>
                                                    <div className="flex items-center gap-2 mt-0.5">
                                                        <span className="text-[10px] text-obsidian-muted">
                                                            <User className="w-2.5 h-2.5 inline mr-0.5 -mt-0.5" />
                                                            {dag.owners.join(', ')}
                                                        </span>
                                                        {dag.tags.slice(0, 3).map(tag => (
                                                            <span key={tag} className="text-[8px] px-1.5 py-0.5 rounded bg-obsidian-panel-hover text-obsidian-muted">
                                                                {tag}
                                                            </span>
                                                        ))}
                                                    </div>
                                                </td>

                                                {/* Schedule */}
                                                <td className="p-1.5 px-3 text-obsidian-muted font-mono text-[11px]">
                                                    {dag.schedule || 'Manual'}
                                                </td>

                                                {/* Last run time */}
                                                <td className="p-1.5 px-3 text-obsidian-muted text-[11px]">
                                                    {dag.last_run ? timeAgo(dag.last_run.end_date || dag.last_run.start_date) : '—'}
                                                </td>

                                                {/* State */}
                                                <td className="p-1.5 px-3">
                                                    {dag.last_run ? (
                                                        <StateBadge state={dag.last_run.state} />
                                                    ) : dag.is_paused ? (
                                                        <span className="text-[9px] text-obsidian-muted uppercase font-semibold">Paused</span>
                                                    ) : (
                                                        <span className="text-[9px] text-obsidian-muted uppercase font-semibold">Ready</span>
                                                    )}
                                                </td>

                                                {/* Duration */}
                                                <td className="p-1.5 px-3 text-obsidian-muted font-mono text-[11px]">
                                                    {formatDuration(dag.last_run?.duration_seconds)}
                                                </td>

                                                {/* ─── ACTIONS: 3 distinct buttons ─── */}
                                                <td className="p-1.5 px-3" onClick={(e) => e.stopPropagation()}>
                                                    <div className="flex items-center gap-1.5 justify-center">

                                                        {/* 1. TRIGGER — green, starts a new run */}
                                                        <button
                                                            onClick={() => handleAction(dag.dag_id, 'trigger')}
                                                            disabled={!!actionLoading}
                                                            className="flex items-center gap-1 px-2 py-1 rounded text-[9px] font-semibold
                                         bg-obsidian-success/10 text-obsidian-success hover:bg-obsidian-success/25
                                         border border-obsidian-success/20 transition-colors disabled:opacity-30"
                                                        >
                                                            {actionLoading === `${dag.dag_id}-trigger`
                                                                ? <Loader2 className="w-3 h-3 animate-spin" />
                                                                : <Play className="w-3 h-3" />}
                                                            <span>Run</span>
                                                        </button>

                                                        {/* 2. CANCEL — red, only if currently running */}
                                                        {isRunning && dag.last_run && (
                                                            <button
                                                                onClick={() => handleAction(dag.dag_id, 'cancel', dag.last_run!.run_id)}
                                                                disabled={!!actionLoading}
                                                                className="flex items-center gap-1 px-2 py-1 rounded text-[9px] font-semibold
                                           bg-obsidian-danger/10 text-obsidian-danger hover:bg-obsidian-danger/25
                                           border border-obsidian-danger/20 transition-colors disabled:opacity-30"
                                                            >
                                                                {actionLoading === `${dag.dag_id}-cancel`
                                                                    ? <Loader2 className="w-3 h-3 animate-spin" />
                                                                    : <CircleStop className="w-3 h-3" />}
                                                                <span>Cancel</span>
                                                            </button>
                                                        )}

                                                        {/* 3. PAUSE/RESUME — schedule toggle */}
                                                        <button
                                                            onClick={() => handleAction(dag.dag_id, dag.is_paused ? 'unpause' : 'pause')}
                                                            disabled={!!actionLoading}
                                                            className={clsx(
                                                                "flex items-center gap-1 px-2 py-1 rounded text-[9px] font-semibold border transition-colors disabled:opacity-30",
                                                                dag.is_paused
                                                                    ? "bg-obsidian-info/10 text-obsidian-info hover:bg-obsidian-info/25 border-obsidian-info/20"
                                                                    : "bg-obsidian-warning/10 text-obsidian-warning hover:bg-obsidian-warning/25 border-obsidian-warning/20"
                                                            )}
                                                        >
                                                            {actionLoading === `${dag.dag_id}-pause` || actionLoading === `${dag.dag_id}-unpause`
                                                                ? <Loader2 className="w-3 h-3 animate-spin" />
                                                                : dag.is_paused
                                                                    ? <RotateCcw className="w-3 h-3" />
                                                                    : <Pause className="w-3 h-3" />}
                                                            <span>{dag.is_paused ? 'Enable' : 'Pause'}</span>
                                                        </button>
                                                    </div>
                                                </td>
                                            </tr>
                                        );
                                    })}
                                    {!loading && filteredDags.length === 0 && (
                                        <tr>
                                            <td colSpan={7} className="p-12 text-center text-obsidian-muted">
                                                {search || filterTag ? 'No pipelines match your filter' : 'No pipelines found'}
                                            </td>
                                        </tr>
                                    )}
                                </tbody>
                            </table>
                        )}
                    </div>

                    {/* ─── Run History Sidebar ─── */}
                    {selectedDag && (
                        <div className={clsx(
                            "flex flex-col bg-obsidian-bg shrink-0 transition-all duration-200",
                            fullscreenSidebar
                                ? "fixed inset-0 z-50 w-full"
                                : "w-[380px]"
                        )}>
                            {/* Header */}
                            <div className={clsx(
                                "bg-obsidian-panel border-b border-obsidian-border flex items-center justify-between shrink-0",
                                fullscreenSidebar ? "h-12 px-6" : "h-10 px-4"
                            )}>
                                <div className="flex items-center gap-2 min-w-0">
                                    <Zap className={clsx("text-obsidian-info shrink-0", fullscreenSidebar ? "w-4 h-4" : "w-3.5 h-3.5")} />
                                    <span className={clsx("font-bold text-foreground truncate", fullscreenSidebar ? "text-sm" : "text-[11px]")}>{selectedDag}</span>
                                </div>
                                <div className="flex items-center gap-1">
                                    <button
                                        onClick={() => setFullscreenSidebar(!fullscreenSidebar)}
                                        className="p-1 hover:bg-obsidian-panel-hover rounded text-obsidian-muted hover:text-foreground transition-colors"
                                        title={fullscreenSidebar ? 'Minimize' : 'Maximize'}
                                    >
                                        {fullscreenSidebar
                                            ? <Minimize2 className="w-3.5 h-3.5" />
                                            : <Maximize2 className="w-3.5 h-3.5" />
                                        }
                                    </button>
                                    <button
                                        onClick={() => { setSelectedDag(null); setFullscreenSidebar(false); }}
                                        className="p-1 hover:bg-obsidian-panel-hover rounded text-obsidian-muted hover:text-foreground text-[12px]"
                                    >
                                        ✕
                                    </button>
                                </div>
                            </div>

                            {/* DAG Info */}
                            {selectedDagData && (
                                <div className="px-4 py-3 border-b border-obsidian-border text-[11px] space-y-2">
                                    {selectedDagData.description && (
                                        <p className="text-obsidian-muted leading-relaxed">{selectedDagData.description}</p>
                                    )}
                                    <div className="flex items-center gap-4 text-[10px] text-obsidian-muted">
                                        <span><Calendar className="w-3 h-3 inline mr-1 -mt-0.5" />{selectedDagData.schedule || 'Manual'}</span>
                                        <span><User className="w-3 h-3 inline mr-1 -mt-0.5" />{selectedDagData.owners.join(', ')}</span>
                                    </div>
                                    {/* Schedule status + toggle */}
                                    <div className="flex items-center gap-2 pt-1">
                                        <span className={clsx(
                                            "text-[9px] px-2 py-0.5 rounded font-semibold uppercase",
                                            selectedDagData.is_paused
                                                ? "bg-obsidian-warning/10 text-obsidian-warning"
                                                : "bg-obsidian-success/10 text-obsidian-success"
                                        )}>
                                            Schedule: {selectedDagData.is_paused ? 'Paused' : 'Active'}
                                        </span>
                                        <button
                                            onClick={() => handleAction(selectedDag, selectedDagData.is_paused ? 'unpause' : 'pause')}
                                            disabled={!!actionLoading}
                                            className={clsx(
                                                "flex items-center gap-1 px-2 py-1 rounded text-[10px] font-medium border transition-colors disabled:opacity-50",
                                                selectedDagData.is_paused
                                                    ? "bg-obsidian-info/10 text-obsidian-info hover:bg-obsidian-info/25 border-obsidian-info/20"
                                                    : "bg-obsidian-warning/10 text-obsidian-warning hover:bg-obsidian-warning/25 border-obsidian-warning/20"
                                            )}
                                        >
                                            {selectedDagData.is_paused
                                                ? <><RotateCcw className="w-3 h-3" /> Enable Schedule</>
                                                : <><Pause className="w-3 h-3" /> Pause Schedule</>}
                                        </button>
                                    </div>
                                </div>
                            )}

                            {/* Tab Bar */}
                            <div className="flex border-b border-obsidian-border shrink-0">
                                <button
                                    onClick={() => setSidebarTab('graph')}
                                    className={clsx(
                                        "flex-1 flex items-center justify-center gap-1.5 py-2 text-[10px] font-semibold transition-colors",
                                        sidebarTab === 'graph'
                                            ? "text-foreground border-b-2 border-[#3574f0] bg-obsidian-panel/50"
                                            : "text-obsidian-muted hover:text-foreground hover:bg-obsidian-panel/30"
                                    )}
                                >
                                    <GitBranch className="w-3 h-3" /> Graph
                                </button>
                                <button
                                    onClick={() => setSidebarTab('runs')}
                                    className={clsx(
                                        "flex-1 flex items-center justify-center gap-1.5 py-2 text-[10px] font-semibold transition-colors",
                                        sidebarTab === 'runs'
                                            ? "text-foreground border-b-2 border-[#3574f0] bg-obsidian-panel/50"
                                            : "text-obsidian-muted hover:text-foreground hover:bg-obsidian-panel/30"
                                    )}
                                >
                                    <FileText className="w-3 h-3" /> Runs
                                </button>
                                <button
                                    onClick={() => setSidebarTab('code')}
                                    className={clsx(
                                        "flex-1 flex items-center justify-center gap-1.5 py-2 text-[10px] font-semibold transition-colors",
                                        sidebarTab === 'code'
                                            ? "text-foreground border-b-2 border-[#3574f0] bg-obsidian-panel/50"
                                            : "text-obsidian-muted hover:text-foreground hover:bg-obsidian-panel/30"
                                    )}
                                >
                                    <Code2 className="w-3 h-3" /> Code
                                </button>
                            </div>

                            {/* Runs List header */}
                            {sidebarTab === 'runs' && (
                                <div className="px-4 py-2 border-b border-obsidian-border flex items-center justify-between shrink-0">
                                    <span className="text-[10px] text-obsidian-muted uppercase font-semibold tracking-wider">Recent Runs</span>
                                    <button
                                        onClick={() => handleAction(selectedDag, 'trigger')}
                                        disabled={!!actionLoading}
                                        className="flex items-center gap-1 px-2 py-1 bg-obsidian-success/15 text-obsidian-success rounded text-[10px]
                             font-medium hover:bg-obsidian-success/25 border border-obsidian-success/20 transition-colors disabled:opacity-50"
                                    >
                                        <Play className="w-3 h-3" /> Trigger New Run
                                    </button>
                                </div>
                            )}

                            {/* Runs */}
                            {sidebarTab === 'runs' && (
                                <div className="flex-1 overflow-auto">
                                    {runsLoading ? (
                                        <div className="flex items-center justify-center py-8">
                                            <Loader2 className="w-5 h-5 text-obsidian-info animate-spin" />
                                        </div>
                                    ) : runs.length === 0 ? (
                                        <div className="text-center py-8 text-obsidian-muted text-[11px]">No runs yet</div>
                                    ) : (
                                        <div>
                                            {runs.map((run) => {
                                                const isExpanded = expandedRun === run.run_id;
                                                const isRunRunning = run.state === 'running' || run.state === 'queued';
                                                return (
                                                    <div key={run.run_id} className="border-b border-obsidian-border/30">
                                                        {/* Run row */}
                                                        <div
                                                            onClick={() => toggleRunExpand(run.run_id)}
                                                            className={clsx(
                                                                "px-4 py-2.5 cursor-pointer transition-colors",
                                                                isExpanded ? "bg-obsidian-panel" : "hover:bg-obsidian-panel/50"
                                                            )}
                                                        >
                                                            <div className="flex items-center justify-between mb-1">
                                                                <div className="flex items-center gap-1.5">
                                                                    {isExpanded
                                                                        ? <ChevronDown className="w-3 h-3 text-obsidian-muted" />
                                                                        : <ChevronRight className="w-3 h-3 text-obsidian-muted" />}
                                                                    <StateBadge state={run.state} />
                                                                    {/* Cancel button inline for running runs */}
                                                                    {isRunRunning && (
                                                                        <button
                                                                            onClick={(e) => { e.stopPropagation(); handleAction(selectedDag!, 'cancel', run.run_id); }}
                                                                            disabled={!!actionLoading}
                                                                            className="flex items-center gap-1 ml-1 px-1.5 py-0.5 rounded text-[8px] font-semibold
                                               bg-obsidian-danger/10 text-obsidian-danger hover:bg-obsidian-danger/25
                                               border border-obsidian-danger/20 transition-colors disabled:opacity-30"
                                                                        >
                                                                            <CircleStop className="w-2.5 h-2.5" />
                                                                            Cancel
                                                                        </button>
                                                                    )}
                                                                </div>
                                                                <span className="text-[10px] text-obsidian-muted">{timeAgo(run.end_date || run.start_date)}</span>
                                                            </div>
                                                            <div className="flex items-center justify-between text-[10px] text-obsidian-muted mt-1 pl-[18px]">
                                                                <span className="font-mono truncate max-w-[200px]">
                                                                    {run.run_type} • {run.triggered_by}
                                                                </span>
                                                                <span className="font-mono">{formatDuration(run.duration_seconds)}</span>
                                                            </div>
                                                        </div>

                                                        {/* Expanded: Task Instances */}
                                                        {isExpanded && (
                                                            <div className="bg-[#1a1b1e] border-t border-obsidian-border/30">
                                                                {tasksLoading ? (
                                                                    <div className="flex items-center justify-center py-4">
                                                                        <Loader2 className="w-4 h-4 text-obsidian-info animate-spin" />
                                                                    </div>
                                                                ) : taskInstances.length === 0 ? (
                                                                    <div className="text-center py-4 text-obsidian-muted text-[10px]">No task details available</div>
                                                                ) : (
                                                                    <div className="py-1">
                                                                        <div className="px-4 py-1.5 text-[9px] text-obsidian-muted uppercase font-semibold tracking-wider flex items-center gap-1">
                                                                            <Activity className="w-2.5 h-2.5" />
                                                                            Task Details
                                                                        </div>
                                                                        {taskInstances.map((ti) => {
                                                                            const taskColor = getStateColor(ti.state);
                                                                            const isFailed = ti.state === 'failed';
                                                                            return (
                                                                                <div
                                                                                    key={ti.id}
                                                                                    className={clsx(
                                                                                        "px-4 py-2 flex items-start gap-2 text-[11px]",
                                                                                        isFailed && "bg-obsidian-danger/5"
                                                                                    )}
                                                                                >
                                                                                    {/* State icon */}
                                                                                    <div className="mt-0.5">
                                                                                        {ti.state === 'success' ? (
                                                                                            <CheckCircle2 className="w-3 h-3" style={{ color: taskColor.text }} />
                                                                                        ) : ti.state === 'failed' ? (
                                                                                            <XCircle className="w-3 h-3" style={{ color: taskColor.text }} />
                                                                                        ) : ti.state === 'running' ? (
                                                                                            <Loader2 className="w-3 h-3 animate-spin" style={{ color: taskColor.text }} />
                                                                                        ) : ti.state === 'skipped' ? (
                                                                                            <ChevronRight className="w-3 h-3" style={{ color: taskColor.text }} />
                                                                                        ) : (
                                                                                            <Activity className="w-3 h-3" style={{ color: taskColor.text }} />
                                                                                        )}
                                                                                    </div>

                                                                                    {/* Task info */}
                                                                                    <div className="flex-1 min-w-0">
                                                                                        <div className="flex items-center justify-between">
                                                                                            <span className={clsx(
                                                                                                "font-mono font-medium truncate",
                                                                                                isFailed ? "text-obsidian-danger" : "text-foreground"
                                                                                            )}>
                                                                                                {ti.task_display_name}
                                                                                            </span>
                                                                                            <span className="text-[10px] text-obsidian-muted font-mono ml-2 shrink-0">
                                                                                                {formatDuration(ti.duration)}
                                                                                            </span>
                                                                                        </div>
                                                                                        <div className="text-[9px] text-obsidian-muted mt-0.5 flex items-center gap-2">
                                                                                            <span>
                                                                                                {ti.operator}
                                                                                                {ti.try_number > 1 && (
                                                                                                    <span className="ml-1 text-obsidian-warning">
                                                                                                        (try {ti.try_number}/{ti.max_tries})
                                                                                                    </span>
                                                                                                )}
                                                                                            </span>
                                                                                            {/* View Code button for tasks that run scripts */}
                                                                                            {(ti.operator === 'SparkSubmitOperator' || ti.operator === 'PythonOperator') && (
                                                                                                <button
                                                                                                    onClick={async (e) => {
                                                                                                        e.stopPropagation();
                                                                                                        if (viewingTaskCode === ti.task_id) {
                                                                                                            setViewingTaskCode(null);
                                                                                                            setTaskCode(null);
                                                                                                            return;
                                                                                                        }
                                                                                                        setViewingTaskCode(ti.task_id);
                                                                                                        setTaskCodeLoading(true);
                                                                                                        try {
                                                                                                            const res = await fetch('/api/orchestrator/notebooks', {
                                                                                                                method: 'POST',
                                                                                                                headers: { 'Content-Type': 'application/json' },
                                                                                                                body: JSON.stringify({ task_id: ti.task_id }),
                                                                                                            });
                                                                                                            const data = await res.json();
                                                                                                            if (data.error) {
                                                                                                                setTaskCode({ task_id: ti.task_id, filename: 'Error', content: `# ${data.error}` });
                                                                                                            } else {
                                                                                                                setTaskCode(data);
                                                                                                            }
                                                                                                        } catch {
                                                                                                            setTaskCode({ task_id: ti.task_id, filename: 'Error', content: '# Failed to load source' });
                                                                                                        } finally {
                                                                                                            setTaskCodeLoading(false);
                                                                                                        }
                                                                                                    }}
                                                                                                    className={clsx(
                                                                                                        "flex items-center gap-0.5 px-1.5 py-0.5 rounded text-[8px] font-semibold border transition-colors",
                                                                                                        viewingTaskCode === ti.task_id
                                                                                                            ? "bg-obsidian-info/20 text-obsidian-info border-obsidian-info/30"
                                                                                                            : "bg-obsidian-panel/30 text-obsidian-muted hover:text-foreground hover:bg-obsidian-panel/50 border-obsidian-border/50"
                                                                                                    )}
                                                                                                >
                                                                                                    <Code2 className="w-2.5 h-2.5" />
                                                                                                    {viewingTaskCode === ti.task_id ? 'Hide Code' : 'View Code'}
                                                                                                </button>
                                                                                            )}
                                                                                        </div>
                                                                                        {/* Inline Task Code Viewer */}
                                                                                        {viewingTaskCode === ti.task_id && (
                                                                                            <div className={clsx(
                                                                                                fullscreenCode
                                                                                                    ? "fixed inset-0 z-50 flex flex-col bg-[#1a1b1e]"
                                                                                                    : "mt-2 rounded border border-obsidian-border overflow-hidden"
                                                                                            )}>
                                                                                                {/* Backdrop for fullscreen */}
                                                                                                {fullscreenCode && (
                                                                                                    <div className="fixed inset-0 bg-black/60 -z-10" onClick={() => setFullscreenCode(false)} />
                                                                                                )}
                                                                                                {taskCodeLoading ? (
                                                                                                    <div className="flex items-center justify-center py-4 text-obsidian-muted text-[10px]">
                                                                                                        <Loader2 className="w-3 h-3 animate-spin mr-1.5" /> Loading...
                                                                                                    </div>
                                                                                                ) : taskCode ? (
                                                                                                    <>
                                                                                                        <div className={clsx(
                                                                                                            "bg-obsidian-panel border-b border-obsidian-border flex items-center justify-between shrink-0",
                                                                                                            fullscreenCode ? "px-5 py-3" : "px-2 py-1"
                                                                                                        )}>
                                                                                                            <div className="flex items-center gap-2">
                                                                                                                <Code2 className={clsx(fullscreenCode ? "w-4 h-4" : "w-3 h-3", "text-obsidian-info")} />
                                                                                                                <span className={clsx(
                                                                                                                    "text-foreground font-mono font-medium",
                                                                                                                    fullscreenCode ? "text-sm" : "text-[9px]"
                                                                                                                )}>{taskCode.filename}</span>
                                                                                                                <span className={clsx(
                                                                                                                    "text-obsidian-muted",
                                                                                                                    fullscreenCode ? "text-xs" : "text-[8px]"
                                                                                                                )}>{taskCode.content.split('\n').length} lines</span>
                                                                                                            </div>
                                                                                                            <div className="flex items-center gap-1">
                                                                                                                <button
                                                                                                                    onClick={(e) => { e.stopPropagation(); setFullscreenCode(!fullscreenCode); }}
                                                                                                                    className="p-1 rounded hover:bg-obsidian-panel/60 text-obsidian-muted hover:text-foreground transition-colors"
                                                                                                                    title={fullscreenCode ? 'Minimize' : 'Maximize'}
                                                                                                                >
                                                                                                                    {fullscreenCode
                                                                                                                        ? <Minimize2 className="w-3.5 h-3.5" />
                                                                                                                        : <Maximize2 className="w-3 h-3" />
                                                                                                                    }
                                                                                                                </button>
                                                                                                                {fullscreenCode && (
                                                                                                                    <button
                                                                                                                        onClick={(e) => {
                                                                                                                            e.stopPropagation();
                                                                                                                            setFullscreenCode(false);
                                                                                                                            setViewingTaskCode(null);
                                                                                                                            setTaskCode(null);
                                                                                                                        }}
                                                                                                                        className="p-1 rounded hover:bg-obsidian-danger/20 text-obsidian-muted hover:text-obsidian-danger transition-colors"
                                                                                                                        title="Close"
                                                                                                                    >
                                                                                                                        <XCircle className="w-3.5 h-3.5" />
                                                                                                                    </button>
                                                                                                                )}
                                                                                                            </div>
                                                                                                        </div>
                                                                                                        <div className={clsx(
                                                                                                            "overflow-auto bg-[#1a1b1e]",
                                                                                                            fullscreenCode ? "flex-1" : "max-h-[300px]"
                                                                                                        )}>
                                                                                                            {taskCode.content.split('\n').map((codeLine, li) => (
                                                                                                                <div key={li} className={clsx(
                                                                                                                    "flex font-mono hover:bg-obsidian-panel/40",
                                                                                                                    fullscreenCode ? "text-[13px] leading-[22px]" : "text-[10px] leading-[16px]"
                                                                                                                )}>
                                                                                                                    <span className={clsx(
                                                                                                                        "text-right text-obsidian-muted/40 select-none shrink-0",
                                                                                                                        fullscreenCode ? "w-14 pr-4 pl-4" : "w-8 pr-2"
                                                                                                                    )}>{li + 1}</span>
                                                                                                                    <span className="flex-1 whitespace-pre" dangerouslySetInnerHTML={{ __html: highlightPython(codeLine) }} />
                                                                                                                </div>
                                                                                                            ))}
                                                                                                        </div>
                                                                                                    </>
                                                                                                ) : null}
                                                                                            </div>
                                                                                        )}
                                                                                        {/* Failed task: prominent error box */}
                                                                                        {isFailed && (
                                                                                            <div className="mt-1.5 px-2 py-1.5 bg-obsidian-danger/10 rounded border border-obsidian-danger/20 text-[10px]">
                                                                                                <span className="text-obsidian-danger font-semibold">✕ Task failed</span>
                                                                                                <span className="text-[#ff7b86] ml-1">
                                                                                                    after {formatDuration(ti.duration)}
                                                                                                    {ti.hostname && <span className="text-obsidian-muted"> on {ti.hostname}</span>}
                                                                                                </span>
                                                                                                <div className="mt-1 text-[9px] text-obsidian-muted">
                                                                                                    Started: {ti.start_date ? new Date(ti.start_date).toLocaleString() : '—'}
                                                                                                    {' → '}
                                                                                                    Ended: {ti.end_date ? new Date(ti.end_date).toLocaleString() : '—'}
                                                                                                </div>
                                                                                            </div>
                                                                                        )}
                                                                                    </div>
                                                                                </div>
                                                                            );
                                                                        })}
                                                                    </div>
                                                                )}
                                                            </div>
                                                        )}
                                                    </div>
                                                );
                                            })}
                                        </div>
                                    )}
                                </div>
                            )}

                            {/* Code Viewer */}
                            {sidebarTab === 'code' && (
                                <div className="flex-1 flex flex-col overflow-hidden">
                                    {sourceLoading ? (
                                        <div className="flex-1 flex items-center justify-center text-obsidian-muted text-[11px]">
                                            <Loader2 className="w-4 h-4 animate-spin mr-2" /> Loading source...
                                        </div>
                                    ) : dagSource ? (
                                        <>
                                            <div className="px-3 py-1.5 bg-obsidian-panel border-b border-obsidian-border flex items-center justify-between shrink-0">
                                                <span className="text-[10px] text-obsidian-muted font-mono">{selectedDag}.py</span>
                                                <span className="text-[9px] text-obsidian-muted">v{dagSource.version}</span>
                                            </div>
                                            <div className="flex-1 overflow-auto">
                                                <pre className="text-[11px] leading-[18px] font-mono p-0 m-0">
                                                    <code>
                                                        {dagSource.content.split('\n').map((line, i) => (
                                                            <div key={i} className="flex hover:bg-obsidian-panel/50">
                                                                <span className="w-10 text-right pr-3 text-obsidian-muted/50 select-none shrink-0 text-[10px] leading-[18px]">{i + 1}</span>
                                                                <span className="flex-1 whitespace-pre" dangerouslySetInnerHTML={{ __html: highlightPython(line) }} />
                                                            </div>
                                                        ))}
                                                    </code>
                                                </pre>
                                            </div>
                                        </>
                                    ) : (
                                        <div className="flex-1 flex items-center justify-center text-obsidian-muted text-[11px]">
                                            No source code available
                                        </div>
                                    )}
                                </div>
                            )}
                        </div>
                    )}
                </div>
            </main >
        </div >
    );
}
