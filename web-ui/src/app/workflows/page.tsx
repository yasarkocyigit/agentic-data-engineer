
import React, { useState, useEffect, useCallback, useMemo, useRef } from 'react';
import { Sidebar } from '@/components/Sidebar';
import DagGraph from '@/components/DagGraph';
import {
    Play, RefreshCw, Pause, Search, ChevronRight, ChevronDown,
    LayoutPanelLeft, Clock, AlertCircle, CheckCircle2, XCircle,
    Loader2, Tag, User, Calendar, Zap,
    Activity, CircleStop, RotateCcw, Code2, FileText, Maximize2, Minimize2, GitBranch,
    AlignLeft, Download, SlidersHorizontal
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

function normalizeLogContent(content: unknown): string {
    if (typeof content === 'string') return content;
    if (Array.isArray(content)) {
        return content
            .map((entry) => {
                if (typeof entry === 'string') return entry;
                if (entry && typeof entry === 'object') {
                    const obj = entry as Record<string, unknown>;
                    if (typeof obj.event === 'string') return obj.event;
                    if (typeof obj.message === 'string') return obj.message;
                    return JSON.stringify(obj);
                }
                return String(entry);
            })
            .join('');
    }
    if (content && typeof content === 'object') return JSON.stringify(content, null, 2);
    return String(content ?? '');
}

function detailToMessage(detail: unknown): string {
    if (typeof detail === 'string') return detail;
    if (Array.isArray(detail)) {
        return detail
            .map((item) => {
                if (typeof item === 'string') return item;
                if (item && typeof item === 'object' && 'msg' in item) return String((item as { msg: unknown }).msg);
                return JSON.stringify(item);
            })
            .join('; ');
    }
    if (detail && typeof detail === 'object') return JSON.stringify(detail);
    return '';
}

function downloadTextFile(filename: string, content: string) {
    const blob = new Blob([content], { type: 'text/plain;charset=utf-8' });
    const url = URL.createObjectURL(blob);
    const anchor = document.createElement('a');
    anchor.href = url;
    anchor.download = filename;
    document.body.appendChild(anchor);
    anchor.click();
    anchor.remove();
    URL.revokeObjectURL(url);
}

async function parseApiResponse<T>(res: Response): Promise<T> {
    const raw = await res.text();
    let data: unknown = {};

    if (raw) {
        try {
            data = JSON.parse(raw);
        } catch {
            data = { text: raw };
        }
    }

    if (!res.ok) {
        const message =
            detailToMessage((data as { detail?: unknown })?.detail) ||
            detailToMessage((data as { error?: unknown })?.error) ||
            `${res.status} ${res.statusText}`.trim();
        throw new Error(message);
    }

    if (data && typeof data === 'object' && 'error' in data && (data as { error?: unknown }).error) {
        throw new Error(detailToMessage((data as { error?: unknown }).error) || 'Request failed');
    }

    return data as T;
}

type RunsView = 'list' | 'calendar' | 'gantt' | 'task';

function getStoredAirflowToken(): string | null {
    const candidates = [
        localStorage.getItem('openclaw-airflow-token'),
        localStorage.getItem('openclaw-airflow-jwt'),
        sessionStorage.getItem('openclaw-airflow-token'),
        sessionStorage.getItem('openclaw-airflow-jwt'),
    ];
    for (const value of candidates) {
        if (value && value.trim()) return value.trim();
    }
    return null;
}

function taskInstanceKey(taskId: string, mapIndex: number): string {
    return `${taskId}:${mapIndex}`;
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
                "inline-flex items-center gap-1.5 rounded-full font-medium tracking-wide shadow-sm border border-white/5",
                size === 'sm' ? "px-2 py-0.5 text-[10px]" : "px-3 py-1 text-[11px]"
            )}
            style={{ backgroundColor: colors.bg, color: colors.text }}
        >
            <Icon className={clsx(
                "flex-shrink-0 opacity-80",
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
    const RUNS_PAGE_SIZE = 20;

    const [dags, setDags] = useState<PipelineDAG[]>([]);
    const [loading, setLoading] = useState(true);
    const [error, setError] = useState<string | null>(null);
    const [health, setHealth] = useState<HealthStatus | null>(null);
    const [search, setSearch] = useState('');
    const [filterTag, setFilterTag] = useState<string | null>(null);
    const [selectedDag, setSelectedDag] = useState<string | null>(null);
    const [runs, setRuns] = useState<PipelineRun[]>([]);
    const [runsTotal, setRunsTotal] = useState(0);
    const [runsOffset, setRunsOffset] = useState(0);
    const [runsLoading, setRunsLoading] = useState(false);
    const [actionLoading, setActionLoading] = useState<string | null>(null);
    const [actionError, setActionError] = useState<string | null>(null);
    // Sidebar tabs
    const [sidebarTab, setSidebarTab] = useState<'runs' | 'code' | 'graph'>('runs');
    const [runsView, setRunsView] = useState<RunsView>('list');
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
    const [urlStateReady, setUrlStateReady] = useState(false);
    const [selectedTaskId, setSelectedTaskId] = useState<string | null>(null);

    // Task Action Loading
    const [taskActionLoading, setTaskActionLoading] = useState<string | null>(null);

    // Task-level Logs Viewer
    const [taskLogs, setTaskLogs] = useState<{ task_id: string; map_index: number; try_number: number; content: string } | null>(null);
    const [taskLogsLoading, setTaskLogsLoading] = useState(false);
    const [viewingTaskLogs, setViewingTaskLogs] = useState<string | null>(null);
    const [taskTryNumbers, setTaskTryNumbers] = useState<Record<string, number[]>>({});
    const [selectedLogTry, setSelectedLogTry] = useState<Record<string, number>>({});
    const [logAutoFollow, setLogAutoFollow] = useState(true);
    const logsScrollRef = useRef<HTMLDivElement | null>(null);

    // Run filters / trigger config / RBAC token
    const [runStateFilter, setRunStateFilter] = useState<string>('all');
    const [runTypeFilter, setRunTypeFilter] = useState<string>('all');
    const [runTriggeredByFilter, setRunTriggeredByFilter] = useState('');
    const [runStartFrom, setRunStartFrom] = useState('');
    const [runStartTo, setRunStartTo] = useState('');
    const [showFilters, setShowFilters] = useState(false);
    const [showMappedOnly, setShowMappedOnly] = useState(false);

    const [showTriggerDialog, setShowTriggerDialog] = useState(false);
    const [triggerConfText, setTriggerConfText] = useState('{\n  \n}');
    const [triggerConfError, setTriggerConfError] = useState<string | null>(null);
    const [customTaskState, setCustomTaskState] = useState('running');
    const [clearScope, setClearScope] = useState({
        include_upstream: false,
        include_downstream: false,
        include_future: false,
        include_past: false,
    });

    const [airflowToken, setAirflowToken] = useState('');

    const apiFetch = useCallback((input: string, init: RequestInit = {}) => {
        const headers = new Headers(init.headers || {});
        const raw = airflowToken.trim() || getStoredAirflowToken();
        if (raw) {
            const value = raw.toLowerCase().startsWith('bearer ') ? raw : `Bearer ${raw}`;
            headers.set('X-Airflow-Authorization', value);
        }
        return fetch(input, { ...init, headers });
    }, [airflowToken]);

    // ─── Fetch DAGs ───
    const fetchDags = useCallback(async () => {
        setLoading(true);
        try {
            const res = await apiFetch('/api/orchestrator/dags');
            const data = await parseApiResponse<{ dags?: PipelineDAG[] }>(res);
            setDags(data.dags || []);
            setError(null);
        } catch (err: unknown) {
            const message = err instanceof Error ? err.message : 'Failed to connect';
            setError(message);
        } finally {
            setLoading(false);
        }
    }, [apiFetch]);

    // ─── Fetch Health ───
    const fetchHealth = useCallback(async () => {
        try {
            const res = await apiFetch('/api/orchestrator/health');
            const data = await parseApiResponse<HealthStatus>(res);
            setHealth(data);
        } catch { /* silent */ }
    }, [apiFetch]);

    // ─── Fetch Runs for selected DAG ───
    const fetchRuns = useCallback(async (
        dagId: string,
        opts: { offset?: number; silent?: boolean } = {}
    ) => {
        const offset = opts.offset ?? 0;
        if (!opts.silent) setRunsLoading(true);
        try {
            const params = new URLSearchParams({
                limit: String(RUNS_PAGE_SIZE),
                offset: String(offset),
                order_by: '-start_date',
            });
            if (runStateFilter !== 'all') params.set('state', runStateFilter);
            if (runTypeFilter !== 'all') params.set('run_type', runTypeFilter);
            if (runTriggeredByFilter.trim()) params.set('triggered_by', runTriggeredByFilter.trim());
            if (runStartFrom) params.set('start_date_gte', new Date(`${runStartFrom}T00:00:00`).toISOString());
            if (runStartTo) params.set('start_date_lte', new Date(`${runStartTo}T23:59:59`).toISOString());
            const res = await apiFetch(`/api/orchestrator/dags/${encodeURIComponent(dagId)}/runs?${params.toString()}`);
            const data = await parseApiResponse<{ runs?: PipelineRun[]; total?: number }>(res);
            const serverRuns = data.runs || [];
            setRuns(serverRuns);
            setRunsTotal(data.total || serverRuns.length);
        } catch (err: unknown) {
            setRuns([]);
            setRunsTotal(0);
            if (!opts.silent) {
                const message = err instanceof Error ? err.message : 'Failed to load runs';
                setActionError(`Runs: ${message}`);
            }
        } finally {
            if (!opts.silent) setRunsLoading(false);
        }
    }, [RUNS_PAGE_SIZE, apiFetch, runStateFilter, runTypeFilter, runStartFrom, runStartTo, runTriggeredByFilter]);

    // ─── Fetch Task Instances for a run ───
    const fetchTaskInstances = useCallback(async (
        dagId: string,
        runId: string,
        opts: { silent?: boolean } = {}
    ) => {
        if (!opts.silent) setTasksLoading(true);
        try {
            const res = await apiFetch(
                `/api/orchestrator/dags/${encodeURIComponent(dagId)}/runs/${encodeURIComponent(runId)}/tasks`
            );
            const data = await parseApiResponse<{ task_instances?: AirflowTaskInstance[] }>(res);
            setTaskInstances(data.task_instances || []);
        } catch (err: unknown) {
            setTaskInstances([]);
            if (!opts.silent) {
                const message = err instanceof Error ? err.message : 'Failed to load task instances';
                setActionError(`Tasks: ${message}`);
            }
        } finally {
            if (!opts.silent) setTasksLoading(false);
        }
    }, [apiFetch]);

    const fetchTaskTries = useCallback(async (dagId: string, runId: string, taskId: string, mapIndex: number) => {
        const params = new URLSearchParams();
        if (mapIndex >= 0) params.set('map_index', String(mapIndex));
        const suffix = params.toString() ? `?${params.toString()}` : '';
        const res = await apiFetch(
            `/api/orchestrator/dags/${encodeURIComponent(dagId)}/runs/${encodeURIComponent(runId)}/tasks/${encodeURIComponent(taskId)}/tries${suffix}`
        );
        const data = await parseApiResponse<{ task_instances?: Array<{ try_number?: number }> }>(res);
        const tries = (data.task_instances || [])
            .map((item) => item.try_number)
            .filter((value): value is number => typeof value === 'number' && value > 0);
        const uniqueSorted = [...new Set(tries)].sort((a, b) => b - a);
        const key = taskInstanceKey(taskId, mapIndex);
        setTaskTryNumbers((prev) => ({ ...prev, [key]: uniqueSorted }));
        return uniqueSorted;
    }, [apiFetch]);

    const fetchTaskLogs = useCallback(async (
        dagId: string,
        runId: string,
        taskId: string,
        tryNumber: number,
        mapIndex: number
    ) => {
        const params = new URLSearchParams({ full_content: 'true' });
        if (mapIndex >= 0) params.set('map_index', String(mapIndex));
        const res = await apiFetch(
            `/api/orchestrator/dags/${encodeURIComponent(dagId)}/runs/${encodeURIComponent(runId)}/tasks/${encodeURIComponent(taskId)}/logs/${tryNumber}?${params.toString()}`
        );
        const data = await parseApiResponse<{ content?: unknown; text?: unknown }>(res);
        const contentCandidate = data.content ?? data.text ?? '';
        return normalizeLogContent(contentCandidate);
    }, [apiFetch]);

    // ─── Restore URL state on first load ───
    useEffect(() => {
        const params = new URLSearchParams(window.location.search);
        const dag = params.get('dag');
        const run = params.get('run');
        const tab = params.get('tab');
        const view = params.get('view');
        const offsetParam = params.get('offset');
        const qState = params.get('state');
        const qType = params.get('type');
        const qBy = params.get('by');
        const qFrom = params.get('from');
        const qTo = params.get('to');
        const parsedOffset = Number.parseInt(offsetParam || '0', 10);

        if (dag) setSelectedDag(dag);
        if (run) setExpandedRun(run);
        if (tab === 'runs' || tab === 'code' || tab === 'graph') setSidebarTab(tab);
        if (view === 'list' || view === 'calendar' || view === 'gantt' || view === 'task') setRunsView(view);
        if (Number.isFinite(parsedOffset) && parsedOffset >= 0) setRunsOffset(parsedOffset);
        if (qState) setRunStateFilter(qState);
        if (qType) setRunTypeFilter(qType);
        if (qBy) setRunTriggeredByFilter(qBy);
        if (qFrom) setRunStartFrom(qFrom);
        if (qTo) setRunStartTo(qTo);

        setUrlStateReady(true);
    }, []);

    useEffect(() => {
        const existingToken = getStoredAirflowToken();
        if (existingToken) setAirflowToken(existingToken);
    }, []);

    // ─── Keep URL state in sync ───
    useEffect(() => {
        if (!urlStateReady) return;
        const params = new URLSearchParams();
        if (selectedDag) params.set('dag', selectedDag);
        if (expandedRun) params.set('run', expandedRun);
        if (sidebarTab !== 'runs') params.set('tab', sidebarTab);
        if (runsView !== 'list') params.set('view', runsView);
        if (runsOffset > 0) params.set('offset', String(runsOffset));
        if (runStateFilter !== 'all') params.set('state', runStateFilter);
        if (runTypeFilter !== 'all') params.set('type', runTypeFilter);
        if (runTriggeredByFilter.trim()) params.set('by', runTriggeredByFilter.trim());
        if (runStartFrom) params.set('from', runStartFrom);
        if (runStartTo) params.set('to', runStartTo);

        const query = params.toString();
        const nextUrl = query ? `${window.location.pathname}?${query}` : window.location.pathname;
        window.history.replaceState({}, '', nextUrl);
    }, [
        urlStateReady,
        selectedDag,
        expandedRun,
        sidebarTab,
        runsView,
        runsOffset,
        runStateFilter,
        runTypeFilter,
        runTriggeredByFilter,
        runStartFrom,
        runStartTo,
    ]);

    // ─── Initial load + polling ───
    useEffect(() => {
        fetchDags();
        fetchHealth();
        const dagInterval = setInterval(fetchDags, 30000);
        const healthInterval = setInterval(fetchHealth, 30000);
        return () => { clearInterval(dagInterval); clearInterval(healthInterval); };
    }, [fetchDags, fetchHealth]);

    // ─── Select DAG / runs page change → load runs ───
    useEffect(() => {
        if (!selectedDag) {
            setRuns([]);
            setRunsTotal(0);
            setTaskInstances([]);
            return;
        }
        fetchRuns(selectedDag, { offset: runsOffset });
    }, [selectedDag, runsOffset, fetchRuns]);

    // ─── Expanded run → load task instances ───
    useEffect(() => {
        if (!selectedDag || !expandedRun) {
            setTaskInstances([]);
            return;
        }
        fetchTaskInstances(selectedDag, expandedRun);
    }, [selectedDag, expandedRun, fetchTaskInstances]);

    useEffect(() => {
        if (!expandedRun) {
            setSelectedTaskId(null);
            setShowMappedOnly(false);
        }
    }, [expandedRun]);

    // ─── Live polling for runs/task instances ───
    useEffect(() => {
        if (!selectedDag || sidebarTab !== 'runs') return;
        const runsInterval = setInterval(() => {
            fetchRuns(selectedDag, { offset: runsOffset, silent: true });
        }, 10000);
        return () => clearInterval(runsInterval);
    }, [selectedDag, sidebarTab, runsOffset, fetchRuns]);

    useEffect(() => {
        if (!selectedDag || !expandedRun || sidebarTab !== 'runs') return;
        const tasksInterval = setInterval(() => {
            fetchTaskInstances(selectedDag, expandedRun, { silent: true });
        }, 8000);
        return () => clearInterval(tasksInterval);
    }, [selectedDag, expandedRun, sidebarTab, fetchTaskInstances]);

    // ─── Fetch DAG Source Code ───
    const fetchDagSource = useCallback(async (dagId: string) => {
        setSourceLoading(true);
        try {
            const res = await apiFetch('/api/orchestrator/dags', {
                method: 'POST',
                headers: { 'Content-Type': 'application/json' },
                body: JSON.stringify({ dag_id: dagId, action: 'source' }),
            });
            const data = await parseApiResponse<{ content: string; version: number }>(res);
            setDagSource(data);
        } catch (err) {
            const message = err instanceof Error ? err.message : 'Failed to load source code';
            setActionError(`Source: ${message}`);
            setDagSource({ content: '# Failed to load source code', version: 0 });
        } finally {
            setSourceLoading(false);
        }
    }, [apiFetch]);

    // Auto-fetch source when switching to code tab
    useEffect(() => {
        if (sidebarTab === 'code' && selectedDag && !dagSource) {
            fetchDagSource(selectedDag);
        }
    }, [sidebarTab, selectedDag, dagSource, fetchDagSource]);

    // ─── Actions ───
    const handleAction = async (
        dagId: string,
        action: 'trigger' | 'pause' | 'unpause' | 'cancel',
        runId?: string,
        conf?: Record<string, unknown>,
    ) => {
        setActionLoading(`${dagId}-${action}`);
        setActionError(null);
        try {
            const res = await apiFetch('/api/orchestrator/dags', {
                method: 'POST',
                headers: { 'Content-Type': 'application/json' },
                body: JSON.stringify({ dag_id: dagId, action, run_id: runId, conf }),
            });
            await parseApiResponse<Record<string, unknown>>(res);
            // Refresh immediately
            await fetchDags();
            if (selectedDag === dagId) fetchRuns(dagId, { offset: runsOffset });
        } catch (err: unknown) {
            const message = err instanceof Error ? err.message : 'Action failed';
            setActionError(message);
            setTimeout(() => setActionError(null), 5000);
        } finally {
            setActionLoading(null);
        }
    };

    const handleTaskAction = async (
        dagId: string,
        runId: string,
        taskId: string,
        action: 'clear' | 'mark_success' | 'mark_failed' | 'mark_running' | 'mark_queued' | 'set_state',
        options: {
            map_index?: number;
            new_state?: string;
            note?: string;
            include_upstream?: boolean;
            include_downstream?: boolean;
            include_future?: boolean;
            include_past?: boolean;
        } = {},
    ) => {
        setTaskActionLoading(`${taskId}-${action}`);
        try {
            const res = await apiFetch(
                `/api/orchestrator/dags/${encodeURIComponent(dagId)}/runs/${encodeURIComponent(runId)}/tasks/${encodeURIComponent(taskId)}/action`,
                {
                    method: 'POST',
                    headers: { 'Content-Type': 'application/json' },
                    body: JSON.stringify({
                        action,
                        map_index: options.map_index ?? -1,
                        new_state: options.new_state,
                        note: options.note,
                        include_upstream: options.include_upstream ?? false,
                        include_downstream: options.include_downstream ?? false,
                        include_future: options.include_future ?? false,
                        include_past: options.include_past ?? false,
                    }),
                });
            await parseApiResponse<Record<string, unknown>>(res);
            // Refresh task instances and runs
            fetchTaskInstances(dagId, runId);
            fetchRuns(dagId, { offset: runsOffset });
        } catch (err: unknown) {
            const message = err instanceof Error ? err.message : 'Task action failed';
            setActionError(`Task ${taskId}: ${message}`);
        } finally {
            setTaskActionLoading(null);
        }
    };

    // ─── Expand a run to see task details ───
    const toggleRunExpand = (runId: string) => {
        if (expandedRun === runId) {
            setExpandedRun(null);
            setTaskInstances([]);
            setSelectedTaskId(null);
            setViewingTaskCode(null);
            setTaskCode(null);
            setViewingTaskLogs(null);
            setTaskLogs(null);
        } else {
            setExpandedRun(runId);
            setSelectedTaskId(null);
            setRunsView((prev) => (prev === 'task' ? 'list' : prev));
            setTaskTryNumbers({});
            setSelectedLogTry({});
            setViewingTaskCode(null);
            setTaskCode(null);
            setViewingTaskLogs(null);
            setTaskLogs(null);
        }
    };

    const toggleTaskLogs = useCallback(async (ti: AirflowTaskInstance) => {
        if (!selectedDag || !expandedRun) return;
        const key = taskInstanceKey(ti.task_id, ti.map_index);
        if (viewingTaskLogs === key) {
            setViewingTaskLogs(null);
            setTaskLogs(null);
            return;
        }

        setViewingTaskLogs(key);
        setTaskLogsLoading(true);
        try {
            let tries: number[] = [];
            try {
                tries = await fetchTaskTries(selectedDag, expandedRun, ti.task_id, ti.map_index);
            } catch {
                tries = [];
            }
            const activeTry = selectedLogTry[key] || tries[0] || ti.try_number || 1;
            setSelectedLogTry((prev) => ({ ...prev, [key]: activeTry }));
            const content = await fetchTaskLogs(selectedDag, expandedRun, ti.task_id, activeTry, ti.map_index);
            setTaskLogs({ task_id: ti.task_id, map_index: ti.map_index, try_number: activeTry, content });
        } catch (err: unknown) {
            const msg = err instanceof Error ? err.message : String(err);
            setTaskLogs({ task_id: ti.task_id, map_index: ti.map_index, try_number: ti.try_number || 1, content: `Failed to load logs: ${msg}` });
        } finally {
            setTaskLogsLoading(false);
        }
    }, [expandedRun, fetchTaskLogs, fetchTaskTries, selectedDag, selectedLogTry, viewingTaskLogs]);

    const changeTaskLogTry = useCallback(async (ti: AirflowTaskInstance, tryNumber: number) => {
        if (!selectedDag || !expandedRun) return;
        const key = taskInstanceKey(ti.task_id, ti.map_index);
        setTaskLogsLoading(true);
        setSelectedLogTry((prev) => ({ ...prev, [key]: tryNumber }));
        try {
            const content = await fetchTaskLogs(selectedDag, expandedRun, ti.task_id, tryNumber, ti.map_index);
            setTaskLogs({ task_id: ti.task_id, map_index: ti.map_index, try_number: tryNumber, content });
        } catch (err: unknown) {
            const msg = err instanceof Error ? err.message : String(err);
            setTaskLogs({ task_id: ti.task_id, map_index: ti.map_index, try_number: tryNumber, content: `Failed to load logs: ${msg}` });
        } finally {
            setTaskLogsLoading(false);
        }
    }, [expandedRun, fetchTaskLogs, selectedDag]);

    useEffect(() => {
        if (!logAutoFollow || !taskLogs || !logsScrollRef.current) return;
        const el = logsScrollRef.current;
        el.scrollTop = el.scrollHeight;
    }, [taskLogs, logAutoFollow]);

    useEffect(() => {
        if (!selectedDag || !expandedRun || !viewingTaskLogs || !taskLogs) return;
        const interval = setInterval(async () => {
            try {
                const [taskId, mapRaw] = viewingTaskLogs.split(':');
                const mapIndex = Number.parseInt(mapRaw ?? '-1', 10);
                const content = await fetchTaskLogs(
                    selectedDag,
                    expandedRun,
                    taskId,
                    taskLogs.try_number,
                    mapIndex,
                );
                setTaskLogs((prev) => prev ? { ...prev, content } : prev);
            } catch {
                // keep last successful logs
            }
        }, 5000);
        return () => clearInterval(interval);
    }, [selectedDag, expandedRun, viewingTaskLogs, taskLogs, fetchTaskLogs]);

    const submitTriggerWithConf = async () => {
        if (!selectedDag) return;
        setTriggerConfError(null);
        try {
            const parsed = triggerConfText.trim() ? JSON.parse(triggerConfText) as Record<string, unknown> : {};
            await handleAction(selectedDag, 'trigger', undefined, parsed);
            setShowTriggerDialog(false);
        } catch (err: unknown) {
            const message = err instanceof Error ? err.message : 'Invalid JSON';
            setTriggerConfError(message);
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
    const visibleTaskInstances = showMappedOnly
        ? taskInstances.filter((item) => item.map_index >= 0)
        : taskInstances;
    const selectedTaskInstance = selectedTaskId
        ? visibleTaskInstances.find((item) => taskInstanceKey(item.task_id, item.map_index) === selectedTaskId) || null
        : null;

    useEffect(() => {
        if (!expandedRun) return;
        if (!visibleTaskInstances.length) {
            setSelectedTaskId(null);
            return;
        }
        if (!selectedTaskId) {
            const first = visibleTaskInstances[0];
            setSelectedTaskId(taskInstanceKey(first.task_id, first.map_index));
            return;
        }
        const exists = visibleTaskInstances.some((item) => taskInstanceKey(item.task_id, item.map_index) === selectedTaskId);
        if (!exists) {
            const first = visibleTaskInstances[0];
            setSelectedTaskId(taskInstanceKey(first.task_id, first.map_index));
        }
    }, [expandedRun, visibleTaskInstances, selectedTaskId]);

    const runsByDay = useMemo(() => {
        const grouped = new Map<string, PipelineRun[]>();
        for (const run of runs) {
            const dateCandidate = run.start_date || run.logical_date || run.queued_at || run.run_after;
            if (!dateCandidate) continue;
            const key = new Date(dateCandidate).toISOString().slice(0, 10);
            const existing = grouped.get(key) || [];
            existing.push(run);
            grouped.set(key, existing);
        }
        return Array.from(grouped.entries())
            .sort((a, b) => (a[0] < b[0] ? 1 : -1))
            .map(([day, items]) => ({ day, items }));
    }, [runs]);

    const ganttRows = useMemo(() => {
        if (!taskInstances.length) return [];
        const startTimes = taskInstances
            .map((ti) => ti.start_date ? new Date(ti.start_date).getTime() : null)
            .filter((value): value is number => value !== null);
        const minStart = startTimes.length ? Math.min(...startTimes) : null;
        return taskInstances
            .map((ti) => {
                const start = ti.start_date ? new Date(ti.start_date).getTime() : null;
                const end = ti.end_date ? new Date(ti.end_date).getTime() : null;
                if (minStart === null || start === null || end === null || end < start) {
                    return {
                        key: `${ti.task_id}:${ti.map_index}`,
                        label: ti.task_id,
                        offsetPct: 0,
                        widthPct: 0,
                        state: ti.state,
                        duration: ti.duration,
                    };
                }
                const total = Math.max(1, (Math.max(...taskInstances
                    .map((row) => row.end_date ? new Date(row.end_date).getTime() : null)
                    .filter((value): value is number => value !== null)) - minStart));
                return {
                    key: `${ti.task_id}:${ti.map_index}`,
                    label: ti.task_id,
                    offsetPct: ((start - minStart) / total) * 100,
                    widthPct: Math.max(2, ((end - start) / total) * 100),
                    state: ti.state,
                    duration: ti.duration,
                };
            });
    }, [taskInstances]);

    return (
        <div className="flex h-screen bg-[#09090b] text-foreground font-sans overflow-hidden relative" style={{ fontFamily: "'Inter', -apple-system, sans-serif" }}>

            {/* Ambient Lighting */}
            <div className="absolute top-0 left-0 w-[800px] h-[800px] bg-purple-500/5 rounded-full blur-[120px] pointer-events-none -translate-x-1/4 -translate-y-1/4 z-0" />
            <div className="absolute bottom-0 right-0 w-[600px] h-[600px] bg-sky-500/5 rounded-full blur-[100px] pointer-events-none translate-x-1/4 -translate-y-1/4 z-0" />

            <div className="relative z-10 shrink-0">
                <Sidebar />
            </div>

            <main className="flex-1 flex flex-col min-w-0 bg-transparent relative z-10">

                {/* ─── Top Toolbar ─── */}
                <header className="flex items-center px-4 justify-between shrink-0 h-10 bg-black/40 backdrop-blur-md border-b border-white/5 z-10 w-full relative">
                    <div className="absolute top-0 left-0 right-0 h-[1px] bg-gradient-to-r from-obsidian-info/30 to-obsidian-purple/30" />
                    <div className="flex items-center gap-3">
                        <div className="flex items-center gap-2">
                            <button
                                onClick={() => window.dispatchEvent(new CustomEvent('openclaw:toggle-sidebar'))}
                                className="p-1.5 hover:bg-white/10 rounded-md text-obsidian-muted hover:text-white transition-all active:scale-95 border border-transparent hover:border-obsidian-border/50"
                                title="Toggle Explorer"
                            >
                                <LayoutPanelLeft className="w-4 h-4 drop-shadow-[0_0_8px_rgba(255,255,255,0.2)]" />
                            </button>
                            <div className="w-[1px] h-4 bg-obsidian-border/50 mx-1"></div>
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
                <div className="h-10 bg-black/20 backdrop-blur-md border-b border-white/5 flex items-center px-4 gap-4 shrink-0 z-10">
                    <div className="flex items-center gap-2 bg-white/[0.03] border border-white/10 rounded-md px-2.5 py-1.5 flex-1 max-w-sm transition-all focus-within:border-obsidian-info/50 focus-within:bg-white/[0.05]">
                        <Search className="w-4 h-4 text-obsidian-muted" />
                        <input
                            type="text"
                            placeholder="Search pipelines..."
                            value={search}
                            onChange={(e) => setSearch(e.target.value)}
                            className="bg-transparent text-[12px] text-foreground placeholder-[#6c707e] outline-none w-full"
                        />
                    </div>
                    <div className="flex items-center gap-1.5">
                        <Tag className="w-3.5 h-3.5 text-obsidian-muted" />
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
                    <button
                        onClick={() => setShowFilters((prev) => !prev)}
                        className={clsx(
                            "inline-flex items-center gap-1.5 px-2 py-1 rounded border text-[10px] transition-colors",
                            showFilters
                                ? "border-obsidian-info/40 text-obsidian-info bg-obsidian-info/10"
                                : "border-white/10 text-obsidian-muted hover:text-foreground hover:bg-white/[0.04]"
                        )}
                    >
                        <SlidersHorizontal className="w-3.5 h-3.5" />
                        Filters
                    </button>
                    <div className="flex items-center gap-1.5 ml-auto">
                        <input
                            type="password"
                            placeholder="Airflow token (optional)"
                            value={airflowToken}
                            onChange={(e) => setAirflowToken(e.target.value)}
                            className="h-7 w-56 bg-black/40 border border-white/10 rounded px-2 text-[10px] text-foreground placeholder:text-obsidian-muted outline-none focus:border-obsidian-info/60"
                        />
                        <button
                            onClick={() => {
                                if (airflowToken.trim()) {
                                    localStorage.setItem('openclaw-airflow-token', airflowToken.trim());
                                } else {
                                    localStorage.removeItem('openclaw-airflow-token');
                                }
                            }}
                            className="px-2 py-1 rounded border border-white/10 text-[10px] text-obsidian-muted hover:text-foreground hover:bg-white/[0.04]"
                        >
                            Save Token
                        </button>
                    </div>
                </div>

                {showFilters && (
                    <div className="px-4 py-2 border-b border-white/5 bg-black/30 grid grid-cols-5 gap-2 text-[10px]">
                        <select
                            value={runStateFilter}
                            onChange={(e) => { setRunsOffset(0); setRunStateFilter(e.target.value); }}
                            className="h-8 bg-black/40 border border-white/10 rounded px-2 text-foreground"
                        >
                            <option value="all">State: all</option>
                            <option value="queued">queued</option>
                            <option value="running">running</option>
                            <option value="success">success</option>
                            <option value="failed">failed</option>
                        </select>
                        <select
                            value={runTypeFilter}
                            onChange={(e) => { setRunsOffset(0); setRunTypeFilter(e.target.value); }}
                            className="h-8 bg-black/40 border border-white/10 rounded px-2 text-foreground"
                        >
                            <option value="all">Type: all</option>
                            <option value="manual">manual</option>
                            <option value="scheduled">scheduled</option>
                            <option value="backfill">backfill</option>
                            <option value="asset_triggered">asset_triggered</option>
                        </select>
                        <input
                            type="text"
                            placeholder="Triggered by..."
                            value={runTriggeredByFilter}
                            onChange={(e) => { setRunsOffset(0); setRunTriggeredByFilter(e.target.value); }}
                            className="h-8 bg-black/40 border border-white/10 rounded px-2 text-foreground placeholder:text-obsidian-muted"
                        />
                        <input
                            type="date"
                            value={runStartFrom}
                            onChange={(e) => { setRunsOffset(0); setRunStartFrom(e.target.value); }}
                            className="h-8 bg-black/40 border border-white/10 rounded px-2 text-foreground"
                        />
                        <div className="flex items-center gap-2">
                            <input
                                type="date"
                                value={runStartTo}
                                onChange={(e) => { setRunsOffset(0); setRunStartTo(e.target.value); }}
                                className="h-8 flex-1 bg-black/40 border border-white/10 rounded px-2 text-foreground"
                            />
                            <button
                                onClick={() => {
                                    setRunStateFilter('all');
                                    setRunTypeFilter('all');
                                    setRunTriggeredByFilter('');
                                    setRunStartFrom('');
                                    setRunStartTo('');
                                    setRunsOffset(0);
                                }}
                                className="h-8 px-2 rounded border border-white/10 text-obsidian-muted hover:text-foreground hover:bg-white/[0.04]"
                            >
                                Reset
                            </button>
                        </div>
                    </div>
                )}

                {/* ─── Error Toast ─── */}
                {actionError && (
                    <div className="mx-4 mt-1 px-3 py-2 bg-obsidian-danger/10 border border-obsidian-danger/30 rounded text-[11px] text-obsidian-danger-light flex items-center gap-2 shrink-0 transition-all active:scale-95">
                        <AlertCircle className="w-3.5 h-3.5 text-obsidian-danger shrink-0" />
                        <span className="flex-1 truncate">{actionError}</span>
                        <button onClick={() => setActionError(null)} className="text-obsidian-danger hover:text-white text-[10px] font-bold">✕</button>
                    </div>
                )}

                {/* ─── Content Area ─── */}
                <div className="flex-1 flex overflow-hidden">

                    {/* DAG List / Graph View */}
                    <div className={clsx("flex-1 overflow-auto", selectedDag ? "border-r border-white/5" : "")}>
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
                                    className="mt-2 px-3 py-1.5 bg-obsidian-info/20 text-obsidian-info rounded-md text-[11px] font-medium hover:bg-obsidian-info/30 transition-colors border border-obsidian-info/30 active:scale-95"
                                >
                                    Retry Connection
                                </button>
                            </div>
                        ) : (
                            <table className="w-full text-left border-collapse">
                                <thead className="sticky top-0 bg-black/40 backdrop-blur-md z-10 shadow-[0_1px_0_rgba(255,255,255,0.05)]">
                                    <tr>
                                        <th className="p-2 px-4 text-[10px] text-obsidian-muted font-semibold tracking-wider w-8"></th>
                                        <th className="p-2 px-4 text-[10px] text-obsidian-muted font-semibold tracking-wider text-left">Pipeline</th>
                                        <th className="p-2 px-4 text-[10px] text-obsidian-muted font-semibold tracking-wider text-left w-28">Schedule</th>
                                        <th className="p-2 px-4 text-[10px] text-obsidian-muted font-semibold tracking-wider text-left w-28">Last Run</th>
                                        <th className="p-2 px-4 text-[10px] text-obsidian-muted font-semibold tracking-wider text-left w-24">State</th>
                                        <th className="p-2 px-4 text-[10px] text-obsidian-muted font-semibold tracking-wider text-left w-24">Duration</th>
                                        <th className="p-2 px-4 text-[10px] text-obsidian-muted font-semibold tracking-wider text-center w-48">Actions</th>
                                    </tr>
                                </thead>
                                <tbody className="text-[12px]">
                                    {filteredDags.map((dag) => {
                                        const isSelected = selectedDag === dag.dag_id;
                                        const isRunning = dag.last_run?.state === 'running' || dag.last_run?.state === 'queued';
                                        return (
                                            <tr
                                                key={dag.dag_id}
                                                onClick={() => {
                                                    if (isSelected) {
                                                        setSelectedDag(null);
                                                        setExpandedRun(null);
                                                        setRuns([]);
                                                        setRunsTotal(0);
                                                        setTaskInstances([]);
                                                        setViewingTaskCode(null);
                                                        setTaskCode(null);
                                                        setViewingTaskLogs(null);
                                                        setTaskLogs(null);
                                                        setRunsOffset(0);
                                                        return;
                                                    }
                                                    setSelectedDag(dag.dag_id);
                                                    setExpandedRun(null);
                                                    setTaskInstances([]);
                                                    setTaskTryNumbers({});
                                                    setSelectedLogTry({});
                                                    setViewingTaskCode(null);
                                                    setTaskCode(null);
                                                    setViewingTaskLogs(null);
                                                    setTaskLogs(null);
                                                    setRunsOffset(0);
                                                    setSidebarTab('graph');
                                                    setDagSource(null);
                                                }}
                                                className={clsx(
                                                    "border-b border-white/5 group cursor-pointer transition-colors",
                                                    isSelected ? "bg-white/[0.04]" : "hover:bg-white/[0.02]"
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
                                                            <span title="Import errors"><AlertCircle className="w-3.5 h-3.5 text-obsidian-danger" /></span>
                                                        )}
                                                    </div>
                                                    <div className="flex items-center gap-2 mt-0.5">
                                                        <span className="text-[10px] text-obsidian-muted">
                                                            <User className="w-2.5 h-2.5 inline mr-0.5 -mt-0.5" />
                                                            {dag.owners.join(', ')}
                                                        </span>
                                                        {dag.tags.slice(0, 3).map(tag => (
                                                            <span key={tag} className="text-[8px] px-1.5 py-0.5 rounded bg-obsidian-panel-hover text-obsidian-muted transition-all active:scale-95">
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

                                                        {/* 1. TRIGGER — outline style */}
                                                        <button
                                                            onClick={() => handleAction(dag.dag_id, 'trigger')}
                                                            disabled={!!actionLoading}
                                                            className="flex items-center gap-1.5 px-2.5 py-1.5 rounded-md text-[10px] font-medium
                                         bg-transparent text-obsidian-success hover:bg-obsidian-success/10
                                         border border-obsidian-success/30 hover:border-obsidian-success/50 transition-all disabled:opacity-30 active:scale-95"
                                                        >
                                                            {actionLoading === `${dag.dag_id}-trigger`
                                                                ? <Loader2 className="w-3.5 h-3.5 animate-spin" />
                                                                : <Play className="w-3.5 h-3.5" />}
                                                            <span>Run</span>
                                                        </button>

                                                        {/* 2. CANCEL — outline style */}
                                                        {isRunning && dag.last_run && (
                                                            <button
                                                                onClick={() => handleAction(dag.dag_id, 'cancel', dag.last_run!.run_id)}
                                                                disabled={!!actionLoading}
                                                                className="flex items-center gap-1.5 px-2.5 py-1.5 rounded-md text-[10px] font-medium
                                           bg-transparent text-obsidian-danger hover:bg-obsidian-danger/10
                                           border border-obsidian-danger/30 hover:border-obsidian-danger/50 transition-all disabled:opacity-30 active:scale-95"
                                                            >
                                                                {actionLoading === `${dag.dag_id}-cancel`
                                                                    ? <Loader2 className="w-3.5 h-3.5 animate-spin" />
                                                                    : <CircleStop className="w-3.5 h-3.5" />}
                                                                <span>Cancel</span>
                                                            </button>
                                                        )}

                                                        {/* 3. PAUSE/RESUME — outline style */}
                                                        <button
                                                            onClick={() => handleAction(dag.dag_id, dag.is_paused ? 'unpause' : 'pause')}
                                                            disabled={!!actionLoading}
                                                            className={clsx(
                                                                "flex items-center gap-1.5 px-2.5 py-1.5 rounded-md text-[10px] font-medium border bg-transparent transition-all disabled:opacity-30",
                                                                dag.is_paused
                                                                    ? "text-obsidian-info hover:bg-obsidian-info/10 border-obsidian-info/30 hover:border-obsidian-info/50"
                                                                    : "text-obsidian-warning hover:bg-obsidian-warning/10 border-obsidian-warning/30 hover:border-obsidian-warning/50"
                                                            )}
                                                        >
                                                            {actionLoading === `${dag.dag_id}-pause` || actionLoading === `${dag.dag_id}-unpause`
                                                                ? <Loader2 className="w-3.5 h-3.5 animate-spin" />
                                                                : dag.is_paused
                                                                    ? <RotateCcw className="w-3.5 h-3.5" />
                                                                    : <Pause className="w-3.5 h-3.5" />}
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
                            "flex flex-col bg-[#09090b]/80 backdrop-blur-2xl border-l border-white/5 shrink-0 transition-all duration-200 z-20",
                            fullscreenSidebar
                                ? "fixed inset-0 w-full"
                                : "w-[400px]"
                        )}>
                            {/* Header */}
                            <div className={clsx(
                                "bg-black/40 border-b border-white/5 flex items-center justify-between shrink-0",
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
                                        onClick={() => {
                                            setSelectedDag(null);
                                            setExpandedRun(null);
                                            setRuns([]);
                                            setRunsTotal(0);
                                            setTaskInstances([]);
                                            setTaskTryNumbers({});
                                            setSelectedLogTry({});
                                            setViewingTaskCode(null);
                                            setTaskCode(null);
                                            setViewingTaskLogs(null);
                                            setTaskLogs(null);
                                            setRunsOffset(0);
                                            setFullscreenSidebar(false);
                                        }}
                                        className="p-1 hover:bg-obsidian-panel-hover rounded text-obsidian-muted hover:text-foreground text-[12px]"
                                    >
                                        ✕
                                    </button>
                                </div>
                            </div>

                            {/* DAG Info */}
                            {selectedDagData && (
                                <div className="px-4 py-3 border-b border-white/5 text-[11px] space-y-2">
                                    {selectedDagData.description && (
                                        <p className="text-obsidian-muted leading-relaxed">{selectedDagData.description}</p>
                                    )}
                                    <div className="flex items-center gap-4 text-[10px] text-obsidian-muted">
                                        <span><Calendar className="w-3.5 h-3.5 inline mr-1 -mt-0.5" />{selectedDagData.schedule || 'Manual'}</span>
                                        <span><User className="w-3.5 h-3.5 inline mr-1 -mt-0.5" />{selectedDagData.owners.join(', ')}</span>
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
                                                ? <><RotateCcw className="w-3.5 h-3.5" /> Enable Schedule</>
                                                : <><Pause className="w-3.5 h-3.5" /> Pause Schedule</>}
                                        </button>
                                    </div>
                                </div>
                            )}

                            {/* Tab Bar */}
                            <div className="flex border-b border-white/5 shrink-0">
                                <button
                                    onClick={() => setSidebarTab('graph')}
                                    className={clsx(
                                        "flex-1 flex items-center justify-center gap-1.5 py-2.5 text-[10px] font-medium transition-all",
                                        sidebarTab === 'graph'
                                            ? "text-foreground border-b-2 border-obsidian-info bg-white/[0.04]"
                                            : "text-obsidian-muted hover:text-foreground hover:bg-white/[0.02]"
                                    )}
                                >
                                    <GitBranch className="w-3.5 h-3.5" /> Graph
                                </button>
                                <button
                                    onClick={() => setSidebarTab('runs')}
                                    className={clsx(
                                        "flex-1 flex items-center justify-center gap-1.5 py-2.5 text-[10px] font-medium transition-all",
                                        sidebarTab === 'runs'
                                            ? "text-foreground border-b-2 border-obsidian-info bg-white/[0.04]"
                                            : "text-obsidian-muted hover:text-foreground hover:bg-white/[0.02]"
                                    )}
                                >
                                    <FileText className="w-3.5 h-3.5" /> Runs
                                </button>
                                <button
                                    onClick={() => setSidebarTab('code')}
                                    className={clsx(
                                        "flex-1 flex items-center justify-center gap-1.5 py-2.5 text-[10px] font-medium transition-all",
                                        sidebarTab === 'code'
                                            ? "text-foreground border-b-2 border-obsidian-info bg-white/[0.04]"
                                            : "text-obsidian-muted hover:text-foreground hover:bg-white/[0.02]"
                                    )}
                                >
                                    <Code2 className="w-3.5 h-3.5" /> Code
                                </button>
                            </div>

                            {/* Runs List header */}
                            {sidebarTab === 'runs' && (
                                <div className="px-4 py-2 border-b border-white/5 flex flex-col gap-2 shrink-0">
                                    <div className="flex items-center justify-between gap-2">
                                        <div className="flex items-center gap-2">
                                            <span className="text-[10px] text-obsidian-muted uppercase font-semibold tracking-wider">Recent Runs</span>
                                            <span className="text-[9px] text-obsidian-muted/80">
                                                {runsTotal > 0
                                                    ? `${runsOffset + 1}-${Math.min(runsOffset + RUNS_PAGE_SIZE, runsTotal)} / ${runsTotal}`
                                                    : '0 / 0'}
                                            </span>
                                            <div className="flex items-center gap-1">
                                                <button
                                                    onClick={() => setRunsOffset((prev) => Math.max(0, prev - RUNS_PAGE_SIZE))}
                                                    disabled={runsOffset === 0 || runsLoading}
                                                    className="px-1.5 py-0.5 rounded border border-white/10 text-[9px] text-obsidian-muted hover:text-foreground hover:bg-white/[0.04] disabled:opacity-30"
                                                >
                                                    Prev
                                                </button>
                                                <button
                                                    onClick={() => setRunsOffset((prev) => prev + RUNS_PAGE_SIZE)}
                                                    disabled={runsOffset + RUNS_PAGE_SIZE >= runsTotal || runsLoading}
                                                    className="px-1.5 py-0.5 rounded border border-white/10 text-[9px] text-obsidian-muted hover:text-foreground hover:bg-white/[0.04] disabled:opacity-30"
                                                >
                                                    Next
                                                </button>
                                            </div>
                                        </div>
                                        <div className="flex items-center gap-1.5">
                                            <button
                                                onClick={() => handleAction(selectedDag, 'trigger')}
                                                disabled={!!actionLoading}
                                                className="flex items-center gap-1 px-2 py-1 bg-obsidian-success/15 text-obsidian-success rounded text-[10px]
                                font-medium hover:bg-obsidian-success/25 border border-obsidian-success/20 transition-colors disabled:opacity-50 active:scale-95"
                                            >
                                                <Play className="w-3.5 h-3.5" /> Quick Run
                                            </button>
                                            <button
                                                onClick={() => {
                                                    setTriggerConfError(null);
                                                    setShowTriggerDialog(true);
                                                }}
                                                className="flex items-center gap-1 px-2 py-1 rounded text-[10px] border border-obsidian-info/30 text-obsidian-info hover:bg-obsidian-info/15 transition-colors"
                                            >
                                                <Code2 className="w-3.5 h-3.5" /> Run + Conf
                                            </button>
                                        </div>
                                    </div>
                                    <div className="flex items-center gap-1.5">
                                        {(['list', 'calendar', 'gantt', 'task'] as RunsView[]).map((view) => (
                                            <button
                                                key={view}
                                                onClick={() => setRunsView(view)}
                                                className={clsx(
                                                    "px-2 py-0.5 rounded border text-[9px] uppercase tracking-wide",
                                                    runsView === view
                                                        ? "border-obsidian-info/40 text-obsidian-info bg-obsidian-info/10"
                                                        : "border-white/10 text-obsidian-muted hover:text-foreground hover:bg-white/[0.04]"
                                                )}
                                            >
                                                {view}
                                            </button>
                                        ))}
                                        {expandedRun && (
                                            <span className="ml-auto text-[9px] text-obsidian-muted font-mono">
                                                Run: {expandedRun}
                                            </span>
                                        )}
                                    </div>
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
                                    ) : runsView === 'calendar' ? (
                                        <div className="p-3 space-y-3">
                                            {runsByDay.map((group) => (
                                                <div key={group.day} className="border border-white/10 rounded-md overflow-hidden">
                                                    <div className="px-3 py-1.5 bg-white/[0.03] text-[10px] text-obsidian-muted font-mono">
                                                        {new Date(group.day).toLocaleDateString()}
                                                    </div>
                                                    <div className="divide-y divide-white/5">
                                                        {group.items.map((run) => (
                                                            <button
                                                                key={run.run_id}
                                                                onClick={() => {
                                                                    setRunsView('list');
                                                                    toggleRunExpand(run.run_id);
                                                                }}
                                                                className="w-full text-left px-3 py-2 hover:bg-white/[0.03] transition-colors"
                                                            >
                                                                <div className="flex items-center justify-between">
                                                                    <div className="flex items-center gap-2">
                                                                        <StateBadge state={run.state} />
                                                                        <span className="text-[10px] text-foreground font-mono">{run.run_id}</span>
                                                                    </div>
                                                                    <span className="text-[9px] text-obsidian-muted">
                                                                        {timeAgo(run.end_date || run.start_date || run.logical_date || null)}
                                                                    </span>
                                                                </div>
                                                                <div className="mt-1 text-[9px] text-obsidian-muted font-mono">
                                                                    {run.run_type} • {run.triggered_by || 'system'}
                                                                </div>
                                                            </button>
                                                        ))}
                                                    </div>
                                                </div>
                                            ))}
                                        </div>
                                    ) : runsView === 'gantt' ? (
                                        <div className="p-3 space-y-3">
                                            {!expandedRun ? (
                                                <div className="text-center py-8 text-obsidian-muted text-[11px]">
                                                    Select a run in List/Calendar view to render task timeline.
                                                </div>
                                            ) : tasksLoading ? (
                                                <div className="flex items-center justify-center py-8">
                                                    <Loader2 className="w-5 h-5 text-obsidian-info animate-spin" />
                                                </div>
                                            ) : visibleTaskInstances.length === 0 ? (
                                                <div className="text-center py-8 text-obsidian-muted text-[11px]">
                                                    No task instances for selected run.
                                                </div>
                                            ) : (
                                                <div className="space-y-2">
                                                    {ganttRows
                                                        .filter((row) => !showMappedOnly || row.key.split(':')[1] !== '-1')
                                                        .map((row) => (
                                                            <div key={row.key} className="grid grid-cols-[160px_1fr_56px] items-center gap-2">
                                                                <span className="text-[10px] text-foreground font-mono truncate">{row.label}</span>
                                                                <div className="relative h-4 bg-white/[0.05] rounded overflow-hidden">
                                                                    <div
                                                                        className="absolute top-0 h-full rounded"
                                                                        style={{
                                                                            left: `${row.offsetPct}%`,
                                                                            width: `${row.widthPct}%`,
                                                                            backgroundColor: getStateColor(row.state).text,
                                                                            opacity: 0.8,
                                                                        }}
                                                                    />
                                                                </div>
                                                                <span className="text-[9px] text-obsidian-muted font-mono">
                                                                    {formatDuration(row.duration)}
                                                                </span>
                                                            </div>
                                                        ))}
                                                </div>
                                            )}
                                        </div>
                                    ) : runsView === 'task' ? (
                                        <div className="p-3 space-y-3">
                                            {!expandedRun ? (
                                                <div className="text-center py-8 text-obsidian-muted text-[11px]">
                                                    Select a run and a task to open task detail view.
                                                </div>
                                            ) : !selectedTaskInstance ? (
                                                <div className="text-center py-8 text-obsidian-muted text-[11px]">
                                                    Choose a task from List view.
                                                </div>
                                            ) : (
                                                <div className="border border-white/10 rounded-md p-3 space-y-2">
                                                    <div className="flex items-center justify-between">
                                                        <div className="flex items-center gap-2">
                                                            <span className="text-[12px] font-semibold text-foreground font-mono">
                                                                {selectedTaskInstance.task_id}
                                                            </span>
                                                            {selectedTaskInstance.map_index >= 0 && (
                                                                <span className="text-[9px] px-1.5 py-0.5 rounded bg-obsidian-info/15 text-obsidian-info font-mono">
                                                                    map[{selectedTaskInstance.map_index}]
                                                                </span>
                                                            )}
                                                            <StateBadge state={selectedTaskInstance.state} />
                                                        </div>
                                                        <span className="text-[10px] text-obsidian-muted font-mono">
                                                            {formatDuration(selectedTaskInstance.duration)}
                                                        </span>
                                                    </div>
                                                    <div className="text-[10px] text-obsidian-muted grid grid-cols-2 gap-2 font-mono">
                                                        <span>Operator: {selectedTaskInstance.operator}</span>
                                                        <span>Try: {selectedTaskInstance.try_number}/{selectedTaskInstance.max_tries}</span>
                                                        <span>Queue: {selectedTaskInstance.queue}</span>
                                                        <span>Host: {selectedTaskInstance.hostname || '—'}</span>
                                                    </div>
                                                    <div className="grid grid-cols-2 gap-2 rounded border border-white/10 bg-black/20 p-2 text-[10px] text-obsidian-muted">
                                                        <label className="flex items-center gap-1.5">
                                                            <input
                                                                type="checkbox"
                                                                checked={clearScope.include_upstream}
                                                                onChange={(e) => setClearScope((prev) => ({ ...prev, include_upstream: e.target.checked }))}
                                                            />
                                                            clear upstream
                                                        </label>
                                                        <label className="flex items-center gap-1.5">
                                                            <input
                                                                type="checkbox"
                                                                checked={clearScope.include_downstream}
                                                                onChange={(e) => setClearScope((prev) => ({ ...prev, include_downstream: e.target.checked }))}
                                                            />
                                                            clear downstream
                                                        </label>
                                                        <label className="flex items-center gap-1.5">
                                                            <input
                                                                type="checkbox"
                                                                checked={clearScope.include_past}
                                                                onChange={(e) => setClearScope((prev) => ({ ...prev, include_past: e.target.checked }))}
                                                            />
                                                            include past
                                                        </label>
                                                        <label className="flex items-center gap-1.5">
                                                            <input
                                                                type="checkbox"
                                                                checked={clearScope.include_future}
                                                                onChange={(e) => setClearScope((prev) => ({ ...prev, include_future: e.target.checked }))}
                                                            />
                                                            include future
                                                        </label>
                                                    </div>
                                                    <div className="flex items-center gap-2 pt-1">
                                                        <button
                                                            onClick={() => handleTaskAction(selectedDag!, expandedRun, selectedTaskInstance.task_id, 'clear', {
                                                                map_index: selectedTaskInstance.map_index,
                                                                ...clearScope,
                                                            })}
                                                            disabled={!!taskActionLoading}
                                                            className="px-2 py-1 rounded text-[10px] border border-obsidian-warning/30 text-obsidian-warning hover:bg-obsidian-warning/15 disabled:opacity-40"
                                                        >
                                                            Clear
                                                        </button>
                                                        <button
                                                            onClick={() => handleTaskAction(selectedDag!, expandedRun, selectedTaskInstance.task_id, 'mark_success', {
                                                                map_index: selectedTaskInstance.map_index,
                                                            })}
                                                            disabled={!!taskActionLoading}
                                                            className="px-2 py-1 rounded text-[10px] border border-obsidian-success/30 text-obsidian-success hover:bg-obsidian-success/15 disabled:opacity-40"
                                                        >
                                                            Mark Success
                                                        </button>
                                                        <button
                                                            onClick={() => handleTaskAction(selectedDag!, expandedRun, selectedTaskInstance.task_id, 'mark_failed', {
                                                                map_index: selectedTaskInstance.map_index,
                                                            })}
                                                            disabled={!!taskActionLoading}
                                                            className="px-2 py-1 rounded text-[10px] border border-obsidian-danger/30 text-obsidian-danger hover:bg-obsidian-danger/15 disabled:opacity-40"
                                                        >
                                                            Mark Failed
                                                        </button>
                                                        <button
                                                            onClick={() => handleTaskAction(selectedDag!, expandedRun, selectedTaskInstance.task_id, 'mark_running', {
                                                                map_index: selectedTaskInstance.map_index,
                                                            })}
                                                            disabled={!!taskActionLoading}
                                                            className="px-2 py-1 rounded text-[10px] border border-obsidian-info/30 text-obsidian-info hover:bg-obsidian-info/15 disabled:opacity-40"
                                                        >
                                                            Mark Running
                                                        </button>
                                                        <button
                                                            onClick={() => handleTaskAction(selectedDag!, expandedRun, selectedTaskInstance.task_id, 'mark_queued', {
                                                                map_index: selectedTaskInstance.map_index,
                                                            })}
                                                            disabled={!!taskActionLoading}
                                                            className="px-2 py-1 rounded text-[10px] border border-white/20 text-obsidian-muted hover:text-foreground hover:bg-white/[0.08] disabled:opacity-40"
                                                        >
                                                            Mark Queued
                                                        </button>
                                                    </div>
                                                    <div className="flex items-center gap-2 pt-1">
                                                        <select
                                                            value={customTaskState}
                                                            onChange={(e) => setCustomTaskState(e.target.value)}
                                                            className="h-8 bg-black/40 border border-white/10 rounded px-2 text-[10px] text-foreground"
                                                        >
                                                            <option value="queued">queued</option>
                                                            <option value="running">running</option>
                                                            <option value="success">success</option>
                                                            <option value="failed">failed</option>
                                                            <option value="skipped">skipped</option>
                                                            <option value="upstream_failed">upstream_failed</option>
                                                            <option value="removed">removed</option>
                                                        </select>
                                                        <button
                                                            onClick={() => handleTaskAction(selectedDag!, expandedRun, selectedTaskInstance.task_id, 'set_state', {
                                                                map_index: selectedTaskInstance.map_index,
                                                                new_state: customTaskState,
                                                            })}
                                                            disabled={!!taskActionLoading}
                                                            className="px-2 py-1 rounded text-[10px] border border-obsidian-info/30 text-obsidian-info hover:bg-obsidian-info/15 disabled:opacity-40"
                                                        >
                                                            Set State
                                                        </button>
                                                    </div>
                                                </div>
                                            )}
                                        </div>
                                    ) : (
                                        <div>
                                            {runs.map((run) => {
                                                const isExpanded = expandedRun === run.run_id;
                                                const isRunRunning = run.state === 'running' || run.state === 'queued';
                                                return (
                                                    <div key={run.run_id} className="border-b border-white/5">
                                                        {/* Run row */}
                                                        <div
                                                            onClick={() => toggleRunExpand(run.run_id)}
                                                            className={clsx(
                                                                "px-4 py-2.5 cursor-pointer transition-colors",
                                                                isExpanded ? "bg-white/[0.04]" : "hover:bg-white/[0.02]"
                                                            )}
                                                        >
                                                            <div className="flex items-center justify-between mb-1">
                                                                <div className="flex items-center gap-1.5">
                                                                    {isExpanded
                                                                        ? <ChevronDown className="w-3.5 h-3.5 text-obsidian-muted" />
                                                                        : <ChevronRight className="w-3.5 h-3.5 text-obsidian-muted" />}
                                                                    <StateBadge state={run.state} />
                                                                    {/* Cancel button inline for running runs */}
                                                                    {isRunRunning && (
                                                                        <button
                                                                            onClick={(e) => { e.stopPropagation(); handleAction(selectedDag!, 'cancel', run.run_id); }}
                                                                            disabled={!!actionLoading}
                                                                            className="flex items-center gap-1 ml-1 px-1.5 py-0.5 rounded text-[8px] font-semibold
                                               bg-obsidian-danger/10 text-obsidian-danger hover:bg-obsidian-danger/25
                                               border border-obsidian-danger/20 transition-colors disabled:opacity-30 active:scale-95"
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
                                                            <div className="bg-black/20 backdrop-blur-md border-t border-white/5">
                                                                {tasksLoading ? (
                                                                    <div className="flex items-center justify-center py-4">
                                                                        <Loader2 className="w-4 h-4 text-obsidian-info animate-spin" />
                                                                    </div>
                                                                ) : visibleTaskInstances.length === 0 ? (
                                                                    <div className="text-center py-4 text-obsidian-muted text-[10px]">No task details available</div>
                                                                ) : (
                                                                    <div className="py-1">
                                                                        <div className="px-4 py-1.5 text-[9px] text-obsidian-muted uppercase font-semibold tracking-wider flex items-center gap-2">
                                                                            <Activity className="w-2.5 h-2.5" />
                                                                            <span>Task Details</span>
                                                                            <span className="text-[8px] normal-case tracking-normal font-mono text-obsidian-muted/90">
                                                                                {visibleTaskInstances.length} items
                                                                            </span>
                                                                            <button
                                                                                onClick={() => setShowMappedOnly((prev) => !prev)}
                                                                                className={clsx(
                                                                                    "ml-auto px-1.5 py-0.5 rounded border text-[8px] normal-case tracking-normal",
                                                                                    showMappedOnly
                                                                                        ? "border-obsidian-info/30 text-obsidian-info bg-obsidian-info/10"
                                                                                        : "border-white/10 text-obsidian-muted hover:text-foreground hover:bg-white/[0.04]"
                                                                                )}
                                                                            >
                                                                                mapped only
                                                                            </button>
                                                                        </div>
                                                                        {visibleTaskInstances.map((ti) => {
                                                                            const taskColor = getStateColor(ti.state);
                                                                            const isFailed = ti.state === 'failed';
                                                                            return (
                                                                                <div
                                                                                    key={ti.id}
                                                                                    onClick={() => {
                                                                                        setSelectedTaskId(taskInstanceKey(ti.task_id, ti.map_index));
                                                                                        setRunsView('task');
                                                                                    }}
                                                                                    className={clsx(
                                                                                        "px-4 py-2 flex items-start gap-2 text-[11px] cursor-pointer transition-colors",
                                                                                        isFailed && "bg-obsidian-danger/5",
                                                                                        selectedTaskId === taskInstanceKey(ti.task_id, ti.map_index) && "bg-obsidian-info/10 ring-1 ring-obsidian-info/20"
                                                                                    )}
                                                                                >
                                                                                    {/* State icon */}
                                                                                    <div className="mt-0.5">
                                                                                        {ti.state === 'success' ? (
                                                                                            <CheckCircle2 className="w-3.5 h-3.5" style={{ color: taskColor.text }} />
                                                                                        ) : ti.state === 'failed' ? (
                                                                                            <XCircle className="w-3.5 h-3.5" style={{ color: taskColor.text }} />
                                                                                        ) : ti.state === 'running' ? (
                                                                                            <Loader2 className="w-3.5 h-3.5 animate-spin" style={{ color: taskColor.text }} />
                                                                                        ) : ti.state === 'skipped' ? (
                                                                                            <ChevronRight className="w-3.5 h-3.5" style={{ color: taskColor.text }} />
                                                                                        ) : (
                                                                                            <Activity className="w-3.5 h-3.5" style={{ color: taskColor.text }} />
                                                                                        )}
                                                                                    </div>

                                                                                    {/* Task info */}
                                                                                    <div className="flex-1 min-w-0">
                                                                                        <div className="flex items-center justify-between">
                                                                                            <div className="flex items-center gap-1.5 min-w-0">
                                                                                                <span className={clsx(
                                                                                                    "font-mono font-medium truncate",
                                                                                                    isFailed ? "text-obsidian-danger" : "text-foreground"
                                                                                                )}>
                                                                                                    {ti.task_display_name}
                                                                                                </span>
                                                                                                {ti.map_index >= 0 && (
                                                                                                    <span className="text-[8px] px-1.5 py-0.5 rounded bg-obsidian-info/15 text-obsidian-info font-mono shrink-0">
                                                                                                        map[{ti.map_index}]
                                                                                                    </span>
                                                                                                )}
                                                                                            </div>
                                                                                            <span className="text-[10px] text-obsidian-muted font-mono ml-2 shrink-0">
                                                                                                {formatDuration(ti.duration)}
                                                                                            </span>
                                                                                        </div>
                                                                                        <div className="text-[9px] text-obsidian-muted mt-0.5 flex items-center gap-2 flex-wrap">
                                                                                            <span>
                                                                                                {ti.operator}
                                                                                                {ti.try_number > 1 && (
                                                                                                    <span className="ml-1 text-obsidian-warning">
                                                                                                        (try {ti.try_number}/{ti.max_tries})
                                                                                                    </span>
                                                                                                )}
                                                                                            </span>
                                                                                            {/* Clear / Retry Task button */}
                                                                                            <button
                                                                                                onClick={(e) => { e.stopPropagation(); handleTaskAction(selectedDag!, expandedRun!, ti.task_id, 'clear', { map_index: ti.map_index }); }}
                                                                                                disabled={!!taskActionLoading}
                                                                                                className="flex items-center gap-0.5 px-1.5 py-0.5 rounded text-[8px] font-semibold border transition-colors bg-obsidian-warning/10 text-obsidian-warning border-obsidian-warning/20 hover:bg-obsidian-warning/25 disabled:opacity-30 active:scale-95"
                                                                                                title="Clear Task (Retry)"
                                                                                            >
                                                                                                {taskActionLoading === `${ti.task_id}-clear` ? <Loader2 className="w-2.5 h-2.5 animate-spin" /> : <RotateCcw className="w-2.5 h-2.5" />}
                                                                                                Clear
                                                                                            </button>

                                                                                            {/* Mark Success */}
                                                                                            <button
                                                                                                onClick={(e) => { e.stopPropagation(); handleTaskAction(selectedDag!, expandedRun!, ti.task_id, 'mark_success', { map_index: ti.map_index }); }}
                                                                                                disabled={!!taskActionLoading}
                                                                                                className="flex items-center gap-0.5 px-1.5 py-0.5 rounded text-[8px] font-semibold border transition-colors bg-obsidian-success/10 text-obsidian-success border-obsidian-success/20 hover:bg-obsidian-success/25 disabled:opacity-30 active:scale-95"
                                                                                                title="Mark Success"
                                                                                            >
                                                                                                {taskActionLoading === `${ti.task_id}-mark_success` ? <Loader2 className="w-2.5 h-2.5 animate-spin" /> : <CheckCircle2 className="w-2.5 h-2.5" />}
                                                                                                Success
                                                                                            </button>

                                                                                            {/* Mark Failed */}
                                                                                            <button
                                                                                                onClick={(e) => { e.stopPropagation(); handleTaskAction(selectedDag!, expandedRun!, ti.task_id, 'mark_failed', { map_index: ti.map_index }); }}
                                                                                                disabled={!!taskActionLoading}
                                                                                                className="flex items-center gap-0.5 px-1.5 py-0.5 rounded text-[8px] font-semibold border transition-colors bg-obsidian-danger/10 text-obsidian-danger border-obsidian-danger/20 hover:bg-obsidian-danger/25 disabled:opacity-30 active:scale-95"
                                                                                                title="Mark Failed"
                                                                                            >
                                                                                                {taskActionLoading === `${ti.task_id}-mark_failed` ? <Loader2 className="w-2.5 h-2.5 animate-spin" /> : <XCircle className="w-2.5 h-2.5" />}
                                                                                                Failed
                                                                                            </button>

                                                                                            {/* View Logs button */}
                                                                                            <button
                                                                                                onClick={(e) => { e.stopPropagation(); void toggleTaskLogs(ti); }}
                                                                                                className={clsx(
                                                                                                    "flex items-center gap-0.5 px-1.5 py-0.5 rounded text-[8px] font-semibold border transition-colors active:scale-95",
                                                                                                    viewingTaskLogs === taskInstanceKey(ti.task_id, ti.map_index)
                                                                                                        ? "bg-obsidian-info/20 text-obsidian-info border-obsidian-info/30"
                                                                                                        : "bg-obsidian-panel/30 text-obsidian-muted hover:text-foreground hover:bg-obsidian-panel/50 border-obsidian-border/50"
                                                                                                )}
                                                                                            >
                                                                                                <AlignLeft className="w-2.5 h-2.5" />
                                                                                                {viewingTaskLogs === taskInstanceKey(ti.task_id, ti.map_index) ? 'Hide Logs' : 'Logs'}
                                                                                            </button>

                                                                                            {/* View Code button */}
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
                                                                                                        const res = await apiFetch('/api/orchestrator/notebooks', {
                                                                                                            method: 'POST',
                                                                                                            headers: { 'Content-Type': 'application/json' },
                                                                                                            body: JSON.stringify({ dag_id: selectedDag, task_id: ti.task_id }),
                                                                                                        });
                                                                                                        const data = await parseApiResponse<{ task_id: string; filename: string; content: unknown }>(res);
                                                                                                        const contentStr = typeof data.content === 'string'
                                                                                                            ? data.content
                                                                                                            : JSON.stringify(data.content ?? '', null, 2);
                                                                                                        setTaskCode({ ...data, task_id: ti.task_id, content: contentStr });
                                                                                                    } catch (err) {
                                                                                                        const msg = err instanceof Error ? err.message : String(err);
                                                                                                        setTaskCode({ task_id: ti.task_id, filename: 'Error', content: `# Failed to load source: ${msg}` });
                                                                                                    } finally {
                                                                                                        setTaskCodeLoading(false);
                                                                                                    }
                                                                                                }}
                                                                                                className={clsx(
                                                                                                    "flex items-center gap-0.5 px-1.5 py-0.5 rounded text-[8px] font-semibold border transition-colors active:scale-95",
                                                                                                    viewingTaskCode === ti.task_id
                                                                                                        ? "bg-obsidian-info/20 text-obsidian-info border-obsidian-info/30"
                                                                                                        : "bg-obsidian-panel/30 text-obsidian-muted hover:text-foreground hover:bg-obsidian-panel/50 border-obsidian-border/50"
                                                                                                )}
                                                                                            >
                                                                                                <Code2 className="w-2.5 h-2.5" />
                                                                                                {viewingTaskCode === ti.task_id ? 'Hide Code' : 'Code'}
                                                                                            </button>
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
                                                                                                        <Loader2 className="w-3.5 h-3.5 animate-spin mr-1.5" /> Loading...
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
                                                                                                                        : <Maximize2 className="w-3.5 h-3.5" />
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
                                                                                        {/* Inline Task Logs Viewer */}
                                                                                        {viewingTaskLogs === taskInstanceKey(ti.task_id, ti.map_index) && (
                                                                                            <div className="mt-2 rounded border border-obsidian-border overflow-hidden bg-[#111113]">
                                                                                                {taskLogsLoading ? (
                                                                                                    <div className="flex items-center justify-center py-4 text-obsidian-muted text-[10px]">
                                                                                                        <Loader2 className="w-3.5 h-3.5 animate-spin mr-1.5" /> Fetching logs...
                                                                                                    </div>
                                                                                                ) : taskLogs ? (
                                                                                                    <div className="flex flex-col">
                                                                                                        <div className="bg-obsidian-panel border-b border-obsidian-border flex items-center justify-between px-2 py-1 shrink-0">
                                                                                                            <div className="flex items-center gap-2">
                                                                                                                <AlignLeft className="w-3 h-3 text-obsidian-info" />
                                                                                                                <span className="text-foreground font-mono font-medium text-[9px]">Log Output (Try {taskLogs.try_number})</span>
                                                                                                            </div>
                                                                                                            <div className="flex items-center gap-2">
                                                                                                                <label className="flex items-center gap-1 text-[8px] text-obsidian-muted uppercase tracking-wider">
                                                                                                                    <input
                                                                                                                        type="checkbox"
                                                                                                                        checked={logAutoFollow}
                                                                                                                        onChange={(e) => setLogAutoFollow(e.target.checked)}
                                                                                                                        className="w-3 h-3 accent-cyan-500"
                                                                                                                    />
                                                                                                                    follow
                                                                                                                </label>
                                                                                                                <span className="text-[8px] text-obsidian-muted uppercase tracking-wider">Try</span>
                                                                                                                <select
                                                                                                                    className="bg-[#0f1013] border border-white/10 rounded px-1.5 py-0.5 text-[9px] text-foreground"
                                                                                                                    value={selectedLogTry[taskInstanceKey(ti.task_id, ti.map_index)] || taskLogs.try_number}
                                                                                                                    onChange={(e) => void changeTaskLogTry(ti, Number(e.target.value))}
                                                                                                                >
                                                                                                                    {(taskTryNumbers[taskInstanceKey(ti.task_id, ti.map_index)]?.length
                                                                                                                        ? taskTryNumbers[taskInstanceKey(ti.task_id, ti.map_index)]
                                                                                                                        : [taskLogs.try_number]
                                                                                                                    ).map((num) => (
                                                                                                                        <option key={`${ti.task_id}-try-${num}`} value={num}>
                                                                                                                            {num}
                                                                                                                        </option>
                                                                                                                    ))}
                                                                                                                </select>
                                                                                                                <button
                                                                                                                    onClick={() => downloadTextFile(`${selectedDag}-${expandedRun}-${ti.task_id}-${ti.map_index}-try${taskLogs.try_number}.log`, taskLogs.content)}
                                                                                                                    className="inline-flex items-center gap-1 px-1.5 py-0.5 rounded border border-white/10 text-[8px] text-obsidian-muted hover:text-foreground hover:bg-white/[0.05]"
                                                                                                                    title="Download log"
                                                                                                                >
                                                                                                                    <Download className="w-2.5 h-2.5" />
                                                                                                                    save
                                                                                                                </button>
                                                                                                            </div>
                                                                                                        </div>
                                                                                                        <div ref={logsScrollRef} className="overflow-auto max-h-[400px] p-2 bg-[#0c0c0e]">
                                                                                                            <pre className="text-[10px] text-obsidian-muted font-mono whitespace-pre-wrap break-all leading-[14px]">
                                                                                                                {taskLogs.content}
                                                                                                            </pre>
                                                                                                        </div>
                                                                                                    </div>
                                                                                                ) : null}
                                                                                            </div>
                                                                                        )}
                                                                                        {/* Failed task: prominent error box */}
                                                                                        {isFailed && (
                                                                                            <div className="mt-1.5 px-2 py-1.5 bg-obsidian-danger/10 rounded border border-obsidian-danger/20 text-[10px] transition-all active:scale-95">
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
                                            <div className="px-3 py-1.5 bg-obsidian-panel border-b border-obsidian-border flex items-center justify-between shrink-0 transition-all active:scale-95">
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

                {showTriggerDialog && (
                    <div
                        className="fixed inset-0 z-[120] bg-black/70 backdrop-blur-sm flex items-center justify-center"
                        onClick={() => setShowTriggerDialog(false)}
                    >
                        <div
                            className="w-[640px] max-w-[95vw] border border-white/10 rounded-lg bg-[#0f1013] shadow-2xl"
                            onClick={(e) => e.stopPropagation()}
                        >
                            <div className="px-4 py-3 border-b border-white/10 flex items-center justify-between">
                                <div className="flex items-center gap-2">
                                    <Code2 className="w-4 h-4 text-obsidian-info" />
                                    <span className="text-[12px] font-semibold text-foreground">Trigger DAG with JSON conf</span>
                                </div>
                                <button
                                    onClick={() => setShowTriggerDialog(false)}
                                    className="text-obsidian-muted hover:text-foreground text-[12px]"
                                >
                                    ✕
                                </button>
                            </div>
                            <div className="p-4 space-y-3">
                                <div className="text-[10px] text-obsidian-muted font-mono">
                                    DAG: {selectedDag || '—'}
                                </div>
                                <textarea
                                    value={triggerConfText}
                                    onChange={(e) => setTriggerConfText(e.target.value)}
                                    className="w-full h-56 bg-black/40 border border-white/10 rounded px-3 py-2 text-[11px] text-foreground font-mono outline-none focus:border-obsidian-info/50"
                                    spellCheck={false}
                                />
                                {triggerConfError && (
                                    <div className="text-[10px] text-obsidian-danger bg-obsidian-danger/10 border border-obsidian-danger/30 rounded px-2 py-1">
                                        {triggerConfError}
                                    </div>
                                )}
                            </div>
                            <div className="px-4 py-3 border-t border-white/10 flex items-center justify-end gap-2">
                                <button
                                    onClick={() => setShowTriggerDialog(false)}
                                    className="px-3 py-1.5 rounded border border-white/10 text-[11px] text-obsidian-muted hover:text-foreground hover:bg-white/[0.04]"
                                >
                                    Cancel
                                </button>
                                <button
                                    onClick={() => void submitTriggerWithConf()}
                                    className="px-3 py-1.5 rounded border border-obsidian-success/30 text-[11px] text-obsidian-success hover:bg-obsidian-success/15"
                                >
                                    Trigger Run
                                </button>
                            </div>
                        </div>
                    </div>
                )}
            </main >
        </div >
    );
}
