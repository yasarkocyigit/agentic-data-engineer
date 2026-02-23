
import React, { useState, useEffect, useRef, useCallback, useMemo } from 'react';
import { createPortal } from 'react-dom';
import { Sidebar } from '@/components/Sidebar';
import clsx from 'clsx';
import Editor, { useMonaco, type OnMount } from '@monaco-editor/react';
import { format as formatSqlText } from 'sql-formatter';
import {
    Play, Square, Save, Download, RefreshCw, Filter, Columns, ChevronDown, ChevronUp, X, ChevronRight,
    Database, FileCode, XCircle, Plus, Loader2, Clock, Zap, Hash, Terminal, LayoutPanelLeft,
    ArrowUpDown, ArrowUp, ArrowDown, Copy, ClipboardList, FileJson, FileSpreadsheet,
    Code2, Timer, History, Trash2, Search, MoreVertical, SlidersHorizontal, Layers, Layout, Table as TableIcon,
    Eye, BarChart3, Shield, Wrench, GitBranch, Activity, Key, Puzzle, ListTree, Info, Check, RotateCcw, AlertTriangle
} from 'lucide-react';

// ─── Types ───
type SortDirection = 'asc' | 'desc' | null;
type SortConfig = { column: string; direction: SortDirection };
type BottomPanel = 'results' | 'history' | 'queries' | 'saved' | 'properties';
type Engine = 'trino' | 'spark' | 'postgres';

type SavedQuery = {
    id: string;
    name: string;
    sql: string;
    engine: Engine;
    savedAt: number;
};

type QueryHistoryItem = {
    id: string;
    query: string;
    engine: Engine;
    timestamp: number;
    duration: number;
    rowCount: number | null;
    status: 'success' | 'error';
    error?: string;
};

type QueryTab = {
    id: string;
    name: string;
    query: string;
    active: boolean;
    results?: {
        columns: string[];
        data: any[];
        stats?: any;
    } | null;
    resultSets?: {
        name: string;
        columns: string[];
        data: any[];
        stats?: any;
    }[];
    activeResultSetIndex?: number;
    pgSessionId: string;
    error?: string | null;
    executionTime?: number;
};

// ─── Schema Explorer Types ───
type SchemaNode = {
    id: string;
    name: string;
    type: 'database' | 'catalog' | 'schema' | 'folder' | 'table' | 'view' | 'materialized_view' | 'function' | 'procedure' | 'column' | 'sequence' | 'type' | 'extension' | 'index' | 'constraint' | 'trigger';
    children?: SchemaNode[];
    isOpen?: boolean;
    isLoading?: boolean;
    dataType?: string; // For columns
    sqlName?: string;
    routineOid?: number;
    routineSignature?: string;
};

type QueryApiResponse = {
    columns?: string[];
    data?: any[];
    stats?: any;
    message?: string;
    detail?: string;
    error?: string;
    notices?: string[];
    warnings?: string[];
};

type ConfirmDialogState = {
    title: string;
    message: string;
    confirmLabel?: string;
    cancelLabel?: string;
    danger?: boolean;
    onConfirm: () => void;
};

type TrinoQueryStage = {
    stageId: string;
    state: string;
    depth: number;
    taskCount: number;
    completedTaskCount: number;
    raw: any;
};

type StatementExecution = {
    statement: string;
    columns: string[];
    data: any[];
    stats?: any;
    message?: string | null;
    label: string;
};

const TRANSACTION_CONTROL_PATTERN = /\b(BEGIN|START\s+TRANSACTION|COMMIT|ROLLBACK|SAVEPOINT|RELEASE\s+SAVEPOINT)\b/i;
const SCHEMA_MUTATION_PATTERN = /\b(CREATE|ALTER|DROP|TRUNCATE|COMMENT\s+ON|RENAME)\b/i;
const ENGINE_ORDER: Engine[] = ['trino', 'postgres', 'spark'];
const ENGINE_LABELS: Record<Engine, string> = {
    trino: 'Trino',
    spark: 'Spark',
    postgres: 'PostgreSQL',
};

const normalizeEngine = (value: string | Engine | undefined | null): Engine => {
    if (value === 'postgres' || value === 'spark') return value;
    return 'trino';
};

const hasExplicitTransactionControl = (sql: string) => TRANSACTION_CONTROL_PATTERN.test(sql);
const isSchemaMutationStatement = (sql: string) => SCHEMA_MUTATION_PATTERN.test(sql);

const escapeSqlLiteral = (value: string) => value.replace(/'/g, "''");
const quoteTrinoIdentifier = (value: string) => `"${String(value).replace(/"/g, '""')}"`;
const quotePostgresIdentifier = (value: string) => `"${String(value).replace(/"/g, '""')}"`;
const readTrinoCatalogName = (row: any): string => {
    const value = row?.Catalog ?? row?.catalog ?? row?.CATALOG ?? row?.catalog_name ?? row?.CATALOG_NAME;
    return typeof value === 'string' ? value.trim() : '';
};
const loadMonacoSqlKeywords = async (monacoInstance: any): Promise<string[]> => {
    try {
        const sqlRegistration = monacoInstance?.languages?.getLanguages?.().find((lang: any) => lang?.id === 'sql');
        if (!sqlRegistration || typeof sqlRegistration.loader !== 'function') return [];
        const loaded = await sqlRegistration.loader();
        const rawKeywords = loaded?.language?.keywords;
        if (!Array.isArray(rawKeywords)) return [];
        return Array.from(
            new Set(
                rawKeywords
                    .map((item: any) => String(item).trim())
                    .filter((item: string) => item.length > 0)
            )
        );
    } catch {
        return [];
    }
};

const makeStableId = (prefix: string): string => {
    if (typeof crypto !== 'undefined' && typeof crypto.randomUUID === 'function') {
        return `${prefix}_${crypto.randomUUID()}`;
    }
    return `${prefix}_${Date.now()}_${Math.random().toString(36).slice(2, 10)}`;
};

const readDollarTag = (sql: string, index: number): string | null => {
    if (sql[index] !== '$') return null;
    let end = index + 1;
    while (end < sql.length && /[A-Za-z0-9_]/.test(sql[end])) end += 1;
    if (end < sql.length && sql[end] === '$') return sql.slice(index, end + 1);
    return null;
};

const splitSqlStatements = (sql: string): string[] => {
    const statements: string[] = [];
    let current = '';
    let inSingleQuote = false;
    let inDoubleQuote = false;
    let inLineComment = false;
    let inBlockComment = false;
    let dollarTag: string | null = null;

    for (let i = 0; i < sql.length; i += 1) {
        const ch = sql[i];
        const next = sql[i + 1];

        if (inLineComment) {
            current += ch;
            if (ch === '\n') inLineComment = false;
            continue;
        }

        if (inBlockComment) {
            current += ch;
            if (ch === '*' && next === '/') {
                current += '/';
                i += 1;
                inBlockComment = false;
            }
            continue;
        }

        if (dollarTag) {
            if (sql.startsWith(dollarTag, i)) {
                current += dollarTag;
                i += dollarTag.length - 1;
                dollarTag = null;
                continue;
            }
            current += ch;
            continue;
        }

        if (inSingleQuote) {
            current += ch;
            if (ch === '\'' && next === '\'') {
                current += '\'';
                i += 1;
                continue;
            }
            if (ch === '\'') inSingleQuote = false;
            continue;
        }

        if (inDoubleQuote) {
            current += ch;
            if (ch === '"' && next === '"') {
                current += '"';
                i += 1;
                continue;
            }
            if (ch === '"') inDoubleQuote = false;
            continue;
        }

        if (ch === '-' && next === '-') {
            current += '--';
            i += 1;
            inLineComment = true;
            continue;
        }

        if (ch === '/' && next === '*') {
            current += '/*';
            i += 1;
            inBlockComment = true;
            continue;
        }

        if (ch === '\'') {
            inSingleQuote = true;
            current += ch;
            continue;
        }

        if (ch === '"') {
            inDoubleQuote = true;
            current += ch;
            continue;
        }

        if (ch === '$') {
            const tag = readDollarTag(sql, i);
            if (tag) {
                dollarTag = tag;
                current += tag;
                i += tag.length - 1;
                continue;
            }
        }

        if (ch === ';') {
            const trimmed = current.trim();
            if (trimmed) statements.push(trimmed);
            current = '';
            continue;
        }

        current += ch;
    }

    const tail = current.trim();
    if (tail) statements.push(tail);
    return statements;
};

// ─── Schema Tree Node Component ───
const SchemaTreeNode = ({
    node,
    parentNames,
    onToggle,
    onDoubleClick,
    onContextMenu,
    depth = 0
}: {
    node: SchemaNode;
    parentNames: string[];
    onToggle: (id: string, type: string, names: string[]) => void;
    onDoubleClick: (name: string) => void;
    onContextMenu: (e: React.MouseEvent, node: SchemaNode, parentNames: string[]) => void;
    depth?: number;
}) => {
    // Functions/Procedures don't have columns, so they aren't expandable like tables.
    const isExpandable = !['column', 'function', 'procedure', 'sequence', 'type', 'extension', 'index', 'constraint', 'trigger'].includes(node.type);

    // Icon mapping
    const getIcon = () => {
        switch (node.type) {
            case 'database':
            case 'catalog': return <Database className="w-3.5 h-3.5 text-white/40 drop-shadow-sm" />;
            case 'schema': return <Layers className="w-3.5 h-3.5 text-white/40 drop-shadow-sm" />;
            case 'folder': return <Layout className="w-3.5 h-3.5 text-white/40 drop-shadow-sm" />;
            case 'table': return <TableIcon className="w-3 h-3 text-white/40 drop-shadow-sm" />;
            case 'view':
            case 'materialized_view': return <Eye className="w-3 h-3 text-sky-400/70 drop-shadow-sm" />;
            case 'function':
            case 'procedure': return <Code2 className="w-3 h-3 text-emerald-400/70 drop-shadow-sm" />;
            case 'sequence': return <Hash className="w-3 h-3 text-violet-400/70 drop-shadow-sm" />;
            case 'type': return <Puzzle className="w-3 h-3 text-amber-400/70 drop-shadow-sm" />;
            case 'extension': return <Wrench className="w-3 h-3 text-rose-400/70 drop-shadow-sm" />;
            case 'index': return <Key className="w-3 h-3 text-sky-400/60" />;
            case 'constraint': return <GitBranch className="w-3 h-3 text-amber-400/60" />;
            case 'trigger': return <Zap className="w-3 h-3 text-rose-400/60" />;
            case 'column': return <Columns className="w-3 h-3 text-white/20" />;
        }
    };

    return (
        <div className="flex flex-col select-none">
            <div
                className="flex items-center gap-1.5 py-1 px-1 hover:bg-white/5 rounded transition-colors cursor-pointer group"
                style={{ paddingLeft: `${depth * 12 + 4}px` }}
                onClick={(e) => {
                    e.stopPropagation();
                    if (isExpandable) onToggle(node.id, node.type, parentNames);
                }}
                onDoubleClick={(e) => {
                    e.stopPropagation();
                    if (['table', 'view', 'materialized_view', 'column', 'sequence', 'function', 'procedure'].includes(node.type)) {
                        onDoubleClick(node.sqlName || node.name);
                    }
                }}
                onContextMenu={(e) => {
                    e.preventDefault();
                    e.stopPropagation();
                    onContextMenu(e, node, parentNames);
                }}
            >
                {/* Expand Indicator */}
                <div className="w-4 h-4 flex items-center justify-center shrink-0">
                    {isExpandable ? (
                        node.isLoading ? <Loader2 className="w-3 h-3 animate-spin text-obsidian-muted" /> :
                            node.isOpen ? <ChevronDown className="w-3 h-3 text-obsidian-muted" /> :
                                <ChevronRight className="w-3 h-3 text-obsidian-muted opacity-0 group-hover:opacity-100 transition-opacity" />
                    ) : null}
                </div>

                {/* Icon */}
                <div className="shrink-0">{getIcon()}</div>

                {/* Name */}
                <span className={clsx(
                    "truncate text-[11px]",
                    node.type === 'column' ? "text-white/40 font-mono" : "text-white/80"
                )}>
                    {node.name}
                </span>

                {/* Data Type for Columns */}
                {node.type === 'column' && node.dataType && (
                    <span className="ml-auto text-[9px] text-white/30 font-mono pl-2 truncate shrink-0">
                        {node.dataType}
                    </span>
                )}
            </div>

            {/* Children */}
            {node.isOpen && node.children && node.children.length > 0 && (
                <div className="flex flex-col">
                    {node.children.map(child => (
                        <SchemaTreeNode
                            key={child.id}
                            node={child}
                            parentNames={[...parentNames, child.name]}
                            onToggle={onToggle}
                            onDoubleClick={onDoubleClick}
                            onContextMenu={onContextMenu}
                            depth={depth + 1}
                        />
                    ))}
                </div>
            )}
        </div>
    );
};

// ─── Main Component ───
export default function DataExplorer() {
    const [tabs, setTabs] = useState<QueryTab[]>([
        { id: '1', name: 'console.sql', query: '', active: true, results: null, pgSessionId: makeStableId('pg') }
    ]);
    const activeTab = tabs.find(t => t.active) || tabs[0];
    const [isExecuting, setIsExecuting] = useState(false);
    const [engine, setEngine] = useState<Engine>('trino');
    const [pgDatabase, setPgDatabase] = useState<string>('controldb');
    const [trinoCatalog, setTrinoCatalog] = useState<string>('');
    const [trinoSchema, setTrinoSchema] = useState<string>('');
    const [sparkDatabase, setSparkDatabase] = useState<string>('default');
    const [showTrinoSettings, setShowTrinoSettings] = useState(false);
    const [trinoRole, setTrinoRole] = useState('');
    const [trinoClientTagsInput, setTrinoClientTagsInput] = useState('');
    const [trinoSessionPropsText, setTrinoSessionPropsText] = useState('');
    const monaco = useMonaco();
    const abortRef = useRef<AbortController | null>(null);
    const executeRef = useRef<() => void>(() => { });
    const editorRef = useRef<any>(null);
    const tabsRef = useRef<QueryTab[]>([]);

    // ─── Enhanced State ───
    const [sortConfig, setSortConfig] = useState<SortConfig>({ column: '', direction: null });
    const [selectedCell, setSelectedCell] = useState<{ row: number; col: string } | null>(null);
    const [columnWidths, setColumnWidths] = useState<Record<string, number>>({});
    const [resizingCol, setResizingCol] = useState<string | null>(null);
    const [resizeStart, setResizeStart] = useState<{ x: number; width: number }>({ x: 0, width: 0 });
    const [bottomPanel, setBottomPanel] = useState<BottomPanel>('results');
    const [queryHistory, setQueryHistory] = useState<QueryHistoryItem[]>([]);
    const [historyFilter, setHistoryFilter] = useState('');
    const [liveTimer, setLiveTimer] = useState(0);
    const liveTimerRef = useRef<NodeJS.Timeout | null>(null);
    const [showExportMenu, setShowExportMenu] = useState(false);
    const [copiedCell, setCopiedCell] = useState(false);
    const [resultFilter, setResultFilter] = useState('');
    const [showFilterMenu, setShowFilterMenu] = useState(false);
    const [rowLimit, setRowLimit] = useState<number | null>(500);
    const [showLimitMenu, setShowLimitMenu] = useState(false);
    const [limitMenuPos, setLimitMenuPos] = useState({ top: 0, right: 0 });
    const [showDbMenu, setShowDbMenu] = useState(false);
    const [dbMenuPos, setDbMenuPos] = useState({ top: 0, left: 0 });

    // ─── Context Menu State ───
    const [contextMenu, setContextMenu] = useState<{
        x: number; y: number;
        node: SchemaNode;
        parentNames: string[];
    } | null>(null);

    // ─── Properties Panel State ───
    type PropertiesTab = 'columns' | 'indexes' | 'constraints' | 'triggers' | 'ddl' | 'stats';
    const [propertiesTab, setPropertiesTab] = useState<PropertiesTab>('columns');
    const [selectedObject, setSelectedObject] = useState<{ schema: string; name: string; database: string; type: string } | null>(null);
    const [propertiesData, setPropertiesData] = useState<Record<string, any>>({});
    const [propertiesLoading, setPropertiesLoading] = useState(false);
    const [propertiesError, setPropertiesError] = useState<string | null>(null);

    // ─── Virtual Scroll State ───
    const ROW_HEIGHT = 28;
    const OVERSCAN = 10;
    const tableContainerRef = useRef<HTMLDivElement>(null);
    const [scrollTop, setScrollTop] = useState(0);
    const [viewportHeight, setViewportHeight] = useState(400);

    // Resizable Split Pane State (Editor / Results)
    const [editorHeight, setEditorHeight] = useState(400);
    const [isDragging, setIsDragging] = useState(false);
    const containerRef = useRef<HTMLDivElement>(null);

    // Resizable Schema Explorer State
    const [sidebarWidth, setSidebarWidth] = useState(260);
    const [isDraggingSidebar, setIsDraggingSidebar] = useState(false);
    const [showSchemaExplorer, setShowSchemaExplorer] = useState(true);
    const [schemaTree, setSchemaTree] = useState<SchemaNode[]>([]);
    const [isSchemaLoading, setIsSchemaLoading] = useState(false);
    const [schemaLoadError, setSchemaLoadError] = useState<string | null>(null);
    const [sqlLanguageKeywords, setSqlLanguageKeywords] = useState<string[]>([]);

    // ─── Trino Queries & Cluster Info State ───
    const [trinoQueries, setTrinoQueries] = useState<any[]>([]);
    const [trinoQueriesLoading, setTrinoQueriesLoading] = useState(false);
    const [queriesPanelError, setQueriesPanelError] = useState<string | null>(null);
    const [trinoInfo, setTrinoInfo] = useState<{ version?: string; uptime?: string; state?: string } | null>(null);
    const [selectedTrinoQueryId, setSelectedTrinoQueryId] = useState<string | null>(null);
    const [trinoQueryDetail, setTrinoQueryDetail] = useState<any | null>(null);
    const [trinoQueryDetailLoading, setTrinoQueryDetailLoading] = useState(false);
    const [selectedTrinoStageId, setSelectedTrinoStageId] = useState<string | null>(null);

    // ─── PostgreSQL Queries & Cluster Info State ───
    const [pgQueries, setPgQueries] = useState<any[]>([]);
    const [pgQueriesLoading, setPgQueriesLoading] = useState(false);
    const [pgInfo, setPgInfo] = useState<{ version?: string; uptime?: string; activeConnections?: number; databaseSize?: string; state?: string } | null>(null);
    const [sparkInfo, setSparkInfo] = useState<{ version?: string; master?: string; appName?: string; defaultDatabase?: string; state?: string } | null>(null);

    // ─── Saved Queries State ───
    const [savedQueries, setSavedQueries] = useState<SavedQuery[]>([]);
    const [showSaveDialog, setShowSaveDialog] = useState(false);
    const [saveQueryName, setSaveQueryName] = useState('');
    const [confirmDialog, setConfirmDialog] = useState<ConfirmDialogState | null>(null);
    const [currentExecution, setCurrentExecution] = useState<{
        engine: Engine;
        queryTag: string;
        sessionId?: string;
    } | null>(null);
    const cycleEngine = useCallback(() => {
        setEngine(prev => {
            const idx = ENGINE_ORDER.indexOf(prev);
            return ENGINE_ORDER[(idx + 1) % ENGINE_ORDER.length];
        });
    }, []);

    // ─── Load query history from localStorage ───
    useEffect(() => {
        try {
            const saved = localStorage.getItem('openclaw-query-history');
            if (saved) setQueryHistory(JSON.parse(saved));
        } catch { }
    }, []);

    // ─── Save query history to localStorage ───
    useEffect(() => {
        if (queryHistory.length > 0) {
            try {
                localStorage.setItem('openclaw-query-history', JSON.stringify(queryHistory.slice(0, 50)));
            } catch { }
        }
    }, [queryHistory]);

    useEffect(() => {
        tabsRef.current = tabs;
    }, [tabs]);

    useEffect(() => {
        return () => {
            for (const tab of tabsRef.current) {
                fetch(`/api/postgres/sessions/${encodeURIComponent(tab.pgSessionId)}`, { method: 'DELETE', keepalive: true }).catch(() => { });
            }
        };
    }, []);

    // Monaco/provider cleanup can emit benign object rejections; suppress them for UI stability.
    useEffect(() => {
        const onUnhandled = (event: PromiseRejectionEvent) => {
            const reason = event.reason as { type?: string; msg?: string } | null;
            if (reason?.type === 'cancelation' && reason?.msg === 'operation is manually canceled') {
                event.preventDefault();
            }
        };
        window.addEventListener('unhandledrejection', onUnhandled);
        return () => window.removeEventListener('unhandledrejection', onUnhandled);
    }, []);

    // ─── Load saved queries from localStorage ───
    useEffect(() => {
        try {
            const saved = localStorage.getItem('openclaw-saved-queries');
            if (saved) setSavedQueries(JSON.parse(saved));
        } catch { }
    }, []);

    // ─── Save saved queries to localStorage ───
    useEffect(() => {
        try {
            localStorage.setItem('openclaw-saved-queries', JSON.stringify(savedQueries));
        } catch { }
    }, [savedQueries]);

    useEffect(() => {
        try {
            const raw = localStorage.getItem('openclaw-trino-settings');
            if (!raw) return;
            const parsed = JSON.parse(raw);
            if (typeof parsed.role === 'string') setTrinoRole(parsed.role);
            if (typeof parsed.clientTags === 'string') setTrinoClientTagsInput(parsed.clientTags);
            if (typeof parsed.sessionProps === 'string') setTrinoSessionPropsText(parsed.sessionProps);
        } catch { }
    }, []);

    useEffect(() => {
        try {
            localStorage.setItem('openclaw-trino-settings', JSON.stringify({
                role: trinoRole,
                clientTags: trinoClientTagsInput,
                sessionProps: trinoSessionPropsText,
            }));
        } catch { }
    }, [trinoRole, trinoClientTagsInput, trinoSessionPropsText]);

    const saveCurrentQuery = useCallback(() => {
        if (!activeTab.query.trim() || !saveQueryName.trim()) return;
        const newQuery: SavedQuery = {
            id: Date.now().toString(),
            name: saveQueryName.trim(),
            sql: activeTab.query,
            engine,
            savedAt: Date.now(),
        };
        setSavedQueries(prev => [newQuery, ...prev]);
        setShowSaveDialog(false);
        setSaveQueryName('');
    }, [activeTab.query, saveQueryName, engine]);

    const deleteSavedQuery = useCallback((id: string) => {
        setSavedQueries(prev => prev.filter(q => q.id !== id));
    }, []);

    // ─── Live Timer ───
    useEffect(() => {
        if (isExecuting) {
            setLiveTimer(0);
            liveTimerRef.current = setInterval(() => {
                setLiveTimer(prev => prev + 100);
            }, 100);
        } else {
            if (liveTimerRef.current) {
                clearInterval(liveTimerRef.current);
                liveTimerRef.current = null;
            }
        }
        return () => {
            if (liveTimerRef.current) clearInterval(liveTimerRef.current);
        };
    }, [isExecuting]);

    const filteredData = useMemo(() => {
        const rows = activeTab.results?.data || [];
        const needle = resultFilter.trim().toLowerCase();
        if (!needle) return rows;
        return rows.filter((row: any) =>
            Object.values(row || {}).some(value =>
                value !== null && value !== undefined && String(value).toLowerCase().includes(needle)
            )
        );
    }, [activeTab.results?.data, resultFilter]);

    // ─── Sorted Data ───
    const sortedData = useMemo(() => {
        if (!sortConfig.column || !sortConfig.direction) {
            return filteredData;
        }
        const sorted = [...filteredData].sort((a, b) => {
            const aVal = a[sortConfig.column];
            const bVal = b[sortConfig.column];
            if (aVal === null || aVal === undefined) return 1;
            if (bVal === null || bVal === undefined) return -1;
            if (typeof aVal === 'number' && typeof bVal === 'number') {
                return sortConfig.direction === 'asc' ? aVal - bVal : bVal - aVal;
            }
            const strA = String(aVal).toLowerCase();
            const strB = String(bVal).toLowerCase();
            if (sortConfig.direction === 'asc') return strA.localeCompare(strB);
            return strB.localeCompare(strA);
        });
        return sorted;
    }, [filteredData, sortConfig]);

    const parsedTrinoClientTags = useMemo(() => {
        const tags = trinoClientTagsInput
            .split(',')
            .map(tag => tag.trim())
            .filter(Boolean);
        return tags.length > 0 ? tags : undefined;
    }, [trinoClientTagsInput]);

    const parsedTrinoSessionProperties = useMemo(() => {
        const lines = trinoSessionPropsText
            .split('\n')
            .map(line => line.trim())
            .filter(Boolean);
        if (lines.length === 0) return undefined;

        const props: Record<string, string> = {};
        for (const line of lines) {
            const eqIdx = line.indexOf('=');
            if (eqIdx <= 0 || eqIdx >= line.length - 1) continue;
            const key = line.slice(0, eqIdx).trim();
            const value = line.slice(eqIdx + 1).trim();
            if (!key || !value) continue;
            props[key] = value;
        }

        return Object.keys(props).length > 0 ? props : undefined;
    }, [trinoSessionPropsText]);

    const trinoDetailStages = useMemo<TrinoQueryStage[]>(() => {
        if (!trinoQueryDetail) return [];
        const rootStage = trinoQueryDetail.outputStage || trinoQueryDetail.stageInfo || trinoQueryDetail?.queryStats?.rootStage;
        if (!rootStage) return [];

        const rows: TrinoQueryStage[] = [];
        const stack: Array<{ stage: any; depth: number }> = [{ stage: rootStage, depth: 0 }];
        while (stack.length > 0) {
            const item = stack.pop();
            if (!item) continue;
            const stage = item.stage;
            const depth = item.depth;
            const stageId = String(stage?.stageId ?? stage?.stage_id ?? stage?.id ?? `stage_${rows.length + 1}`);
            const tasks = Array.isArray(stage?.tasks) ? stage.tasks : [];
            const completedTaskCount = tasks.filter((task: any) => {
                const st = String(task?.taskStatus?.state ?? task?.state ?? '').toUpperCase();
                return st === 'FINISHED';
            }).length;

            rows.push({
                stageId,
                state: String(stage?.state ?? stage?.stageState ?? 'UNKNOWN'),
                depth,
                taskCount: tasks.length,
                completedTaskCount,
                raw: stage,
            });

            const subStages = Array.isArray(stage?.subStages) ? stage.subStages : [];
            for (let i = subStages.length - 1; i >= 0; i -= 1) {
                stack.push({ stage: subStages[i], depth: depth + 1 });
            }
        }
        return rows;
    }, [trinoQueryDetail]);

    const selectedTrinoStage = useMemo(() => {
        if (!selectedTrinoStageId) return trinoDetailStages[0] || null;
        return trinoDetailStages.find(stage => stage.stageId === selectedTrinoStageId) || trinoDetailStages[0] || null;
    }, [selectedTrinoStageId, trinoDetailStages]);

    const selectedTrinoStageTasks = useMemo(() => {
        if (!selectedTrinoStage) return [];
        const tasks = Array.isArray(selectedTrinoStage.raw?.tasks) ? selectedTrinoStage.raw.tasks : [];
        return tasks;
    }, [selectedTrinoStage]);

    const selectedTrinoStageOperators = useMemo(() => {
        if (!selectedTrinoStage) return [];
        const stageStats = selectedTrinoStage.raw?.stageStats || selectedTrinoStage.raw?.stats || {};
        const operators = Array.isArray(stageStats?.operatorSummaries)
            ? stageStats.operatorSummaries
            : Array.isArray(selectedTrinoStage.raw?.operatorSummaries)
                ? selectedTrinoStage.raw.operatorSummaries
                : [];
        return operators;
    }, [selectedTrinoStage]);

    // ─── Resize Logic (Editor / Results) ───
    useEffect(() => {
        const handleMouseMove = (e: MouseEvent) => {
            if (!isDragging || !containerRef.current) return;
            e.preventDefault();
            const containerTop = containerRef.current.getBoundingClientRect().top;
            const newHeight = e.clientY - containerTop;
            const containerH = containerRef.current.clientHeight;
            if (newHeight > 100 && newHeight < containerH - 100) {
                setEditorHeight(newHeight);
            }
        };

        const handleMouseUp = () => {
            if (isDragging) {
                setIsDragging(false);
                document.body.style.cursor = 'default';
            }
        };

        if (isDragging) {
            window.addEventListener('mousemove', handleMouseMove);
            window.addEventListener('mouseup', handleMouseUp);
            document.body.style.cursor = 'row-resize';
        }

        return () => {
            window.removeEventListener('mousemove', handleMouseMove);
            window.removeEventListener('mouseup', handleMouseUp);
        };
    }, [isDragging]);

    // ─── Resize Logic (Schema Sidebar) ───
    useEffect(() => {
        const handleMouseMove = (e: MouseEvent) => {
            if (!isDraggingSidebar) return;
            e.preventDefault();
            // Sidebar component Activity Bar is 52px wide
            const newWidth = e.clientX - 52;
            if (newWidth > 150 && newWidth < 800) {
                setSidebarWidth(newWidth);
            }
        };

        const handleMouseUp = () => {
            if (isDraggingSidebar) {
                setIsDraggingSidebar(false);
                document.body.style.cursor = 'default';
            }
        };

        if (isDraggingSidebar) {
            window.addEventListener('mousemove', handleMouseMove);
            window.addEventListener('mouseup', handleMouseUp);
            document.body.style.cursor = 'col-resize';
        }

        return () => {
            window.removeEventListener('mousemove', handleMouseMove);
            window.removeEventListener('mouseup', handleMouseUp);
        };
    }, [isDraggingSidebar]);

    // ─── Column Resize Logic ───
    useEffect(() => {
        if (!resizingCol) return;
        const handleMouseMove = (e: MouseEvent) => {
            const diff = e.clientX - resizeStart.x;
            const newWidth = Math.max(60, resizeStart.width + diff);
            setColumnWidths(prev => ({ ...prev, [resizingCol!]: newWidth }));
        };
        const handleMouseUp = () => {
            setResizingCol(null);
            document.body.style.cursor = 'default';
        };
        window.addEventListener('mousemove', handleMouseMove);
        window.addEventListener('mouseup', handleMouseUp);
        document.body.style.cursor = 'col-resize';
        return () => {
            window.removeEventListener('mousemove', handleMouseMove);
            window.removeEventListener('mouseup', handleMouseUp);
        };
    }, [resizingCol, resizeStart]);

    // ─── Actions ───
    const insertIntoEditor = (text: string) => {
        if (!editorRef.current) return;
        const editor = editorRef.current;
        const position = editor.getPosition();
        editor.executeEdits('', [{
            range: { startLineNumber: position.lineNumber, startColumn: position.column, endLineNumber: position.lineNumber, endColumn: position.column },
            text: text + ' '
        }]);
        editor.focus();
    };

    const setActiveTab = (id: string) => {
        setTabs(tabs.map(t => ({ ...t, active: t.id === id })));
        setSortConfig({ column: '', direction: null });
        setSelectedCell(null);
        setResultFilter('');
    };

    const closePgSession = useCallback(async (sessionId: string) => {
        try {
            await fetch(`/api/postgres/sessions/${encodeURIComponent(sessionId)}`, { method: 'DELETE' });
        } catch { }
    }, []);

    const closeTab = (e: React.MouseEvent, id: string) => {
        e.stopPropagation();
        if (tabs.length > 1) {
            const tabToClose = tabs.find(t => t.id === id);
            if (tabToClose) closePgSession(tabToClose.pgSessionId);

            const newTabs = tabs.filter(t => t.id !== id);
            setTabs(newTabs);
            if (activeTab.id === id) {
                setTabs(newTabs.map((t, i) => ({ ...t, active: i === newTabs.length - 1 })));
            }
        }
    };

    const addTab = () => {
        const newId = (Math.max(...tabs.map(t => parseInt(t.id))) + 1).toString();
        const newTab: QueryTab = {
            id: newId,
            name: `query_${newId}.sql`,
            query: '',
            active: true,
            pgSessionId: makeStableId('pg')
        };
        setTabs([...tabs.map(t => ({ ...t, active: false })), newTab]);
    };

    const cancelQuery = async () => {
        if (abortRef.current) {
            abortRef.current.abort();
            abortRef.current = null;
        }

        const executing = currentExecution;
        if (executing) {
            try {
                if (executing.engine === 'trino') {
                    const res = await fetch('/api/trino/queries');
                    const data = await parseApiResponse(res, 'Failed to fetch Trino queries for cancellation');
                    const matched = (data.queries || []).find((q: any) => q?.source === `openclaw:${executing.queryTag}`);
                    if (matched?.queryId) {
                        const killRes = await fetch(`/api/trino/queries/${matched.queryId}`, { method: 'DELETE' });
                        await parseApiResponse(killRes, `Failed to cancel Trino query ${matched.queryId}`);
                    }
                } else if (executing.engine === 'spark') {
                    // Spark query cancellation is engine/deployment specific.
                    // Request abortion above already stops waiting on the client side.
                } else {
                    const res = await fetch(`/api/postgres/queries?database=${encodeURIComponent(pgDatabase)}`);
                    const data = await parseApiResponse(res, 'Failed to fetch PostgreSQL queries for cancellation');
                    const matched = (data.queries || []).find((q: any) => q?.queryTag === executing.queryTag);
                    if (matched?.pid) {
                        const killRes = await fetch(`/api/postgres/queries/${matched.pid}?database=${encodeURIComponent(pgDatabase)}`, { method: 'DELETE' });
                        await parseApiResponse(killRes, `Failed to cancel PostgreSQL PID ${matched.pid}`);
                    }
                }
            } catch (err) {
                console.error('Failed to cancel running query', err);
            }
        }

        setCurrentExecution(null);
        setIsExecuting(false);
    };

    const setActiveResultSet = useCallback((index: number) => {
        setTabs(prev => prev.map(t => {
            if (!t.active || !t.resultSets || !t.resultSets[index]) return t;
            return {
                ...t,
                activeResultSetIndex: index,
                results: {
                    columns: t.resultSets[index].columns,
                    data: t.resultSets[index].data,
                    stats: t.resultSets[index].stats,
                }
            };
        }));
        setSortConfig({ column: '', direction: null });
        setSelectedCell(null);
        setResultFilter('');
    }, []);

    const updateQuery = useCallback((val: string | undefined) => {
        setTabs(prev => prev.map(t => t.active ? { ...t, query: val || '' } : t));
    }, []);

    const parseApiResponse = useCallback(async (res: Response, fallbackMessage: string) => {
        const payload = await res.json().catch(() => ({} as Record<string, any>));
        if (!res.ok) {
            const detail =
                (typeof payload?.detail === 'string' && payload.detail) ||
                (typeof payload?.error === 'string' && payload.error) ||
                `${fallbackMessage} (HTTP ${res.status})`;
            throw new Error(detail);
        }
        return payload;
    }, []);

    const requestConfirmation = useCallback((dialog: ConfirmDialogState) => {
        setConfirmDialog(dialog);
    }, []);

    const runConfirmedAction = useCallback(() => {
        if (!confirmDialog) return;
        const action = confirmDialog.onConfirm;
        setConfirmDialog(null);
        action();
    }, [confirmDialog]);

    const toggleSort = (column: string) => {
        setSortConfig(prev => {
            if (prev.column === column) {
                if (prev.direction === 'asc') return { column, direction: 'desc' };
                if (prev.direction === 'desc') return { column: '', direction: null };
            }
            return { column, direction: 'asc' };
        });
    };

    const handleCellClick = (row: number, col: string) => {
        setSelectedCell({ row, col });
    };

    const copyToClipboard = (text: string) => {
        navigator.clipboard.writeText(text).then(() => {
            setCopiedCell(true);
            setTimeout(() => setCopiedCell(false), 1500);
        });
    };

    const copyCellValue = () => {
        if (!selectedCell || !activeTab.results) return;
        const value = sortedData[selectedCell.row]?.[selectedCell.col];
        copyToClipboard(value === null ? 'NULL' : String(value));
    };

    const copyRowAsJSON = () => {
        if (!selectedCell || !activeTab.results) return;
        const row = sortedData[selectedCell.row];
        copyToClipboard(JSON.stringify(row, null, 2));
    };

    const startColumnResize = (e: React.MouseEvent, col: string) => {
        e.preventDefault();
        e.stopPropagation();
        const currentWidth = columnWidths[col] || 150;
        setResizingCol(col);
        setResizeStart({ x: e.clientX, width: currentWidth });
    };

    // ─── Schema Context Menu Actions ───
    const handleSchemaContextMenu = useCallback((e: React.MouseEvent, node: SchemaNode, parentNames: string[]) => {
        setContextMenu({ x: e.clientX, y: e.clientY, node, parentNames });
    }, []);

    const closeContextMenu = useCallback(() => setContextMenu(null), []);

    // Close context menu on click anywhere
    useEffect(() => {
        if (!contextMenu) return;
        const handler = () => setContextMenu(null);
        window.addEventListener('click', handler);
        return () => window.removeEventListener('click', handler);
    }, [contextMenu]);

    useEffect(() => {
        if (!showFilterMenu) return;
        const handler = () => setShowFilterMenu(false);
        window.addEventListener('click', handler);
        return () => window.removeEventListener('click', handler);
    }, [showFilterMenu]);

    useEffect(() => {
        if (bottomPanel !== 'results') setShowFilterMenu(false);
    }, [bottomPanel]);

    const contextMenuActions = useMemo(() => {
        if (!contextMenu) return [];
        const { node, parentNames } = contextMenu;
        const isPostgres = engine === 'postgres';
        const isSpark = engine === 'spark';

        const buildTrinoFqn = (names: string[]) => names.map(quoteTrinoIdentifier).join('.');

        if (isSpark) {
            if (['table', 'view', 'materialized_view'].includes(node.type)) {
                const dbName = parentNames[0] || sparkDatabase;
                const objectName = node.sqlName || parentNames[parentNames.length - 1];
                const fqn = `${dbName}.${objectName}`;
                return [
                    { label: 'Preview Top 100', icon: <Eye className="w-3.5 h-3.5" />, action: () => { updateQuery(`SELECT * FROM ${fqn} LIMIT 100`); setTimeout(() => executeRef.current(), 100); } },
                    { label: 'Describe Table', icon: <Info className="w-3.5 h-3.5" />, action: () => { updateQuery(`DESCRIBE TABLE ${fqn}`); setTimeout(() => executeRef.current(), 100); } },
                    { label: 'Row Count', icon: <Columns className="w-3.5 h-3.5" />, action: () => { updateQuery(`SELECT count(*) AS total_rows FROM ${fqn}`); setTimeout(() => executeRef.current(), 100); } },
                    { divider: true },
                    { label: 'Insert Name', icon: <Plus className="w-3.5 h-3.5" />, action: () => { insertIntoEditor(fqn); } },
                ];
            }
            if (node.type === 'database' || node.type === 'schema') {
                const dbName = node.name;
                return [
                    { label: 'Show Tables', icon: <Layers className="w-3.5 h-3.5" />, action: () => { updateQuery(`SHOW TABLES IN ${dbName}`); setTimeout(() => executeRef.current(), 100); } },
                    { label: 'Set Current DB', icon: <Database className="w-3.5 h-3.5" />, action: () => { setSparkDatabase(dbName); } },
                ];
            }
            if (node.type === 'column') {
                return [
                    { label: 'Insert Name', icon: <Plus className="w-3.5 h-3.5" />, action: () => { insertIntoEditor(node.name); } },
                    { label: 'Copy Name', icon: <Copy className="w-3.5 h-3.5" />, action: () => { navigator.clipboard.writeText(node.name); } },
                ];
            }
            return [];
        }

        if (['table', 'view', 'materialized_view', 'function', 'procedure'].includes(node.type)) {
            const isRoutine = ['function', 'procedure'].includes(node.type);
            const trinoFqn = buildTrinoFqn(parentNames);

            // For Postgres, hierarchy is [database, schema, folder, object]
            const pgDb = parentNames[0] || 'controldb';
            const pgSchema = parentNames.length > 1 ? parentNames[1] : 'public';
            const pgObject = node.sqlName || parentNames[parentNames.length - 1];
            const pgRoutineOid = typeof node.routineOid === 'number' ? node.routineOid : undefined;
            const quotedPgSchema = quotePostgresIdentifier(pgSchema);
            const quotedPgObject = quotePostgresIdentifier(pgObject);
            const pgFQN = `${quotedPgSchema}.${quotedPgObject}`;
            const escapedPgSchema = escapeSqlLiteral(pgSchema);
            const escapedPgObject = escapeSqlLiteral(pgObject);

            if (isPostgres) {
                let actions: any[] = [
                    {
                        label: 'Script as CREATE', icon: <FileCode className="w-3.5 h-3.5" />, action: async () => {
                            try {
                                const res = await fetch('/api/postgres', {
                                    method: 'POST',
                                    headers: { 'Content-Type': 'application/json' },
                                    body: JSON.stringify({
                                        action: 'explore',
                                        type: 'ddl',
                                        schema: pgSchema,
                                        table: pgObject,
                                        database: pgDb,
                                        ...(isRoutine && pgRoutineOid ? { routine_oid: pgRoutineOid } : {})
                                    })
                                });
                                const data = await parseApiResponse(res, 'Failed to load PostgreSQL DDL');
                                if (data.data?.definition) {
                                    updateQuery(data.data.definition);
                                }
                            } catch (err) {
                                const message = err instanceof Error ? err.message : 'Failed to load PostgreSQL DDL';
                                setTabs(prev => prev.map(t => t.active ? { ...t, error: message } : t));
                                setBottomPanel('results');
                            }
                        }
                    }
                ];

                if (!isRoutine) {
                    actions = [
                        { label: 'Preview Top 100', icon: <Eye className="w-3.5 h-3.5" />, action: () => { updateQuery(`SELECT * FROM ${pgFQN} LIMIT 100`); setTimeout(() => executeRef.current(), 100); } },
                        ...actions,
                        { label: 'Table Size', icon: <BarChart3 className="w-3.5 h-3.5" />, action: () => { updateQuery(`SELECT pg_size_pretty(pg_total_relation_size('${pgFQN}')) AS total_size,\n       pg_size_pretty(pg_relation_size('${pgFQN}')) AS table_size,\n       pg_size_pretty(pg_indexes_size('${pgFQN}')) AS index_size`); setTimeout(() => executeRef.current(), 100); } },
                        { label: 'Row Count', icon: <Columns className="w-3.5 h-3.5" />, action: () => { updateQuery(`SELECT count(*) AS total_rows FROM ${pgFQN}`); setTimeout(() => executeRef.current(), 100); } },
                        { label: 'Show Grants', icon: <Shield className="w-3.5 h-3.5" />, action: () => { updateQuery(`SELECT grantee, privilege_type, is_grantable\nFROM information_schema.role_table_grants\nWHERE table_schema = '${escapedPgSchema}' AND table_name = '${escapedPgObject}'`); setTimeout(() => executeRef.current(), 100); } },
                        { label: 'Show Indexes', icon: <Layers className="w-3.5 h-3.5" />, action: () => { updateQuery(`SELECT indexname, indexdef\nFROM pg_indexes\nWHERE schemaname = '${escapedPgSchema}' AND tablename = '${escapedPgObject}'`); setTimeout(() => executeRef.current(), 100); } },
                        { divider: true },
                        { label: 'Show Triggers', icon: <Activity className="w-3.5 h-3.5" />, action: () => { updateQuery(`SELECT trigger_name, event_manipulation, action_timing, action_statement\nFROM information_schema.triggers\nWHERE event_object_schema = '${escapedPgSchema}' AND event_object_table = '${escapedPgObject}'`); setTimeout(() => executeRef.current(), 100); } },
                        { label: 'Vacuum Analyze', icon: <Zap className="w-3.5 h-3.5" />, action: () => { updateQuery(`VACUUM ANALYZE ${pgFQN}`); setTimeout(() => executeRef.current(), 100); } },
                    ];
                } else {
                    actions = [
                        node.type === 'procedure'
                            ? { label: 'Execute Procedure', icon: <Play className="w-3.5 h-3.5" />, action: () => { updateQuery(`CALL ${pgFQN}()`); } }
                            : { label: 'Execute Function', icon: <Play className="w-3.5 h-3.5" />, action: () => { updateQuery(`SELECT * FROM ${pgFQN}()`); } },
                        ...actions
                    ];
                }

                actions.push(
                    { divider: true },
                    { label: 'Insert Name', icon: <Plus className="w-3.5 h-3.5" />, action: () => { insertIntoEditor(pgFQN); } },
                    {
                        label: `Drop ${node.type}`, icon: <Trash2 className="w-3.5 h-3.5" />, danger: true, action: () => {
                            requestConfirmation({
                                title: `Drop ${node.type}`,
                                message: `Drop ${node.type} ${pgFQN}? This cannot be undone.`,
                                confirmLabel: `Drop ${node.type}`,
                                danger: true,
                                onConfirm: () => {
                                    updateQuery(`DROP ${node.type.toUpperCase().replace('_', ' ')} ${pgFQN}`);
                                    setTimeout(() => executeRef.current(), 100);
                                }
                            });
                        }
                    }
                );
                return actions;
            } else {
                // Trino
                const trinoType = node.type.toUpperCase().replace('_', ' ');
                const trinoCatalogName = parentNames[0] || trinoCatalog;
                const trinoSchemaName = parentNames[1] || trinoSchema;
                const trinoObjectName = parentNames[parentNames.length - 1];
                const isIcebergCatalog = trinoCatalogName.toLowerCase() === 'iceberg';
                const isDeltaLakeCatalog = ['deltalake', 'delta_lake', 'delta'].includes(trinoCatalogName.toLowerCase());
                const quotedCatalog = quoteTrinoIdentifier(trinoCatalogName);
                const quotedSchema = quoteTrinoIdentifier(trinoSchemaName);
                const escapedSchemaName = escapeSqlLiteral(trinoSchemaName);
                const escapedObjectName = escapeSqlLiteral(trinoObjectName);
                const trinoMetadataFqn = (suffix: string) =>
                    `${quotedCatalog}.${quotedSchema}.${quoteTrinoIdentifier(`${trinoObjectName}${suffix}`)}`;
                const storageLocationQuery = isIcebergCatalog
                    ? `SELECT value AS storage_location\nFROM ${trinoMetadataFqn('$properties')}\nWHERE key = 'location'`
                    : isDeltaLakeCatalog
                        ? `SELECT\n  COALESCE(regexp_replace(min("$path"), '/[^/]+$', ''), 'N/A (table has no data files yet)') AS storage_location,\n  min("$path") AS sample_file_path\nFROM ${trinoFqn}`
                        : null;
                let actions: any[] = [
                    { label: 'Script as CREATE', icon: <FileCode className="w-3.5 h-3.5" />, action: () => { updateQuery(`SHOW CREATE ${trinoType} ${trinoFqn}`); setTimeout(() => executeRef.current(), 100); } },
                ];

                if (!isRoutine) {
                    actions = [
                        { label: 'Preview Top 100', icon: <Eye className="w-3.5 h-3.5" />, action: () => { updateQuery(`SELECT * FROM ${trinoFqn} LIMIT 100`); setTimeout(() => executeRef.current(), 100); } },
                        ...actions,
                        { label: 'Show Stats', icon: <BarChart3 className="w-3.5 h-3.5" />, action: () => { updateQuery(`SHOW STATS FROM ${trinoFqn}`); setTimeout(() => executeRef.current(), 100); } },
                        { label: 'Row Count', icon: <Columns className="w-3.5 h-3.5" />, action: () => { updateQuery(`SELECT count(*) AS total_rows FROM ${trinoFqn}`); setTimeout(() => executeRef.current(), 100); } },
                        ...(node.type === 'table' && storageLocationQuery ? [
                            { label: 'Show Storage Location', icon: <Database className="w-3.5 h-3.5" />, action: () => { updateQuery(storageLocationQuery); setTimeout(() => executeRef.current(), 100); } },
                        ] : []),
                        { label: 'Show Grants', icon: <Shield className="w-3.5 h-3.5" />, action: () => { updateQuery(`SHOW GRANTS ON ${trinoType} ${trinoFqn}`); setTimeout(() => executeRef.current(), 100); } },
                        { label: 'Show Privileges (IS)', icon: <Shield className="w-3.5 h-3.5" />, action: () => { updateQuery(`SELECT *\nFROM ${quotedCatalog}.information_schema.table_privileges\nWHERE table_schema = '${escapedSchemaName}'\n  AND table_name = '${escapedObjectName}'\nORDER BY grantee`); setTimeout(() => executeRef.current(), 100); } },
                        ...(node.type === 'table' && isDeltaLakeCatalog ? [
                            { label: 'Show Table Info (Delta)', icon: <Info className="w-3.5 h-3.5" />, action: () => { updateQuery(`SELECT\n  table_catalog,\n  table_schema,\n  table_name,\n  table_type\nFROM ${quotedCatalog}.information_schema.tables\nWHERE table_schema = '${escapedSchemaName}'\n  AND table_name = '${escapedObjectName}'`); setTimeout(() => executeRef.current(), 100); } },
                            { label: 'Show Properties (Delta)', icon: <SlidersHorizontal className="w-3.5 h-3.5" />, action: () => { updateQuery(`SELECT key, value\nFROM ${trinoMetadataFqn('$properties')}\nORDER BY key`); setTimeout(() => executeRef.current(), 100); } },
                            { label: 'Show History (Delta)', icon: <History className="w-3.5 h-3.5" />, action: () => { updateQuery(`SELECT version, timestamp, operation, user_name, isolation_level, is_blind_append\nFROM ${trinoMetadataFqn('$history')}\nORDER BY version DESC`); setTimeout(() => executeRef.current(), 100); } },
                            { label: 'Describe Columns (Delta)', icon: <ClipboardList className="w-3.5 h-3.5" />, action: () => { updateQuery(`DESCRIBE ${trinoFqn}`); setTimeout(() => executeRef.current(), 100); } },
                        ] : []),
                        ...(node.type === 'table' && isIcebergCatalog ? [
                            { label: 'Show Partitions (Iceberg)', icon: <ListTree className="w-3.5 h-3.5" />, action: () => { updateQuery(`SELECT * FROM ${trinoMetadataFqn('$partitions')} LIMIT 200`); setTimeout(() => executeRef.current(), 100); } },
                            { label: 'Show Files (Iceberg)', icon: <FileSpreadsheet className="w-3.5 h-3.5" />, action: () => { updateQuery(`SELECT * FROM ${trinoMetadataFqn('$files')} LIMIT 200`); setTimeout(() => executeRef.current(), 100); } },
                            { label: 'Show Snapshots (Iceberg)', icon: <Clock className="w-3.5 h-3.5" />, action: () => { updateQuery(`SELECT * FROM ${trinoMetadataFqn('$snapshots')} ORDER BY committed_at DESC LIMIT 200`); setTimeout(() => executeRef.current(), 100); } },
                            { label: 'Show Manifests (Iceberg)', icon: <Layers className="w-3.5 h-3.5" />, action: () => { updateQuery(`SELECT * FROM ${trinoMetadataFqn('$manifests')} LIMIT 200`); setTimeout(() => executeRef.current(), 100); } },
                        ] : []),
                        { divider: true },
                        { label: 'Analyze Table', icon: <Zap className="w-3.5 h-3.5" />, action: () => { updateQuery(`ANALYZE ${trinoFqn}`); setTimeout(() => executeRef.current(), 100); } }
                    ];
                }

                actions.push(
                    { divider: true },
                    { label: 'Insert Name', icon: <Plus className="w-3.5 h-3.5" />, action: () => { insertIntoEditor(trinoFqn); } },
                    {
                        label: `Drop ${node.type}`, icon: <Trash2 className="w-3.5 h-3.5" />, danger: true, action: () => {
                            requestConfirmation({
                                title: `Drop ${node.type}`,
                                message: `Drop ${trinoType} ${trinoFqn}? This cannot be undone.`,
                                confirmLabel: `Drop ${node.type}`,
                                danger: true,
                                onConfirm: () => {
                                    updateQuery(`DROP ${trinoType} ${trinoFqn}`);
                                    setTimeout(() => executeRef.current(), 100);
                                }
                            });
                        }
                    }
                );
                return actions;
            }
        }

        if (node.type === 'schema') {
            if (isPostgres) {
                const schemaName = parentNames[parentNames.length - 1] || node.name;
                const quotedSchemaName = quotePostgresIdentifier(schemaName);
                const escapedSchemaName = escapeSqlLiteral(schemaName);
                return [
                    { label: 'Create Table...', icon: <Plus className="w-3.5 h-3.5" />, action: () => { updateQuery(`CREATE TABLE ${quotedSchemaName}.new_table (\n    id SERIAL PRIMARY KEY,\n    name VARCHAR(255),\n    created_at TIMESTAMP DEFAULT NOW()\n)`); } },
                    { label: 'Create View...', icon: <Eye className="w-3.5 h-3.5" />, action: () => { updateQuery(`CREATE VIEW ${quotedSchemaName}.new_view AS\nSELECT * FROM ${quotedSchemaName}.`); } },
                    { label: 'Create Materialized View...', icon: <Eye className="w-3.5 h-3.5" />, action: () => { updateQuery(`CREATE MATERIALIZED VIEW ${quotedSchemaName}.mv_name AS\nSELECT *\nFROM ${quotedSchemaName}.source_table\nWITH DATA`); } },
                    { label: 'Create Function...', icon: <Code2 className="w-3.5 h-3.5" />, action: () => { updateQuery(`CREATE OR REPLACE FUNCTION ${quotedSchemaName}.my_function(param1 INTEGER, param2 TEXT)\nRETURNS TABLE(id INTEGER, result TEXT) AS $$\nBEGIN\n    RETURN QUERY\n    SELECT 1 AS id, 'hello' AS result;\nEND;\n$$ LANGUAGE plpgsql`); } },
                    { label: 'Create Procedure...', icon: <Wrench className="w-3.5 h-3.5" />, action: () => { updateQuery(`CREATE OR REPLACE PROCEDURE ${quotedSchemaName}.my_procedure(param1 INTEGER)\nLANGUAGE plpgsql AS $$\nBEGIN\n    -- procedure logic here\n    RAISE NOTICE 'Processing %', param1;\nEND;\n$$`); } },
                    { label: 'Create Index...', icon: <Wrench className="w-3.5 h-3.5" />, action: () => { updateQuery(`CREATE INDEX idx_name\nON ${quotedSchemaName}.table_name (column_name)`); } },
                    { label: 'Create Trigger...', icon: <Activity className="w-3.5 h-3.5" />, action: () => { updateQuery(`CREATE OR REPLACE FUNCTION ${quotedSchemaName}.trigger_fn()\nRETURNS TRIGGER AS $$\nBEGIN\n    -- trigger logic\n    RETURN NEW;\nEND;\n$$ LANGUAGE plpgsql;\n\nCREATE TRIGGER trg_name\nBEFORE INSERT ON ${quotedSchemaName}.table_name\nFOR EACH ROW EXECUTE FUNCTION ${quotedSchemaName}.trigger_fn()`); } },
                    { label: 'Create Type...', icon: <Code2 className="w-3.5 h-3.5" />, action: () => { updateQuery(`CREATE TYPE ${quotedSchemaName}.status_type AS ENUM (\n    'active', 'inactive', 'pending'\n)`); } },
                    { divider: true },
                    { label: 'Show Tables', icon: <Layers className="w-3.5 h-3.5" />, action: () => { updateQuery(`SELECT table_name, table_type\nFROM information_schema.tables\nWHERE table_schema = '${escapedSchemaName}'\nORDER BY table_name`); setTimeout(() => executeRef.current(), 100); } },
                    { label: 'Show Views', icon: <Eye className="w-3.5 h-3.5" />, action: () => { updateQuery(`SELECT table_name AS view_name, view_definition\nFROM information_schema.views\nWHERE table_schema = '${escapedSchemaName}'\nORDER BY table_name`); setTimeout(() => executeRef.current(), 100); } },
                    { label: 'Show Functions', icon: <Code2 className="w-3.5 h-3.5" />, action: () => { updateQuery(`SELECT routine_name, routine_type, data_type AS return_type\nFROM information_schema.routines\nWHERE routine_schema = '${escapedSchemaName}'\nORDER BY routine_name`); setTimeout(() => executeRef.current(), 100); } },
                    { label: 'Show Sequences', icon: <Hash className="w-3.5 h-3.5" />, action: () => { updateQuery(`SELECT sequence_name, data_type, start_value, increment\nFROM information_schema.sequences\nWHERE sequence_schema = '${escapedSchemaName}'\nORDER BY sequence_name`); setTimeout(() => executeRef.current(), 100); } },
                    { divider: true },
                    {
                        label: 'Drop Schema', icon: <Trash2 className="w-3.5 h-3.5" />, danger: true, action: () => {
                            requestConfirmation({
                                title: 'Drop schema',
                                message: `Drop schema ${schemaName}? This cannot be undone.`,
                                confirmLabel: 'Drop schema',
                                danger: true,
                                onConfirm: () => {
                                    updateQuery(`DROP SCHEMA ${quotedSchemaName} CASCADE`);
                                    setTimeout(() => executeRef.current(), 100);
                                }
                            });
                        }
                    },
                ];
            } else {
                const catalogName = parentNames[0];
                const schemaName = parentNames[1] || node.name;
                const fqn = `${quoteTrinoIdentifier(catalogName)}.${quoteTrinoIdentifier(schemaName)}`;
                return [
                    { label: 'Create Table...', icon: <Plus className="w-3.5 h-3.5" />, action: () => { updateQuery(`CREATE TABLE ${fqn}.new_table (\n    id BIGINT,\n    name VARCHAR,\n    created_at TIMESTAMP\n)\nWITH (\n    format = 'PARQUET'\n)`); } },
                    { label: 'Create View...', icon: <Eye className="w-3.5 h-3.5" />, action: () => { updateQuery(`CREATE VIEW ${fqn}.new_view AS\nSELECT * FROM ${fqn}.`); } },
                    { divider: true },
                    {
                        label: 'Drop Schema', icon: <Trash2 className="w-3.5 h-3.5" />, danger: true, action: () => {
                            requestConfirmation({
                                title: 'Drop schema',
                                message: `Drop schema ${fqn}? This cannot be undone.`,
                                confirmLabel: 'Drop schema',
                                danger: true,
                                onConfirm: () => {
                                    updateQuery(`DROP SCHEMA ${fqn}`);
                                    setTimeout(() => executeRef.current(), 100);
                                }
                            });
                        }
                    },
                ];
            }
        }

        if (node.type === 'catalog' || node.type === 'database') {
            const name = node.name;
            if (isPostgres) {
                return [
                    { label: 'Create Schema...', icon: <Plus className="w-3.5 h-3.5" />, action: () => { updateQuery(`CREATE SCHEMA new_schema`); } },
                    { label: 'Show Schemas', icon: <Layers className="w-3.5 h-3.5" />, action: () => { updateQuery(`SELECT schema_name FROM information_schema.schemata\nWHERE schema_name NOT IN ('pg_toast','pg_catalog','information_schema')\nORDER BY schema_name`); setTimeout(() => executeRef.current(), 100); } },
                    { label: 'Show Extensions', icon: <Code2 className="w-3.5 h-3.5" />, action: () => { updateQuery(`SELECT extname, extversion FROM pg_extension ORDER BY extname`); setTimeout(() => executeRef.current(), 100); } },
                    { label: 'Show Settings', icon: <SlidersHorizontal className="w-3.5 h-3.5" />, action: () => { updateQuery(`SHOW ALL`); setTimeout(() => executeRef.current(), 100); } },
                    { label: 'Active Queries', icon: <Search className="w-3.5 h-3.5" />, action: () => { updateQuery(`SELECT pid, state, query, query_start, usename\nFROM pg_stat_activity\nWHERE state IS NOT NULL AND query NOT LIKE '%pg_stat_activity%'\nORDER BY query_start DESC`); setTimeout(() => executeRef.current(), 100); } },
                    { label: 'DB Size', icon: <BarChart3 className="w-3.5 h-3.5" />, action: () => { updateQuery(`SELECT pg_size_pretty(pg_database_size(current_database())) AS database_size`); setTimeout(() => executeRef.current(), 100); } },
                    { divider: true },
                    { label: 'Show Roles', icon: <Shield className="w-3.5 h-3.5" />, action: () => { updateQuery(`SELECT rolname, rolsuper, rolcreatedb, rolcreaterole, rolcanlogin\nFROM pg_roles\nWHERE rolname NOT LIKE 'pg_%'\nORDER BY rolname`); setTimeout(() => executeRef.current(), 100); } },
                    { label: 'Show Tablespaces', icon: <Database className="w-3.5 h-3.5" />, action: () => { updateQuery(`SELECT spcname, pg_size_pretty(pg_tablespace_size(spcname)) AS size\nFROM pg_tablespace\nORDER BY spcname`); setTimeout(() => executeRef.current(), 100); } },
                ];
            } else {
                return [
                    { label: 'Create Schema...', icon: <Plus className="w-3.5 h-3.5" />, action: () => { updateQuery(`CREATE SCHEMA ${quoteTrinoIdentifier(name)}.new_schema`); } },
                    { label: 'Show Schemas', icon: <Layers className="w-3.5 h-3.5" />, action: () => { updateQuery(`SHOW SCHEMAS FROM ${quoteTrinoIdentifier(name)}`); setTimeout(() => executeRef.current(), 100); } },
                    { label: 'Show Functions', icon: <Code2 className="w-3.5 h-3.5" />, action: () => { updateQuery(`SHOW FUNCTIONS FROM ${quoteTrinoIdentifier(name)}`); setTimeout(() => executeRef.current(), 100); } },
                    { label: 'Show Session', icon: <SlidersHorizontal className="w-3.5 h-3.5" />, action: () => { updateQuery(`SHOW SESSION`); setTimeout(() => executeRef.current(), 100); } },
                ];
            }
        }

        if (node.type === 'column') {
            return [
                { label: 'Insert Name', icon: <Plus className="w-3.5 h-3.5" />, action: () => { insertIntoEditor(node.name); } },
                { label: 'Copy Name', icon: <Copy className="w-3.5 h-3.5" />, action: () => { navigator.clipboard.writeText(node.name); } },
            ];
        }

        return [];
    }, [contextMenu, engine, updateQuery, insertIntoEditor, parseApiResponse, requestConfirmation, trinoCatalog, trinoSchema, sparkDatabase]);

    // ─── Properties Panel Fetch ───
    const fetchProperties = useCallback(async (obj: { schema: string; name: string; database: string; type: string }, tab: string) => {
        if (engine !== 'postgres') return;
        setPropertiesLoading(true);
        setPropertiesError(null);
        try {
            const res = await fetch('/api/postgres', {
                method: 'POST',
                headers: { 'Content-Type': 'application/json' },
                body: JSON.stringify({ action: 'explore', type: tab, schema: obj.schema, table: obj.name, database: obj.database })
            });
            const data = await parseApiResponse(res, `Failed to load PostgreSQL ${tab}`);
            setPropertiesData(prev => ({ ...prev, [tab]: data.data }));
        } catch (err) {
            console.error('Failed to fetch properties:', err);
            setPropertiesData(prev => ({ ...prev, [tab]: [] }));
            setPropertiesError(err instanceof Error ? err.message : 'Failed to load object properties');
        } finally {
            setPropertiesLoading(false);
        }
    }, [engine, parseApiResponse]);

    // Trigger properties fetch when object or tab changes
    useEffect(() => {
        if (selectedObject && bottomPanel === 'properties') {
            fetchProperties(selectedObject, propertiesTab);
        }
    }, [selectedObject, propertiesTab, bottomPanel, fetchProperties]);

    // ─── Trino Queries Fetch ───
    const fetchTrinoQueries = useCallback(async () => {
        if (engine !== 'trino') return;
        setTrinoQueriesLoading(true);
        setQueriesPanelError(null);
        try {
            const res = await fetch('/api/trino/queries');
            const data = await parseApiResponse(res, 'Failed to load Trino queries');
            setTrinoQueries(data.queries || []);
        } catch (err) {
            console.error('Failed to load Trino queries', err);
            setTrinoQueries([]);
            setQueriesPanelError(err instanceof Error ? err.message : 'Failed to load Trino queries');
        } finally {
            setTrinoQueriesLoading(false);
        }
    }, [engine, parseApiResponse]);

    const fetchTrinoQueryDetail = useCallback(async (queryId: string) => {
        if (!queryId) return;
        setSelectedTrinoQueryId(queryId);
        setTrinoQueryDetailLoading(true);
        try {
            const res = await fetch(`/api/trino/queries/${encodeURIComponent(queryId)}`);
            const data = await parseApiResponse(res, `Failed to load query detail (${queryId})`);
            setTrinoQueryDetail(data);
            const stageId = String(data?.outputStage?.stageId ?? data?.stageInfo?.stageId ?? '');
            setSelectedTrinoStageId(stageId || null);
        } catch (err) {
            console.error('Failed to fetch Trino query detail', err);
            setTrinoQueryDetail(null);
            setSelectedTrinoStageId(null);
        } finally {
            setTrinoQueryDetailLoading(false);
        }
    }, [parseApiResponse]);

    const killTrinoQuery = useCallback(async (queryId: string) => {
        try {
            const res = await fetch(`/api/trino/queries/${queryId}`, { method: 'DELETE' });
            await parseApiResponse(res, `Failed to kill Trino query ${queryId}`);
            fetchTrinoQueries();
        } catch (err) {
            console.error('Failed to kill Trino query', err);
            setQueriesPanelError(err instanceof Error ? err.message : 'Failed to kill Trino query');
        }
    }, [fetchTrinoQueries, parseApiResponse]);

    // ─── PostgreSQL Queries Fetch ───
    const fetchPgQueries = useCallback(async () => {
        if (engine !== 'postgres') return;
        setPgQueriesLoading(true);
        setQueriesPanelError(null);
        try {
            const res = await fetch(`/api/postgres/queries?database=${encodeURIComponent(pgDatabase)}`);
            const data = await parseApiResponse(res, 'Failed to load PostgreSQL queries');
            setPgQueries(data.queries || []);
        } catch (err) {
            console.error('Failed to load PostgreSQL queries', err);
            setPgQueries([]);
            setQueriesPanelError(err instanceof Error ? err.message : 'Failed to load PostgreSQL queries');
        } finally {
            setPgQueriesLoading(false);
        }
    }, [engine, pgDatabase, parseApiResponse]);

    const killPgQuery = useCallback(async (pid: number) => {
        try {
            const res = await fetch(`/api/postgres/queries/${pid}?database=${encodeURIComponent(pgDatabase)}`, { method: 'DELETE' });
            await parseApiResponse(res, `Failed to cancel PostgreSQL PID ${pid}`);
            fetchPgQueries();
        } catch (err) {
            console.error('Failed to kill PostgreSQL query', err);
            setQueriesPanelError(err instanceof Error ? err.message : 'Failed to kill PostgreSQL query');
        }
    }, [fetchPgQueries, pgDatabase, parseApiResponse]);

    // Fetch cluster info on mount (engine-aware)
    useEffect(() => {
        if (engine === 'trino') {
            (async () => {
                try {
                    const res = await fetch('/api/trino/info');
                    const data = await parseApiResponse(res, 'Failed to load Trino cluster info');
                    setTrinoInfo({ version: data.nodeVersion?.version, uptime: data.uptime, state: data.state });
                } catch (err) {
                    console.error('Failed to load Trino info', err);
                    setTrinoInfo(null);
                }
            })();
        } else if (engine === 'spark') {
            (async () => {
                try {
                    const res = await fetch('/api/spark/info');
                    const data = await parseApiResponse(res, 'Failed to load Spark cluster info');
                    setSparkInfo({
                        version: data.version,
                        master: data.master,
                        appName: data.appName,
                        defaultDatabase: data.defaultDatabase,
                        state: data.state,
                    });
                } catch (err) {
                    console.error('Failed to load Spark info', err);
                    setSparkInfo(null);
                }
            })();
        } else {
            (async () => {
                try {
                    const res = await fetch('/api/postgres/info');
                    const data = await parseApiResponse(res, 'Failed to load PostgreSQL cluster info');
                    setPgInfo({ version: data.version, uptime: data.uptime, activeConnections: data.activeConnections, databaseSize: data.databaseSize, state: data.state });
                } catch (err) {
                    console.error('Failed to load PostgreSQL info', err);
                    setPgInfo(null);
                }
            })();
        }
    }, [engine, parseApiResponse]);

    // Auto-refresh queries when Queries tab is active (engine-aware)
    useEffect(() => {
        if (bottomPanel !== 'queries') return;
        if (engine === 'trino') {
            fetchTrinoQueries();
            const interval = setInterval(fetchTrinoQueries, 5000);
            return () => clearInterval(interval);
        }
        if (engine === 'postgres') {
            fetchPgQueries();
            const interval = setInterval(fetchPgQueries, 5000);
            return () => clearInterval(interval);
        }
        return undefined;
    }, [bottomPanel, engine, fetchTrinoQueries, fetchPgQueries]);

    useEffect(() => {
        if (engine !== 'trino') {
            setShowTrinoSettings(false);
            setSelectedTrinoQueryId(null);
            setTrinoQueryDetail(null);
            setSelectedTrinoStageId(null);
        }
    }, [engine]);

    const executeQuery = useCallback(async () => {
        let queryToRun = activeTab.query.trim();

        // Check if there's a highlighted selection in Monaco Editor
        if (editorRef.current) {
            const selection = editorRef.current.getSelection();
            if (selection && !selection.isEmpty()) {
                const model = editorRef.current.getModel();
                if (model) {
                    const selectedText = model.getValueInRange(selection).trim();
                    if (selectedText) {
                        queryToRun = selectedText;
                    }
                }
            }
        }

        if (!queryToRun || isExecuting) return;
        if (engine === 'trino' && !trinoCatalog) {
            setTabs(prev => prev.map(t => t.active ? {
                ...t,
                results: null,
                error: 'No Trino catalog selected. Please select a catalog from the database dropdown.'
            } : t));
            setBottomPanel('results');
            return;
        }

        const runAsSingleScript = engine === 'postgres' && hasExplicitTransactionControl(queryToRun);
        const statements = runAsSingleScript ? [queryToRun] : splitSqlStatements(queryToRun);
        if (statements.length === 0) return;

        if (abortRef.current) abortRef.current.abort();
        const controller = new AbortController();
        abortRef.current = controller;

        setIsExecuting(true);
        setBottomPanel('results');
        setSortConfig({ column: '', direction: null });
        setSelectedCell(null);
        setResultFilter('');
        const startTime = performance.now();
        setTabs(prev => prev.map(t => t.active ? { ...t, results: null, resultSets: undefined, activeResultSetIndex: undefined, error: null } : t));

        try {
            const apiUrl = engine === 'postgres'
                ? '/api/postgres'
                : engine === 'spark'
                    ? '/api/spark'
                    : '/api/trino';
            const executionResults: StatementExecution[] = [];
            const queryTag = makeStableId('q');
            setCurrentExecution({
                engine,
                queryTag,
                sessionId: engine === 'postgres' ? activeTab.pgSessionId : undefined,
            });

            for (let i = 0; i < statements.length; i += 1) {
                const statement = statements[i];
                const bodyPayload = engine === 'postgres'
                    ? {
                        action: 'query',
                        query: statement,
                        database: pgDatabase,
                        session_id: activeTab.pgSessionId,
                        query_tag: queryTag,
                        ...(rowLimit ? { limit: rowLimit } : {})
                    }
                    : engine === 'spark'
                        ? {
                            action: 'query',
                            query: statement,
                            database: sparkDatabase,
                            ...(rowLimit ? { limit: rowLimit } : {})
                        }
                    : {
                        query: statement,
                        ...(trinoCatalog ? { catalog: trinoCatalog } : {}),
                        ...(trinoSchema ? { schema: trinoSchema } : {}),
                        ...(trinoRole.trim() ? { role: trinoRole.trim() } : {}),
                        ...(parsedTrinoSessionProperties ? { session_properties: parsedTrinoSessionProperties } : {}),
                        ...(parsedTrinoClientTags ? { client_tags: parsedTrinoClientTags } : {}),
                        query_tag: queryTag,
                        ...(rowLimit ? { limit: rowLimit } : {})
                    };

                const response = await fetch(apiUrl, {
                    method: 'POST',
                    headers: { 'Content-Type': 'application/json' },
                    body: JSON.stringify(bodyPayload),
                    signal: controller.signal
                });
                const data = await response.json().catch(() => ({} as QueryApiResponse)) as QueryApiResponse;

                if (!response.ok) {
                    const rawError = data.detail || data.error || `HTTP ${response.status}`;
                    const prefix = statements.length > 1 ? `Statement ${i + 1} failed` : 'Query execution failed';
                    throw new Error(`${prefix}: ${rawError}`);
                }

                const columns = Array.isArray(data.columns) ? data.columns : [];
                const rows = Array.isArray(data.data) ? data.data : [];
                const infoMessages = [
                    ...(Array.isArray(data.notices) ? data.notices : []),
                    ...(Array.isArray(data.warnings) ? data.warnings : []),
                ];
                const noticeText = infoMessages.length > 0
                    ? `\n${infoMessages.join('\n')}`
                    : '';
                const successMessage = columns.length === 0
                    ? `${data.stats?.command || data.message || 'Query executed successfully'}${noticeText}`
                    : null;

                executionResults.push({
                    statement,
                    columns,
                    data: rows,
                    stats: data.stats,
                    message: successMessage,
                    label: statements.length > 1 ? `Result ${i + 1}` : 'Result'
                });
            }

            const endTime = performance.now();
            const duration = Math.round(endTime - startTime);
            const resultSets = executionResults.filter(item => item.columns.length > 0);
            const latestStats = resultSets.length > 0
                ? resultSets[resultSets.length - 1].stats
                : executionResults[executionResults.length - 1]?.stats;
            const statsWithMeta = {
                ...(latestStats || {}),
                statementCount: executionResults.length,
                resultSetCount: resultSets.length,
            };
            const resultSetPayloads = resultSets.map((set, index) => ({
                name: set.label || `Result ${index + 1}`,
                columns: set.columns,
                data: set.data,
                stats: { ...(set.stats || {}), statement: set.statement }
            }));
            const activeResultSetIndex = resultSetPayloads.length > 0 ? resultSetPayloads.length - 1 : undefined;

            const finalResults = (() => {
                if (resultSetPayloads.length > 0) {
                    const lastResultSet = resultSetPayloads[resultSetPayloads.length - 1];
                    return {
                        columns: lastResultSet.columns,
                        data: lastResultSet.data,
                        stats: statsWithMeta
                    };
                }

                const messages = executionResults.map((item, index) => ({
                    Statement: String(index + 1),
                    Result: item.message || 'Query executed successfully'
                }));

                if (messages.length === 1) {
                    return {
                        columns: ['Result'],
                        data: [{ Result: messages[0].Result }],
                        stats: statsWithMeta
                    };
                }

                return {
                    columns: ['Statement', 'Result'],
                    data: messages,
                    stats: statsWithMeta
                };
            })();

            setTabs(prev => prev.map(t => t.active ? {
                ...t,
                results: finalResults,
                resultSets: resultSetPayloads.length > 0 ? resultSetPayloads : undefined,
                activeResultSetIndex,
                executionTime: duration,
                error: null
            } : t));

            // Add to history
            const historyRowCount = resultSets.length > 0 ? resultSets[resultSets.length - 1].data.length : 0;
            setQueryHistory(prev => [{
                id: Date.now().toString(),
                query: queryToRun,
                engine,
                timestamp: Date.now(),
                duration,
                rowCount: historyRowCount,
                status: 'success' as const
            }, ...prev].slice(0, 50));

            if (executionResults.some(item => isSchemaMutationStatement(item.statement))) {
                window.dispatchEvent(new CustomEvent('openclaw:refresh-schema'));
            }

        } catch (err: any) {
            if (err.name === 'AbortError') return;
            const endTime = performance.now();
            const errorMsg = typeof err.message === 'string' ? err.message : JSON.stringify(err);
            setTabs(prev => prev.map(t => t.active ? {
                ...t,
                results: null,
                error: errorMsg
            } : t));

            setQueryHistory(prev => [{
                id: Date.now().toString(),
                query: queryToRun,
                engine,
                timestamp: Date.now(),
                duration: Math.round(endTime - startTime),
                rowCount: null,
                status: 'error' as const,
                error: errorMsg
            }, ...prev].slice(0, 50));
        } finally {
            setIsExecuting(false);
            setCurrentExecution(null);
            abortRef.current = null;
        }
    }, [
        activeTab.query,
        activeTab.pgSessionId,
        isExecuting,
        engine,
        rowLimit,
        pgDatabase,
        sparkDatabase,
        trinoCatalog,
        trinoSchema,
        trinoRole,
        parsedTrinoSessionProperties,
        parsedTrinoClientTags,
    ]);

    const executeExplain = useCallback(async () => {
        if (!activeTab.query.trim() || isExecuting) return;
        const cleanQuery = activeTab.query.trim().replace(/;$/, '');
        const explainQuery = engine === 'postgres'
            ? `EXPLAIN (ANALYZE, FORMAT JSON) ${cleanQuery}`
            : engine === 'spark'
                ? `EXPLAIN FORMATTED ${cleanQuery}`
                : `EXPLAIN ANALYZE ${cleanQuery}`;
        updateQuery(explainQuery);
        setTimeout(() => {
            executeRef.current();
        }, 50);
    }, [activeTab.query, isExecuting, engine]);

    const executePgTransactionCommand = useCallback(async (command: 'BEGIN' | 'COMMIT' | 'ROLLBACK') => {
        if (engine !== 'postgres' || isExecuting) return;

        if (abortRef.current) abortRef.current.abort();
        const controller = new AbortController();
        abortRef.current = controller;

        setIsExecuting(true);
        setBottomPanel('results');
        setSortConfig({ column: '', direction: null });
        setSelectedCell(null);
        setResultFilter('');
        const startTime = performance.now();

        try {
            const queryTag = makeStableId('q');
            setCurrentExecution({ engine: 'postgres', queryTag, sessionId: activeTab.pgSessionId });

            const response = await fetch('/api/postgres', {
                method: 'POST',
                headers: { 'Content-Type': 'application/json' },
                body: JSON.stringify({
                    action: 'query',
                    query: command,
                    database: pgDatabase,
                    session_id: activeTab.pgSessionId,
                    query_tag: queryTag
                }),
                signal: controller.signal
            });
            const data = await response.json().catch(() => ({} as QueryApiResponse)) as QueryApiResponse;
            if (!response.ok) {
                const rawError = data.detail || data.error || `HTTP ${response.status}`;
                throw new Error(`${command} failed: ${rawError}`);
            }

            const duration = Math.round(performance.now() - startTime);
            const statusText = data.stats?.command || `${command} executed`;
            setTabs(prev => prev.map(t => t.active ? {
                ...t,
                results: {
                    columns: ['Result'],
                    data: [{ Result: statusText }],
                    stats: data.stats
                },
                resultSets: undefined,
                activeResultSetIndex: undefined,
                executionTime: duration,
                error: null
            } : t));

            setQueryHistory(prev => [{
                id: Date.now().toString(),
                query: command,
                engine: 'postgres' as const,
                timestamp: Date.now(),
                duration,
                rowCount: 1,
                status: 'success' as const
            }, ...prev].slice(0, 50));
        } catch (err: any) {
            if (err.name === 'AbortError') return;
            const errorMsg = typeof err.message === 'string' ? err.message : JSON.stringify(err);
            setTabs(prev => prev.map(t => t.active ? {
                ...t,
                results: null,
                error: errorMsg
            } : t));
        } finally {
            setIsExecuting(false);
            setCurrentExecution(null);
            abortRef.current = null;
        }
    }, [engine, isExecuting, activeTab.pgSessionId, pgDatabase]);

    useEffect(() => {
        executeRef.current = executeQuery;
    }, [executeQuery]);

    // ─── Schema Explorer Data Fetching ───
    const fetchSchemaRoot = useCallback(async () => {
        setIsSchemaLoading(true);
        setSchemaLoadError(null);
        setSchemaTree([]);
        try {
            if (engine === 'postgres') {
                const res = await fetch('/api/postgres', {
                    method: 'POST',
                    headers: { 'Content-Type': 'application/json' },
                    body: JSON.stringify({ action: 'explore', type: 'databases' })
                });
                if (!res.ok) {
                    const err = await res.json().catch(() => ({}));
                    throw new Error(err?.detail || 'Failed to load PostgreSQL databases');
                }
                const data = await res.json();
                if (data.data) {
                    setSchemaTree(data.data.map((d: any) => ({
                        id: `pg_${d.name}`, name: d.name, type: 'database', isOpen: false, children: []
                    })));
                }
            } else if (engine === 'spark') {
                const res = await fetch('/api/spark', {
                    method: 'POST',
                    headers: { 'Content-Type': 'application/json' },
                    body: JSON.stringify({ action: 'explore', type: 'databases' })
                });
                if (!res.ok) {
                    const err = await res.json().catch(() => ({}));
                    throw new Error(err?.detail || 'Failed to load Spark databases');
                }
                const data = await res.json();
                if (data.data) {
                    const dbs = data.data.map((d: any) => ({
                        id: `spark_${d.name}`, name: d.name, type: 'database' as const, isOpen: false, children: []
                    }));
                    setSchemaTree(dbs);
                    if (dbs.length > 0) {
                        setSparkDatabase(prev => dbs.some((d: any) => d.name === prev) ? prev : dbs[0].name);
                    } else {
                        setSparkDatabase('default');
                    }
                }
            } else {
                // Trino - Show Catalogs
                const res = await fetch('/api/trino', {
                    method: 'POST',
                    headers: { 'Content-Type': 'application/json' },
                    body: JSON.stringify({ query: 'SHOW CATALOGS' })
                });
                if (!res.ok) {
                    const err = await res.json().catch(() => ({}));
                    throw new Error(err?.detail || 'Failed to load Trino catalogs');
                }
                const data = await res.json();
                if (data.data) {
                    const catalogs = data.data
                        .map((row: any) => readTrinoCatalogName(row))
                        .filter((name: string) => Boolean(name))
                        .map((name: string) => ({
                            id: `trino_${name}`, name, type: 'catalog', isOpen: false, children: []
                        }));
                    setSchemaTree(catalogs);
                    if (catalogs.length > 0) {
                        setTrinoCatalog(prev => catalogs.some((c: any) => c.name === prev) ? prev : catalogs[0].name);
                        setTrinoSchema('');
                    } else {
                        setTrinoCatalog('');
                        setTrinoSchema('');
                    }
                }
            }
        } catch (err) {
            console.error("Failed to load schema root", err);
            setSchemaTree([]);
            setSchemaLoadError(err instanceof Error ? err.message : 'Schema load failed');
        } finally {
            setIsSchemaLoading(false);
        }
    }, [engine]);

    useEffect(() => {
        fetchSchemaRoot();
    }, [fetchSchemaRoot]);

    useEffect(() => {
        const handleRefreshSchema = () => { fetchSchemaRoot(); };
        window.addEventListener('openclaw:refresh-schema', handleRefreshSchema);
        return () => window.removeEventListener('openclaw:refresh-schema', handleRefreshSchema);
    }, [fetchSchemaRoot]);

    const toggleSchemaNode = async (nodeId: string, nodeType: string, parentNames: string[]) => {
        // Recursive helper to update tree
        const updateNode = (nodes: SchemaNode[], id: string, updater: (n: SchemaNode) => SchemaNode): SchemaNode[] => {
            return nodes.map((n: SchemaNode) => {
                if (n.id === id) return updater(n);
                if (n.children && n.children.length > 0) return { ...n, children: updateNode(n.children, id, updater) };
                return n;
            });
        };

        // Find current state
        let foundNode: SchemaNode | undefined;
        const findNode = (nodes: SchemaNode[]) => {
            for (const n of nodes) {
                if (foundNode) return;
                if (n.id === nodeId) { foundNode = n; return; }
                if (n.children) findNode(n.children);
            }
        };
        findNode(schemaTree);

        const targetNode = foundNode;
        if (!targetNode) return;


        // Toggle if already loaded
        if (targetNode.children && targetNode.children.length > 0) {
            setSchemaTree(prev => updateNode(prev, nodeId, n => ({ ...n, isOpen: !n.isOpen })));
            return;
        }

        // Fetch children
        setSchemaTree(prev => updateNode(prev, nodeId, n => ({ ...n, isLoading: true, isOpen: true })));

        try {
            let newChildren: SchemaNode[] = [];

            if (engine === 'postgres') {
                if (nodeType === 'database') {
                    const dbName = parentNames[0];
                    setPgDatabase(dbName);  // Track selected database
                    const res = await fetch('/api/postgres', {
                        method: 'POST',
                        headers: { 'Content-Type': 'application/json' },
                        body: JSON.stringify({ action: 'explore', type: 'schemas', database: dbName })
                    });
                    const data = await parseApiResponse(res, `Failed to load PostgreSQL schemas for ${dbName}`);
                    if (data.data) {
                        newChildren = data.data.map((s: any) => ({
                            id: `${nodeId}_${s.name}`, name: s.name, type: 'schema', isOpen: false, children: []
                        }));
                    }
                } else if (nodeType === 'schema') {
                    // Introduce static folders for organization
                    newChildren = [
                        { id: `${nodeId}_tables`, name: 'Tables', type: 'folder' as const, isOpen: false, children: [] },
                        { id: `${nodeId}_views`, name: 'Views', type: 'folder' as const, isOpen: false, children: [] },
                        { id: `${nodeId}_functions`, name: 'Functions', type: 'folder' as const, isOpen: false, children: [] },
                        { id: `${nodeId}_sequences`, name: 'Sequences', type: 'folder' as const, isOpen: false, children: [] },
                        { id: `${nodeId}_types`, name: 'Types', type: 'folder' as const, isOpen: false, children: [] },
                        { id: `${nodeId}_extensions`, name: 'Extensions', type: 'folder' as const, isOpen: false, children: [] },
                    ];
                } else if (nodeType === 'folder') {
                    const dbName = parentNames[0];
                    const schemaName = parentNames[1];
                    const folderName = parentNames[2];

                    const folderTypeMap: Record<string, string> = {
                        'Tables': 'tables', 'Views': 'views', 'Functions': 'functions',
                        'Sequences': 'sequences', 'Types': 'types', 'Extensions': 'extensions'
                    };
                    const exploreType = folderTypeMap[folderName] || 'tables';
                    const nodeTypeMap: Record<string, string> = {
                        'Tables': 'table', 'Views': 'view', 'Functions': 'function',
                        'Sequences': 'sequence', 'Types': 'type', 'Extensions': 'extension'
                    };

                    const res = await fetch('/api/postgres', {
                        method: 'POST',
                        headers: { 'Content-Type': 'application/json' },
                        body: JSON.stringify({ action: 'explore', type: exploreType, schema: schemaName, database: dbName })
                    });
                    const data = await parseApiResponse(res, `Failed to load PostgreSQL ${exploreType} in ${schemaName}`);
                    if (data.data) {
                        const defaultType = nodeTypeMap[folderName] || 'table';
                        newChildren = data.data.map((item: any) => ({
                            id: `${nodeId}_${item.oid || item.name}_${item.identity_arguments || ''}`,
                            name: item.display_name || item.name,
                            sqlName: item.name,
                            routineOid: typeof item.oid === 'number' ? item.oid : undefined,
                            routineSignature: item.identity_arguments || undefined,
                            type: item.object_type || defaultType,
                            isOpen: false,
                            children: []
                        }));
                    }
                } else if (['table', 'view', 'materialized_view'].includes(nodeType)) {
                    const dbName = parentNames[0];
                    const schemaName = parentNames[1];
                    const tableName = parentNames[3]; // Hierarchy is db > schema > folder > table

                    // Populate properties panel
                    setSelectedObject({ schema: schemaName, name: tableName, database: dbName, type: nodeType });
                    setPropertiesData({});
                    setBottomPanel('properties');

                    const res = await fetch('/api/postgres', {
                        method: 'POST',
                        headers: { 'Content-Type': 'application/json' },
                        body: JSON.stringify({ action: 'explore', type: 'columns', schema: schemaName, table: tableName, database: dbName })
                    });
                    const data = await parseApiResponse(res, `Failed to load PostgreSQL columns for ${schemaName}.${tableName}`);
                    if (data.data) {
                        newChildren = data.data.map((c: any) => ({
                            id: `${nodeId}_${c.name}`, name: c.name, type: 'column' as const, dataType: c.data_type
                        }));
                    }
                }
            } else if (engine === 'spark') {
                if (nodeType === 'database') {
                    const dbName = parentNames[0];
                    setSparkDatabase(dbName);
                    const res = await fetch('/api/spark', {
                        method: 'POST',
                        headers: { 'Content-Type': 'application/json' },
                        body: JSON.stringify({ action: 'explore', type: 'tables', schema: dbName, database: dbName })
                    });
                    const data = await parseApiResponse(res, `Failed to load Spark tables for ${dbName}`);
                    if (data.data) {
                        newChildren = data.data.map((item: any) => ({
                            id: `${nodeId}_${item.name}`,
                            name: item.name,
                            type: item.object_type || 'table',
                            isOpen: false,
                            children: [],
                        }));
                    }
                } else if (['table', 'view', 'materialized_view'].includes(nodeType)) {
                    const dbName = parentNames[0];
                    const tableName = targetNode.sqlName || parentNames[parentNames.length - 1];
                    const res = await fetch('/api/spark', {
                        method: 'POST',
                        headers: { 'Content-Type': 'application/json' },
                        body: JSON.stringify({ action: 'explore', type: 'columns', schema: dbName, table: tableName, database: dbName })
                    });
                    const data = await parseApiResponse(res, `Failed to load Spark columns for ${dbName}.${tableName}`);
                    if (data.data) {
                        newChildren = data.data.map((c: any) => ({
                            id: `${nodeId}_${c.name}`,
                            name: c.name,
                            type: 'column' as const,
                            dataType: c.data_type,
                        }));
                    }
                }
            } else {
                // Trino
                if (nodeType === 'catalog') {
                    const catalogName = parentNames[0];
                    setTrinoSchema('');
                    const res = await fetch('/api/trino', {
                        method: 'POST', headers: { 'Content-Type': 'application/json' },
                        body: JSON.stringify({ query: `SHOW SCHEMAS FROM ${quoteTrinoIdentifier(catalogName)}`, catalog: catalogName })
                    });
                    const data = await parseApiResponse(res, `Failed to load Trino schemas for ${catalogName}`);
                    if (data.data) {
                        newChildren = data.data.filter((s: any) => s.Schema !== 'information_schema').map((s: any) => ({
                            id: `${nodeId}_${s.Schema}`, name: s.Schema, type: 'schema', isOpen: false, children: []
                        }));
                    }
                } else if (nodeType === 'schema') {
                    const catalogName = parentNames[0];
                    const schemaName = parentNames[1];
                    setTrinoSchema(schemaName);
                    const escapedSchemaName = escapeSqlLiteral(schemaName);
                    let res = await fetch('/api/trino', {
                        method: 'POST',
                        headers: { 'Content-Type': 'application/json' },
                        body: JSON.stringify({
                            query: `SELECT table_name AS name, table_type FROM ${quoteTrinoIdentifier(catalogName)}.information_schema.tables WHERE table_schema = '${escapedSchemaName}' ORDER BY table_name`,
                            catalog: catalogName
                        })
                    });
                    let data = await res.json().catch(() => ({}));
                    if (!res.ok || !data.data) {
                        // Fallback for catalogs/connectors where information_schema.tables is restricted.
                        res = await fetch('/api/trino', {
                            method: 'POST',
                            headers: { 'Content-Type': 'application/json' },
                            body: JSON.stringify({
                                query: `SHOW TABLES FROM ${quoteTrinoIdentifier(catalogName)}.${quoteTrinoIdentifier(schemaName)}`,
                                catalog: catalogName
                            })
                        });
                        data = await parseApiResponse(res, `Failed to load Trino tables for ${catalogName}.${schemaName}`);
                    }
                    if (data.data) {
                        newChildren = data.data.map((t: any) => {
                            const objectName = t.name || t.table_name || t.Table || t.TABLE_NAME;
                            const rawType = String(t.table_type || t.TABLE_TYPE || t.tableType || 'BASE TABLE').toUpperCase();
                            let objectType: SchemaNode['type'] = 'table';
                            if (rawType.includes('MATERIALIZED VIEW')) objectType = 'materialized_view';
                            else if (rawType.includes('VIEW')) objectType = 'view';

                            return {
                                id: `${nodeId}_${objectName}`,
                                name: objectName,
                                type: objectType,
                                isOpen: false,
                                children: []
                            };
                        }).filter((node: SchemaNode) => Boolean(node.name));
                    }
                    try {
                        const fnRes = await fetch('/api/trino', {
                            method: 'POST',
                            headers: { 'Content-Type': 'application/json' },
                            body: JSON.stringify({ query: `SHOW FUNCTIONS FROM ${quoteTrinoIdentifier(catalogName)}.${quoteTrinoIdentifier(schemaName)}`, catalog: catalogName })
                        });
                        const fnData = await fnRes.json().catch(() => ({}));
                        if (fnRes.ok && fnData.data) {
                            const seen = new Set(newChildren.map(node => node.name.toLowerCase()));
                            const fnNodes = fnData.data
                                .map((f: any) => f.Function || f.function || f.name || f.Name)
                                .filter((name: string) => typeof name === 'string' && name.trim().length > 0)
                                .map((name: string) => name.trim())
                                .filter((name: string) => {
                                    const key = name.toLowerCase();
                                    if (seen.has(key)) return false;
                                    seen.add(key);
                                    return true;
                                })
                                .map((name: string) => ({
                                    id: `${nodeId}_fn_${name}`,
                                    name,
                                    type: 'function' as const,
                                }));
                            newChildren = [...newChildren, ...fnNodes];
                        }
                    } catch (err) {
                        console.warn('Failed to load Trino functions for schema explorer', err);
                    }
                } else if (['table', 'view', 'materialized_view'].includes(nodeType)) {
                    const catalogName = parentNames[0];
                    const schemaName = parentNames[1];
                    const tableName = parentNames[2];
                    setTrinoSchema(schemaName);
                    const res = await fetch('/api/trino', {
                        method: 'POST', headers: { 'Content-Type': 'application/json' },
                        body: JSON.stringify({
                            query: `SHOW COLUMNS FROM ${quoteTrinoIdentifier(catalogName)}.${quoteTrinoIdentifier(schemaName)}.${quoteTrinoIdentifier(tableName)}`,
                            catalog: catalogName
                        })
                    });
                    const data = await parseApiResponse(res, `Failed to load Trino columns for ${catalogName}.${schemaName}.${tableName}`);
                    if (data.data) {
                        newChildren = data.data.map((c: any) => ({
                            id: `${nodeId}_${c.Column}`, name: c.Column, type: 'column', dataType: c.Type
                        }));
                    }
                }
            }

            setSchemaTree(prev => updateNode(prev, nodeId, n => ({ ...n, isLoading: false, children: newChildren })));

        } catch (err) {
            console.error("Failed to fetch schema node", err);
            setSchemaTree(prev => updateNode(prev, nodeId, n => ({ ...n, isLoading: false, isOpen: false })));
        }
    };

    // ─── Listen for sidebar events (context menu / double-click) ───
    useEffect(() => {
        const handleRunQuery = (e: Event) => {
            const detail = (e as CustomEvent).detail;
            if (detail?.query) {
                updateQuery(detail.query);
                if (detail.engine) setEngine(normalizeEngine(detail.engine));
                // Small delay to let state update, then execute
                setTimeout(() => executeRef.current(), 100);
            }
        };
        const handleInsertQuery = (e: Event) => {
            const detail = (e as CustomEvent).detail;
            if (detail?.query) {
                updateQuery(detail.query);
            }
        };
        window.addEventListener('openclaw:run-query', handleRunQuery);
        window.addEventListener('openclaw:insert-query', handleInsertQuery);
        return () => {
            window.removeEventListener('openclaw:run-query', handleRunQuery);
            window.removeEventListener('openclaw:insert-query', handleInsertQuery);
        };
    }, []);

    // ─── Export Functions ───
    const inferExportTableName = useCallback(() => {
        if (selectedObject?.schema && selectedObject?.name) {
            return `${selectedObject.schema}.${selectedObject.name}`;
        }
        const fromMatch = activeTab.query.match(/\bFROM\s+([A-Za-z0-9_."`]+)/i);
        if (fromMatch?.[1]) return fromMatch[1].replace(/`/g, '"');
        return 'export_table';
    }, [activeTab.query, selectedObject]);

    const downloadCSV = () => {
        if (!activeTab.results) return;
        const headers = activeTab.results.columns.join(',');
        const rows = activeTab.results.data.map((row: any) =>
            activeTab.results!.columns.map(col => {
                const val = row[col];
                return typeof val === 'string' ? `"${val.replace(/"/g, '""')}"` : val;
            }).join(',')
        ).join('\n');
        downloadFile(headers + '\n' + rows, 'csv', 'text/csv');
    };

    const downloadJSON = () => {
        if (!activeTab.results) return;
        const json = JSON.stringify(activeTab.results.data, null, 2);
        downloadFile(json, 'json', 'application/json');
    };

    const downloadSQLInsert = () => {
        if (!activeTab.results) return;
        const tableName = inferExportTableName();
        const cols = activeTab.results.columns.join(', ');
        const inserts = activeTab.results.data.map((row: any) => {
            const values = activeTab.results!.columns.map(col => {
                const val = row[col];
                if (val === null) return 'NULL';
                if (typeof val === 'number') return val;
                return `'${String(val).replace(/'/g, "''")}'`;
            }).join(', ');
            return `INSERT INTO ${tableName} (${cols}) VALUES (${values});`;
        }).join('\n');
        downloadFile(inserts, 'sql', 'text/plain');
    };

    const copyAllToClipboard = () => {
        if (!activeTab.results) return;
        const headers = activeTab.results.columns.join('\t');
        const rows = activeTab.results.data.map((row: any) =>
            activeTab.results!.columns.map(col => row[col] === null ? 'NULL' : String(row[col])).join('\t')
        ).join('\n');
        copyToClipboard(headers + '\n' + rows);
        setShowExportMenu(false);
    };

    const downloadFile = (content: string, ext: string, type: string) => {
        const blob = new Blob([content], { type: `${type};charset=utf-8;` });
        const url = URL.createObjectURL(blob);
        const link = document.createElement("a");
        link.setAttribute("href", url);
        link.setAttribute("download", `export_${activeTab.name.replace('.sql', '')}_${Date.now()}.${ext}`);
        document.body.appendChild(link);
        link.click();
        document.body.removeChild(link);
        setShowExportMenu(false);
    };

    const formatSQL = () => {
        try {
            const q = formatSqlText(activeTab.query, {
                language: engine === 'postgres' ? 'postgresql' : engine === 'spark' ? 'spark' : 'trino',
                keywordCase: 'upper',
                linesBetweenQueries: 1,
                tabWidth: 4,
            });
            updateQuery(q);
        } catch {
            updateQuery(activeTab.query);
        }
    };

    const loadHistoryQuery = (item: QueryHistoryItem) => {
        updateQuery(item.query);
        setEngine(normalizeEngine(item.engine));
        setBottomPanel('results');
    };

    const clearHistory = () => {
        setQueryHistory([]);
        localStorage.removeItem('openclaw-query-history');
    };

    const formatTimestamp = (ts: number) => {
        const d = new Date(ts);
        return d.toLocaleTimeString('en-US', { hour: '2-digit', minute: '2-digit', second: '2-digit', hour12: false });
    };

    const formatDate = (ts: number) => {
        const d = new Date(ts);
        const today = new Date();
        if (d.toDateString() === today.toDateString()) return 'Today';
        const yesterday = new Date(today);
        yesterday.setDate(yesterday.getDate() - 1);
        if (d.toDateString() === yesterday.toDateString()) return 'Yesterday';
        return d.toLocaleDateString('en-US', { month: 'short', day: 'numeric' });
    };

    // ─── Filtered History ───
    const filteredHistory = useMemo(() => {
        if (!historyFilter) return queryHistory;
        const f = historyFilter.toLowerCase();
        return queryHistory.filter(h => h.query.toLowerCase().includes(f));
    }, [queryHistory, historyFilter]);

    useEffect(() => {
        if (!monaco) return;
        let active = true;
        void (async () => {
            const keywords = await loadMonacoSqlKeywords(monaco);
            if (active) setSqlLanguageKeywords(keywords);
        })();
        return () => {
            active = false;
        };
    }, [monaco]);

    // ─── Monaco Config ───
    useEffect(() => {
        if (!monaco) return;

        // ── Custom Obsidian Dark Theme ──
        monaco.editor.defineTheme('obsidian-premium', {
            base: 'vs-dark',
            inherit: true,
            rules: [
                { token: 'comment', foreground: '6272a4', fontStyle: 'italic' },
                { token: 'keyword', foreground: 'ff79c6', fontStyle: 'bold' }, // Pink
                { token: 'keyword.sql', foreground: 'ff79c6', fontStyle: 'bold' }, // Pink
                { token: 'string', foreground: 'f1fa8c' }, // Yellow
                { token: 'string.sql', foreground: 'f1fa8c' },
                { token: 'number', foreground: 'bd93f9' }, // Purple
                { token: 'number.sql', foreground: 'bd93f9' },
                { token: 'operator', foreground: 'ff79c6' }, // Pink
                { token: 'identifier', foreground: 'f8f8f2' },
                { token: 'function', foreground: '8be9fd' }, // Cyan
                { token: 'delimiter', foreground: 'f8f8f2' },
                { token: 'type', foreground: 'bd93f9', fontStyle: 'italic' }, // Purple
            ],
            colors: {
                'editor.background': '#00000000', // Transparent so the page's ambient glassmorphic background shows through
                'editor.foreground': '#f8f8f2',
                'editor.lineHighlightBackground': '#ffffff05',
                'editor.lineHighlightBorder': '#00000000',
                'editorLineNumber.foreground': '#6272a4',
                'editorLineNumber.activeForeground': '#f8f8f2',
                'editorGutter.background': '#00000000',
                'editor.selectionBackground': '#44475a',
                'editor.inactiveSelectionBackground': '#44475a80',
                'editorCursor.foreground': '#ff79c6',
                'editor.wordHighlightBackground': '#ff79c633',
                'editorBracketMatch.background': '#bd93f940',
                'editorBracketMatch.border': '#bd93f9',
                'editorIndentGuide.background': '#ffffff10',
                'editorSuggestWidget.background': '#1e1e24',
                'editorSuggestWidget.border': '#33333d',
                'editorSuggestWidget.selectedBackground': '#44475a',
                'editorSuggestWidget.highlightForeground': '#ff79c6',
                'editorWidget.background': '#1e1e24',
                'editorWidget.border': '#33333d',
                'focusBorder': '#ff79c6',
            }
        });
        monaco.editor.setTheme('obsidian-premium');

        // ── SQL Autocomplete Provider ──
        // SQL keywords/functions/types are provided by Monaco language service.
        // Here we only provide live metadata suggestions from the loaded schema tree.
        const schemaItems: { label: string; detail: string; kind: any; fqn?: string }[] = [];
        const collectNodes = (nodes: SchemaNode[], path: string[] = []) => {
            for (const node of nodes) {
                const currentPath = [...path, node.name];
                const fqn = currentPath.join('.');
                if (node.type === 'catalog' || node.type === 'database') {
                    schemaItems.push({ label: node.name, detail: 'catalog', kind: monaco.languages.CompletionItemKind.Module, fqn });
                } else if (node.type === 'schema') {
                    schemaItems.push({ label: node.name, detail: `schema (${path[0] || ''})`, kind: monaco.languages.CompletionItemKind.Struct, fqn });
                } else if (node.type === 'table') {
                    schemaItems.push({ label: node.name, detail: `table (${fqn})`, kind: monaco.languages.CompletionItemKind.Value, fqn });
                } else if (node.type === 'view' || node.type === 'materialized_view') {
                    schemaItems.push({ label: node.name, detail: `view (${fqn})`, kind: monaco.languages.CompletionItemKind.Interface, fqn });
                } else if (node.type === 'function' || node.type === 'procedure') {
                    schemaItems.push({ label: node.name, detail: `function`, kind: monaco.languages.CompletionItemKind.Function, fqn });
                } else if (node.type === 'sequence') {
                    schemaItems.push({ label: node.name, detail: `sequence`, kind: monaco.languages.CompletionItemKind.Enum, fqn });
                } else if (node.type === 'type') {
                    schemaItems.push({ label: node.name, detail: `type`, kind: monaco.languages.CompletionItemKind.TypeParameter, fqn });
                } else if (node.type === 'column') {
                    schemaItems.push({ label: node.name, detail: `column`, kind: monaco.languages.CompletionItemKind.Field, fqn });
                }
                if (node.children) collectNodes(node.children, currentPath);
            }
        };
        collectNodes(schemaTree);

        const disposable = monaco.languages.registerCompletionItemProvider('sql', {
            triggerCharacters: ['.', ' '],
            provideCompletionItems: (model, position) => {
                const word = model.getWordUntilPosition(position);
                const range = {
                    startLineNumber: position.lineNumber,
                    endLineNumber: position.lineNumber,
                    startColumn: word.startColumn,
                    endColumn: word.endColumn,
                };

                // Check text before cursor for dot-context
                const textBefore = model.getValueInRange({
                    startLineNumber: position.lineNumber,
                    startColumn: 1,
                    endLineNumber: position.lineNumber,
                    endColumn: position.column,
                });
                const dotMatch = textBefore.match(/(\w+(?:\.\w+)*)\.\s*$/);
                const prefix = dotMatch ? dotMatch[1].toLowerCase() : null;

                const typedPrefix = (word.word || '').toLowerCase();

                // Filter schema items by context
                let contextItems = schemaItems;
                if (prefix) {
                    // After a dot, show children whose FQN starts with the prefix
                    contextItems = schemaItems.filter(item =>
                        item.fqn && item.fqn.toLowerCase().startsWith(prefix + '.') &&
                        item.fqn.toLowerCase().split('.').length === prefix.split('.').length + 1
                    );
                } else if (typedPrefix) {
                    contextItems = schemaItems.filter(item => item.label.toLowerCase().startsWith(typedPrefix));
                }

                const keywordSuggestions = prefix
                    ? []
                    : sqlLanguageKeywords
                        .filter(keyword => !typedPrefix || keyword.toLowerCase().startsWith(typedPrefix))
                        .map(keyword => ({
                            label: keyword.toUpperCase(),
                            kind: monaco.languages.CompletionItemKind.Keyword,
                            insertText: keyword.toUpperCase(),
                            range,
                            detail: 'sql keyword',
                            sortText: '0_' + keyword,
                        }));

                const metadataSuggestions = contextItems.map(item => ({
                    label: item.label,
                    kind: item.kind,
                    insertText: item.label,
                    range,
                    detail: item.detail,
                    sortText: '1_' + item.label,
                }));

                const seen = new Set<string>();
                const suggestions = [...keywordSuggestions, ...metadataSuggestions].filter((item: any) => {
                    const key = String(item.label).toLowerCase();
                    if (seen.has(key)) return false;
                    seen.add(key);
                    return true;
                });
                return { suggestions } as any;
            }
        });

        return () => disposable.dispose();
    }, [monaco, engine, schemaTree, sqlLanguageKeywords]);

    // ─── Monaco Mount ───
    const handleEditorMount: OnMount = (editor, monacoInstance) => {
        editorRef.current = editor;



        editor.addAction({
            id: 'execute-query',
            label: 'Execute Query',
            keybindings: [monacoInstance.KeyMod.CtrlCmd | monacoInstance.KeyCode.Enter],
            run: () => { executeRef.current(); },
        });
        editor.addAction({
            id: 'format-sql',
            label: 'Format SQL',
            keybindings: [monacoInstance.KeyMod.CtrlCmd | monacoInstance.KeyMod.Shift | monacoInstance.KeyCode.KeyF],

            run: () => { formatSQL(); },
        });
        editor.focus();
    };

    // ─── Get selected cell value for status bar ───
    const selectedValue = useMemo(() => {
        if (!selectedCell || !activeTab.results) return null;
        const val = sortedData[selectedCell.row]?.[selectedCell.col];
        if (val === null) return 'NULL';
        if (val === undefined) return '';
        return String(val);
    }, [selectedCell, sortedData, activeTab.results]);

    const explainPlanTree = useMemo(() => {
        if (!activeTab.results || activeTab.results.data.length === 0 || activeTab.results.columns.length === 0) return null;
        const trimmed = activeTab.query.trim().toUpperCase();
        if (!trimmed.startsWith('EXPLAIN')) return null;

        const firstColumn = activeTab.results.columns[0];
        const firstValue = activeTab.results.data[0]?.[firstColumn];
        let parsed: any = null;
        if (typeof firstValue === 'string') {
            const t = firstValue.trim();
            if (t.startsWith('{') || t.startsWith('[')) {
                try {
                    parsed = JSON.parse(t);
                } catch {
                    parsed = null;
                }
            }
        } else if (firstValue && typeof firstValue === 'object') {
            parsed = firstValue;
        }

        if (Array.isArray(parsed) && parsed[0]?.Plan) return parsed[0].Plan;
        if (parsed?.Plan) return parsed.Plan;
        return null;
    }, [activeTab.query, activeTab.results]);

    const explainPlanText = useMemo(() => {
        if (!activeTab.results || activeTab.results.data.length === 0 || activeTab.results.columns.length === 0) return null;
        const trimmed = activeTab.query.trim().toUpperCase();
        if (!trimmed.startsWith('EXPLAIN')) return null;
        if (explainPlanTree) return null;

        const firstColumn = activeTab.results.columns[0];
        const firstValue = activeTab.results.data[0]?.[firstColumn];
        if (typeof firstValue === 'string' && firstValue.trim().length > 0) {
            return firstValue;
        }
        return null;
    }, [activeTab.query, activeTab.results, explainPlanTree]);

    const renderExplainNode = (node: any, depth = 0): React.ReactNode => {
        if (!node || typeof node !== 'object') return null;
        const children = Array.isArray(node.Plans) ? node.Plans : [];
        return (
            <div key={`${node['Node Type'] || 'node'}_${depth}_${node['Relation Name'] || ''}`} className="py-1">
                <div className="flex items-center gap-2" style={{ paddingLeft: depth * 18 }}>
                    <span className="text-[10px] px-1.5 py-0.5 rounded bg-sky-500/10 border border-sky-400/20 text-sky-300 font-mono">
                        {node['Node Type'] || 'Plan'}
                    </span>
                    {node['Relation Name'] && <span className="text-[10px] text-white/60 font-mono">{node['Relation Name']}</span>}
                    {node['Index Name'] && <span className="text-[10px] text-amber-300/80 font-mono">{node['Index Name']}</span>}
                    {node['Total Cost'] !== undefined && <span className="text-[10px] text-white/40">cost {node['Total Cost']}</span>}
                    {node['Actual Total Time'] !== undefined && <span className="text-[10px] text-emerald-300/70">{node['Actual Total Time']} ms</span>}
                </div>
                {children.length > 0 && (
                    <div className="mt-1">
                        {children.map((child: any, idx: number) => (
                            <React.Fragment key={`${depth}_${idx}`}>
                                {renderExplainNode(child, depth + 1)}
                            </React.Fragment>
                        ))}
                    </div>
                )}
            </div>
        );
    };

    // ─── Cell value renderer (Premium Monochrome typed coloring) ───
    const renderCellValue = (val: any) => {
        if (val === null || val === undefined) {
            return <span style={{ color: 'rgba(255,255,255,0.15)', fontStyle: 'italic', fontSize: 10, letterSpacing: '0.5px' }}>NULL</span>;
        }
        if (typeof val === 'number') {
            return <span style={{ color: '#7dd3fc' }}>{val.toLocaleString()}</span>; // sky-300
        }
        if (typeof val === 'boolean') {
            return <span style={{ color: val ? '#fbbf24' : '#92400e', fontWeight: 600 }}>{String(val)}</span>; // amber-400 / amber-800
        }
        const str = String(val);
        // Detect date-time patterns
        if (/^\d{4}-\d{2}-\d{2}/.test(str)) {
            return <span style={{ color: '#c4b5fd' }}>{str}</span>; // violet-300
        }
        // JSON objects
        if (str.startsWith('{') || str.startsWith('[')) {
            return <span style={{ color: '#86efac' }}>{str}</span>; // green-300
        }
        return <span style={{ color: 'rgba(255,255,255,0.55)' }}>{str}</span>;
    };

    const connectedHost = useMemo(() => {
        if (typeof window === 'undefined') return 'unknown-host';
        return window.location.hostname || 'localhost';
    }, []);

    return (
        <div className="flex h-screen bg-[#09090b] text-foreground font-sans overflow-hidden relative" style={{ fontFamily: "'Inter', -apple-system, sans-serif" }}>
            <Sidebar />

            {/* Background ambient light */}
            <div className="absolute top-0 left-0 w-[800px] h-[800px] bg-purple-500/5 rounded-full blur-[120px] pointer-events-none -translate-x-1/4 -translate-y-1/4 z-0" />
            <div className="absolute bottom-0 right-0 w-[600px] h-[600px] bg-sky-500/5 rounded-full blur-[100px] pointer-events-none translate-x-1/4 translate-y-1/4 z-0" />

            {/* Left Schema Sidebar (Collapsible & Resizable) */}
            <div
                className={clsx(
                    "relative flex flex-col bg-transparent z-20 overflow-hidden",
                    !isDraggingSidebar && "transition-[width] duration-300",
                    !showSchemaExplorer && "w-0 border-r-0"
                )}
                style={{ width: showSchemaExplorer ? sidebarWidth : 0 }}
            >
                {/* Explorer Header */}
                <div className="h-10 border-b border-white/5 flex items-center justify-between px-4 shrink-0">
                    <div
                        onClick={cycleEngine}
                        className="flex items-center gap-2 cursor-pointer group hover:bg-white/5 px-2 py-1 -ml-2 rounded transition-colors"
                        title="Switch Connection"
                    >
                        <Database className="w-3.5 h-3.5 text-sky-400 opacity-80 group-hover:opacity-100 transition-opacity" />
                        <span className="text-xs font-semibold text-white/90 tracking-wide">
                            {ENGINE_LABELS[engine]}
                        </span>
                        <ArrowUpDown className="w-3 h-3 text-white/30 group-hover:text-white/70 transition-colors ml-1" />
                    </div>
                    <div className="flex gap-1 shrink-0">
                        <button onClick={fetchSchemaRoot} className="p-1.5 hover:bg-white/10 rounded-md text-white/40 hover:text-white/80 transition-all active:scale-95">
                            <RefreshCw className={clsx("w-3.5 h-3.5", isSchemaLoading && "animate-spin")} />
                        </button>
                    </div>
                </div>

                {/* Explorer Tree */}
                <div className="flex-1 overflow-auto bg-black/20 backdrop-blur-xl py-2 custom-scrollbar">
                    {isSchemaLoading && schemaTree.length === 0 ? (
                        <div className="flex flex-col items-center justify-center py-12 text-white/40 gap-3">
                            <Loader2 className="w-5 h-5 animate-spin opacity-50" />
                            <span className="text-[11px] tracking-wide">Loading schemas...</span>
                        </div>
                    ) : schemaLoadError ? (
                        <div className="px-3 py-3">
                            <div className="rounded-md border border-red-400/20 bg-red-500/5 p-2 text-[11px] text-red-200/90 leading-relaxed break-words">
                                {schemaLoadError}
                            </div>
                        </div>
                    ) : (
                        <div className="flex flex-col pl-2 pr-2">
                            {schemaTree.map(node => (
                                <SchemaTreeNode
                                    key={node.id}
                                    node={node}
                                    parentNames={[node.name]}
                                    onToggle={toggleSchemaNode}
                                    onDoubleClick={(name) => insertIntoEditor(name)}
                                    onContextMenu={handleSchemaContextMenu}
                                />
                            ))}
                        </div>
                    )}
                </div>

                {/* Sidebar Resizer Handle */}
                <div
                    className="absolute top-0 right-0 w-1 h-full cursor-col-resize hover:bg-white/10 transition-colors z-30"
                    onMouseDown={(e) => { e.preventDefault(); setIsDraggingSidebar(true); }}
                />
            </div>

            <div className="flex-1 flex flex-col min-w-0 relative bg-transparent z-10">

                {/* Drag Overlays */}
                {isDragging && <div className="absolute inset-0 z-50 cursor-row-resize bg-transparent" />}
                {isDraggingSidebar && <div className="absolute inset-0 z-50 cursor-col-resize bg-transparent" />}
                {resizingCol && <div className="absolute inset-0 z-50 cursor-col-resize bg-transparent" />}

                {/* ─── Top Navigation Bar ─── */}
                <div
                    className="flex items-center px-4 justify-between shrink-0 h-10 bg-black/40 backdrop-blur-md border-b border-white/5 z-10 w-full relative"
                >
                    <div className="absolute top-0 left-0 right-0 h-[1px] bg-gradient-to-r from-transparent via-obsidian-info/30 to-transparent opacity-50" />
                    <div className="flex items-center gap-2">
                        {/* Toggle Sidebar Button */}
                        <button
                            onClick={() => setShowSchemaExplorer(!showSchemaExplorer)}
                            className="p-1.5 hover:bg-white/10 rounded-md text-obsidian-muted hover:text-white transition-all active:scale-95 border border-transparent hover:border-obsidian-border/50"
                            title="Toggle Database Explorer"
                        >
                            <LayoutPanelLeft className="w-4 h-4 drop-shadow-[0_0_8px_rgba(255,255,255,0.2)]" />
                        </button>
                        <div className="w-[1px] h-4 bg-obsidian-border/50 mx-1"></div>

                        {/* Breadcrumb / Database Selector */}
                        <div className="relative">
                            <button
                                className="flex items-center gap-2 text-xs font-mono hover:bg-white/5 py-1 px-2 rounded transition-colors active:scale-95"
                                onClick={(e) => {
                                    const rect = e.currentTarget.getBoundingClientRect();
                                    setDbMenuPos({ top: rect.bottom + 4, left: rect.left });
                                    setShowDbMenu(!showDbMenu);
                                }}
                            >
                                <Database className="w-3 h-3 text-obsidian-info" />
                                <span className="text-foreground/80 font-medium">
                                    {engine === 'trino'
                                        ? (trinoCatalog
                                            ? (trinoSchema ? `${trinoCatalog}.${trinoSchema}` : trinoCatalog)
                                            : 'Select catalog')
                                        : engine === 'spark'
                                            ? sparkDatabase
                                            : pgDatabase}
                                </span>
                                <ChevronDown className="w-3.5 h-3.5 text-obsidian-muted" />
                            </button>

                            {/* Dropdown Menu (Click based, portal to body) */}
                            {showDbMenu && createPortal(
                                <>
                                    <div className="fixed inset-0 z-[9998]" onClick={() => setShowDbMenu(false)} />
                                    <div
                                        className="fixed w-48 bg-[#09090b] border border-white/10 rounded shadow-2xl z-[9999] overflow-hidden"
                                        style={{ top: dbMenuPos.top, left: dbMenuPos.left }}
                                    >
                                        <div className="p-1.5 flex flex-col gap-0.5">
                                            <div className="px-2 py-1.5 text-[10px] items-center text-obsidian-muted font-bold uppercase tracking-wider mb-1 border-b border-white/5">
                                                Select Database
                                            </div>
                                            {engine === 'trino' ? (
                                                <>
                                                    {schemaTree.length === 0 && (
                                                        <div className="px-2 py-2 text-[11px] text-white/40">
                                                            No catalogs loaded
                                                        </div>
                                                    )}
                                                    {schemaTree.map((catalog: any) => (
                                                        <button
                                                            key={catalog.name}
                                                            onClick={() => { setTrinoCatalog(catalog.name); setTrinoSchema(''); setShowDbMenu(false); }}
                                                            className={`flex items-center gap-2 px-2 py-1.5 text-xs text-left rounded transition-colors ${trinoCatalog === catalog.name ? 'bg-white/[0.04] text-white font-medium' : 'hover:bg-white/[0.02] text-obsidian-muted'}`}
                                                        >
                                                            <Database className={`w-3 h-3 ${trinoCatalog === catalog.name ? 'text-obsidian-info' : 'opacity-50'}`} /> {catalog.name}
                                                        </button>
                                                    ))}
                                                </>
                                            ) : engine === 'spark' ? (
                                                <>
                                                    {schemaTree.map(db => (
                                                        <button key={db.name} onClick={() => { setSparkDatabase(db.name); setShowDbMenu(false); }} className={`flex items-center gap-2 px-2 py-1.5 text-xs text-left rounded transition-colors ${sparkDatabase === db.name ? 'bg-white/[0.04] text-white font-medium' : 'hover:bg-white/[0.02] text-obsidian-muted'}`}>
                                                            <Database className={`w-3 h-3 ${sparkDatabase === db.name ? 'text-obsidian-info' : 'opacity-50'}`} /> {db.name}
                                                        </button>
                                                    ))}
                                                </>
                                            ) : (
                                                <>
                                                    {schemaTree.map(db => (
                                                        <button key={db.name} onClick={() => { setPgDatabase(db.name); setShowDbMenu(false); }} className={`flex items-center gap-2 px-2 py-1.5 text-xs text-left rounded transition-colors ${pgDatabase === db.name ? 'bg-white/[0.04] text-white font-medium' : 'hover:bg-white/[0.02] text-obsidian-muted'}`}>
                                                            <Database className={`w-3 h-3 ${pgDatabase === db.name ? 'text-obsidian-info' : 'opacity-50'}`} /> {db.name}
                                                        </button>
                                                    ))}
                                                </>
                                            )}
                                        </div>
                                    </div>
                                </>,
                                document.body
                            )}
                        </div>
                    </div>

                    {/* Right side */}
                    <div className="flex items-center gap-3 text-xs">
                        {engine === 'trino' && (
                            <div className="relative">
                                <button
                                    onClick={() => setShowTrinoSettings(prev => !prev)}
                                    className={clsx(
                                        "flex items-center gap-1.5 px-2 py-1 rounded-md border transition-colors",
                                        showTrinoSettings
                                            ? "bg-sky-500/15 text-sky-300 border-sky-400/40"
                                            : "bg-white/[0.02] text-white/60 border-white/10 hover:text-white hover:bg-white/[0.05]"
                                    )}
                                    title="Trino session settings"
                                >
                                    <SlidersHorizontal className="w-3.5 h-3.5" />
                                    <span>Trino Session</span>
                                </button>
                                {showTrinoSettings && (
                                    <>
                                        <div className="fixed inset-0 z-[9998]" onClick={() => setShowTrinoSettings(false)} />
                                        <div className="absolute right-0 top-full mt-1 z-[9999] w-[360px] rounded-xl border border-white/10 bg-[#09090b] shadow-2xl p-3 space-y-2.5">
                                            <div className="text-[10px] uppercase tracking-wider text-white/50 font-semibold">Role / Session Properties / Client Tags</div>
                                            <div className="space-y-1">
                                                <label className="text-[10px] text-white/50 uppercase tracking-wide">Role</label>
                                                <input
                                                    value={trinoRole}
                                                    onChange={(e) => setTrinoRole(e.target.value)}
                                                    placeholder="e.g. catalog_or_schema, role_name"
                                                    className="w-full bg-white/5 border border-white/10 rounded-md px-2.5 py-1.5 text-[11px] text-white placeholder-white/30 focus:outline-none focus:border-sky-400/40"
                                                />
                                            </div>
                                            <div className="space-y-1">
                                                <label className="text-[10px] text-white/50 uppercase tracking-wide">Client Tags (comma separated)</label>
                                                <input
                                                    value={trinoClientTagsInput}
                                                    onChange={(e) => setTrinoClientTagsInput(e.target.value)}
                                                    placeholder="adhoc,finance,priority_high"
                                                    className="w-full bg-white/5 border border-white/10 rounded-md px-2.5 py-1.5 text-[11px] text-white placeholder-white/30 focus:outline-none focus:border-sky-400/40"
                                                />
                                            </div>
                                            <div className="space-y-1">
                                                <label className="text-[10px] text-white/50 uppercase tracking-wide">Session Properties (one per line: key=value)</label>
                                                <textarea
                                                    value={trinoSessionPropsText}
                                                    onChange={(e) => setTrinoSessionPropsText(e.target.value)}
                                                    placeholder={`query_max_run_time=5m\njoin_distribution_type=AUTOMATIC`}
                                                    className="w-full h-24 resize-y bg-white/5 border border-white/10 rounded-md px-2.5 py-1.5 text-[11px] text-white placeholder-white/30 focus:outline-none focus:border-sky-400/40 font-mono"
                                                />
                                            </div>
                                            <div className="flex items-center justify-between text-[10px] text-white/40">
                                                <span>{parsedTrinoClientTags?.length || 0} tags, {parsedTrinoSessionProperties ? Object.keys(parsedTrinoSessionProperties).length : 0} session props</span>
                                                <button
                                                    onClick={() => { setTrinoRole(''); setTrinoClientTagsInput(''); setTrinoSessionPropsText(''); }}
                                                    className="px-2 py-1 rounded hover:bg-white/5 text-white/60 hover:text-white"
                                                >
                                                    Clear
                                                </button>
                                            </div>
                                        </div>
                                    </>
                                )}
                            </div>
                        )}
                        {isExecuting && (
                            <div className="flex items-center gap-2 rounded-md px-2 py-1 bg-white/5 border border-white/10 text-white/60">
                                <div className="w-1.5 h-1.5 rounded-full bg-white/60 animate-ping shadow-[0_0_8px_rgba(255,255,255,0.4)]" />
                                <span className="font-mono">Executing…</span>
                            </div>
                        )}
                        <div className="flex items-center gap-1.5 text-white/50 font-medium">
                            <div className="w-1.5 h-1.5 rounded-full bg-white/40" />
                            <span>Connected</span>
                        </div>
                    </div>
                </div>

                {/* ─── Tab Bar ─── */}
                <div
                    className="flex items-end overflow-x-auto shrink-0 bg-transparent h-9 w-full px-1 gap-0.5 z-10 border-b border-white/5"
                >
                    {tabs.map(tab => (
                        <div
                            key={tab.id}
                            onClick={() => setActiveTab(tab.id)}
                            className={clsx(
                                "relative flex items-center justify-between cursor-pointer select-none group h-full min-w-[120px] max-w-[200px] px-3 rounded-t-md",
                                tab.active ? "bg-white/5 border-t border-x border-white/5 border-b-transparent text-foreground shadow-sm z-20" : "bg-transparent border border-transparent border-b-transparent text-obsidian-muted hover:bg-white/5 hover:text-white/70 z-10"
                            )}
                            style={{
                                fontSize: '12px',
                                transition: 'background 0.1s ease',
                            }}
                        >
                            <div className="flex items-center gap-2.5 truncate">
                                <FileCode className={clsx("w-3.5 h-3.5 flex-shrink-0 transition-colors duration-300", tab.active ? 'text-obsidian-info' : 'opacity-50')} />
                                <span className={clsx("truncate tracking-wide", tab.active ? "font-medium" : "")}>{tab.name}</span>
                            </div>

                            <div className="flex items-center gap-1.5 ml-3">
                                {/* Row count badge */}
                                {tab.results && tab.results.data.length > 0 && (
                                    <span className="text-[10px] bg-white/5 text-white/50 px-1.5 py-0.5 rounded-sm font-mono leading-none border border-white/5">
                                        {tab.results.data.length}
                                    </span>
                                )}

                                {/* Close button */}
                                <X
                                    onClick={(e) => { e.stopPropagation(); closeTab(e, tab.id); }}
                                    className={clsx('w-3.5 h-3.5 rounded transition-all flex-shrink-0', tab.active ? 'opacity-40 hover:opacity-100 hover:bg-white/10' : 'opacity-0 group-hover:opacity-40 hover:!opacity-100 hover:bg-white/10')}
                                />
                            </div>

                            {/* Active Tab Line Indicator */}
                            {tab.active && (
                                <div className="absolute top-0 left-0 w-full h-[2px] bg-sky-400" />
                            )}
                        </div>
                    ))}
                    <div
                        onClick={addTab}
                        className="flex items-center justify-center cursor-pointer h-8 w-8 ml-1 rounded-md text-white/30 hover:text-white/80 hover:bg-white/10 transition-colors mb-1"
                        title="New Tab"
                    >
                        <Plus className="w-4 h-4" />
                    </div>
                </div>

                {/* ─── Main Workspace Area (Edge-to-Edge) ─── */}
                <div className="flex-1 relative flex flex-col min-h-0 bg-transparent" ref={containerRef}>

                    {/* 1. Editor Panel */}
                    <div
                        className="relative flex flex-col w-full bg-transparent shrink-0"
                        style={{ height: editorHeight }}
                    >
                        {/* Editor Toolbar */}
                        <div className="shrink-0 flex items-center px-4 py-2.5 gap-3 border-b border-white/5 bg-black/20 backdrop-blur-md z-10">

                            {/* Run Button — Clean Obsidian Style */}
                            <button
                                onClick={executeQuery}
                                disabled={isExecuting}
                                className={clsx(
                                    "flex items-center gap-1.5 rounded transition-all px-2.5 py-1 text-[11px] font-medium active:scale-95",
                                    isExecuting
                                        ? "bg-white/[0.02] text-obsidian-muted cursor-not-allowed border border-transparent"
                                        : "bg-white/[0.02] border border-white/5 text-obsidian-muted hover:text-white hover:bg-white/[0.05]"
                                )}
                                title="Execute (⌘+Enter)"
                            >
                                {isExecuting ? (
                                    <Loader2 className="w-3.5 h-3.5 animate-spin" />
                                ) : (
                                    <Play className="w-3.5 h-3.5 fill-current" />
                                )}
                                <span>RUN</span>
                            </button>

                            {/* Stop Button */}
                            <button
                                onClick={cancelQuery}
                                disabled={!isExecuting}
                                className={clsx(
                                    "flex items-center justify-center rounded transition-all w-7 h-7 active:scale-95",
                                    isExecuting ? "bg-obsidian-danger/10 text-obsidian-danger border border-obsidian-danger/20 hover:bg-obsidian-danger/20 cursor-pointer" : "text-white/20 cursor-not-allowed border border-transparent"
                                )}
                                title="Cancel"
                            >
                                <Square className="w-3 h-3 fill-current" />
                            </button>

                            {engine === 'postgres' && (
                                <>
                                    <div className="w-[1px] h-4 bg-white/10 mx-1" />
                                    <button
                                        onClick={() => executePgTransactionCommand('BEGIN')}
                                        disabled={isExecuting}
                                        className={clsx(
                                            "flex items-center gap-1.5 rounded transition-all px-2 py-1 text-[10px] font-medium active:scale-95",
                                            isExecuting ? "opacity-30 cursor-not-allowed text-obsidian-muted bg-transparent border border-transparent" : "text-amber-300/80 hover:text-amber-200 hover:bg-amber-500/10 bg-amber-500/5 border border-amber-400/20"
                                        )}
                                        title="BEGIN transaction"
                                    >
                                        <Activity className="w-3 h-3" />
                                        <span>BEGIN</span>
                                    </button>
                                    <button
                                        onClick={() => executePgTransactionCommand('COMMIT')}
                                        disabled={isExecuting}
                                        className={clsx(
                                            "flex items-center gap-1.5 rounded transition-all px-2 py-1 text-[10px] font-medium active:scale-95",
                                            isExecuting ? "opacity-30 cursor-not-allowed text-obsidian-muted bg-transparent border border-transparent" : "text-emerald-300/80 hover:text-emerald-200 hover:bg-emerald-500/10 bg-emerald-500/5 border border-emerald-400/20"
                                        )}
                                        title="COMMIT transaction"
                                    >
                                        <Check className="w-3 h-3" />
                                        <span>COMMIT</span>
                                    </button>
                                    <button
                                        onClick={() => executePgTransactionCommand('ROLLBACK')}
                                        disabled={isExecuting}
                                        className={clsx(
                                            "flex items-center gap-1.5 rounded transition-all px-2 py-1 text-[10px] font-medium active:scale-95",
                                            isExecuting ? "opacity-30 cursor-not-allowed text-obsidian-muted bg-transparent border border-transparent" : "text-red-300/80 hover:text-red-200 hover:bg-red-500/10 bg-red-500/5 border border-red-400/20"
                                        )}
                                        title="ROLLBACK transaction"
                                    >
                                        <RotateCcw className="w-3 h-3" />
                                        <span>ROLLBACK</span>
                                    </button>
                                </>
                            )}

                            {/* Divider */}
                            <div className="w-[1px] h-4 bg-white/10 mx-1" />

                            {/* Secondary actions */}
                            <button
                                onClick={formatSQL}
                                disabled={isExecuting}
                                className={clsx(
                                    "flex items-center gap-1.5 rounded transition-all px-2.5 py-1 text-[11px] font-medium active:scale-95",
                                    isExecuting ? "opacity-30 cursor-not-allowed text-obsidian-muted bg-transparent border border-transparent" : "text-obsidian-muted hover:text-white hover:bg-white/[0.05] bg-white/[0.02] border border-white/5"
                                )}
                                title="Format"
                            >
                                <SlidersHorizontal className="w-3.5 h-3.5 opacity-70" />
                                <span>Format</span>
                            </button>
                            <button
                                onClick={executeExplain}
                                disabled={isExecuting}
                                className={clsx(
                                    "flex items-center gap-1.5 rounded transition-all px-2.5 py-1 text-[11px] font-medium active:scale-95",
                                    isExecuting ? "opacity-30 cursor-not-allowed text-obsidian-muted bg-transparent border border-transparent" : "text-obsidian-muted hover:text-white hover:bg-white/[0.05] bg-white/[0.02] border border-white/5"
                                )}
                                title="Explain"
                            >
                                <Code2 className="w-3.5 h-3.5 opacity-70" />
                                <span>Explain</span>
                            </button>
                            <button
                                onClick={() => { setSaveQueryName(activeTab.name); setShowSaveDialog(true); }}
                                disabled={isExecuting}
                                className={clsx(
                                    "flex items-center gap-1.5 rounded transition-all px-2.5 py-1 text-[11px] font-medium active:scale-95",
                                    isExecuting ? "opacity-30 cursor-not-allowed text-obsidian-muted bg-transparent border border-transparent" : "text-obsidian-muted hover:text-white hover:bg-white/[0.05] bg-white/[0.02] border border-white/5"
                                )}
                                title="Save"
                            >
                                <Save className="w-3.5 h-3.5 opacity-70" />
                                <span>Save</span>
                            </button>


                            {/* Divider */}
                            <div className="w-[1px] h-5 bg-obsidian-border/50 mx-1" />

                            {/* Live timer */}
                            {isExecuting && (
                                <div className="flex items-center gap-1.5 text-white/60 bg-white/5 px-2 py-1 rounded border border-white/10 text-xs">
                                    <Timer className="w-3.5 h-3.5" />
                                    <span className="font-mono tabular-nums font-medium">{(liveTimer / 1000).toFixed(1)}s</span>
                                </div>
                            )}
                            {!isExecuting && activeTab.executionTime && (
                                <div className="flex items-center gap-1.5 text-obsidian-muted text-xs font-mono bg-white/5 px-2 py-1 rounded border border-white/5">
                                    <Clock className="w-3.5 h-3.5" />
                                    <span>{activeTab.executionTime}ms</span>
                                </div>
                            )}

                            {/* Right: shortcut + limit */}
                            <div className="ml-auto flex items-center gap-3 text-xs">
                                <kbd className="text-[10px] text-obsidian-muted bg-white/5 border border-white/10 rounded px-1.5 py-0.5 font-mono shadow-inner shadow-black/50">⌘↵</kbd>
                                <div className="w-[1px] h-4 bg-obsidian-border/50" />
                                <div className="relative">
                                    <button
                                        className="flex items-center gap-1.5 rounded-md bg-white/[0.02] border border-white/5 hover:border-white/10 text-xs px-2.5 py-1.5 transition-colors active:scale-95 text-foreground/80 hover:text-white"
                                        onClick={(e) => {
                                            const rect = e.currentTarget.getBoundingClientRect();
                                            setLimitMenuPos({ top: rect.bottom + 4, right: window.innerWidth - rect.right });
                                            setShowLimitMenu(!showLimitMenu);
                                        }}
                                    >
                                        <span>Limit</span>
                                        <span className="text-obsidian-purple font-semibold">{rowLimit ?? '∞'}</span>
                                        <ChevronDown className="w-3 h-3 opacity-70" />
                                    </button>
                                    {showLimitMenu && createPortal(
                                        <>
                                            <div className="fixed inset-0 z-[9998]" onClick={() => setShowLimitMenu(false)} />
                                            <div className="fixed rounded shadow-2xl z-[9999] py-1 min-w-[140px] overflow-hidden bg-[#09090b] border border-white/10"
                                                style={{ top: limitMenuPos.top, right: limitMenuPos.right }}>
                                                {[100, 500, 1000, 5000, null].map((val) => (
                                                    <button
                                                        key={val ?? 'none'}
                                                        className={clsx(
                                                            "w-full text-left flex items-center justify-between transition-colors px-3.5 py-1.5 text-xs font-medium",
                                                            rowLimit === val ? "text-white bg-white/[0.04] border-l-2 border-obsidian-info" : "text-obsidian-muted hover:text-white hover:bg-white/[0.02] border-l-2 border-transparent"
                                                        )}
                                                        onClick={() => { setRowLimit(val); setShowLimitMenu(false); }}
                                                    >
                                                        <span>{val ?? 'No Limit'}</span>
                                                        {rowLimit === val && <span className="text-obsidian-info text-[10px]">✓</span>}
                                                    </button>
                                                ))}
                                            </div>
                                        </>,
                                        document.body
                                    )}
                                </div>
                            </div>
                        </div>

                        {/* Monaco Editor */}
                        <div className="flex-1 relative min-h-0">
                            <Editor
                                height="100%"
                                width="100%"
                                defaultLanguage="sql"
                                value={activeTab.query}
                                theme="obsidian"
                                onChange={updateQuery}
                                onMount={handleEditorMount}
                                options={{
                                    minimap: { enabled: false },
                                    fontSize: 13,
                                    fontFamily: 'JetBrains Mono, Fira Code, DM Mono, Menlo, monospace',
                                    fontLigatures: true,
                                    lineHeight: 22,
                                    letterSpacing: 0.3,
                                    padding: { top: 16, bottom: 16 },
                                    scrollBeyondLastLine: false,
                                    renderLineHighlight: 'all',
                                    automaticLayout: true,
                                    smoothScrolling: true,
                                    cursorBlinking: 'smooth',
                                    cursorSmoothCaretAnimation: 'on',
                                    cursorWidth: 2,
                                    suggestOnTriggerCharacters: true,
                                    wordBasedSuggestions: 'currentDocument',
                                    quickSuggestions: { other: true, comments: false, strings: false },
                                    acceptSuggestionOnCommitCharacter: true,
                                    suggestSelection: 'first',
                                    snippetSuggestions: 'inline',
                                    tabCompletion: 'on',
                                    tabSize: 4,
                                    bracketPairColorization: { enabled: true },
                                    guides: { bracketPairs: true, indentation: true },
                                    matchBrackets: 'always',
                                    renderWhitespace: 'selection',
                                    overviewRulerLanes: 0,
                                    hideCursorInOverviewRuler: true,
                                    overviewRulerBorder: false,
                                    folding: true,
                                    foldingStrategy: 'indentation',
                                    showFoldingControls: 'mouseover',
                                    lineNumbers: 'on',
                                    lineDecorationsWidth: 12,
                                    glyphMargin: false,
                                    fixedOverflowWidgets: true,
                                    mouseWheelZoom: true,
                                    find: {
                                        addExtraSpaceOnTop: false,
                                        autoFindInSelection: 'multiline',
                                        seedSearchStringFromSelection: 'selection',
                                    },
                                    scrollbar: {
                                        verticalScrollbarSize: 10,
                                        horizontalScrollbarSize: 10,
                                        useShadows: false,
                                        verticalHasArrows: false,
                                        horizontalHasArrows: false,
                                    },
                                    contextmenu: true,
                                    multiCursorModifier: 'alt',
                                    dragAndDrop: true,
                                    links: true,
                                    colorDecorators: true,
                                    codeLens: false,
                                    inlayHints: { enabled: 'off' },
                                }}
                            />
                        </div>
                    </div>

                    {/* 2. Resizer */}
                    <div
                        className="shrink-0 z-20 cursor-row-resize transition-colors group relative"
                        style={{ height: 5, background: 'transparent' }}
                        onMouseDown={(e) => { e.preventDefault(); setIsDragging(true); }}
                    ></div>

                    {/* 3. Bottom Panel */}
                    <div
                        className={clsx("flex flex-col min-h-0 overflow-hidden", isDragging && "pointer-events-none select-none")}
                        style={{ height: `calc(100% - ${editorHeight}px - 5px)`, background: 'transparent' }}
                    >
                        {/* Bottom Panel Header */}
                        <div className="shrink-0 flex items-center bg-black/40 backdrop-blur-xl border-t border-b border-white/5 px-2 h-9 z-10">
                            {/* Results Tab */}
                            {[{ id: 'results' as const, label: 'Results', icon: Terminal, count: activeTab.results?.data.length, isError: !!activeTab.error },
                            { id: 'history' as const, label: 'History', icon: History, count: queryHistory.length || undefined, isError: false },
                            {
                                id: 'queries' as const,
                                label: 'Queries',
                                icon: Search,
                                count: (
                                    engine === 'trino'
                                        ? trinoQueries.filter(q => ['RUNNING', 'QUEUED', 'PLANNING', 'STARTING'].includes(String(q.state).toUpperCase())).length
                                        : pgQueries.filter(q => String(q.state).toUpperCase() === 'ACTIVE').length
                                ) || undefined,
                                isError: false
                            },
                            { id: 'saved' as const, label: 'Saved', icon: Save, count: savedQueries.length || undefined, isError: false },
                            { id: 'properties' as const, label: 'Properties', icon: Info, count: undefined, isError: false }].map(tab => {
                                const Icon = tab.icon;
                                const isActive = bottomPanel === tab.id;
                                const isError = tab.id === 'results' && activeTab.error;
                                return (
                                    <button
                                        key={tab.id}
                                        onClick={() => setBottomPanel(tab.id)}
                                        className={clsx(
                                            "flex items-center gap-1.5 transition-colors relative px-2.5 h-full text-[11px]",
                                            isActive
                                                ? isError ? "text-obsidian-danger border-t-2 border-obsidian-danger font-medium" : "text-foreground/80 border-t-2 border-obsidian-info font-medium"
                                                : "text-obsidian-muted hover:text-white/70 border-t-2 border-transparent"
                                        )}
                                    >
                                        <Icon className="w-3.5 h-3.5" />
                                        <span>{tab.label}</span>
                                        {tab.count !== undefined && tab.count > 0 && (
                                            <span className={clsx(
                                                "text-[10px] font-mono px-2 py-0.5 rounded-full bg-white/[0.02] border border-white/5 ml-1",
                                                isActive ? (isError ? "text-obsidian-danger" : "text-obsidian-muted") : "text-obsidian-muted"
                                            )}>
                                                {tab.count}
                                            </span>
                                        )}
                                    </button>
                                );
                            })}

                            <div className="ml-auto flex gap-1">
                                {bottomPanel === 'results' && (
                                    <>
                                        <div className="relative">
                                            <button
                                                onClick={(e) => { e.stopPropagation(); setShowFilterMenu(prev => !prev); }}
                                                className={clsx(
                                                    "p-1.5 rounded-md transition-colors",
                                                    showFilterMenu || resultFilter
                                                        ? "bg-sky-500/15 text-sky-300"
                                                        : "hover:bg-white/5 text-obsidian-muted hover:text-white"
                                                )}
                                                title="Filter rows"
                                            >
                                                <Filter className="w-3.5 h-3.5" />
                                            </button>
                                            {showFilterMenu && (
                                                <div onClick={(e) => e.stopPropagation()} className="absolute right-0 top-full mt-1 w-64 rounded-xl shadow-2xl z-50 p-2.5" style={{ background: 'rgba(13,13,18,0.98)', border: '1px solid rgba(255,255,255,0.1)', backdropFilter: 'blur(20px)' }}>
                                                    <input
                                                        type="text"
                                                        value={resultFilter}
                                                        onChange={(e) => setResultFilter(e.target.value)}
                                                        placeholder="Filter all columns..."
                                                        className="w-full bg-white/5 border border-white/10 rounded-md px-2.5 py-1.5 text-[11px] text-white placeholder-obsidian-muted focus:outline-none focus:border-obsidian-info/50"
                                                        autoFocus
                                                    />
                                                    <div className="flex items-center justify-between mt-2 text-[10px] text-white/40">
                                                        <span>{sortedData.length} rows</span>
                                                        <button
                                                            onClick={() => { setResultFilter(''); setShowFilterMenu(false); }}
                                                            className="px-2 py-1 rounded hover:bg-white/5 text-white/60 hover:text-white"
                                                        >
                                                            Clear
                                                        </button>
                                                    </div>
                                                </div>
                                            )}
                                        </div>
                                        <button onClick={executeQuery} className="p-1.5 hover:bg-white/5 rounded-md text-obsidian-muted hover:text-white transition-colors" title="Refresh"><RefreshCw className="w-3.5 h-3.5" /></button>

                                        {/* Export Dropdown */}
                                        <div className="relative">
                                            <button
                                                onClick={() => setShowExportMenu(!showExportMenu)}
                                                className={clsx("p-1.5 rounded-md transition-colors", activeTab.results ? "hover:bg-white/5 text-obsidian-muted hover:text-white" : "text-obsidian-muted cursor-not-allowed opacity-50")}
                                                title="Export"
                                            >
                                                <Download className="w-3.5 h-3.5" />
                                            </button>
                                            {showExportMenu && activeTab.results && (
                                                <div className="absolute right-0 top-full mt-1 rounded-xl shadow-2xl z-50 py-1 w-48" style={{ background: 'rgba(13,13,18,0.98)', border: '1px solid rgba(255,255,255,0.1)', backdropFilter: 'blur(20px)' }}>
                                                    <button onClick={downloadCSV} className="w-full px-3 py-2 text-left text-[11px] hover:bg-white/5 flex items-center gap-2 transition-colors active:scale-95" style={{ color: 'rgba(255,255,255,0.55)' }}>
                                                        <FileSpreadsheet className="w-3.5 h-3.5 text-green-400" /> Export as CSV
                                                    </button>
                                                    <button onClick={downloadJSON} className="w-full px-3 py-2 text-left text-[11px] hover:bg-white/5 flex items-center gap-2 transition-colors active:scale-95" style={{ color: 'rgba(255,255,255,0.55)' }}>
                                                        <FileJson className="w-3.5 h-3.5 text-amber-400" /> Export as JSON
                                                    </button>
                                                    <button onClick={downloadSQLInsert} className="w-full px-3 py-2 text-left text-[11px] hover:bg-white/5 flex items-center gap-2 transition-colors active:scale-95" style={{ color: 'rgba(255,255,255,0.55)' }}>
                                                        <Code2 className="w-3.5 h-3.5 text-blue-400" /> Export as SQL INSERT
                                                    </button>
                                                    <div className="h-[1px] bg-white/5 my-1" />
                                                    <button onClick={copyAllToClipboard} className="w-full px-3 py-2 text-left text-[11px] hover:bg-white/5 flex items-center gap-2 transition-colors active:scale-95" style={{ color: 'rgba(255,255,255,0.55)' }}>
                                                        <Copy className="w-3.5 h-3.5 text-gray-400" /> Copy to Clipboard
                                                    </button>
                                                </div>
                                            )}
                                        </div>
                                    </>
                                )}
                                {bottomPanel === 'history' && (
                                    <button
                                        onClick={() => {
                                            requestConfirmation({
                                                title: 'Clear query history',
                                                message: 'Clear query history?',
                                                confirmLabel: 'Clear history',
                                                danger: true,
                                                onConfirm: () => clearHistory()
                                            });
                                        }}
                                        className="p-1.5 hover:bg-red-500/10 rounded-md text-obsidian-muted hover:text-red-400 transition-colors"
                                        title="Clear History"
                                    >
                                        <Trash2 className="w-3.5 h-3.5" />
                                    </button>
                                )}
                            </div>
                        </div>

                        {/* Bottom Panel Content */}
                        <div
                            className="flex-1 overflow-auto"
                            style={{ background: 'transparent' }}
                            ref={tableContainerRef}
                            onScroll={(e) => {
                                const el = e.currentTarget;
                                setScrollTop(el.scrollTop);
                                setViewportHeight(el.clientHeight);
                            }}
                        >
                            {/* ... (Existing table implementation details, keeping virtual scroll logic) */}
                            {bottomPanel === 'results' && (
                                <>
                                    {activeTab.resultSets && activeTab.resultSets.length > 1 && (
                                        <div className="sticky top-0 z-40 flex items-center gap-1 px-2 py-1.5 bg-black/60 border-b border-white/5 backdrop-blur">
                                            {activeTab.resultSets.map((set, idx) => (
                                                <button
                                                    key={`${set.name}_${idx}`}
                                                    onClick={() => setActiveResultSet(idx)}
                                                    className={clsx(
                                                        "px-2.5 py-1 rounded-md text-[10px] font-medium transition-colors",
                                                        activeTab.activeResultSetIndex === idx
                                                            ? "bg-sky-500/20 text-sky-300 border border-sky-400/30"
                                                            : "text-white/50 hover:text-white hover:bg-white/5 border border-transparent"
                                                    )}
                                                >
                                                    {set.name}
                                                </button>
                                            ))}
                                        </div>
                                    )}
                                    {activeTab.error ? (
                                        <div className="p-4">
                                            <div className="rounded-lg border border-red-400/25 bg-red-500/10 p-3">
                                                <div className="flex items-start gap-2.5">
                                                    <XCircle className="w-4 h-4 mt-0.5 text-red-300 shrink-0" />
                                                    <div className="min-w-0">
                                                        <div className="text-[11px] font-semibold tracking-wide uppercase text-red-200/90">Query Error</div>
                                                        <pre className="mt-1 text-[12px] font-mono text-red-100/90 whitespace-pre-wrap break-words">{activeTab.error}</pre>
                                                    </div>
                                                </div>
                                            </div>
                                        </div>
                                    ) : !activeTab.results ? (
                                        <div className="flex items-center justify-center h-full text-obsidian-muted text-[11px] select-none">
                                            <div className="flex flex-col items-center gap-2 opacity-50">
                                                <Terminal className="w-6 h-6" />
                                                <span>No query results yet. Press <kbd className="bg-white/10 px-1 py-0.5 rounded ml-1 font-mono text-[9px] border border-white/20 shadow-sm">⌘↵</kbd> to execute.</span>
                                            </div>
                                        </div>
                                    ) : explainPlanTree ? (
                                        <div className="p-4 overflow-auto">
                                            <div className="text-[11px] text-white/70 mb-3 font-medium">Execution Plan</div>
                                            <div className="rounded-lg border border-white/10 bg-white/[0.02] p-3">
                                                {renderExplainNode(explainPlanTree)}
                                            </div>
                                        </div>
                                    ) : explainPlanText ? (
                                        <div className="p-4 overflow-auto">
                                            <div className="text-[11px] text-white/70 mb-3 font-medium">Execution Plan (Text)</div>
                                            <pre className="rounded-lg border border-white/10 bg-white/[0.02] p-3 text-[11px] leading-5 font-mono text-white/70 whitespace-pre-wrap break-words">
                                                {explainPlanText}
                                            </pre>
                                        </div>
                                    ) : (
                                        (() => {
                                            const totalRows = sortedData.length;
                                            const startIdx = Math.max(0, Math.floor(scrollTop / ROW_HEIGHT) - OVERSCAN);
                                            const endIdx = Math.min(totalRows, Math.ceil((scrollTop + viewportHeight) / ROW_HEIGHT) + OVERSCAN);
                                            const visibleRows = sortedData.slice(startIdx, endIdx);
                                            const topPad = startIdx * ROW_HEIGHT;
                                            const bottomPad = (totalRows - endIdx) * ROW_HEIGHT;

                                            return (
                                                <div className="inline-block min-w-full">
                                                    <table className="w-full border-collapse" style={{ tableLayout: 'fixed' }}>
                                                        <thead className="sticky top-0 z-30 bg-black/20 backdrop-blur-md border-b border-white/5 text-left">
                                                            <tr>
                                                                <th className="w-10 px-1 py-2 text-center text-[10px] text-obsidian-muted/50 font-mono select-none bg-transparent sticky left-0 z-40 border-r border-white/5">#</th>
                                                                {activeTab.results.columns.map((col: string, idx: number) => {
                                                                    const width = columnWidths[col] || 150;
                                                                    const isSorted = sortConfig.column === col;
                                                                    return (
                                                                        <th
                                                                            key={col}
                                                                            className="relative p-2 px-3 text-[10px] text-obsidian-muted/70 font-medium uppercase tracking-wider group select-none hover:text-white transition-colors border-b border-white/5"
                                                                            style={{ width, minWidth: 60, maxWidth: 800 }}
                                                                            onClick={() => toggleSort(col)}
                                                                        >
                                                                            <div className="flex justify-between items-center gap-2">
                                                                                <span className="truncate tracking-wide">{col}</span>
                                                                                <span className="flex-shrink-0 text-white/30 opacity-0 group-hover:opacity-100 transition-opacity">
                                                                                    {isSorted ? (
                                                                                        sortConfig.direction === 'asc' ? <ArrowUp className="w-3.5 h-3.5 text-sky-400" /> :
                                                                                            sortConfig.direction === 'desc' ? <ArrowDown className="w-3.5 h-3.5 text-sky-400" /> :
                                                                                                <ArrowUpDown className="w-3.5 h-3.5" />
                                                                                    ) : <ArrowUpDown className="w-3.5 h-3.5" />}
                                                                                </span>
                                                                            </div>
                                                                            <div
                                                                                className="absolute right-0 top-1 bottom-1 w-[1px] bg-white/5 group-hover:bg-white/20 transition-colors"
                                                                            />
                                                                            <div
                                                                                className="absolute right-[-2px] inset-y-0 w-2 cursor-col-resize z-40"
                                                                                onClick={(e) => e.stopPropagation()}
                                                                                onMouseDown={(e) => {
                                                                                    e.preventDefault(); e.stopPropagation();
                                                                                    setResizingCol(col);
                                                                                    setResizeStart({ x: e.clientX, width });
                                                                                }}
                                                                            />
                                                                        </th>
                                                                    );
                                                                })}
                                                            </tr>
                                                        </thead>
                                                        <tbody className="font-mono text-[11.5px] tracking-tight">
                                                            {/* Top spacer for virtual scroll */}
                                                            {topPad > 0 && (
                                                                <tr><td colSpan={activeTab.results.columns.length + 1} style={{ height: topPad, padding: 0, border: 'none' }} /></tr>
                                                            )}
                                                            {visibleRows.map((row: any, vi: number) => {
                                                                const i = startIdx + vi;
                                                                const isSelected = selectedCell?.row === i;
                                                                return (
                                                                    <tr
                                                                        key={i}
                                                                        className={clsx(
                                                                            isSelected ? "bg-white/[0.04]" : i % 2 === 0 ? 'bg-transparent' : 'bg-white/[0.015]',
                                                                            "hover:bg-white/[0.02] transition-colors group cursor-default border-b border-white/5"
                                                                        )}
                                                                        style={{ height: ROW_HEIGHT }}
                                                                    >
                                                                        <td className="w-10 px-1 text-center text-white/20 opacity-40 group-hover:opacity-100 select-none sticky left-0 bg-transparent backdrop-blur-sm z-20 transition-opacity border-r border-white/5">
                                                                            {i + 1}
                                                                        </td>
                                                                        {activeTab.results!.columns.map((col: string) => {
                                                                            const isCellSelected = selectedCell?.row === i && selectedCell?.col === col;
                                                                            return (
                                                                                <td
                                                                                    key={col}
                                                                                    className={clsx(
                                                                                        "px-3 whitespace-nowrap overflow-hidden text-ellipsis cursor-text transition-colors",
                                                                                        isCellSelected && "ring-1 ring-inset ring-white/20 bg-white/[0.04] text-white font-medium"
                                                                                    )}
                                                                                    style={{ maxWidth: columnWidths[col] || 150 }}
                                                                                    onClick={() => handleCellClick(i, col)}
                                                                                    onDoubleClick={() => {
                                                                                        const val = row[col] === null ? 'NULL' : String(row[col]);
                                                                                        copyToClipboard(val);
                                                                                    }}
                                                                                >
                                                                                    {renderCellValue(row[col])}
                                                                                </td>
                                                                            );
                                                                        })}
                                                                    </tr>
                                                                );
                                                            })}
                                                            {/* Bottom spacer for virtual scroll */}
                                                            {bottomPad > 0 && (
                                                                <tr><td colSpan={activeTab.results.columns.length + 1} style={{ height: bottomPad, padding: 0, border: 'none' }} /></tr>
                                                            )}
                                                        </tbody>
                                                    </table>
                                                </div>
                                            );
                                        })()
                                    )}
                                </>
                            )}
                            {bottomPanel === 'history' && (
                                /* Query History Panel */
                                <div className="flex flex-col h-full bg-obsidian-bg">
                                    <div className="p-3 border-b border-obsidian-border/30 flex items-center justify-between shrink-0 bg-black/40">
                                        <div className="relative w-64">
                                            <Search className="absolute left-2.5 top-2 w-3.5 h-3.5 text-obsidian-muted" />
                                            <input
                                                type="text"
                                                disabled={isExecuting}
                                                placeholder="Search history..."
                                                value={historyFilter}
                                                onChange={e => setHistoryFilter(e.target.value)}
                                                className="w-full bg-white/5 border border-white/10 rounded-md pl-8 pr-3 py-1.5 text-xs text-white placeholder-obsidian-muted focus:outline-none focus:border-obsidian-info/50 focus:ring-1 focus:ring-obsidian-info/50 transition-all shadow-inner"
                                            />
                                        </div>
                                        <button
                                            onClick={() => {
                                                requestConfirmation({
                                                    title: 'Clear query history',
                                                    message: 'Clear query history?',
                                                    confirmLabel: 'Clear history',
                                                    danger: true,
                                                    onConfirm: () => clearHistory()
                                                });
                                            }}
                                            className="text-xs flex items-center gap-1.5 text-red-400 hover:text-red-300 hover:bg-red-500/10 px-2 py-1.5 rounded transition-colors"
                                        >
                                            <Trash2 className="w-3.5 h-3.5" /> Clear
                                        </button>
                                    </div>
                                    <div className="flex-1 overflow-auto custom-scrollbar">
                                        {filteredHistory.length === 0 ? (
                                            <div className="flex items-center justify-center p-8 text-obsidian-muted text-[11px]">No history found.</div>
                                        ) : (
                                            <table className="w-full text-left border-collapse">
                                                <thead className="sticky top-0 bg-black/80 backdrop-blur z-20 shadow-sm border-b border-obsidian-border/30 text-[10px] text-obsidian-muted tracking-wider uppercase font-semibold">
                                                    <tr>
                                                        <th className="px-4 py-2 w-10 text-center">St</th>
                                                        <th className="px-4 py-2">Query</th>
                                                        <th className="px-4 py-2 w-24">Engine</th>
                                                        <th className="px-4 py-2 w-24">Rows</th>
                                                        <th className="px-4 py-2 w-24">Time</th>
                                                        <th className="px-4 py-2 w-32">Date</th>
                                                    </tr>
                                                </thead>
                                                <tbody className="text-[11px]">
                                                    {filteredHistory.map((item, idx) => (
                                                        <tr
                                                            key={item.id}
                                                            className={clsx(
                                                                "cursor-pointer transition-colors border-b border-obsidian-border/30 group",
                                                                idx % 2 === 0 ? "bg-obsidian-bg" : "bg-obsidian-panel/30",
                                                                "hover:bg-white/5"
                                                            )}
                                                            onClick={() => loadHistoryQuery(item)}
                                                            title="Click to load query"
                                                        >
                                                            <td className="px-4 py-2.5 text-center">
                                                                {item.status === 'success'
                                                                    ? <div className="w-2 h-2 rounded-full bg-white/40 mx-auto" title="Success" />
                                                                    : <div className="w-2 h-2 rounded-full bg-red-500 shadow-[0_0_8px_rgba(239,68,68,0.6)] mx-auto" title={item.error} />
                                                                }
                                                            </td>
                                                            <td className="px-4 py-2.5 font-mono truncate max-w-[200px] sm:max-w-[400px] text-white/80 group-hover:text-white transition-colors">{item.query}</td>
                                                            <td className="px-4 py-2.5">
                                                                <span className="px-2 py-0.5 rounded text-[10px] font-medium border bg-white/5 text-white/50 border-white/10">
                                                                    {item.engine}
                                                                </span>
                                                            </td>
                                                            <td className="px-4 py-2.5 text-obsidian-muted tabular-nums">{item.rowCount ?? '-'}</td>
                                                            <td className="px-4 py-2.5 text-obsidian-muted tabular-nums">{item.duration.toFixed(0)} ms</td>
                                                            <td className="px-4 py-2.5 text-obsidian-muted/60">{new Date(item.timestamp).toLocaleString(undefined, { month: 'short', day: 'numeric', hour: '2-digit', minute: '2-digit' })}</td>
                                                        </tr>
                                                    ))}
                                                </tbody>
                                            </table>
                                        )}
                                    </div>
                                </div>
                            )}

                            {/* Queries Panel */}
                            {bottomPanel === 'queries' && (
                                <div className="flex flex-col h-full bg-obsidian-bg">
                                    <div className="p-3 border-b border-obsidian-border/30 flex items-center justify-between shrink-0 bg-black/40">
                                        <span className="text-[11px] text-white/60">
                                            {engine === 'trino'
                                                ? 'Trino Active & Recent Queries'
                                                : engine === 'spark'
                                                    ? 'Spark Query Monitoring'
                                                    : 'PostgreSQL Active Queries'}
                                        </span>
                                        <button
                                            onClick={engine === 'trino' ? fetchTrinoQueries : engine === 'postgres' ? fetchPgQueries : undefined}
                                            disabled={engine === 'spark'}
                                            className="text-xs flex items-center gap-1.5 text-white/50 hover:text-white hover:bg-white/5 px-2 py-1 rounded transition-colors disabled:opacity-40 disabled:cursor-not-allowed"
                                        >
                                            <RefreshCw className={clsx("w-3.5 h-3.5", (engine === 'trino' ? trinoQueriesLoading : pgQueriesLoading) && "animate-spin")} /> Refresh
                                        </button>
                                    </div>
                                    <div className="flex-1 overflow-auto custom-scrollbar">
                                        {queriesPanelError && (
                                            <div className="m-3 rounded-md border border-red-400/25 bg-red-500/10 px-3 py-2 text-[11px] text-red-200/90">
                                                {queriesPanelError}
                                            </div>
                                        )}
                                        {engine === 'trino' ? (
                                            /* ── Trino Queries ── */
                                            trinoQueries.length === 0 ? (
                                                <div className="flex items-center justify-center p-8 text-obsidian-muted text-[11px]">No queries found.</div>
                                            ) : (
                                                <div className="h-full flex flex-col">
                                                    <div className="overflow-auto custom-scrollbar">
                                                        <table className="w-full text-left border-collapse">
                                                            <thead className="sticky top-0 bg-black/80 backdrop-blur z-20 shadow-sm border-b border-obsidian-border/30 text-[10px] text-obsidian-muted tracking-wider uppercase font-semibold">
                                                                <tr>
                                                                    <th className="px-3 py-2 w-10">St</th>
                                                                    <th className="px-3 py-2 w-24">State</th>
                                                                    <th className="px-3 py-2">SQL</th>
                                                                    <th className="px-3 py-2 w-20">Elapsed</th>
                                                                    <th className="px-3 py-2 w-20">Memory</th>
                                                                    <th className="px-3 py-2 w-20">Rows</th>
                                                                    <th className="px-3 py-2 w-20">Ops</th>
                                                                </tr>
                                                            </thead>
                                                            <tbody className="text-[11px]">
                                                                {trinoQueries.map((q, idx) => (
                                                                    <tr
                                                                        key={q.queryId}
                                                                        className={clsx(
                                                                            "border-b border-obsidian-border/30 transition-colors group",
                                                                            q.queryId === selectedTrinoQueryId ? "bg-sky-500/10" : idx % 2 === 0 ? "bg-obsidian-bg" : "bg-obsidian-panel/30",
                                                                            "hover:bg-white/5"
                                                                        )}
                                                                    >
                                                                        <td className="px-3 py-2">
                                                                            <div className={clsx(
                                                                                "w-2 h-2 rounded-full mx-auto",
                                                                                (q.state === 'RUNNING' || q.state === 'PLANNING' || q.state === 'STARTING') && "bg-blue-400 shadow-[0_0_6px_rgba(96,165,250,0.6)] animate-pulse",
                                                                                q.state === 'FINISHED' && "bg-white/30",
                                                                                q.state === 'FAILED' && "bg-red-500 shadow-[0_0_6px_rgba(239,68,68,0.6)]",
                                                                                q.state === 'QUEUED' && "bg-yellow-400",
                                                                            )} />
                                                                        </td>
                                                                        <td className="px-3 py-2">
                                                                            <span className={clsx(
                                                                                "px-1.5 py-0.5 rounded text-[9px] font-mono",
                                                                                (q.state === 'RUNNING' || q.state === 'PLANNING' || q.state === 'STARTING') && "bg-blue-500/15 text-blue-300 border border-blue-500/20",
                                                                                q.state === 'FINISHED' && "bg-white/5 text-white/40 border border-white/10",
                                                                                q.state === 'FAILED' && "bg-red-500/15 text-red-300 border border-red-500/20",
                                                                                q.state === 'QUEUED' && "bg-yellow-500/15 text-yellow-300 border border-yellow-500/20",
                                                                            )}>{q.state}</span>
                                                                        </td>
                                                                        <td className="px-3 py-2 font-mono truncate max-w-[300px] text-white/70 cursor-pointer" onClick={() => { updateQuery(q.query); setBottomPanel('results'); }} title={q.query}>{q.queryShort || q.query}</td>
                                                                        <td className="px-3 py-2 text-obsidian-muted tabular-nums font-mono">{q.elapsedTime || '-'}</td>
                                                                        <td className="px-3 py-2 text-obsidian-muted tabular-nums font-mono">{q.peakMemory || '-'}</td>
                                                                        <td className="px-3 py-2 text-obsidian-muted tabular-nums font-mono">{q.processedRows?.toLocaleString() || '-'}</td>
                                                                        <td className="px-3 py-2">
                                                                            <div className="flex items-center gap-1">
                                                                                <button
                                                                                    onClick={() => fetchTrinoQueryDetail(q.queryId)}
                                                                                    className="p-1 rounded hover:bg-sky-500/20 text-sky-300/80 hover:text-sky-200 transition-colors opacity-60 group-hover:opacity-100"
                                                                                    title="Show stage/task/operator details"
                                                                                >
                                                                                    <ListTree className="w-3 h-3" />
                                                                                </button>
                                                                                {(q.state === 'RUNNING' || q.state === 'QUEUED' || q.state === 'PLANNING' || q.state === 'STARTING') && (
                                                                                    <button
                                                                                        onClick={() => killTrinoQuery(q.queryId)}
                                                                                        className="p-1 rounded hover:bg-red-500/20 text-red-400 transition-colors opacity-60 group-hover:opacity-100"
                                                                                        title="Kill query"
                                                                                    >
                                                                                        <Square className="w-3 h-3" />
                                                                                    </button>
                                                                                )}
                                                                            </div>
                                                                        </td>
                                                                    </tr>
                                                                ))}
                                                            </tbody>
                                                        </table>
                                                    </div>

                                                    {selectedTrinoQueryId && (
                                                        <div className="border-t border-white/10 bg-black/30 p-3 space-y-2">
                                                            <div className="flex items-center justify-between">
                                                                <div className="text-[11px] font-semibold text-white/70">Query Detail: {selectedTrinoQueryId}</div>
                                                                <div className="flex items-center gap-2">
                                                                    <button
                                                                        onClick={() => fetchTrinoQueryDetail(selectedTrinoQueryId)}
                                                                        className="text-[10px] px-2 py-1 rounded hover:bg-white/5 text-white/60 hover:text-white"
                                                                    >
                                                                        Reload Detail
                                                                    </button>
                                                                    <button
                                                                        onClick={() => { setSelectedTrinoQueryId(null); setTrinoQueryDetail(null); setSelectedTrinoStageId(null); }}
                                                                        className="text-[10px] px-2 py-1 rounded hover:bg-white/5 text-white/60 hover:text-white"
                                                                    >
                                                                        Close
                                                                    </button>
                                                                </div>
                                                            </div>

                                                            {trinoQueryDetailLoading ? (
                                                                <div className="text-[11px] text-white/40 flex items-center gap-2"><Loader2 className="w-3.5 h-3.5 animate-spin" /> Loading query detail...</div>
                                                            ) : !trinoQueryDetail ? (
                                                                <div className="text-[11px] text-white/40">No detail loaded.</div>
                                                            ) : (
                                                                <div className="grid grid-cols-1 lg:grid-cols-2 gap-3">
                                                                    <div className="rounded-lg border border-white/10 bg-white/[0.02] p-2.5">
                                                                        <div className="text-[10px] uppercase tracking-wider text-white/40 mb-2">Stages</div>
                                                                        <div className="space-y-1 max-h-52 overflow-auto custom-scrollbar">
                                                                            {trinoDetailStages.map(stage => (
                                                                                <button
                                                                                    key={stage.stageId}
                                                                                    onClick={() => setSelectedTrinoStageId(stage.stageId)}
                                                                                    className={clsx(
                                                                                        "w-full text-left px-2 py-1.5 rounded border transition-colors text-[10px] font-mono",
                                                                                        selectedTrinoStage?.stageId === stage.stageId
                                                                                            ? "bg-sky-500/15 border-sky-400/30 text-sky-200"
                                                                                            : "bg-white/[0.02] border-white/10 text-white/60 hover:text-white hover:bg-white/[0.05]"
                                                                                    )}
                                                                                    style={{ paddingLeft: 8 + stage.depth * 14 }}
                                                                                >
                                                                                    <div className="flex items-center justify-between gap-2">
                                                                                        <span>Stage {stage.stageId}</span>
                                                                                        <span>{stage.completedTaskCount}/{stage.taskCount} tasks</span>
                                                                                    </div>
                                                                                    <div className="text-[9px] opacity-70">{stage.state}</div>
                                                                                </button>
                                                                            ))}
                                                                        </div>
                                                                    </div>

                                                                    <div className="rounded-lg border border-white/10 bg-white/[0.02] p-2.5">
                                                                        <div className="text-[10px] uppercase tracking-wider text-white/40 mb-2">Tasks ({selectedTrinoStageTasks.length})</div>
                                                                        <div className="max-h-52 overflow-auto custom-scrollbar">
                                                                            {selectedTrinoStageTasks.length === 0 ? (
                                                                                <div className="text-[10px] text-white/40">No task details available for this stage.</div>
                                                                            ) : (
                                                                                <table className="w-full text-[10px]">
                                                                                    <thead className="text-white/40">
                                                                                        <tr>
                                                                                            <th className="text-left py-1 pr-2">Task</th>
                                                                                            <th className="text-left py-1 pr-2">State</th>
                                                                                            <th className="text-left py-1">Node</th>
                                                                                        </tr>
                                                                                    </thead>
                                                                                    <tbody>
                                                                                        {selectedTrinoStageTasks.slice(0, 200).map((task: any, idx: number) => (
                                                                                            <tr key={`${task?.taskStatus?.taskId || task?.taskId || idx}`} className="border-t border-white/5">
                                                                                                <td className="py-1 pr-2 text-white/70 font-mono truncate max-w-[180px]">{task?.taskStatus?.taskId || task?.taskId || `task_${idx + 1}`}</td>
                                                                                                <td className="py-1 pr-2 text-white/60">{task?.taskStatus?.state || task?.state || '-'}</td>
                                                                                                <td className="py-1 text-white/40">{task?.taskStatus?.self || task?.nodeId || '-'}</td>
                                                                                            </tr>
                                                                                        ))}
                                                                                    </tbody>
                                                                                </table>
                                                                            )}
                                                                        </div>
                                                                    </div>

                                                                    <div className="rounded-lg border border-white/10 bg-white/[0.02] p-2.5 lg:col-span-2">
                                                                        <div className="text-[10px] uppercase tracking-wider text-white/40 mb-2">Operators ({selectedTrinoStageOperators.length})</div>
                                                                        <div className="max-h-44 overflow-auto custom-scrollbar">
                                                                            {selectedTrinoStageOperators.length === 0 ? (
                                                                                <div className="text-[10px] text-white/40">No operator summaries available for this stage.</div>
                                                                            ) : (
                                                                                <table className="w-full text-[10px]">
                                                                                    <thead className="text-white/40">
                                                                                        <tr>
                                                                                            <th className="text-left py-1 pr-2">Operator</th>
                                                                                            <th className="text-left py-1 pr-2">Input Rows</th>
                                                                                            <th className="text-left py-1">CPU</th>
                                                                                        </tr>
                                                                                    </thead>
                                                                                    <tbody>
                                                                                        {selectedTrinoStageOperators.slice(0, 300).map((op: any, idx: number) => (
                                                                                            <tr key={`${op.operatorId || op.planNodeId || idx}`} className="border-t border-white/5">
                                                                                                <td className="py-1 pr-2 text-white/70">{op.operatorType || op.operator || op.planNodeId || `operator_${idx + 1}`}</td>
                                                                                                <td className="py-1 pr-2 text-white/60">{(op.inputPositions ?? op.addInputCalls ?? op.inputRows ?? '-').toString()}</td>
                                                                                                <td className="py-1 text-white/40">{op.totalCpuTime || op.cpuTime || '-'}</td>
                                                                                            </tr>
                                                                                        ))}
                                                                                    </tbody>
                                                                                </table>
                                                                            )}
                                                                        </div>
                                                                    </div>
                                                                </div>
                                                            )}
                                                        </div>
                                                    )}
                                                </div>
                                            )
                                        ) : engine === 'spark' ? (
                                            <div className="flex h-full items-center justify-center p-8 text-obsidian-muted text-[11px]">
                                                Spark query list/cancel metrics are not wired yet. Query execution works via `/api/spark`.
                                            </div>
                                        ) : (
                                            /* ── PostgreSQL Queries ── */
                                            pgQueries.length === 0 ? (
                                                <div className="flex items-center justify-center p-8 text-obsidian-muted text-[11px]">No active queries found.</div>
                                            ) : (
                                                <table className="w-full text-left border-collapse">
                                                    <thead className="sticky top-0 bg-black/80 backdrop-blur z-20 shadow-sm border-b border-obsidian-border/30 text-[10px] text-obsidian-muted tracking-wider uppercase font-semibold">
                                                        <tr>
                                                            <th className="px-3 py-2 w-10">St</th>
                                                            <th className="px-3 py-2 w-24">State</th>
                                                            <th className="px-3 py-2 w-16">PID</th>
                                                            <th className="px-3 py-2">SQL</th>
                                                            <th className="px-3 py-2 w-20">User</th>
                                                            <th className="px-3 py-2 w-20">Elapsed</th>
                                                            <th className="px-3 py-2 w-12"></th>
                                                        </tr>
                                                    </thead>
                                                    <tbody className="text-[11px]">
                                                        {pgQueries.map((q, idx) => (
                                                            <tr
                                                                key={q.pid}
                                                                className={clsx(
                                                                    "border-b border-obsidian-border/30 transition-colors group",
                                                                    idx % 2 === 0 ? "bg-obsidian-bg" : "bg-obsidian-panel/30",
                                                                    "hover:bg-white/5"
                                                                )}
                                                            >
                                                                <td className="px-3 py-2">
                                                                    <div className={clsx(
                                                                        "w-2 h-2 rounded-full mx-auto",
                                                                        q.state === 'ACTIVE' && "bg-blue-400 shadow-[0_0_6px_rgba(96,165,250,0.6)] animate-pulse",
                                                                        q.state === 'IDLE' && "bg-white/30",
                                                                        q.state === 'IDLE IN TRANSACTION' && "bg-yellow-400 shadow-[0_0_6px_rgba(234,179,8,0.6)]",
                                                                    )} />
                                                                </td>
                                                                <td className="px-3 py-2">
                                                                    <span className={clsx(
                                                                        "px-1.5 py-0.5 rounded text-[9px] font-mono",
                                                                        q.state === 'ACTIVE' && "bg-blue-500/15 text-blue-300 border border-blue-500/20",
                                                                        q.state === 'IDLE' && "bg-white/5 text-white/40 border border-white/10",
                                                                        q.state === 'IDLE IN TRANSACTION' && "bg-yellow-500/15 text-yellow-300 border border-yellow-500/20",
                                                                    )}>{q.state}</span>
                                                                </td>
                                                                <td className="px-3 py-2 font-mono text-obsidian-muted">{q.pid}</td>
                                                                <td className="px-3 py-2 font-mono truncate max-w-[300px] text-white/70 cursor-pointer" onClick={() => { updateQuery(q.query); setBottomPanel('results'); }} title={q.query}>{q.query}</td>
                                                                <td className="px-3 py-2 text-obsidian-muted">{q.username}</td>
                                                                <td className="px-3 py-2 text-obsidian-muted tabular-nums font-mono">{q.elapsedTime || '-'}</td>
                                                                <td className="px-3 py-2">
                                                                    {q.state === 'ACTIVE' && (
                                                                        <button
                                                                            onClick={() => killPgQuery(q.pid)}
                                                                            className="p-1 rounded hover:bg-red-500/20 text-red-400 transition-colors opacity-0 group-hover:opacity-100"
                                                                            title="Cancel query (terminate fallback)"
                                                                        >
                                                                            <Square className="w-3 h-3" />
                                                                        </button>
                                                                    )}
                                                                </td>
                                                            </tr>
                                                        ))}
                                                    </tbody>
                                                </table>
                                            )
                                        )}
                                    </div>
                                </div>
                            )}

                            {/* Saved Queries Panel */}
                            {bottomPanel === 'saved' && (
                                <div className="flex flex-col h-full bg-obsidian-bg">
                                    <div className="p-3 border-b border-obsidian-border/30 flex items-center justify-between shrink-0 bg-black/40">
                                        <span className="text-[11px] text-white/60">Saved Queries ({savedQueries.length})</span>
                                        <button onClick={() => { setSaveQueryName(activeTab.name); setShowSaveDialog(true); }} className="text-xs flex items-center gap-1.5 text-obsidian-info/70 hover:text-obsidian-info hover:bg-obsidian-info/5 px-2 py-1 rounded transition-colors">
                                            <Save className="w-3.5 h-3.5" /> Save Current
                                        </button>
                                    </div>
                                    <div className="flex-1 overflow-auto custom-scrollbar">
                                        {savedQueries.length === 0 ? (
                                            <div className="flex items-center justify-center p-8 text-obsidian-muted text-[11px]">No saved queries.</div>
                                        ) : (
                                            <table className="w-full text-left border-collapse">
                                                <thead className="sticky top-0 bg-black/80 backdrop-blur z-20 shadow-sm border-b border-obsidian-border/30 text-[10px] text-obsidian-muted tracking-wider uppercase font-semibold">
                                                    <tr>
                                                        <th className="px-4 py-2 w-48">Name</th>
                                                        <th className="px-4 py-2">SQL</th>
                                                        <th className="px-4 py-2 w-20">Engine</th>
                                                        <th className="px-4 py-2 w-32">Saved</th>
                                                        <th className="px-4 py-2 w-16"></th>
                                                    </tr>
                                                </thead>
                                                <tbody className="text-[11px]">
                                                    {savedQueries.map((sq, idx) => (
                                                        <tr
                                                            key={sq.id}
                                                            className={clsx(
                                                                "cursor-pointer transition-colors border-b border-obsidian-border/30 group",
                                                                idx % 2 === 0 ? "bg-obsidian-bg" : "bg-obsidian-panel/30",
                                                                "hover:bg-white/5"
                                                            )}
                                                            onClick={() => { updateQuery(sq.sql); setBottomPanel('results'); }}
                                                        >
                                                            <td className="px-4 py-2.5 text-white/80 font-medium">{sq.name}</td>
                                                            <td className="px-4 py-2.5 font-mono truncate max-w-[300px] text-white/50">{sq.sql}</td>
                                                            <td className="px-4 py-2.5">
                                                                <span className="px-2 py-0.5 rounded text-[9px] font-medium border bg-white/5 text-white/50 border-white/10">{sq.engine}</span>
                                                            </td>
                                                            <td className="px-4 py-2.5 text-obsidian-muted/60">{new Date(sq.savedAt).toLocaleString(undefined, { month: 'short', day: 'numeric', hour: '2-digit', minute: '2-digit' })}</td>
                                                            <td className="px-4 py-2.5">
                                                                <button
                                                                    onClick={(e) => { e.stopPropagation(); deleteSavedQuery(sq.id); }}
                                                                    className="p-1 rounded hover:bg-red-500/20 text-red-400/50 hover:text-red-400 transition-colors opacity-0 group-hover:opacity-100"
                                                                    title="Delete"
                                                                >
                                                                    <Trash2 className="w-3 h-3" />
                                                                </button>
                                                            </td>
                                                        </tr>
                                                    ))}
                                                </tbody>
                                            </table>
                                        )}
                                    </div>
                                </div>
                            )}

                            {/* ─── Properties Panel ─── */}
                            {bottomPanel === 'properties' && (
                                <div className="flex flex-col h-full bg-obsidian-bg">
                                    {/* Properties Sub-Tabs */}
                                    <div className="shrink-0 flex items-center gap-0.5 px-2 border-b border-obsidian-border/30 bg-black/40" style={{ height: 32 }}>
                                        {selectedObject && (
                                            <span className="text-[10px] text-white/40 mr-3 font-mono">{selectedObject.schema}.{selectedObject.name}</span>
                                        )}
                                        {(['columns', 'indexes', 'constraints', 'triggers', 'ddl', 'stats'] as const).map(tab => (
                                            <button
                                                key={tab}
                                                onClick={() => { setPropertiesTab(tab); }}
                                                className={clsx(
                                                    "px-2.5 py-1 text-[10px] rounded transition-colors capitalize",
                                                    propertiesTab === tab ? "bg-white/10 text-white/90" : "text-white/40 hover:text-white/60 hover:bg-white/5"
                                                )}
                                            >{tab}</button>
                                        ))}
                                    </div>
                                    {/* Properties Content */}
                                    <div className="flex-1 overflow-auto custom-scrollbar p-0">
                                        {!selectedObject ? (
                                            <div className="flex items-center justify-center h-full text-obsidian-muted text-[11px]">
                                                <Info className="w-4 h-4 mr-2 opacity-40" /> Click a table or view in the Schema Explorer to inspect its properties.
                                            </div>
                                        ) : propertiesLoading ? (
                                            <div className="flex items-center justify-center h-full text-obsidian-muted text-[11px]">
                                                <Loader2 className="w-4 h-4 mr-2 animate-spin" /> Loading...
                                            </div>
                                        ) : propertiesError ? (
                                            <div className="p-4">
                                                <div className="rounded-md border border-red-400/25 bg-red-500/10 px-3 py-2 text-[11px] text-red-200/90">
                                                    {propertiesError}
                                                </div>
                                            </div>
                                        ) : propertiesTab === 'ddl' ? (
                                            <pre className="p-4 text-[11px] font-mono text-white/70 whitespace-pre-wrap leading-relaxed">
                                                {propertiesData.ddl?.definition || '-- No DDL available'}
                                            </pre>
                                        ) : propertiesTab === 'stats' ? (
                                            <div className="p-4 grid grid-cols-3 gap-4">
                                                {propertiesData.stats && Object.entries(propertiesData.stats).map(([key, val]) => (
                                                    <div key={key} className="bg-white/5 rounded-lg p-3 border border-white/5">
                                                        <div className="text-[9px] text-white/30 uppercase tracking-wider mb-1">{key.replace(/_/g, ' ')}</div>
                                                        <div className="text-[13px] text-white/80 font-medium">{val === null ? <span className="italic text-white/20">NULL</span> : String(val)}</div>
                                                    </div>
                                                ))}
                                                {(!propertiesData.stats || Object.keys(propertiesData.stats).length === 0) && (
                                                    <div className="col-span-3 text-center text-obsidian-muted text-[11px] py-4">No statistics available.</div>
                                                )}
                                            </div>
                                        ) : (
                                            /* Generic table for columns, indexes, constraints, triggers */
                                            <table className="w-full text-left border-collapse">
                                                <thead className="sticky top-0 bg-black/80 backdrop-blur z-20 border-b border-obsidian-border/30 text-[10px] text-obsidian-muted tracking-wider uppercase font-semibold">
                                                    <tr>
                                                        {propertiesData[propertiesTab] && Array.isArray(propertiesData[propertiesTab]) && propertiesData[propertiesTab].length > 0 &&
                                                            Object.keys(propertiesData[propertiesTab][0]).map((col: string) => (
                                                                <th key={col} className="px-4 py-2">{col.replace(/_/g, ' ')}</th>
                                                            ))
                                                        }
                                                    </tr>
                                                </thead>
                                                <tbody className="text-[11px]">
                                                    {propertiesData[propertiesTab] && Array.isArray(propertiesData[propertiesTab]) ? propertiesData[propertiesTab].map((row: any, idx: number) => (
                                                        <tr key={idx} className={clsx("border-b border-obsidian-border/20", idx % 2 === 0 ? "bg-obsidian-bg" : "bg-obsidian-panel/30")}>
                                                            {Object.values(row).map((val: any, ci: number) => (
                                                                <td key={ci} className="px-4 py-2 font-mono text-white/60">
                                                                    {val === null ? <span className="italic text-white/20">NULL</span> : typeof val === 'boolean' ? (val ? 'true' : 'false') : String(val)}
                                                                </td>
                                                            ))}
                                                        </tr>
                                                    )) : (
                                                        <tr><td className="px-4 py-4 text-obsidian-muted text-center" colSpan={10}>No data available.</td></tr>
                                                    )}
                                                </tbody>
                                            </table>
                                        )}
                                    </div>
                                </div>
                            )}

                            {/* Context Menu overlay (Cell copy text indicator) */}
                            {copiedCell && (
                                <div className="absolute top-4 right-4 bg-obsidian-success/20 text-obsidian-success border border-obsidian-success/30 px-3 py-1.5 rounded-md shadow-lg flex items-center gap-2 text-xs font-medium animate-in slide-in-from-top-2 fade-in">
                                    <Copy className="w-3.5 h-3.5" /> Copied!
                                </div>
                            )}
                        </div>
                    </div>
                </div>
                {/* End Floating Card Wrapper */}

                {/* ─── Status Bar ─── */}
                <div className="absolute bottom-0 left-0 right-0 flex items-center px-5 justify-between h-8 bg-black/40 backdrop-blur-xl text-[10px] text-white/40 font-mono select-none z-10 border-t border-white/5">
                    <div className="flex items-center gap-4">
                        <span>Connected to <span className="text-white/80">{ENGINE_LABELS[engine]}</span> @ {connectedHost}</span>
                        <div className="w-[1px] h-3 bg-white/10" />
                        {engine === 'trino' && activeTab.results && activeTab.results.stats ? (
                            <span className="flex items-center gap-1 text-sky-400/60">
                                <Zap className="w-3 h-3" /> {activeTab.results.stats.elapsedTimeMillis}ms
                            </span>
                        ) : engine === 'spark' && sparkInfo ? (
                            <>
                                <div className="w-[1px] h-3 bg-white/10" />
                                <span className="flex items-center gap-1 text-sky-400/60">
                                    <Zap className="w-3 h-3" /> v{sparkInfo.version} · {sparkInfo.master} · {sparkInfo.defaultDatabase}
                                </span>
                            </>
                        ) : engine === 'postgres' && pgInfo && (
                            <>
                                <div className="w-[1px] h-3 bg-white/10" />
                                <span className="flex items-center gap-1 text-sky-400/60">
                                    <Zap className="w-3 h-3" /> v{pgInfo.version} · {pgInfo.uptime} · {pgInfo.activeConnections} conns · {pgInfo.databaseSize}
                                </span>
                            </>
                        )}
                    </div>
                    <div className="flex items-center gap-4">
                        {engine === 'postgres' && activeTab.results?.stats?.inTransaction && (
                            <span className="flex items-center gap-1.5 text-amber-300/90 bg-amber-500/10 px-2 py-0.5 rounded border border-amber-400/20">
                                <Activity className="w-3 h-3" /> TX OPEN
                            </span>
                        )}
                        {activeTab.results?.stats && (
                            <span className="flex items-center gap-1.5 text-sky-400/80 bg-sky-400/10 px-2 py-0.5 rounded border border-sky-400/20 shadow-inner">
                                <Zap className="w-3 h-3 fill-current brightness-150" />
                                {activeTab.results.stats.processedBytes ? (activeTab.results.stats.processedBytes / 1024 / 1024).toFixed(2) + ' MB' : 'Cached'}
                            </span>
                        )}
                        {activeTab.results?.stats?.truncated && (
                            <span className="flex items-center gap-1.5 text-amber-300/90 bg-amber-500/10 px-2 py-0.5 rounded border border-amber-400/20">
                                <AlertTriangle className="w-3 h-3" />
                                capped at {activeTab.results.stats.maxRows} rows
                            </span>
                        )}
                        <span className="text-white/60">
                            {activeTab.results
                                ? resultFilter
                                    ? `${sortedData.length}/${activeTab.results.data.length} rows`
                                    : `${activeTab.results.data.length} rows`
                                : ''}
                        </span>
                        <div className="w-[1px] h-3 bg-white/10" />
                        <span className="text-white/60">UTF-8</span>
                    </div>
                </div>
            </div>

            {/* ─── Save Query Dialog ─── */}
            {showSaveDialog && (
                <div className="fixed inset-0 z-[9998] flex items-center justify-center bg-black/60 backdrop-blur-sm" onClick={() => setShowSaveDialog(false)}>
                    <div className="bg-[#1a1a2e]/95 border border-white/10 rounded-xl shadow-2xl p-6 w-[400px] backdrop-blur-xl" onClick={(e) => e.stopPropagation()}>
                        <h3 className="text-sm font-semibold text-white mb-4 flex items-center gap-2"><Save className="w-4 h-4 text-obsidian-info" /> Save Query</h3>
                        <input
                            type="text"
                            placeholder="Query name..."
                            value={saveQueryName}
                            onChange={(e) => setSaveQueryName(e.target.value)}
                            onKeyDown={(e) => e.key === 'Enter' && saveCurrentQuery()}
                            className="w-full bg-white/5 border border-white/10 rounded-lg px-3 py-2 text-sm text-white placeholder-obsidian-muted focus:outline-none focus:border-obsidian-info/50 focus:ring-1 focus:ring-obsidian-info/50 mb-3"
                            autoFocus
                        />
                        <div className="text-[10px] text-obsidian-muted font-mono mb-4 p-2 bg-white/5 rounded border border-white/5 max-h-20 overflow-auto">{activeTab.query.slice(0, 200)}{activeTab.query.length > 200 ? '...' : ''}</div>
                        <div className="flex gap-2 justify-end">
                            <button onClick={() => setShowSaveDialog(false)} className="px-3 py-1.5 text-xs text-white/50 hover:text-white hover:bg-white/5 rounded-lg transition-colors">Cancel</button>
                            <button onClick={saveCurrentQuery} className="px-3 py-1.5 text-xs bg-obsidian-info/20 text-obsidian-info border border-obsidian-info/30 hover:bg-obsidian-info/30 rounded-lg transition-colors font-medium">Save</button>
                        </div>
                    </div>
                </div>
            )}

            {confirmDialog && (
                <div className="fixed inset-0 z-[9998] flex items-center justify-center bg-black/60 backdrop-blur-sm" onClick={() => setConfirmDialog(null)}>
                    <div
                        className="bg-[#1a1a2e]/95 border border-white/10 rounded-xl shadow-2xl p-5 w-[420px] backdrop-blur-xl"
                        onClick={(e) => e.stopPropagation()}
                    >
                        <h3 className={clsx(
                            "text-sm font-semibold mb-2 flex items-center gap-2",
                            confirmDialog.danger ? "text-red-200" : "text-white"
                        )}>
                            {confirmDialog.danger ? <AlertTriangle className="w-4 h-4 text-red-300" /> : <Info className="w-4 h-4 text-sky-300" />}
                            {confirmDialog.title}
                        </h3>
                        <p className="text-[12px] text-white/70 leading-relaxed">{confirmDialog.message}</p>
                        <div className="flex justify-end gap-2 mt-5">
                            <button
                                onClick={() => setConfirmDialog(null)}
                                className="px-3 py-1.5 text-xs text-white/60 hover:text-white hover:bg-white/5 rounded-lg transition-colors"
                            >
                                {confirmDialog.cancelLabel || 'Cancel'}
                            </button>
                            <button
                                onClick={runConfirmedAction}
                                className={clsx(
                                    "px-3 py-1.5 text-xs rounded-lg transition-colors font-medium border",
                                    confirmDialog.danger
                                        ? "bg-red-500/20 text-red-200 border-red-400/30 hover:bg-red-500/30"
                                        : "bg-sky-500/20 text-sky-200 border-sky-400/30 hover:bg-sky-500/30"
                                )}
                            >
                                {confirmDialog.confirmLabel || 'Confirm'}
                            </button>
                        </div>
                    </div>
                </div>
            )}

            {/* ─── Context Menu Overlay ─── */}
            {contextMenu && contextMenuActions.length > 0 && (
                <div
                    className="fixed z-[9999] min-w-[180px] py-1 rounded-lg border border-white/10 bg-[#1a1a2e]/95 backdrop-blur-xl shadow-2xl"
                    style={{ top: contextMenu.y, left: contextMenu.x }}
                    onClick={(e) => e.stopPropagation()}
                >
                    {contextMenuActions.map((item: any, i: number) =>
                        item.divider ? (
                            <div key={i} className="my-1 border-t border-white/5" />
                        ) : (
                            <button
                                key={i}
                                className={clsx(
                                    "w-full flex items-center gap-2.5 px-3 py-1.5 text-[11px] transition-colors text-left",
                                    item.danger
                                        ? "text-red-400 hover:bg-red-500/10"
                                        : "text-white/70 hover:bg-white/10 hover:text-white"
                                )}
                                onClick={() => { item.action(); closeContextMenu(); }}
                            >
                                <span className="shrink-0 opacity-60">{item.icon}</span>
                                {item.label}
                            </button>
                        )
                    )}
                </div>
            )}
        </div>
    );
}
