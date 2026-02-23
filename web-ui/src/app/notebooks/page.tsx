
import React, { useState, useEffect, useCallback, useRef } from 'react';
import { Sidebar } from '@/components/Sidebar';
import clsx from 'clsx';
import {
    Play, Square, Plus, Trash2, ChevronUp, ChevronDown,
    Save, RotateCcw, Loader2, Code2, Database, BookOpen,
    FileText, Terminal, X, Check, LayoutPanelLeft, ChevronRight,
    GripVertical, MoreHorizontal, Maximize2,
    Bold, Italic, Link, Image as ImageIcon, List, Quote, Sparkles,
    RefreshCw, Search, Download, Scissors, Copy, ClipboardPaste, Keyboard,
    StopCircle, RotateCw, EyeOff, PanelLeft, Eraser, Activity, ExternalLink, Server,
    type TypeIcon
} from 'lucide-react';
import Editor, { useMonaco } from '@monaco-editor/react';
import ReactMarkdown from 'react-markdown';
import remarkGfm from 'remark-gfm';

// ─── Types ───
type CellOutput = {
    output_type: 'stream' | 'execute_result' | 'display_data' | 'error';
    name?: string;
    text?: string;
    data?: Record<string, any>;
    execution_count?: number;
    ename?: string;
    evalue?: string;
    traceback?: string[];
};

type Cell = {
    id: string;
    cell_type: 'code' | 'markdown';
    source: string;
    language: 'python' | 'sql';
    outputs: CellOutput[];
    execution_count: number | null;
    running: boolean;
};

type NotebookFile = {
    name: string;
    filename: string;
    size: number;
    modified: string;
};

type NotebookSqlSuggestionKind = 'catalog' | 'schema' | 'table' | 'view' | 'column' | 'function';

type NotebookSqlSuggestionItem = {
    label: string;
    detail: string;
    fqn: string;
    kind: NotebookSqlSuggestionKind;
};

type NotebookClusterRuntime = {
    status: string;
    active_sessions: number;
    last_used?: string | null;
    started_at?: string | null;
    last_stopped?: string | null;
    auto_stops?: number;
    spark_ui_url?: string | null;
    application_id?: string | null;
    last_error?: string | null;
};

type NotebookCluster = {
    id: string;
    label: string;
    description?: string;
    runtime_mode?: string;
    spark_master_url?: string;
    spark_remote_url?: string | null;
    spark_conf?: Record<string, string>;
    limits?: {
        max_rows?: number;
        idle_timeout_seconds?: number;
    };
    resources?: {
        executor_instances?: number;
        executor_cores?: number;
        executor_memory?: string;
        total_cores?: number;
        total_memory_mb?: number;
        shuffle_partitions?: number;
    };
    effective_resources?: {
        executor_instances?: number;
        executor_cores?: number;
        executor_memory?: string;
        total_cores?: number;
        total_memory_mb?: number;
        shuffle_partitions?: number;
    };
    available_resources?: {
        workers?: number;
        total_cores?: number;
        total_memory_mb?: number;
        total_memory?: string;
    };
    shuffle_tuning?: {
        current?: number;
        recommended?: number;
        recommended_min?: number;
        recommended_max?: number;
        status?: 'ok' | 'low' | 'high' | 'unknown' | string;
        message?: string;
    };
    runtime?: NotebookClusterRuntime;
};

type NotebookObservabilityItem = {
    cluster: NotebookCluster;
    spark_ui_url?: string | null;
    spark_master_ui_url?: string | null;
    workers?: {
        count: number;
        cores_total: number;
        cores_used: number;
        memory_total_mb: number;
        memory_used_mb: number;
    };
    active_applications?: Array<{
        id: string;
        name: string;
        executors: number;
        spark_ui_url?: string | null;
    }>;
    logs?: Array<{
        application_id: string;
        running_jobs: number;
        active_stages: number;
    }>;
};

type NotebookConfirmDialog = {
    title: string;
    message: string;
    confirmLabel?: string;
    tone?: 'default' | 'danger';
};

const formatMemoryGbFromMb = (mb: number | undefined): string => {
    if (!mb || mb <= 0) return '0G';
    return `${(mb / 1024).toFixed(mb % 1024 === 0 ? 0 : 1)}G`;
};

const clusterPowerLabel = (cluster: NotebookCluster | null | undefined): string => {
    if (!cluster) return '-';
    const source = cluster.effective_resources || cluster.resources || {};
    const available = cluster.available_resources || {};
    const availCores = Number(available.total_cores || 0);
    const availMemMb = Number(available.total_memory_mb || 0);
    const shuffle = Number(source.shuffle_partitions || 0);
    if (availCores > 0 || availMemMb > 0) {
        return `Avail ${availCores} cores · ${formatMemoryGbFromMb(availMemMb)} RAM · shuffle ${shuffle}`;
    }
    const totalCores = Number(source.total_cores || 0);
    const totalMemory = formatMemoryGbFromMb(Number(source.total_memory_mb || 0));
    return `${totalCores} cores · ${totalMemory} RAM · shuffle ${shuffle}`;
};

const clusterShuffleLabel = (cluster: NotebookCluster | null | undefined): string => {
    if (!cluster?.shuffle_tuning) return '';
    const tuning = cluster.shuffle_tuning;
    const current = Number(tuning.current || 0);
    const recommended = Number(tuning.recommended || 0);
    if (!current || !recommended) return '';
    return `shuffle ${current} (recommended ~${recommended})`;
};

const clusterShuffleWarningLabel = (cluster: NotebookCluster | null | undefined): string => {
    const status = cluster?.shuffle_tuning?.status;
    if (status === 'low') return 'Shuffle Low';
    if (status === 'high') return 'Shuffle High';
    return '';
};

const emitNotebookNotice = (message: string) => {
    window.dispatchEvent(new CustomEvent('openclaw:notebooks-notice', { detail: { message } }));
};

// ─── Helpers ───
const genId = () => Math.random().toString(36).substring(2, 10);
const quoteSqlIdentifier = (value: string) => `"${String(value).replace(/"/g, '""')}"`;
const escapeSqlLiteral = (value: string) => String(value).replace(/'/g, "''");

const readStringFromRow = (row: any, preferredKeys: string[] = []): string | null => {
    if (!row || typeof row !== 'object') return null;
    for (const key of preferredKeys) {
        const val = row[key];
        if (val !== null && val !== undefined && String(val).trim()) return String(val).trim();
    }
    for (const val of Object.values(row)) {
        if (val !== null && val !== undefined && String(val).trim()) return String(val).trim();
    }
    return null;
};

const loadMonacoLanguageTokens = async (
    monacoInstance: any,
    languageId: string,
    tokenKeys: string[]
): Promise<string[]> => {
    try {
        const registration = monacoInstance?.languages?.getLanguages?.().find((lang: any) => lang?.id === languageId);
        if (!registration || typeof registration.loader !== 'function') return [];
        const loaded = await registration.loader();
        const languageDef = loaded?.language || {};
        const tokens = new Set<string>();
        for (const key of tokenKeys) {
            const arr = languageDef?.[key];
            if (!Array.isArray(arr)) continue;
            for (const item of arr) {
                const token = String(item || '').trim();
                if (token) tokens.add(token);
            }
        }
        return Array.from(tokens);
    } catch {
        return [];
    }
};

const isPythonIdentifier = (value: string): boolean => /^[A-Za-z_][A-Za-z0-9_]*$/.test(value);

const collectPythonNotebookSymbols = (cells: Cell[]): string[] => {
    const symbols = new Set<string>();

    for (const cell of cells) {
        if (cell.cell_type !== 'code' || cell.language !== 'python') continue;
        const lines = String(cell.source || '').split('\n');

        for (const rawLine of lines) {
            const line = rawLine.trim();
            if (!line || line.startsWith('#')) continue;

            const defMatch = line.match(/^def\s+([A-Za-z_][A-Za-z0-9_]*)\s*\(/);
            if (defMatch) {
                symbols.add(defMatch[1]);
                continue;
            }

            const classMatch = line.match(/^class\s+([A-Za-z_][A-Za-z0-9_]*)\s*[\(:]/);
            if (classMatch) {
                symbols.add(classMatch[1]);
                continue;
            }

            const importMatch = line.match(/^import\s+(.+)$/);
            if (importMatch) {
                const parts = importMatch[1].split(',');
                for (const part of parts) {
                    const chunk = part.trim();
                    if (!chunk) continue;
                    const aliasMatch = chunk.match(/\s+as\s+([A-Za-z_][A-Za-z0-9_]*)$/);
                    if (aliasMatch) {
                        symbols.add(aliasMatch[1]);
                        continue;
                    }
                    const moduleName = chunk.split('.')[0]?.trim();
                    if (moduleName && isPythonIdentifier(moduleName)) symbols.add(moduleName);
                }
                continue;
            }

            const fromImportMatch = line.match(/^from\s+[A-Za-z0-9_\.]+\s+import\s+(.+)$/);
            if (fromImportMatch) {
                const parts = fromImportMatch[1].split(',');
                for (const part of parts) {
                    const chunk = part.trim();
                    if (!chunk || chunk === '*') continue;
                    const aliasMatch = chunk.match(/^([A-Za-z_][A-Za-z0-9_]*)(?:\s+as\s+([A-Za-z_][A-Za-z0-9_]*))?$/);
                    if (!aliasMatch) continue;
                    symbols.add(aliasMatch[2] || aliasMatch[1]);
                }
                continue;
            }

            const assignMatch = line.match(/^([A-Za-z_][A-Za-z0-9_]*)\s*=/);
            if (assignMatch) {
                symbols.add(assignMatch[1]);
            }
        }
    }

    return Array.from(symbols).sort((a, b) => a.localeCompare(b));
};

const INITIAL_CELL: Cell = {
    id: genId(),
    cell_type: 'code',
    source: '',
    language: 'python',
    outputs: [],
    execution_count: null,
    running: false,
};

const mapSuggestionKindToMonaco = (monaco: any, kind: NotebookSqlSuggestionKind) => {
    if (kind === 'catalog') return monaco.languages.CompletionItemKind.Module;
    if (kind === 'schema') return monaco.languages.CompletionItemKind.Struct;
    if (kind === 'table') return monaco.languages.CompletionItemKind.Value;
    if (kind === 'view') return monaco.languages.CompletionItemKind.Interface;
    if (kind === 'column') return monaco.languages.CompletionItemKind.Field;
    return monaco.languages.CompletionItemKind.Function;
};

const registerNotebookSqlAutocomplete = (
    monaco: any,
    getItems: () => NotebookSqlSuggestionItem[],
    loadColumnsForTableFqn: (tableFqn: string) => Promise<NotebookSqlSuggestionItem[]>,
    getSqlKeywords: () => string[]
) => {
    const monacoAny = monaco as any;
    monacoAny.__openclawNotebookSqlSuggestionSource = getItems;
    monacoAny.__openclawNotebookSqlColumnLoader = loadColumnsForTableFqn;
    monacoAny.__openclawNotebookSqlKeywordSource = getSqlKeywords;
    if (monacoAny.__openclawNotebookSqlAutocompleteRegistered) return;
    monacoAny.__openclawNotebookSqlAutocompleteRegistered = true;

    monaco.languages.registerCompletionItemProvider('sql', {
        triggerCharacters: [' ', '.'],
        provideCompletionItems: async (model: any, position: any) => {
            const source = monacoAny.__openclawNotebookSqlSuggestionSource;
            const metadataItems: NotebookSqlSuggestionItem[] = typeof source === 'function' ? source() : [];
            const keywordSource = monacoAny.__openclawNotebookSqlKeywordSource;
            const sqlKeywords: string[] = typeof keywordSource === 'function' ? keywordSource() : [];
            const word = model.getWordUntilPosition(position);
            const range = {
                startLineNumber: position.lineNumber,
                endLineNumber: position.lineNumber,
                startColumn: word.startColumn,
                endColumn: word.endColumn,
            };

            const textBefore = model.getValueInRange({
                startLineNumber: position.lineNumber,
                startColumn: 1,
                endLineNumber: position.lineNumber,
                endColumn: position.column,
            });
            const dotMatch = textBefore.match(/([A-Za-z0-9_]+(?:\.[A-Za-z0-9_]+)*)\.\s*$/);
            const prefix = dotMatch ? dotMatch[1].toLowerCase() : null;
            const typedPrefix = (word.word || '').toLowerCase();

            let contextItems = metadataItems;
            if (prefix) {
                const prefixDepth = prefix.split('.').length;
                contextItems = metadataItems.filter(item =>
                    item.fqn.toLowerCase().startsWith(prefix + '.')
                    && item.fqn.toLowerCase().split('.').length === prefixDepth + 1
                );

                if (contextItems.length === 0 && prefixDepth >= 3) {
                    const columnLoader = monacoAny.__openclawNotebookSqlColumnLoader;
                    if (typeof columnLoader === 'function') {
                        try {
                            contextItems = await columnLoader(prefix);
                        } catch {
                            contextItems = [];
                        }
                    }
                }
            } else if (typedPrefix) {
                contextItems = metadataItems.filter(item => item.label.toLowerCase().startsWith(typedPrefix));
            }

            const keywordSuggestions = prefix
                ? []
                : sqlKeywords
                    .filter(keyword => !typedPrefix || keyword.toLowerCase().startsWith(typedPrefix))
                    .map(keyword => ({
                        label: keyword.toUpperCase(),
                        kind: monaco.languages.CompletionItemKind.Keyword,
                        insertText: keyword.toUpperCase(),
                        detail: 'sql keyword',
                        range,
                        sortText: `0_${keyword}`,
                    }));

            const metadataSuggestions = contextItems.map(item => ({
                label: item.label,
                kind: mapSuggestionKindToMonaco(monaco, item.kind),
                insertText: item.label,
                detail: item.detail,
                range,
                sortText: `1_${item.fqn}`,
            }));

            const seen = new Set<string>();
            const suggestions = [...keywordSuggestions, ...metadataSuggestions].filter((item: any) => {
                const key = String(item.label).toLowerCase();
                if (seen.has(key)) return false;
                seen.add(key);
                return true;
            });

            return {
                suggestions,
            };
        },
    });
};

const registerNotebookPythonAutocomplete = (
    monaco: any,
    getPythonKeywords: () => string[],
    getPythonSymbols: () => string[]
) => {
    const monacoAny = monaco as any;
    monacoAny.__openclawNotebookPythonKeywordSource = getPythonKeywords;
    monacoAny.__openclawNotebookPythonSymbolSource = getPythonSymbols;
    if (monacoAny.__openclawNotebookPythonAutocompleteRegistered) return;
    monacoAny.__openclawNotebookPythonAutocompleteRegistered = true;

    monaco.languages.registerCompletionItemProvider('python', {
        triggerCharacters: ['.', '_'],
        provideCompletionItems: (model: any, position: any) => {
            const keywordSource = monacoAny.__openclawNotebookPythonKeywordSource;
            const symbolSource = monacoAny.__openclawNotebookPythonSymbolSource;
            const keywords: string[] = typeof keywordSource === 'function' ? keywordSource() : [];
            const symbols: string[] = typeof symbolSource === 'function' ? symbolSource() : [];

            const word = model.getWordUntilPosition(position);
            const typedPrefix = (word.word || '').toLowerCase();
            const range = {
                startLineNumber: position.lineNumber,
                endLineNumber: position.lineNumber,
                startColumn: word.startColumn,
                endColumn: word.endColumn,
            };

            const keywordSuggestions = keywords
                .filter(keyword => !typedPrefix || keyword.toLowerCase().startsWith(typedPrefix))
                .map(keyword => ({
                    label: keyword,
                    kind: monaco.languages.CompletionItemKind.Keyword,
                    insertText: keyword,
                    detail: 'python keyword',
                    range,
                    sortText: `0_${keyword}`,
                }));

            const symbolSuggestions = symbols
                .filter(symbol => !typedPrefix || symbol.toLowerCase().startsWith(typedPrefix))
                .map(symbol => ({
                    label: symbol,
                    kind: monaco.languages.CompletionItemKind.Variable,
                    insertText: symbol,
                    detail: 'notebook symbol',
                    range,
                    sortText: `1_${symbol}`,
                }));

            const seen = new Set<string>();
            const suggestions = [...keywordSuggestions, ...symbolSuggestions].filter((item: any) => {
                const key = String(item.label).toLowerCase();
                if (seen.has(key)) return false;
                seen.add(key);
                return true;
            });

            return { suggestions };
        },
    });
};

// ─── Native Output Renderers ───
function StreamOutput({ output }: { output: CellOutput }) {
    const isErr = output.name === 'stderr';
    return (
        <div className={clsx(
            "px-4 py-3 font-mono text-[11.5px] whitespace-pre-wrap leading-relaxed rounded-b-md border border-obsidian-border/40 shadow-inner",
            isErr ? "text-obsidian-danger bg-[#2b1010]" : "text-foreground bg-[#1e1e1e]"
        )}>
            {output.text}
        </div>
    );
}

function TableOutput({ data }: { data: { columns: string[]; rows: any[][] } }) {
    if (!data?.columns?.length) return null;
    return (
        <div className="overflow-auto max-h-[400px] border-t border-obsidian-border bg-obsidian-bg">
            <table className="w-full text-left border-collapse font-sans text-[11px]">
                <thead className="sticky top-0 bg-obsidian-panel z-10">
                    <tr>
                        <th className="px-3 py-1.5 border-b border-r border-obsidian-border font-medium text-obsidian-muted w-10 text-center">#</th>
                        {data.columns.map((col, i) => (
                            <th key={i} className="px-3 py-1.5 border-b border-r border-obsidian-border font-semibold text-obsidian-info truncate">
                                {col}
                            </th>
                        ))}
                    </tr>
                </thead>
                <tbody>
                    {data.rows.map((row, ri) => (
                        <tr key={ri} className="hover:bg-white/5 transition-colors">
                            <td className="px-3 py-1 border-b border-r border-obsidian-border text-obsidian-muted font-mono text-[10px] text-center">{ri + 1}</td>
                            {row.map((val, ci) => (
                                <td key={ci} className="px-3 py-1 border-b border-r border-obsidian-border text-foreground/90 truncate max-w-[300px]" title={String(val)}>
                                    {val === null ? <span className="text-obsidian-muted italic">null</span> : String(val)}
                                </td>
                            ))}
                        </tr>
                    ))}
                </tbody>
            </table>
            <div className="bg-obsidian-panel px-3 py-1 text-[10px] text-obsidian-muted border-b border-obsidian-border flex justify-between">
                <span>{data.rows.length} rows</span>
                <span>{data.columns.length} columns</span>
            </div>
        </div>
    );
}

function ErrorOutput({ output }: { output: CellOutput }) {
    return (
        <div className="bg-obsidian-danger/5 overflow-hidden">
            <div className="px-4 py-2 text-[11px] font-semibold text-obsidian-danger flex items-center gap-1.5">
                <X className="w-3.5 h-3.5" />
                <span>{output.ename}: {output.evalue}</span>
            </div>
            {output.traceback && output.traceback.length > 0 && (
                <div className="px-4 pb-2">
                    <pre className="text-[10px] font-mono text-obsidian-danger/70 whitespace-pre-wrap leading-relaxed">
                        {output.traceback.join('\n').replace(/\u001b\[[0-9;]*m/g, '')}
                    </pre>
                </div>
            )}
        </div>
    );
}

function ExecuteResultOutput({ output }: { output: CellOutput }) {
    const data = output.data;
    if (!data) return null;

    if (data['application/json']?.columns) return <TableOutput data={data['application/json']} />;
    if (data['text/markdown']) {
        return (
            <div className="px-4 py-2 bg-obsidian-bg prose prose-invert prose-sm max-w-none text-foreground/90">
                <ReactMarkdown remarkPlugins={[remarkGfm]}>
                    {String(data['text/markdown'])}
                </ReactMarkdown>
            </div>
        );
    }
    if (data['text/html']) {
        return (
            <div className="px-4 py-2 bg-obsidian-bg">
                <iframe
                    srcDoc={String(data['text/html'])}
                    className="w-full min-h-[120px] border border-obsidian-border rounded bg-white"
                    sandbox=""
                    title="notebook-html-output"
                />
            </div>
        );
    }
    if (data['image/png']) return <div className="px-4 py-2 bg-obsidian-bg"><img src={`data:image/png;base64,${data['image/png']}`} alt="output" className="max-w-full rounded border border-obsidian-border" /></div>;
    if (data['text/plain']) return <pre className="px-4 py-2 font-mono text-[11px] text-foreground/90 whitespace-pre-wrap bg-obsidian-bg">{data['text/plain']}</pre>;

    return null;
}

function CellOutputs({ outputs }: { outputs: CellOutput[] }) {
    if (!outputs || !outputs.length) return null;
    return (
        <div className="flex flex-col gap-2 mt-2">
            {outputs.map((output, i) => {
                switch (output.output_type) {
                    case 'stream': return <StreamOutput key={i} output={output} />;
                    case 'error': return <ErrorOutput key={i} output={output} />;
                    case 'execute_result':
                    case 'display_data':
                        return <ExecuteResultOutput key={i} output={output} />;
                    default: return null;
                }
            })}
        </div>
    );
}

// ─── Native Cell Component ───
function NotebookCell({
    cell,
    index,
    total,
    isActive,
    onActivate,
    onUpdate,
    onDelete,
    onMoveUp,
    onMoveDown,
    onRun,
    onAddBelow,
    sqlSuggestionItems,
    loadSqlColumns,
    sqlKeywordItems,
    pythonKeywordItems,
    pythonSymbolItems,
}: {
    cell: Cell;
    index: number;
    total: number;
    isActive: boolean;
    onActivate: () => void;
    onUpdate: (updates: Partial<Cell>) => void;
    onDelete: () => void;
    onMoveUp: () => void;
    onMoveDown: () => void;
    onRun: () => void;
    onAddBelow: (type: 'code' | 'markdown') => void;
    sqlSuggestionItems: NotebookSqlSuggestionItem[];
    loadSqlColumns: (tableFqn: string) => Promise<NotebookSqlSuggestionItem[]>;
    sqlKeywordItems: string[];
    pythonKeywordItems: string[];
    pythonSymbolItems: string[];
}) {
    const editorRef = useRef<any>(null);
    const monacoRef = useRef<any>(null);
    const lineCount = cell.source.split('\n').length;
    const initialHeight = Math.max(40, Math.min(lineCount * 18 + 16, 500));
    const [editorHeight, setEditorHeight] = useState(initialHeight);
    const [showPreview, setShowPreview] = useState(false);

    // Keep global completion providers in sync with latest async state.
    useEffect(() => {
        if (!monacoRef.current) return;
        registerNotebookSqlAutocomplete(monacoRef.current, () => sqlSuggestionItems, loadSqlColumns, () => sqlKeywordItems);
        registerNotebookPythonAutocomplete(monacoRef.current, () => pythonKeywordItems, () => pythonSymbolItems);
    }, [sqlSuggestionItems, loadSqlColumns, sqlKeywordItems, pythonKeywordItems, pythonSymbolItems]);

    // ─── Markdown Toolbar Actions ───
    const insertMarkdown = useCallback((type: string) => {
        const editor = editorRef.current;
        if (!editor) return;
        const model = editor.getModel();
        if (!model) return;

        const selection = editor.getSelection();
        const selectedText = model.getValueInRange(selection) || '';

        let newText = '';
        let cursorOffset = 0;

        switch (type) {
            case 'bold':
                newText = selectedText ? `**${selectedText}**` : '**bold text**';
                cursorOffset = selectedText ? 0 : -2;
                break;
            case 'italic':
                newText = selectedText ? `*${selectedText}*` : '*italic text*';
                cursorOffset = selectedText ? 0 : -1;
                break;
            case 'code':
                if (selectedText.includes('\n')) {
                    newText = `\`\`\`\n${selectedText || 'code'}\n\`\`\``;
                } else {
                    newText = selectedText ? `\`${selectedText}\`` : '`code`';
                }
                cursorOffset = selectedText ? 0 : -1;
                break;
            case 'link':
                newText = selectedText ? `[${selectedText}](url)` : '[link text](url)';
                cursorOffset = selectedText ? -1 : -1;
                break;
            case 'image':
                newText = selectedText ? `![${selectedText}](url)` : '![alt text](url)';
                cursorOffset = selectedText ? -1 : -1;
                break;
            case 'h1': {
                const line1 = selection.startLineNumber;
                const lineContent1 = model.getLineContent(line1);
                const stripped1 = lineContent1.replace(/^#+\s*/, '');
                newText = `# ${stripped1}`;
                editor.executeEdits('toolbar', [{
                    range: { startLineNumber: line1, startColumn: 1, endLineNumber: line1, endColumn: lineContent1.length + 1 },
                    text: newText,
                }]);
                editor.focus();
                return;
            }
            case 'h2': {
                const line2 = selection.startLineNumber;
                const lineContent2 = model.getLineContent(line2);
                const stripped2 = lineContent2.replace(/^#+\s*/, '');
                newText = `## ${stripped2}`;
                editor.executeEdits('toolbar', [{
                    range: { startLineNumber: line2, startColumn: 1, endLineNumber: line2, endColumn: lineContent2.length + 1 },
                    text: newText,
                }]);
                editor.focus();
                return;
            }
            case 'h3': {
                const line3 = selection.startLineNumber;
                const lineContent3 = model.getLineContent(line3);
                const stripped3 = lineContent3.replace(/^#+\s*/, '');
                newText = `### ${stripped3}`;
                editor.executeEdits('toolbar', [{
                    range: { startLineNumber: line3, startColumn: 1, endLineNumber: line3, endColumn: lineContent3.length + 1 },
                    text: newText,
                }]);
                editor.focus();
                return;
            }
            case 'list': {
                const startLine = selection.startLineNumber;
                const endLine = selection.endLineNumber;
                const edits: any[] = [];
                for (let i = startLine; i <= endLine; i++) {
                    const lc = model.getLineContent(i);
                    if (lc.startsWith('- ')) {
                        edits.push({ range: { startLineNumber: i, startColumn: 1, endLineNumber: i, endColumn: 3 }, text: '' });
                    } else {
                        edits.push({ range: { startLineNumber: i, startColumn: 1, endLineNumber: i, endColumn: 1 }, text: '- ' });
                    }
                }
                editor.executeEdits('toolbar', edits);
                editor.focus();
                return;
            }
            case 'quote': {
                const startLineQ = selection.startLineNumber;
                const endLineQ = selection.endLineNumber;
                const editsQ: any[] = [];
                for (let i = startLineQ; i <= endLineQ; i++) {
                    const lc = model.getLineContent(i);
                    if (lc.startsWith('> ')) {
                        editsQ.push({ range: { startLineNumber: i, startColumn: 1, endLineNumber: i, endColumn: 3 }, text: '' });
                    } else {
                        editsQ.push({ range: { startLineNumber: i, startColumn: 1, endLineNumber: i, endColumn: 1 }, text: '> ' });
                    }
                }
                editor.executeEdits('toolbar', editsQ);
                editor.focus();
                return;
            }
            default:
                return;
        }

        // For wrap-style operations (bold, italic, code, link, image)
        editor.executeEdits('toolbar', [{
            range: selection,
            text: newText,
        }]);
        // Move cursor smartly
        if (cursorOffset && !selectedText) {
            const pos = editor.getPosition();
            if (pos) {
                editor.setPosition({ lineNumber: pos.lineNumber, column: pos.column + cursorOffset });
            }
        }
        editor.focus();
    }, []);

    const handleEditorWillMount = (monaco: any) => {
        // Define data explorer's pristine obsidian theme
        monaco.editor.defineTheme('obsidian', {
            base: 'vs-dark',
            inherit: true,
            rules: [
                { token: 'comment', foreground: '6a737d', fontStyle: 'italic' },
                { token: 'keyword', foreground: 'c792ea', fontStyle: 'bold' },
                { token: 'keyword.sql', foreground: 'c792ea', fontStyle: 'bold' },
                { token: 'string', foreground: 'c3e88d' },
                { token: 'string.sql', foreground: 'c3e88d' },
                { token: 'number', foreground: 'f78c6c' },
                { token: 'number.sql', foreground: 'f78c6c' },
                { token: 'operator', foreground: '89ddff' },
                { token: 'identifier', foreground: 'eeffff' },
                { token: 'function', foreground: '82aaff' },
                { token: 'delimiter', foreground: '89ddff' },
                { token: 'type', foreground: 'ffcb6b' },
            ],
            colors: {
                'editor.background': '#111113',
                'editor.foreground': '#eeffff',
                'editor.selectionBackground': '#2b3040',
                'editor.lineHighlightBackground': '#1a1a1e',
                'editorCursor.foreground': '#c792ea',
                'editorWhitespace.foreground': '#2a2a30',
                'editorIndentGuide.background': '#2a2a30',
                'editorLineNumber.foreground': '#404050',
            }
        });
        registerNotebookSqlAutocomplete(monaco, () => sqlSuggestionItems, loadSqlColumns, () => sqlKeywordItems);
        registerNotebookPythonAutocomplete(monaco, () => pythonKeywordItems, () => pythonSymbolItems);
    };

    const handleEditorMount = (editor: any, monaco: any) => {
        editorRef.current = editor;
        monacoRef.current = monaco;
        monaco.editor.setTheme('obsidian');
        editor.onKeyDown((e: any) => {
            // Shift+Enter OR Cmd/Ctrl+Enter
            if ((e.shiftKey || e.metaKey || e.ctrlKey) && (e.keyCode === 3 || e.keyCode === 13)) {
                e.preventDefault();
                onRun();
            }
        });

        if (selectedLang === 'sql' || selectedLang === 'python') {
            editor.addAction({
                id: `trigger-suggest-${selectedLang}-${cell.id}`,
                label: 'Trigger Suggest',
                keybindings: [monaco.KeyMod.CtrlCmd | monaco.KeyCode.Space],
                run: () => editor.trigger('keyboard', 'editor.action.triggerSuggest', {}),
            });
        }
    };

    const selectedLang = cell.language || 'python';

    return (
        <div className="relative group/cell flex flex-col items-center w-full">
            {/* Top-Level Cell Row */}
            <div className="flex w-full max-w-[1240px] 2xl:max-w-[1440px]">

                {/* 1. Left Gutter: Drag Handle (Outside box) */}
                <div className={clsx(
                    "w-8 shrink-0 flex items-start justify-center pt-2.5 transition-opacity cursor-grab",
                    isActive ? "opacity-100" : "opacity-0 group-hover/cell:opacity-40 hover:opacity-100"
                )}>
                    <GripVertical className="w-4 h-4 text-obsidian-muted" />
                </div>

                {/* 2. Main Box: Unified bordered container for Play Button + Editor + Right Actions */}
                <div
                    className={clsx(
                        "flex-1 flex min-w-0 transition-all duration-200 rounded-md border",
                        cell.cell_type === 'markdown' && isActive ? "flex-col" : "flex-row",
                        isActive ? "border-white/10 bg-black/20 shadow-sm" : "border-transparent hover:border-white/5 bg-transparent"
                    )}
                    onClick={onActivate}
                >
                    {/* Markdown Formatting Toolbar (Visible ONLY when active AND type === markdown) */}
                    {cell.cell_type === 'markdown' && isActive && (
                        <div className="w-full flex items-center justify-between px-3 py-1.5 border-b border-white/10 bg-white/[0.02] rounded-t-lg">
                            {/* Left Side: Show preview, formatting buttons */}
                            <div className="flex items-center gap-1.5 text-obsidian-muted/80">
                                <button
                                    onClick={(e) => { e.stopPropagation(); setShowPreview(!showPreview); }}
                                    className={clsx("px-2.5 py-1 mr-1 text-[11px] font-medium rounded-md border transition-colors", showPreview ? "text-white bg-white/10 border-white/20" : "text-white/70 bg-transparent hover:bg-white/5 border-transparent")}
                                >
                                    {showPreview ? 'Hide preview' : 'Show preview'}
                                </button>
                                <button onClick={(e) => { e.stopPropagation(); insertMarkdown('bold'); }} className="w-6 h-6 flex items-center justify-center rounded hover:bg-white/10 transition-colors" title="Bold (⌘B)"><Bold className="w-3.5 h-3.5" /></button>
                                <button onClick={(e) => { e.stopPropagation(); insertMarkdown('italic'); }} className="w-6 h-6 flex items-center justify-center rounded hover:bg-white/10 transition-colors" title="Italic (⌘I)"><Italic className="w-3.5 h-3.5" /></button>
                                <button onClick={(e) => { e.stopPropagation(); insertMarkdown('code'); }} className="w-6 h-6 flex items-center justify-center rounded hover:bg-white/10 transition-colors" title="Code"><Code2 className="w-3.5 h-3.5" /></button>
                                <button onClick={(e) => { e.stopPropagation(); insertMarkdown('link'); }} className="w-6 h-6 flex items-center justify-center rounded hover:bg-white/10 transition-colors" title="Link"><Link className="w-3.5 h-3.5" /></button>
                                <button onClick={(e) => { e.stopPropagation(); insertMarkdown('image'); }} className="w-6 h-6 flex items-center justify-center rounded hover:bg-white/10 transition-colors" title="Image"><ImageIcon className="w-3.5 h-3.5" /></button>
                                <div className="w-px h-3 bg-obsidian-border/60 mx-1"></div>
                                <button onClick={(e) => { e.stopPropagation(); insertMarkdown('h1'); }} className="w-6 h-6 flex items-center justify-center rounded hover:bg-white/10 transition-colors text-[11px] font-semibold" title="Heading 1">H1</button>
                                <button onClick={(e) => { e.stopPropagation(); insertMarkdown('h2'); }} className="w-6 h-6 flex items-center justify-center rounded hover:bg-white/10 transition-colors text-[11px] font-semibold" title="Heading 2">H2</button>
                                <button onClick={(e) => { e.stopPropagation(); insertMarkdown('h3'); }} className="w-6 h-6 flex items-center justify-center rounded hover:bg-white/10 transition-colors text-[11px] font-semibold" title="Heading 3">H3</button>
                                <button onClick={(e) => { e.stopPropagation(); insertMarkdown('list'); }} className="w-6 h-6 flex items-center justify-center rounded hover:bg-white/10 transition-colors" title="List"><List className="w-3.5 h-3.5" /></button>
                                <button onClick={(e) => { e.stopPropagation(); insertMarkdown('quote'); }} className="w-6 h-6 flex items-center justify-center rounded hover:bg-white/10 transition-colors" title="Quote"><Quote className="w-3.5 h-3.5" /></button>
                            </div>

                            {/* Right Side: Markdown Badge, Sparkles, Expand, More */}
                            <div className="flex items-center gap-1.5 text-obsidian-muted">
                                <span className="px-2 py-0.5 mr-1 text-[11px] font-medium text-foreground/50 bg-[#252830] rounded border border-white/5">Markdown</span>
                                <button onClick={(e) => { e.stopPropagation(); emitNotebookNotice('AI Generate requires an LLM API key configuration. Coming soon!'); }} className="w-6 h-6 flex items-center justify-center rounded hover:bg-white/10 text-[#d7aef2] hover:text-[#f3d0ff] transition-colors" title="AI Generate"><Sparkles className="w-3.5 h-3.5" /></button>
                                <button onClick={(e) => { e.stopPropagation(); emitNotebookNotice('Expand mode coming soon!'); }} className="w-6 h-6 flex items-center justify-center rounded hover:bg-white/10 transition-colors" title="Expand"><Maximize2 className="w-3.5 h-3.5" /></button>
                                <button className="w-6 h-6 flex items-center justify-center rounded hover:bg-white/10 transition-colors" title="More options"><MoreHorizontal className="w-3.5 h-3.5" /></button>
                                <button onClick={(e) => { e.stopPropagation(); onDelete(); }} disabled={total <= 1} className="w-6 h-6 flex items-center justify-center rounded text-obsidian-danger/50 hover:text-obsidian-danger hover:bg-obsidian-danger/10 transition-colors disabled:opacity-20" title="Delete cell"><Trash2 className="w-4 h-4" /></button>
                            </div>
                        </div>
                    )}

                    <div className="flex-1 flex min-w-0 relative">
                        {/* Play Button - Fixed Top Left (Visible for code, or active markdown) */}
                        <div className={clsx(
                            "shrink-0 flex items-start pt-2 pl-2 pr-1 relative z-10 w-9 transition-opacity",
                            cell.cell_type === 'markdown' && !isActive ? "opacity-0 pointer-events-none" : "opacity-100"
                        )}>
                            <button
                                onClick={(e) => { e.stopPropagation(); onRun(); }}
                                disabled={cell.running}
                                className={clsx(
                                    "w-6 h-6 flex items-center justify-center rounded-md transition-all",
                                    cell.running
                                        ? "text-white/60"
                                        : "text-obsidian-muted hover:text-white hover:bg-white/10"
                                )}
                                title="Run cell (⌘↵ or ⇧↵)"
                            >
                                {cell.running ? <Loader2 className="w-4 h-4 animate-spin" /> : <Play className="w-4 h-4 fill-current" />}
                            </button>
                        </div>

                        {/* Editor / Content */}
                        <div className="flex-1 min-w-0 flex flex-col pt-0.5 relative">
                            {/* %md hint for markdown editor */}
                            {cell.cell_type === 'markdown' && isActive && (
                                <div className="absolute top-[8px] left-[32px] text-obsidian-muted/40 text-[12px] font-mono pointer-events-none z-10">
                                    %md
                                </div>
                            )}

                            {/* Markdown / Editor Render */}
                            {cell.cell_type === 'markdown' && !isActive ? (
                                <div className="prose prose-invert prose-sm max-w-none text-foreground/80 min-h-[40px] px-3 py-2 cursor-text">
                                    <ReactMarkdown remarkPlugins={[remarkGfm]}>
                                        {cell.source || '*Double-click or enter to edit markdown*'}
                                    </ReactMarkdown>
                                </div>
                            ) : (
                                <div className={clsx("relative", cell.cell_type === 'markdown' && showPreview && "flex gap-0")}>
                                    <div className={clsx(cell.cell_type === 'markdown' && showPreview ? "w-1/2 border-r border-obsidian-border/30" : "w-full")}>
                                        <Editor
                                            height={editorHeight}
                                            language={cell.cell_type === 'markdown' ? 'markdown' : selectedLang}
                                            theme="obsidian"
                                            value={cell.source}
                                            onChange={(v) => onUpdate({ source: v || '' })}
                                            beforeMount={handleEditorWillMount}
                                            onMount={(editor, monaco) => {
                                                handleEditorMount(editor, monaco);
                                                editor.onDidContentSizeChange((e) => {
                                                    if (e.contentHeightChanged) {
                                                        const newHeight = Math.max(40, Math.min(e.contentHeight, 500));
                                                        setEditorHeight(newHeight);
                                                        if (editorRef.current?.getContainerDomNode()) {
                                                            editorRef.current.getContainerDomNode().style.height = `${newHeight}px`;
                                                        }
                                                    }
                                                });
                                            }}
                                            options={{
                                                scrollBeyondLastLine: false,
                                                wordWrap: 'on',
                                                wrappingIndent: 'indent',
                                                minimap: { enabled: false },
                                                lineNumbers: cell.cell_type === 'markdown' ? 'off' : 'on',
                                                lineNumbersMinChars: 4,
                                                glyphMargin: false,
                                                folding: cell.cell_type === 'markdown' ? false : true,
                                                fontSize: 13,
                                                fontFamily: "'JetBrains Mono', 'Fira Code', 'Cascadia Code', monospace",
                                                tabSize: 4,
                                                renderLineHighlight: 'all',
                                                padding: { top: cell.cell_type === 'markdown' ? 24 : 7, bottom: 20 },
                                                overviewRulerBorder: false,
                                                scrollbar: { vertical: 'hidden', horizontal: 'auto', alwaysConsumeMouseWheel: false },
                                                automaticLayout: true,
                                                contextmenu: false,
                                                matchBrackets: 'near',
                                                fontLigatures: true,
                                                quickSuggestions: { other: true, comments: false, strings: false },
                                                suggestOnTriggerCharacters: true,
                                                acceptSuggestionOnEnter: 'on',
                                            }}
                                        />
                                    </div>
                                    {/* Live Preview Pane */}
                                    {cell.cell_type === 'markdown' && showPreview && (
                                        <div className="w-1/2 overflow-auto px-4 py-3 prose prose-invert prose-sm max-w-none text-foreground/80" style={{ maxHeight: editorHeight }}>
                                            <ReactMarkdown remarkPlugins={[remarkGfm]}>
                                                {cell.source || '*Start typing to see preview...*'}
                                            </ReactMarkdown>
                                        </div>
                                    )}
                                </div>
                            )}

                            {/* Outputs */}
                            {cell.outputs && cell.outputs.length > 0 && (
                                <div className="w-full mt-1 border-t border-obsidian-border/30 bg-[#0c0c0d] rounded-b">
                                    <CellOutputs outputs={cell.outputs} />
                                </div>
                            )}

                            {/* Generate Button (Markdown Only) */}
                            {cell.cell_type === 'markdown' && isActive && (
                                <div className="w-full flex justify-end px-3 py-1 text-obsidian-muted/50 text-[11px] font-sans absolute bottom-0 right-0 pointer-events-none">
                                    Generate (⌘ + I)
                                </div>
                            )}
                        </div>

                        {!(cell.cell_type === 'markdown' && isActive) && (
                            <div className={clsx(
                                "absolute top-0 right-0 flex items-center pt-1.5 pr-1.5 gap-0.5 transition-opacity z-10 bg-transparent pl-6",
                                isActive ? "opacity-100" : "opacity-0 group-hover/cell:opacity-100"
                            )}>
                                {/* Language Badge */}
                                {cell.cell_type === 'code' ? (
                                    <select
                                        value={cell.language}
                                        onChange={(e) => onUpdate({ language: e.target.value as 'python' | 'sql' })}
                                        className="text-[11px] font-medium text-foreground/70 bg-transparent hover:bg-white/5 rounded px-2 py-1 appearance-none cursor-pointer outline-none transition-colors capitalize mr-1"
                                    >
                                        <option value="python" className="bg-obsidian-panel text-foreground">Python</option>
                                        <option value="sql" className="bg-obsidian-panel text-foreground">SQL</option>
                                    </select>
                                ) : (
                                    <span className="text-[11px] font-medium text-foreground/50 px-2 py-1 mr-1">Markdown</span>
                                )}

                                <button onClick={(e) => { e.stopPropagation(); onMoveUp(); }} disabled={index === 0} className="w-6 h-6 flex items-center justify-center rounded hover:bg-white/10 text-foreground/40 hover:text-foreground disabled:opacity-20 transition-colors" title="Move up"><ChevronUp className="w-3.5 h-3.5" /></button>
                                <button onClick={(e) => { e.stopPropagation(); onMoveDown(); }} disabled={index === total - 1} className="w-6 h-6 flex items-center justify-center rounded hover:bg-white/10 text-foreground/40 hover:text-foreground disabled:opacity-20 transition-colors" title="Move down"><ChevronDown className="w-3.5 h-3.5" /></button>
                                <button className="w-6 h-6 flex items-center justify-center rounded hover:bg-white/10 text-foreground/40 hover:text-foreground transition-colors" title="More options"><MoreHorizontal className="w-3.5 h-3.5" /></button>
                                <button onClick={(e) => { e.stopPropagation(); onDelete(); }} disabled={total <= 1} className="w-6 h-6 flex items-center justify-center rounded text-obsidian-danger/50 hover:text-obsidian-danger hover:bg-obsidian-danger/10 transition-colors disabled:opacity-20" title="Delete cell"><Trash2 className="w-4 h-4" /></button>
                            </div>
                        )}
                    </div>
                </div>
            </div>

            {/* Markdown Active Bottom Hints */}
            {
                cell.cell_type === 'markdown' && isActive && (
                    <div className="w-full flex-col items-center justify-center text-obsidian-muted/40 text-[11px] font-mono mt-8 leading-loose text-center mb-4 tracking-wide">
                        [Shift+Enter] to run and move to next cell<br />
                        [Cmd+Shift+P] to open the command palette<br />
                        [Esc H] to see all keyboard shortcuts
                    </div>
                )
            }

            {/* Add Cell Divider (Databricks-style centered + button) */}
            <div className="w-full max-w-[1200px] 2xl:max-w-[1400px] h-6 relative flex items-center justify-center group/divider">
                <div className="w-full h-px bg-obsidian-border/20 group-hover/divider:bg-obsidian-border/40 transition-colors" />
                <div className="absolute top-1/2 left-1/2 -translate-x-1/2 -translate-y-1/2 flex items-center gap-2 opacity-0 group-hover/divider:opacity-100 transition-opacity z-10">
                    <button
                        onClick={(e) => { e.stopPropagation(); onAddBelow('code'); }}
                        className="flex items-center gap-1 px-2.5 py-1 bg-[#121214] border border-white/10 rounded text-[10px] text-white/50 hover:text-white hover:border-white/20 hover:bg-white/5 transition-colors shadow-sm"
                        title="Add Code Cell"
                    >
                        <Plus className="w-3.5 h-3.5" /> Code
                    </button>
                    <button
                        onClick={(e) => { e.stopPropagation(); onAddBelow('markdown'); }}
                        className="flex items-center gap-1 px-2.5 py-1 bg-[#121214] border border-white/10 rounded text-[10px] text-white/50 hover:text-white hover:border-white/20 hover:bg-white/5 transition-colors shadow-sm"
                        title="Add Markdown Cell"
                    >
                        <Plus className="w-3.5 h-3.5" /> Markdown
                    </button>
                </div>
            </div>
        </div >
    );
}

// ─── Main Native Page ───
export default function NotebooksPage() {
    const monaco = useMonaco();
    const [cells, setCells] = useState<Cell[]>([INITIAL_CELL]);
    const [activeCell, setActiveCell] = useState<string>(INITIAL_CELL.id);
    const [kernelId, setKernelId] = useState<string | null>(null);
    const [kernelStatus, setKernelStatus] = useState<'idle' | 'busy' | 'starting' | 'disconnected'>('disconnected');
    const [notebookName, setNotebookName] = useState('Untitled');
    const [isEditingName, setIsEditingName] = useState(false);
    const [originalName, setOriginalName] = useState('');
    const [openTabs, setOpenTabs] = useState<string[]>(['Untitled']);
    const [savedNotebooks, setSavedNotebooks] = useState<NotebookFile[]>([]);
    const [showFilePanel, setShowFilePanel] = useState(true);
    const [engineMode, setEngineMode] = useState<'gateway' | 'local'>('local');
    const [defaultLanguage, setDefaultLanguage] = useState<'python' | 'sql'>('python');
    const [cellClipboard, setCellClipboard] = useState<Cell | null>(null);
    const [activeMenu, setActiveMenu] = useState<string | null>(null);
    const [showShortcutsModal, setShowShortcutsModal] = useState(false);
    const [sqlSuggestionItems, setSqlSuggestionItems] = useState<NotebookSqlSuggestionItem[]>([]);
    const [sqlKeywordItems, setSqlKeywordItems] = useState<string[]>([]);
    const [pythonKeywordItems, setPythonKeywordItems] = useState<string[]>([]);
    const [clusterProfiles, setClusterProfiles] = useState<NotebookCluster[]>([]);
    const [defaultClusterId, setDefaultClusterId] = useState<string>('small');
    const [selectedClusterId, setSelectedClusterId] = useState<string>('small');
    const [attachedClusterId, setAttachedClusterId] = useState<string | null>(null);
    const [clusterAttachBusy, setClusterAttachBusy] = useState(false);
    const [clusterObservability, setClusterObservability] = useState<NotebookObservabilityItem | null>(null);
    const [clustersLoaded, setClustersLoaded] = useState(false);
    const [clusterApiAvailable, setClusterApiAvailable] = useState<boolean>(true);
    const [uiNotice, setUiNotice] = useState<{ tone: 'success' | 'error' | 'info'; message: string } | null>(null);
    const [confirmDialog, setConfirmDialog] = useState<NotebookConfirmDialog | null>(null);
    const sqlColumnCacheRef = useRef<Map<string, NotebookSqlSuggestionItem[]>>(new Map());
    const confirmResolverRef = useRef<((accepted: boolean) => void) | null>(null);
    const runningExecutionsRef = useRef(0);
    const pythonSymbolItems = React.useMemo(() => collectPythonNotebookSymbols(cells), [cells]);

    const requestConfirm = useCallback((config: NotebookConfirmDialog) => {
        return new Promise<boolean>((resolve) => {
            confirmResolverRef.current = resolve;
            setConfirmDialog(config);
        });
    }, []);

    const resolveConfirm = useCallback((accepted: boolean) => {
        const resolver = confirmResolverRef.current;
        confirmResolverRef.current = null;
        setConfirmDialog(null);
        if (resolver) resolver(accepted);
    }, []);

    useEffect(() => {
        if (!uiNotice) return;
        const timer = window.setTimeout(() => setUiNotice(null), 3500);
        return () => window.clearTimeout(timer);
    }, [uiNotice]);

    useEffect(() => {
        return () => {
            if (confirmResolverRef.current) {
                confirmResolverRef.current(false);
                confirmResolverRef.current = null;
            }
        };
    }, []);

    useEffect(() => {
        const handler = (event: Event) => {
            const detail = (event as CustomEvent<{ message?: string }>).detail;
            const message = detail?.message;
            if (!message) return;
            setUiNotice({ tone: 'info', message });
        };
        window.addEventListener('openclaw:notebooks-notice', handler);
        return () => window.removeEventListener('openclaw:notebooks-notice', handler);
    }, []);

    useEffect(() => {
        if (!monaco) return;
        let active = true;
        void (async () => {
            const [sqlTokens, pythonTokens] = await Promise.all([
                loadMonacoLanguageTokens(monaco, 'sql', ['keywords']),
                loadMonacoLanguageTokens(monaco, 'python', ['keywords', 'builtins', 'typeKeywords']),
            ]);
            if (!active) return;
            setSqlKeywordItems(sqlTokens);
            setPythonKeywordItems(pythonTokens);
        })();
        return () => {
            active = false;
        };
    }, [monaco]);

    const fetchJsonOrThrow = useCallback(async (input: RequestInfo | URL, init?: RequestInit) => {
        const resp = await fetch(input, init);
        const data = await resp.json().catch(() => ({} as any));
        if (!resp.ok) {
            const detail = data?.detail || data?.error || `HTTP ${resp.status}`;
            throw new Error(String(detail));
        }
        return data;
    }, []);

    const fetchTrinoRows = useCallback(async (query: string, catalog?: string) => {
        const payload: Record<string, any> = { query };
        if (catalog) payload.catalog = catalog;
        const data = await fetchJsonOrThrow('/api/trino', {
            method: 'POST',
            headers: { 'Content-Type': 'application/json' },
            body: JSON.stringify(payload),
        });
        return Array.isArray(data?.data) ? data.data : [];
    }, [fetchJsonOrThrow]);

    const refreshSqlSuggestions = useCallback(async () => {
        const CATALOG_PRIORITY = ['iceberg', 'lakehouse', 'delta', 'hive', 'postgres'];
        const MAX_CATALOGS = 4;
        const MAX_SCHEMAS_PER_CATALOG = 8;
        const MAX_TABLES_PER_SCHEMA = 200;

        const itemsByFqn = new Map<string, NotebookSqlSuggestionItem>();
        const addItem = (item: NotebookSqlSuggestionItem) => {
            const key = `${item.kind}:${item.fqn}`.toLowerCase();
            if (!itemsByFqn.has(key)) itemsByFqn.set(key, item);
        };

        try {
            const catalogRows = await fetchTrinoRows('SHOW CATALOGS');
            const allCatalogs = catalogRows
                .map((catalogRow: any) => readStringFromRow(catalogRow, ['Catalog', 'catalog', 'catalog_name']))
                .filter((name: string | null): name is string => Boolean(name))
                .filter((name: string, index: number, arr: string[]) => arr.findIndex((item: string) => item.toLowerCase() === name.toLowerCase()) === index);

            const prioritizedCatalogs = allCatalogs
                .sort((a: string, b: string) => {
                    const aIdx = CATALOG_PRIORITY.indexOf(a.toLowerCase());
                    const bIdx = CATALOG_PRIORITY.indexOf(b.toLowerCase());
                    const aRank = aIdx === -1 ? 999 : aIdx;
                    const bRank = bIdx === -1 ? 999 : bIdx;
                    if (aRank !== bRank) return aRank - bRank;
                    return a.localeCompare(b);
                })
                .slice(0, MAX_CATALOGS);

            for (const catalogName of prioritizedCatalogs) {

                addItem({
                    label: catalogName,
                    detail: 'catalog',
                    fqn: catalogName,
                    kind: 'catalog',
                });

                let schemaRows: any[] = [];
                try {
                    schemaRows = await fetchTrinoRows(`SHOW SCHEMAS FROM ${quoteSqlIdentifier(catalogName)}`, catalogName);
                } catch {
                    schemaRows = [];
                }

                const schemaNames = schemaRows
                    .map((schemaRow: any) => readStringFromRow(schemaRow, ['Schema', 'schema', 'schema_name']))
                    .filter((name): name is string => Boolean(name))
                    .slice(0, MAX_SCHEMAS_PER_CATALOG);

                for (const schemaName of schemaNames) {
                    const schemaFqn = `${catalogName}.${schemaName}`;

                    addItem({
                        label: schemaName,
                        detail: `schema (${catalogName})`,
                        fqn: schemaFqn,
                        kind: 'schema',
                    });

                    let tableRows: any[] = [];
                    try {
                        tableRows = await fetchTrinoRows(
                            `SELECT table_name, table_type FROM ${quoteSqlIdentifier(catalogName)}.information_schema.tables WHERE table_schema = '${escapeSqlLiteral(schemaName)}' ORDER BY table_name LIMIT ${MAX_TABLES_PER_SCHEMA}`,
                            catalogName
                        );
                    } catch {
                        try {
                            tableRows = await fetchTrinoRows(
                                `SHOW TABLES FROM ${quoteSqlIdentifier(catalogName)}.${quoteSqlIdentifier(schemaName)}`,
                                catalogName
                            );
                        } catch {
                            tableRows = [];
                        }
                    }

                    for (const tableRow of tableRows) {
                        const tableName = readStringFromRow(tableRow, ['table_name', 'Table', 'name']);
                        if (!tableName) continue;
                        const tableType = (readStringFromRow(tableRow, ['table_type']) || 'TABLE').toUpperCase();
                        const tableKind: NotebookSqlSuggestionKind = tableType.includes('VIEW') ? 'view' : 'table';
                        const tableFqn = `${schemaFqn}.${tableName}`;
                        addItem({
                            label: tableName,
                            detail: `${tableKind} (${schemaFqn})`,
                            fqn: tableFqn,
                            kind: tableKind,
                        });
                    }

                }
            }
        } catch (err) {
            console.error('Failed to refresh notebook SQL metadata:', err);
        }

        const nextItems = Array.from(itemsByFqn.values()).sort((a, b) => a.fqn.localeCompare(b.fqn));
        setSqlSuggestionItems(nextItems);
        sqlColumnCacheRef.current.clear();
    }, [fetchTrinoRows]);

    const loadSqlColumns = useCallback(async (tableFqn: string): Promise<NotebookSqlSuggestionItem[]> => {
        const normalized = tableFqn.trim();
        if (!normalized) return [];
        const cached = sqlColumnCacheRef.current.get(normalized);
        if (cached) return cached;

        const parts = normalized.split('.');
        if (parts.length < 3) return [];
        const catalog = parts[0];
        const schema = parts[1];
        const table = parts.slice(2).join('.');

        try {
            const rows = await fetchTrinoRows(
                `SHOW COLUMNS FROM ${quoteSqlIdentifier(catalog)}.${quoteSqlIdentifier(schema)}.${quoteSqlIdentifier(table)}`,
                catalog
            );
            const columns = rows
                .map((row: any) => readStringFromRow(row, ['Column', 'column', 'Field', 'name']))
                .filter(Boolean)
                .map((columnName: string | null) => ({
                    label: String(columnName),
                    detail: `column (${normalized})`,
                    fqn: `${normalized}.${String(columnName)}`,
                    kind: 'column' as const,
                }));

            sqlColumnCacheRef.current.set(normalized, columns);
            if (columns.length > 0) {
                setSqlSuggestionItems(prev => {
                    const merged = new Map<string, NotebookSqlSuggestionItem>();
                    for (const item of prev) merged.set(`${item.kind}:${item.fqn}`.toLowerCase(), item);
                    for (const item of columns) merged.set(`${item.kind}:${item.fqn}`.toLowerCase(), item);
                    return Array.from(merged.values()).sort((a, b) => a.fqn.localeCompare(b.fqn));
                });
            }
            return columns;
        } catch {
            sqlColumnCacheRef.current.set(normalized, []);
            return [];
        }
    }, [fetchTrinoRows]);

    // ─── Kernel Management ───
    const loadClusterProfiles = useCallback(async (): Promise<string> => {
        try {
            const data = await fetchJsonOrThrow('/api/notebook/clusters');
            const clusters = Array.isArray(data?.clusters) ? data.clusters as NotebookCluster[] : [];
            const defaultId = String(data?.default_cluster_id || clusters[0]?.id || 'small');
            setClusterProfiles(clusters);
            setDefaultClusterId(defaultId);
            setSelectedClusterId(prev => {
                if (prev && clusters.some(cluster => cluster.id === prev)) return prev;
                return defaultId;
            });
            setClusterApiAvailable(true);
            setClustersLoaded(true);
            return defaultId;
        } catch (err) {
            console.warn('Notebook cluster API unavailable; falling back to legacy kernel mode.', err);
            setClusterApiAvailable(false);
            const fallbackProfile: NotebookCluster = {
                id: 'default',
                label: 'Default',
                description: 'Legacy notebook backend (cluster API unavailable)',
                runtime_mode: 'legacy',
                limits: {},
                runtime: { status: 'running', active_sessions: 1 },
            };
            setClusterProfiles([fallbackProfile]);
            setDefaultClusterId('default');
            setSelectedClusterId('default');
            setClustersLoaded(true);
            return 'default';
        }
    }, [fetchJsonOrThrow]);

    const refreshClusterObservability = useCallback(async (targetClusterId?: string | null) => {
        if (!clusterApiAvailable) {
            setClusterObservability(null);
            return;
        }
        const candidate = (targetClusterId || attachedClusterId || selectedClusterId || defaultClusterId || '').trim();
        if (!candidate) return;
        try {
            const data = await fetchJsonOrThrow(`/api/notebook/observability?cluster_id=${encodeURIComponent(candidate)}`);
            const item = Array.isArray(data?.clusters) ? data.clusters[0] : null;
            setClusterObservability(item || null);
        } catch (err) {
            console.error('Failed to refresh cluster observability:', err);
        }
    }, [clusterApiAvailable, attachedClusterId, selectedClusterId, defaultClusterId, fetchJsonOrThrow]);

    const startKernel = useCallback(async (clusterId: string) => {
        const targetClusterId = (clusterId || 'small').trim();
        setKernelStatus('starting');
        runningExecutionsRef.current = 0;
        try {
            const reqInit: RequestInit = clusterApiAvailable
                ? {
                    method: 'POST',
                    headers: { 'Content-Type': 'application/json' },
                    body: JSON.stringify({ cluster_id: targetClusterId }),
                }
                : { method: 'POST' };
            const data = await fetchJsonOrThrow('/api/notebook/kernels', reqInit);
            setKernelId(data.id);
            setEngineMode(data.mode);
            setAttachedClusterId(data.cluster_id || targetClusterId || null);
            if (data.cluster_id) {
                setSelectedClusterId(String(data.cluster_id));
            }
            setKernelStatus('idle');
            await refreshClusterObservability(data.cluster_id || targetClusterId || null);
            void loadClusterProfiles().catch(() => { });
        } catch (e) {
            console.error('Failed to start kernel:', e);
            runningExecutionsRef.current = 0;
            setKernelStatus('disconnected');
        }
    }, [clusterApiAvailable, fetchJsonOrThrow, refreshClusterObservability, loadClusterProfiles]);

    const restartKernel = useCallback(async () => {
        if (!kernelId) return;
        setKernelStatus('starting');
        runningExecutionsRef.current = 0;
        try {
            const data = await fetchJsonOrThrow(`/api/notebook/kernels/${encodeURIComponent(kernelId)}/restart`, { method: 'POST' });
            setKernelStatus('idle');
            if (data?.cluster_id) setAttachedClusterId(String(data.cluster_id));
            setCells(prev => prev.map(c => ({ ...c, outputs: [], execution_count: null })));
            await refreshClusterObservability(data?.cluster_id || attachedClusterId);
            void loadClusterProfiles().catch(() => { });
        } catch (e) {
            console.error('Failed to restart kernel:', e);
            runningExecutionsRef.current = 0;
            setKernelStatus('disconnected');
        }
    }, [kernelId, fetchJsonOrThrow, attachedClusterId, refreshClusterObservability, loadClusterProfiles]);

    const interruptKernel = useCallback(async () => {
        if (!kernelId) return;
        try {
            await fetchJsonOrThrow(`/api/notebook/kernels/${encodeURIComponent(kernelId)}/interrupt`, { method: 'POST' });
        } catch (e) {
            console.error('Failed to interrupt kernel:', e);
        }
    }, [kernelId, fetchJsonOrThrow]);

    const attachCluster = useCallback(async () => {
        if (!kernelId || !selectedClusterId) return;
        if (!clusterApiAvailable) {
            setUiNotice({ tone: 'error', message: 'Bu backend sürümünde cluster attach API yok. openclaw-api containerını güncel kodla yeniden build et.' });
            return;
        }
        setClusterAttachBusy(true);
        setKernelStatus('starting');
        runningExecutionsRef.current = 0;
        try {
            const data = await fetchJsonOrThrow(`/api/notebook/kernels/${encodeURIComponent(kernelId)}/attach`, {
                method: 'POST',
                headers: { 'Content-Type': 'application/json' },
                body: JSON.stringify({
                    cluster_id: selectedClusterId,
                    restart: true,
                }),
            });
            setAttachedClusterId(data.cluster_id || selectedClusterId);
            setKernelStatus('idle');
            setCells(prev => prev.map(c => ({ ...c, outputs: [], execution_count: null })));
            await refreshClusterObservability(data.cluster_id || selectedClusterId);
            void loadClusterProfiles().catch(() => { });
        } catch (err) {
            console.error('Failed to attach cluster:', err);
            runningExecutionsRef.current = 0;
            setKernelStatus('disconnected');
            setUiNotice({ tone: 'error', message: `Failed to attach cluster: ${String(err)}` });
        } finally {
            setClusterAttachBusy(false);
        }
    }, [clusterApiAvailable, kernelId, selectedClusterId, fetchJsonOrThrow, refreshClusterObservability, loadClusterProfiles]);

    useEffect(() => {
        let active = true;
        void (async () => {
            try {
                const initialClusterId = await loadClusterProfiles();
                if (!active) return;
                await startKernel(initialClusterId);
            } catch (err) {
                console.error('Failed to initialize notebook clusters/kernel:', err);
                if (active) {
                    setKernelStatus('disconnected');
                }
            }
        })();
        return () => {
            active = false;
        };
    }, [loadClusterProfiles, startKernel]);

    useEffect(() => {
        if (!clustersLoaded) return;
        const interval = window.setInterval(() => {
            void loadClusterProfiles().catch(() => { });
        }, 30000);
        return () => window.clearInterval(interval);
    }, [clustersLoaded, loadClusterProfiles]);

    useEffect(() => {
        if (!attachedClusterId) return;
        void refreshClusterObservability(attachedClusterId);
        const interval = window.setInterval(() => {
            void refreshClusterObservability(attachedClusterId);
        }, 8000);
        return () => window.clearInterval(interval);
    }, [attachedClusterId, refreshClusterObservability]);

    useEffect(() => {
        void refreshSqlSuggestions();
        const interval = window.setInterval(() => {
            void refreshSqlSuggestions();
        }, 5 * 60 * 1000);
        return () => window.clearInterval(interval);
    }, [refreshSqlSuggestions]);

    // ─── Save / Load ───
    const loadNotebookList = useCallback(async () => {
        try {
            const data = await fetchJsonOrThrow('/api/notebook/files');
            setSavedNotebooks(data.notebooks || []);
        } catch (e) {
            console.error('Failed to load notebooks:', e);
        }
    }, [fetchJsonOrThrow]);

    useEffect(() => { loadNotebookList(); }, [loadNotebookList]);

    const saveNotebook = useCallback(async () => {
        try {
            const data = await fetchJsonOrThrow('/api/notebook/files', {
                method: 'POST',
                headers: { 'Content-Type': 'application/json' },
                body: JSON.stringify({
                    name: notebookName,
                    cells: cells.map(c => ({ id: c.id, cell_type: c.cell_type, source: c.source, language: c.language, outputs: c.outputs, execution_count: c.execution_count })),
                }),
            });
            if (data?.name && data.name !== notebookName) {
                setNotebookName(data.name);
                setOpenTabs(prev => prev.map(t => t === notebookName ? data.name : t));
            }
            loadNotebookList();
        } catch (e) {
            console.error('Failed to save:', e);
            setUiNotice({ tone: 'error', message: `Failed to save notebook: ${String(e)}` });
        }
    }, [notebookName, cells, loadNotebookList, fetchJsonOrThrow]);

    const createNewNotebook = useCallback(async () => {
        let i = 1;
        let newName = `Untitled-${i}`;
        while (savedNotebooks.some(nb => nb.name === newName)) {
            i++;
            newName = `Untitled-${i}`;
        }
        const newCells = [{ ...INITIAL_CELL, id: genId() }];

        try {
            const data = await fetchJsonOrThrow('/api/notebook/files', {
                method: 'POST',
                headers: { 'Content-Type': 'application/json' },
                body: JSON.stringify({
                    name: newName,
                    cells: newCells.map(c => ({ id: c.id, cell_type: c.cell_type, source: c.source, language: c.language, outputs: c.outputs, execution_count: c.execution_count })),
                }),
            });
            const resolvedName = data?.name || newName;

            await loadNotebookList();
            setNotebookName(resolvedName);
            setCells(newCells);
            setActiveCell(newCells[0].id);
            setOpenTabs(prev => prev.includes(resolvedName) ? prev : [...prev, resolvedName]);
            setIsEditingName(false);
        } catch (e) {
            console.error('Failed to create new notebook:', e);
            setUiNotice({ tone: 'error', message: `Failed to create notebook: ${String(e)}` });
        }
    }, [savedNotebooks, loadNotebookList, fetchJsonOrThrow]);

    const loadNotebook = useCallback(async (name: string) => {
        try {
            const data = await fetchJsonOrThrow(`/api/notebook/files/${encodeURIComponent(name)}`);
            const normalizedCells: Cell[] = (data.cells || []).map((c: any) => ({
                ...c,
                id: c.id || genId(),
                running: false,
                outputs: c.outputs || [],
            }));
            const fallbackCell: Cell = { ...INITIAL_CELL, id: genId() };
            setNotebookName(data.name || name);
            setCells(normalizedCells.length > 0 ? normalizedCells : [fallbackCell]);
            setActiveCell(normalizedCells.length > 0 ? normalizedCells[0].id : fallbackCell.id);
            setOpenTabs(prev => prev.includes(data.name || name) ? prev : [...prev, data.name || name]);
            setIsEditingName(false);
        } catch (e) {
            console.error('Failed to load notebook:', e);
            setUiNotice({ tone: 'error', message: `Failed to load notebook '${name}': ${String(e)}` });
        }
    }, [fetchJsonOrThrow]);

    // ─── Cell Operations ───
    const addCell = useCallback((afterId?: string, type: 'code' | 'markdown' = 'code') => {
        const newCell: Cell = { id: genId(), cell_type: type, source: '', language: defaultLanguage, outputs: [], execution_count: null, running: false };
        setCells(prev => {
            if (!afterId) return [...prev, newCell];
            const idx = prev.findIndex(c => c.id === afterId);
            const next = [...prev];
            next.splice(idx + 1, 0, newCell);
            return next;
        });
        setActiveCell(newCell.id);
    }, [defaultLanguage]);

    const updateCell = useCallback((id: string, updates: Partial<Cell>) => { setCells(prev => prev.map(c => c.id === id ? { ...c, ...updates } : c)); }, []);

    const deleteCell = useCallback((id: string) => {
        setCells(prev => {
            if (prev.length <= 1) return prev;
            const idx = prev.findIndex(c => c.id === id);
            const next = prev.filter(c => c.id !== id);
            if (activeCell === id) setActiveCell(next[Math.min(idx, next.length - 1)].id);
            return next;
        });
    }, [activeCell]);

    const deleteSavedNotebook = useCallback(async (name: string, e: React.MouseEvent) => {
        e.stopPropagation();
        const accepted = await requestConfirm({
            title: 'Delete notebook?',
            message: `This will permanently delete '${name}'.`,
            confirmLabel: 'Delete',
            tone: 'danger',
        });
        if (!accepted) return;

        try {
            await fetchJsonOrThrow(`/api/notebook/files/${encodeURIComponent(name)}`, { method: 'DELETE' });
            await loadNotebookList();

            const nextTabs = openTabs.filter(t => t !== name);
            if (nextTabs.length === 0) {
                setOpenTabs(['Untitled']);
                setNotebookName('Untitled');
                const newId = genId();
                setCells([{ ...INITIAL_CELL, id: newId }]);
                setActiveCell(newId);
                return;
            }

            setOpenTabs(nextTabs);
            if (name === notebookName) {
                loadNotebook(nextTabs[nextTabs.length - 1]);
            }
        } catch (err) {
            console.error('Failed to delete notebook:', err);
            setUiNotice({ tone: 'error', message: `Failed to delete '${name}': ${String(err)}` });
        }
    }, [loadNotebookList, notebookName, loadNotebook, openTabs, fetchJsonOrThrow, requestConfirm]);

    const closeTab = useCallback((name: string, e: React.MouseEvent) => {
        e.stopPropagation();

        const nextTabs = openTabs.filter(t => t !== name);
        if (nextTabs.length === 0) {
            setOpenTabs(['Untitled']);
            setNotebookName('Untitled');
            const newId = genId();
            setCells([{ ...INITIAL_CELL, id: newId }]);
            setActiveCell(newId);
        } else {
            setOpenTabs(nextTabs);
            if (name === notebookName || (isEditingName && name === originalName)) {
                loadNotebook(nextTabs[nextTabs.length - 1]);
            }
        }
    }, [openTabs, notebookName, isEditingName, originalName, loadNotebook]);

    const finalizeRename = useCallback(async () => {
        const targetName = notebookName.trim();
        if (!targetName) {
            setNotebookName(originalName || 'Untitled');
            setIsEditingName(false);
            return;
        }

        if (!originalName || originalName === targetName) {
            setNotebookName(targetName);
            setIsEditingName(false);
            return;
        }

        const existsOnDisk = savedNotebooks.some(nb => nb.name === originalName);
        if (!existsOnDisk) {
            setNotebookName(targetName);
            setOpenTabs(prev => prev.map(t => t === originalName ? targetName : t));
            setIsEditingName(false);
            return;
        }

        try {
            const data = await fetchJsonOrThrow('/api/notebook/files/rename', {
                method: 'POST',
                headers: { 'Content-Type': 'application/json' },
                body: JSON.stringify({
                    old_name: originalName,
                    new_name: targetName,
                }),
            });
            const resolvedName = data?.name || targetName;
            setNotebookName(resolvedName);
            setOpenTabs(prev => prev.map(t => t === originalName ? resolvedName : t));
            await loadNotebookList();
        } catch (err) {
            console.error('Failed to rename notebook:', err);
            setNotebookName(originalName);
            setUiNotice({ tone: 'error', message: `Failed to rename notebook: ${String(err)}` });
        } finally {
            setIsEditingName(false);
        }
    }, [originalName, notebookName, savedNotebooks, fetchJsonOrThrow, loadNotebookList]);

    const moveCell = useCallback((id: string, direction: 'up' | 'down') => {
        setCells(prev => {
            const idx = prev.findIndex(c => c.id === id);
            if (direction === 'up' && idx <= 0) return prev;
            if (direction === 'down' && idx >= prev.length - 1) return prev;
            const next = [...prev];
            const swap = direction === 'up' ? idx - 1 : idx + 1;
            [next[idx], next[swap]] = [next[swap], next[idx]];
            return next;
        });
    }, []);

    const runCell = useCallback(async (id: string) => {
        const cell = cells.find(c => c.id === id);
        if (!cell || !kernelId) return;

        runningExecutionsRef.current += 1;
        updateCell(id, { running: true, outputs: [] });
        setKernelStatus('busy');

        let codeToExecute = cell.source;
        if (cell.language === 'sql' && !codeToExecute.trimStart().startsWith('%%sql')) {
            codeToExecute = '%%sql\n' + codeToExecute;
        }

        try {
            const resp = await fetch(`/api/notebook/kernels/${encodeURIComponent(kernelId)}/execute`, {
                method: 'POST', headers: { 'Content-Type': 'application/json' }, body: JSON.stringify({ code: codeToExecute }),
            });
            const result = await resp.json().catch(() => ({} as any));
            if (!resp.ok) {
                throw new Error(result?.detail || result?.error || `HTTP ${resp.status}`);
            }
            updateCell(id, { running: false, outputs: result.outputs || [], execution_count: result.execution_count });
        } catch (e) {
            updateCell(id, { running: false, outputs: [{ output_type: 'error', ename: 'ExecutionError', evalue: String(e), traceback: [] }] });
        } finally {
            runningExecutionsRef.current = Math.max(0, runningExecutionsRef.current - 1);
            setKernelStatus(prev => {
                if (prev === 'disconnected' || prev === 'starting') return prev;
                return runningExecutionsRef.current === 0 ? 'idle' : 'busy';
            });
        }
    }, [cells, kernelId, updateCell]);

    const runAllCells = useCallback(async () => {
        for (const cell of cells) {
            if (cell.cell_type === 'code') await runCell(cell.id);
        }
    }, [cells, runCell]);

    // ─── Databricks-like: Run All Above / Below / Restart & Run All ───
    const runAllAbove = useCallback(async () => {
        const idx = cells.findIndex(c => c.id === activeCell);
        for (let i = 0; i < idx; i++) {
            if (cells[i].cell_type === 'code') await runCell(cells[i].id);
        }
    }, [cells, activeCell, runCell]);

    const runAllBelow = useCallback(async () => {
        const idx = cells.findIndex(c => c.id === activeCell);
        for (let i = idx; i < cells.length; i++) {
            if (cells[i].cell_type === 'code') await runCell(cells[i].id);
        }
    }, [cells, activeCell, runCell]);

    const restartAndRunAll = useCallback(async () => {
        const accepted = await requestConfirm({
            title: 'Restart kernel?',
            message: 'All in-memory variables will be lost, then all code cells will run.',
            confirmLabel: 'Restart & Run',
            tone: 'danger',
        });
        if (!accepted) return;
        await restartKernel();
        await runAllCells();
    }, [restartKernel, runAllCells, requestConfirm]);

    // ─── Databricks-like: Clear All Outputs ───
    const clearAllOutputs = useCallback(() => {
        setCells(prev => prev.map(c => ({ ...c, outputs: [], execution_count: null })));
    }, []);

    // ─── Databricks-like: Cut / Copy / Paste Cell ───
    const cutCellAction = useCallback(() => {
        const cell = cells.find(c => c.id === activeCell);
        if (!cell) return;
        setCellClipboard({ ...cell });
        deleteCell(activeCell);
    }, [cells, activeCell, deleteCell]);

    const copyCellAction = useCallback(() => {
        const cell = cells.find(c => c.id === activeCell);
        if (!cell) return;
        setCellClipboard({ ...cell });
    }, [cells, activeCell]);

    const pasteCellBelow = useCallback(() => {
        if (!cellClipboard) return;
        const newCell: Cell = { ...cellClipboard, id: genId(), running: false, outputs: [], execution_count: null };
        setCells(prev => {
            const idx = prev.findIndex(c => c.id === activeCell);
            const next = [...prev];
            next.splice(idx + 1, 0, newCell);
            return next;
        });
        setActiveCell(newCell.id);
    }, [cellClipboard, activeCell]);

    // ─── Databricks-like: Export Notebook as .ipynb download ───
    const exportNotebook = useCallback(async () => {
        try {
            const data = await fetchJsonOrThrow(`/api/notebook/files/${encodeURIComponent(notebookName)}`);
            const ipynbData = {
                nbformat: 4, nbformat_minor: 5,
                metadata: { kernelspec: { display_name: 'Python 3', language: 'python', name: 'python3' }, language_info: { name: 'python', version: '3.11' } },
                cells: (data.cells || []).map((c: any) => ({
                    cell_type: c.cell_type === 'code' ? 'code' : 'markdown',
                    source: c.source.split('\n').map((line: string, i: number, arr: string[]) => i < arr.length - 1 ? line + '\n' : line),
                    metadata: {},
                    ...(c.cell_type === 'code' ? { outputs: c.outputs || [], execution_count: c.execution_count } : {}),
                }))
            };
            const blob = new Blob([JSON.stringify(ipynbData, null, 2)], { type: 'application/json' });
            const url = URL.createObjectURL(blob);
            const a = document.createElement('a');
            a.href = url;
            a.download = `${notebookName}.ipynb`;
            a.click();
            URL.revokeObjectURL(url);
        } catch (e) {
            console.error('Failed to export:', e);
            setUiNotice({ tone: 'error', message: 'Failed to export notebook' });
        }
    }, [notebookName, fetchJsonOrThrow]);

    // ─── Export as .py (Databricks/VS Code percent format) ───
    const exportAsPy = useCallback(() => {
        const lines: string[] = [
            `# Databricks notebook source`,
            `# Exported from OpenClaw Workspace`,
            '',
        ];

        cells.forEach((cell, i) => {
            if (i > 0) lines.push('');

            if (cell.cell_type === 'markdown') {
                lines.push('# MAGIC %md');
                cell.source.split('\n').forEach(line => {
                    lines.push(`# MAGIC ${line}`);
                });
            } else if (cell.language === 'sql') {
                lines.push('# COMMAND ----------');
                lines.push('');
                lines.push('# MAGIC %sql');
                cell.source.split('\n').forEach(line => {
                    lines.push(`# MAGIC ${line}`);
                });
            } else {
                lines.push('# COMMAND ----------');
                lines.push('');
                lines.push(cell.source);
            }
        });

        lines.push('');

        const blob = new Blob([lines.join('\n')], { type: 'text/x-python' });
        const url = URL.createObjectURL(blob);
        const a = document.createElement('a');
        a.href = url;
        a.download = `${notebookName}.py`;
        a.click();
        URL.revokeObjectURL(url);
    }, [cells, notebookName]);

    // ─── Databricks-like: Magic command detection ───
    const handleCellSourceChange = useCallback((cellId: string, newSource: string) => {
        const firstLine = newSource.split('\n')[0].trim().toLowerCase();
        let detectedLang: 'python' | 'sql' | null = null;
        if (firstLine === '%sql' || firstLine === '%%sql') detectedLang = 'sql';
        else if (firstLine === '%python' || firstLine === '%%python') detectedLang = 'python';

        if (detectedLang) {
            updateCell(cellId, { source: newSource, language: detectedLang });
        } else {
            updateCell(cellId, { source: newSource });
        }
    }, [updateCell]);

    // ─── Close active menu when clicking outside ───
    useEffect(() => {
        if (!activeMenu) return;
        const handler = () => setActiveMenu(null);
        window.addEventListener('click', handler);
        return () => window.removeEventListener('click', handler);
    }, [activeMenu]);

    useEffect(() => {
        const handler = (e: KeyboardEvent) => {
            if ((e.metaKey || e.ctrlKey) && e.key === 's') { e.preventDefault(); saveNotebook(); }
            if ((e.metaKey || e.ctrlKey) && e.shiftKey && e.key === 'Enter') { e.preventDefault(); runAllCells(); }
        };
        window.addEventListener('keydown', handler);
        return () => window.removeEventListener('keydown', handler);
    }, [saveNotebook, runAllCells]);

    const selectedCluster = React.useMemo(
        () => clusterProfiles.find(cluster => cluster.id === selectedClusterId) || null,
        [clusterProfiles, selectedClusterId]
    );
    const attachedCluster = React.useMemo(
        () => clusterProfiles.find(cluster => cluster.id === (attachedClusterId || selectedClusterId)) || null,
        [clusterProfiles, attachedClusterId, selectedClusterId]
    );

    return (
        <div className="flex h-screen bg-[#09090b] text-foreground font-sans overflow-hidden relative" style={{ fontFamily: "'Inter', -apple-system, sans-serif" }}>

            {/* Ambient Lighting */}
            <div className="absolute top-0 left-0 w-[800px] h-[800px] bg-purple-500/5 rounded-full blur-[120px] pointer-events-none -translate-x-1/4 -translate-y-1/4 z-0" />
            <div className="absolute bottom-0 right-0 w-[600px] h-[600px] bg-sky-500/5 rounded-full blur-[100px] pointer-events-none translate-x-1/4 -translate-y-1/4 z-0" />

            <div className="relative z-10 shrink-0">
                <Sidebar />
            </div>

            {/* Left Nav Pane - Notebook Browser */}
            {showFilePanel && (
                <div className="w-[260px] bg-black/20 backdrop-blur-xl border-r border-white/5 shrink-0 flex flex-col z-10 relative shadow-[4px_0_24px_rgba(0,0,0,0.4)]">
                    <div className="h-10 border-b border-white/5 flex items-center justify-between px-4 shrink-0">
                        <div className="flex items-center gap-2">
                            <BookOpen className="w-3.5 h-3.5 text-sky-400 opacity-80" />
                            <span className="text-xs font-semibold text-white/90 tracking-wide">Notebooks</span>
                        </div>
                        <div className="flex gap-1 shrink-0">
                            <button
                                onClick={loadNotebookList}
                                className="p-1 hover:bg-white/10 rounded text-obsidian-muted hover:text-white transition-all active:scale-95"
                                title="Refresh"
                            >
                                <RefreshCw className="w-3 h-3" />
                            </button>
                            <button
                                onClick={createNewNotebook}
                                className="p-1 hover:bg-white/10 rounded text-obsidian-muted hover:text-white transition-all active:scale-95"
                                title="New notebook"
                            >
                                <Plus className="w-3 h-3" />
                            </button>
                        </div>
                    </div>
                    <div className="flex-1 overflow-auto py-2 custom-scrollbar">
                        {savedNotebooks.length === 0 ? (
                            <div className="flex flex-col items-center justify-center py-8 text-obsidian-muted gap-2">
                                <BookOpen className="w-4 h-4 opacity-50" />
                                <span className="text-[10px]">No notebooks saved.</span>
                            </div>
                        ) : (
                            <div className="flex flex-col pl-2 pr-2">
                                {savedNotebooks.map(nb => {
                                    const isActive = notebookName === nb.name;
                                    return (
                                        <div
                                            key={nb.filename}
                                            onClick={() => loadNotebook(nb.name)}
                                            className={clsx(
                                                "flex items-center gap-1.5 py-1.5 px-2 hover:bg-white/5 rounded-md transition-colors cursor-pointer group relative",
                                                isActive ? "bg-white/10" : ""
                                            )}
                                        >
                                            <FileText className={clsx(
                                                "w-3.5 h-3.5",
                                                isActive ? "text-white/80" : "text-obsidian-muted group-hover:text-white/80"
                                            )} />
                                            <div className="min-w-0 flex-1">
                                                <span className={clsx(
                                                    "truncate block text-[12px] font-medium",
                                                    isActive ? "text-white" : "text-foreground/70 group-hover:text-white"
                                                )}>{nb.name}</span>
                                            </div>
                                            <button
                                                onClick={(e) => deleteSavedNotebook(nb.name, e)}
                                                className="opacity-0 group-hover:opacity-100 p-1 hover:bg-red-500/20 text-white/40 hover:text-red-400 rounded transition-all active:scale-95"
                                                title="Delete Notebook"
                                            >
                                                <Trash2 className="w-3 h-3" />
                                            </button>
                                        </div>
                                    );
                                })}
                            </div>
                        )}
                    </div>
                </div>
            )}

            <main className="flex-1 flex flex-col min-w-0 bg-transparent backdrop-blur-xl relative z-10">
                {/* ─── Top Navigation Bar (Premium Obsidian) ─── */}
                <header className="flex items-center px-4 justify-between shrink-0 h-12 bg-[#09090b]/60 backdrop-blur-xl border-b border-white/[0.06] z-20 relative shadow-sm">
                    {/* Top edge subtle highlight */}
                    <div className="absolute top-0 left-0 right-0 h-[1px] bg-gradient-to-r from-transparent via-white/[0.05] to-transparent pointer-events-none" />

                    <div className="flex items-center gap-4">
                        {/* Toggle Sidebar Button */}
                        <button
                            onClick={() => setShowFilePanel(!showFilePanel)}
                            className="p-1.5 hover:bg-white/[0.08] rounded-md text-white/50 hover:text-white transition-all active:scale-95 flex items-center justify-center border border-transparent hover:border-white/[0.05] shadow-sm cursor-pointer"
                            title="Toggle Notebooks Explorer"
                        >
                            <LayoutPanelLeft className="w-[18px] h-[18px]" />
                        </button>

                        <div className="w-[1px] h-5 bg-gradient-to-b from-transparent via-white/10 to-transparent mx-1 rounded-full"></div>

                        <div className="flex items-center gap-0.5 relative">
                            {/* ─── Functional Menu Dropdowns (Databricks-like) ─── */}
                            {[
                                {
                                    label: 'File',
                                    items: [
                                        { label: 'New Notebook', shortcut: '', action: () => createNewNotebook(), icon: <Plus className="w-3.5 h-3.5" /> },
                                        { label: 'Save', shortcut: '⌘S', action: () => saveNotebook(), icon: <Save className="w-3.5 h-3.5" /> },
                                        { label: 'Export as .ipynb', shortcut: '', action: () => exportNotebook(), icon: <Download className="w-3.5 h-3.5" /> },
                                        { label: 'Export as .py', shortcut: '', action: () => exportAsPy(), icon: <Download className="w-3.5 h-3.5" /> },
                                        { divider: true },
                                        { label: 'Rename', shortcut: '', action: () => { setOriginalName(notebookName); setIsEditingName(true); }, icon: <FileText className="w-3.5 h-3.5" /> },
                                    ]
                                },
                                {
                                    label: 'Edit',
                                    items: [
                                        { label: 'Cut Cell', shortcut: '⌘X', action: () => cutCellAction(), icon: <Scissors className="w-3.5 h-3.5" /> },
                                        { label: 'Copy Cell', shortcut: '⌘C', action: () => copyCellAction(), icon: <Copy className="w-3.5 h-3.5" /> },
                                        { label: 'Paste Cell Below', shortcut: '⌘V', action: () => pasteCellBelow(), icon: <ClipboardPaste className="w-3.5 h-3.5" />, disabled: !cellClipboard },
                                        { divider: true },
                                        { label: 'Clear All Outputs', shortcut: '', action: () => clearAllOutputs(), icon: <Eraser className="w-3.5 h-3.5" /> },
                                        { label: 'Delete Cell', shortcut: '⌫', action: () => { if (cells.length > 1) deleteCell(activeCell); }, icon: <Trash2 className="w-3.5 h-3.5" />, disabled: cells.length <= 1 },
                                    ]
                                },
                                {
                                    label: 'View',
                                    items: [
                                        { label: showFilePanel ? 'Hide Sidebar' : 'Show Sidebar', shortcut: '', action: () => setShowFilePanel(!showFilePanel), icon: <PanelLeft className="w-3.5 h-3.5" /> },
                                    ]
                                },
                                {
                                    label: 'Run',
                                    items: [
                                        { label: 'Run Cell', shortcut: '⇧↵', action: () => runCell(activeCell), icon: <Play className="w-3.5 h-3.5" /> },
                                        { label: 'Run All', shortcut: '⌘⇧↵', action: () => runAllCells(), icon: <Play className="w-3.5 h-3.5" /> },
                                        { divider: true },
                                        { label: 'Run All Above', shortcut: '', action: () => runAllAbove(), icon: <ChevronUp className="w-3.5 h-3.5" /> },
                                        { label: 'Run All Below', shortcut: '', action: () => runAllBelow(), icon: <ChevronDown className="w-3.5 h-3.5" /> },
                                        { divider: true },
                                        { label: 'Interrupt Kernel', shortcut: '', action: () => interruptKernel(), icon: <StopCircle className="w-3.5 h-3.5" /> },
                                        {
                                            label: 'Restart Kernel',
                                            shortcut: '',
                                            action: async () => {
                                                const accepted = await requestConfirm({
                                                    title: 'Restart kernel?',
                                                    message: 'All in-memory variables will be lost.',
                                                    confirmLabel: 'Restart',
                                                    tone: 'danger',
                                                });
                                                if (accepted) await restartKernel();
                                            },
                                            icon: <RotateCw className="w-3.5 h-3.5" />
                                        },
                                        { label: 'Restart & Run All', shortcut: '', action: () => restartAndRunAll(), icon: <RotateCcw className="w-3.5 h-3.5" /> },
                                    ]
                                },
                                {
                                    label: 'Help',
                                    items: [
                                        { label: 'Keyboard Shortcuts', shortcut: '', action: () => setShowShortcutsModal(true), icon: <Keyboard className="w-3.5 h-3.5" /> },
                                    ]
                                },
                            ].map(menu => (
                                <div key={menu.label} className="relative group/menu">
                                    <button
                                        onClick={(e) => { e.stopPropagation(); setActiveMenu(activeMenu === menu.label ? null : menu.label); }}
                                        className={clsx(
                                            "px-3 py-1.5 rounded-md text-[13px] font-medium transition-all duration-200 tracking-tight",
                                            activeMenu === menu.label ? "text-white bg-white/10 shadow-inner" : "text-white/60 hover:text-white hover:bg-white/[0.06]"
                                        )}
                                    >
                                        {menu.label}
                                    </button>
                                    {activeMenu === menu.label && (
                                        <div
                                            className="absolute top-[calc(100%+4px)] left-0 w-56 bg-[#18181b]/95 border border-white/10 rounded-xl shadow-2xl z-50 overflow-hidden py-1.5 backdrop-blur-2xl ring-1 ring-black/50"
                                            onClick={(e) => e.stopPropagation()}
                                        >
                                            {(menu.items as any[]).map((item: any, i: number) =>
                                                item.divider ? (
                                                    <div key={`div-${i}`} className="h-[1px] bg-gradient-to-r from-transparent via-white/10 to-transparent my-1.5 mx-2" />
                                                ) : (
                                                    <button
                                                        key={`btn-${i}`}
                                                        onClick={() => { item.action(); setActiveMenu(null); }}
                                                        disabled={item.disabled}
                                                        className="w-full flex items-center gap-3 px-3 py-1.5 text-[12px] font-medium text-white/70 hover:text-white hover:bg-blue-500/10 transition-colors disabled:opacity-30 disabled:cursor-not-allowed group/item"
                                                    >
                                                        <span className="text-white/40 group-hover/item:text-blue-400 transition-colors">{item.icon}</span>
                                                        <span className="flex-1 text-left">{item.label}</span>
                                                        {item.shortcut && <span className="text-[10px] text-white/30 font-mono tracking-wider">{item.shortcut}</span>}
                                                    </button>
                                                )
                                            )}
                                        </div>
                                    )}
                                </div>
                            ))}

                            <div className="w-[1px] h-5 bg-gradient-to-b from-transparent via-white/10 to-transparent mx-3 rounded-full" />

                            {/* ─── Functional Default Language Selector (Databricks-like) ─── */}
                            <div className="relative">
                                <button
                                    onClick={(e) => { e.stopPropagation(); setActiveMenu(activeMenu === 'lang' ? null : 'lang'); }}
                                    className={clsx(
                                        "flex items-center gap-1.5 px-3 py-1.5 rounded-md text-[13px] font-bold tracking-wide transition-all",
                                        activeMenu === 'lang' ? "text-blue-400 bg-blue-500/10 shadow-inner" : "text-white/70 hover:text-white hover:bg-white/[0.06]"
                                    )}
                                >
                                    <span>{defaultLanguage === 'python' ? 'Python' : 'SQL'}</span>
                                    <ChevronDown className={clsx("w-3.5 h-3.5 transition-transform opacity-70", activeMenu === 'lang' && "rotate-180")} />
                                </button>
                                {activeMenu === 'lang' && (
                                    <div
                                        className="absolute top-[calc(100%+4px)] left-0 w-40 bg-[#18181b]/95 border border-white/10 rounded-xl shadow-2xl z-50 overflow-hidden py-1.5 backdrop-blur-2xl ring-1 ring-black/50"
                                        onClick={(e) => e.stopPropagation()}
                                    >
                                        {[{ id: 'python' as const, label: 'Python' }, { id: 'sql' as const, label: 'SQL' }].map(lang => (
                                            <button
                                                key={lang.id}
                                                onClick={() => { setDefaultLanguage(lang.id); setActiveMenu(null); }}
                                                className="w-full flex items-center gap-3 px-3 py-1.5 text-[12px] font-medium transition-colors hover:bg-blue-500/10 hover:text-blue-400 group/lang text-white/70"
                                            >
                                                <div className="flex items-center justify-center w-4 h-4">
                                                    {defaultLanguage === lang.id ? <Check className="w-4 h-4 text-blue-400" /> : null}
                                                </div>
                                                <span className={defaultLanguage === lang.id ? "text-white" : ""}>{lang.label}</span>
                                            </button>
                                        ))}
                                    </div>
                                )}
                            </div>
                        </div>
                    </div>

                    {/* Right side controls */}
                    <div className="flex items-center gap-1 xl:gap-2 justify-end min-w-0 flex-1">
                        <button
                            onClick={runAllCells}
                            disabled={!kernelId || kernelStatus === 'busy' || kernelStatus === 'starting' || kernelStatus === 'disconnected'}
                            className="flex-shrink-0 whitespace-nowrap flex items-center gap-1.5 px-2.5 xl:px-3 py-1.5 rounded-md hover:bg-white/[0.08] transition-all cursor-pointer group/run active:scale-95 text-[12px] font-medium text-white/70 hover:text-white disabled:opacity-50">
                            <Play className="w-3.5 h-3.5 opacity-70 group-hover/run:text-green-400 group-hover/run:opacity-100 transition-colors" />
                            Run all
                        </button>

                        <div className="hidden md:flex items-center h-[28px] p-[2px] rounded border border-white/5 bg-[#09090b]/80 shadow-md relative z-10 flex-shrink-0 ring-1 ring-white/[0.02]">
                            <div className="flex items-center gap-1.5 pl-2 pr-1 h-full hover:bg-white/[0.04] rounded-sm transition-colors cursor-pointer group/select w-[140px] xl:w-[200px]">
                                <Server className="w-[11px] h-[11px] text-white/30 group-hover/select:text-white/50 transition-colors flex-shrink-0" />
                                <div className="relative flex items-center h-full flex-1 min-w-0">
                                    <select
                                        value={selectedClusterId}
                                        onChange={(e) => setSelectedClusterId(e.target.value)}
                                        className="bg-transparent text-[11px] text-white/60 group-hover/select:text-white/90 font-medium outline-none cursor-pointer appearance-none w-full pr-5 pl-0.5 h-full truncate transition-colors"
                                        disabled={clusterAttachBusy || kernelStatus === 'busy'}
                                    >
                                        {clusterProfiles.map(profile => (
                                            <option key={profile.id} value={profile.id} className="bg-[#18181b] text-white">
                                                {profile.label} • {clusterPowerLabel(profile)}
                                            </option>
                                        ))}
                                    </select>
                                    <ChevronDown className="w-3 h-3 text-white/20 group-hover/select:text-white/40 absolute right-0 pointer-events-none flex-shrink-0 transition-colors" />
                                </div>
                            </div>
                            <div className="w-[1px] h-3.5 bg-white/[0.08] mx-0.5 flex-shrink-0" />
                            <button
                                onClick={attachCluster}
                                disabled={
                                    !kernelId
                                    || clusterAttachBusy
                                    || kernelStatus === 'busy'
                                    || !selectedClusterId
                                    || !clusterApiAvailable
                                    || selectedClusterId === attachedClusterId
                                }
                                className="flex-shrink-0 h-full whitespace-nowrap flex items-center gap-1.5 px-2 rounded-sm text-[11px] font-medium transition-all
                                disabled:opacity-40 disabled:cursor-not-allowed
                                hover:bg-white/[0.04] text-white/60 hover:text-white active:scale-95"
                                title="Attach selected cluster profile"
                            >
                                {clusterAttachBusy ? <Loader2 className="w-[11px] h-[11px] animate-spin text-blue-400" /> : null}
                                <span className={attachedClusterId === selectedClusterId ? "text-green-400/90 font-semibold" : ""}>
                                    {attachedClusterId === selectedClusterId ? "Attached" : "Attach"}
                                </span>
                            </button>
                        </div>

                        {selectedCluster && (
                            <div className="hidden xl:flex items-center gap-2 text-[11px] font-mono min-w-0 flex-1 max-w-[200px] 2xl:max-w-[400px] px-2 opacity-50">
                                <span className="text-white/50 truncate block w-full" title={`${selectedCluster.label}: ${clusterPowerLabel(selectedCluster)}`}>
                                    {selectedCluster.label}: <span className="opacity-70">{clusterPowerLabel(selectedCluster)}</span>
                                </span>
                            </div>
                        )}

                        {(clusterObservability?.spark_ui_url || clusterObservability?.spark_master_ui_url) && (
                            <a
                                href={clusterObservability?.spark_ui_url || clusterObservability?.spark_master_ui_url || '#'}
                                target="_blank"
                                rel="noreferrer"
                                className="hidden lg:flex flex-shrink-0 whitespace-nowrap items-center gap-1.5 px-2.5 py-1.5 rounded-md border border-white/10 bg-white/[0.03] hover:bg-white/[0.08] text-[12px] font-medium text-white/70 hover:text-white transition-all shadow-sm active:scale-95 group/spark"
                                title="Open Spark UI"
                            >
                                <ExternalLink className="w-3.5 h-3.5 opacity-70 group-hover/spark:opacity-100 transition-opacity" />
                                <span>Spark UI</span>
                            </a>
                        )}

                        <div className="hidden md:block w-[1px] h-5 bg-gradient-to-b from-transparent via-white/10 to-transparent mx-1 rounded-full flex-shrink-0" />

                        <button onClick={saveNotebook}
                            className="flex-shrink-0 whitespace-nowrap px-3 py-1.5 rounded-md hover:bg-white/[0.08] transition-all text-white/70 hover:text-white text-[12px] font-medium flex items-center gap-1.5 active:scale-95"
                        >
                            <Save className="w-3.5 h-3.5 opacity-70" />
                            Save
                        </button>
                    </div>
                </header>

                {/* ─── Tab Bar (Premium Obsidian) ─── */}
                <div className="flex items-end overflow-x-auto shrink-0 bg-black/20 backdrop-blur-md h-[40px] w-full border-b border-white/[0.06] custom-scrollbar relative z-10 shadow-inner">
                    {/* Shadow overlay for depth */}
                    <div className="absolute inset-x-0 top-0 h-[10px] bg-gradient-to-b from-black/20 to-transparent pointer-events-none" />

                    {openTabs.map(tab => {
                        const isActive = isEditingName ? tab === originalName : tab === notebookName;
                        const displayTitle = isActive && isEditingName ? notebookName : tab;

                        return (
                            <div
                                key={tab}
                                onClick={() => { if (!isActive) loadNotebook(tab); }}
                                className={clsx(
                                    "relative flex items-center justify-between cursor-pointer select-none group h-[40px] min-w-[140px] max-w-[220px] px-3 transition-colors",
                                    isActive ? "bg-[#18181b]/90 text-white" : "text-white/40 hover:text-white/90 hover:bg-white/[0.04] bg-transparent"
                                )}
                                style={{
                                    borderRight: '1px solid rgba(255,255,255,0.05)',
                                    fontSize: '13px',
                                }}
                            >
                                {/* Active Tab Top Highlight */}
                                {isActive && (
                                    <div className="absolute top-0 left-0 right-0 h-[2px] bg-blue-500 shadow-[0_0_8px_rgba(59,130,246,0.5)] z-10 rounded-t-sm" />
                                )}

                                <div className="flex items-center gap-2.5 truncate">
                                    <FileText className={clsx("w-3.5 h-3.5 flex-shrink-0", isActive ? "text-blue-400" : "text-white/30 group-hover:text-white/50")} />
                                    {isActive && isEditingName ? (
                                        <input
                                            autoFocus
                                            className="text-[13px] font-medium text-white bg-transparent outline-none w-32 tracking-wide"
                                            value={notebookName}
                                            onChange={(e) => setNotebookName(e.target.value)}
                                            onBlur={() => { void finalizeRename(); }}
                                            onKeyDown={(e) => { if (e.key === 'Enter') void finalizeRename(); }}
                                            onClick={(e) => e.stopPropagation()}
                                        />
                                    ) : (
                                        <span
                                            className={clsx("truncate tracking-wide", isActive ? "font-semibold" : "font-medium")}
                                            onClick={(e) => {
                                                e.stopPropagation();
                                                if (isActive) {
                                                    setOriginalName(notebookName);
                                                    setIsEditingName(true);
                                                } else {
                                                    loadNotebook(tab);
                                                }
                                            }}
                                            title={isActive ? "Click to rename" : tab}
                                        >
                                            {displayTitle}
                                        </span>
                                    )}
                                </div>
                                <div className="flex items-center gap-1.5 ml-3 z-20">
                                    <X
                                        onClick={(e) => closeTab(tab, e)}
                                        className={clsx(
                                            "w-4 h-4 p-0.5 rounded-md transition-all flex-shrink-0 hover:bg-white/20 active:scale-95",
                                            isActive ? "opacity-60 hover:opacity-100" : "opacity-0 group-hover:opacity-60 hover:opacity-100"
                                        )}
                                    />
                                </div>
                            </div>
                        );
                    })}
                </div>

                {/* Main Content Area */}
                <div className="flex-1 relative flex flex-col min-h-0 bg-transparent overflow-hidden">
                    <div className="flex-1 overflow-y-auto py-4 px-6 flex flex-col gap-2 items-center w-full max-w-[1440px] mx-auto custom-scrollbar">
                        {cells.map((cell, i) => (
                            <div key={cell.id} className="w-full">
                                <NotebookCell
                                    cell={cell}
                                    index={i}
                                    total={cells.length}
                                    isActive={activeCell === cell.id}
                                    onActivate={() => setActiveCell(cell.id)}
                                    onUpdate={(updates) => {
                                        if ('source' in updates && typeof updates.source === 'string') {
                                            handleCellSourceChange(cell.id, updates.source);
                                        } else {
                                            updateCell(cell.id, updates);
                                        }
                                    }}
                                    onDelete={() => deleteCell(cell.id)}
                                    onMoveUp={() => moveCell(cell.id, 'up')}
                                    onMoveDown={() => moveCell(cell.id, 'down')}
                                    onRun={() => runCell(cell.id)}
                                    onAddBelow={(type) => addCell(cell.id, type)}
                                    sqlSuggestionItems={sqlSuggestionItems}
                                    loadSqlColumns={loadSqlColumns}
                                    sqlKeywordItems={sqlKeywordItems}
                                    pythonKeywordItems={pythonKeywordItems}
                                    pythonSymbolItems={pythonSymbolItems}
                                />
                            </div>
                        ))}

                        <div className="w-full flex justify-center py-6">
                            <div className="flex items-center gap-2">
                                <button
                                    onClick={() => addCell(cells[cells.length - 1]?.id, 'code')}
                                    className="flex items-center gap-1 px-3 py-1.5 border border-white/10 bg-white/5 rounded text-[11px] text-white/60 hover:text-white hover:border-white/20 hover:bg-white/10 transition-colors shadow-sm"
                                >
                                    <Plus className="w-3.5 h-3.5" /> Code
                                </button>
                                <button
                                    onClick={() => addCell(cells[cells.length - 1]?.id, 'markdown')}
                                    className="flex items-center gap-1 px-3 py-1.5 border border-white/10 bg-white/5 rounded text-[11px] text-white/60 hover:text-white hover:border-white/20 hover:bg-white/10 transition-colors shadow-sm"
                                >
                                    <Plus className="w-3.5 h-3.5" /> Markdown
                                </button>
                            </div>
                        </div>
                    </div>
                </div>

                {/* ─── Status Bar (Native Style) ─── */}
                <div className="h-8 bg-black/60 backdrop-blur-xl border-t border-white/5 flex items-center justify-between px-4 shrink-0 text-[10.5px] font-mono text-obsidian-muted/80 z-20">
                    <div className="flex items-center gap-4 overflow-hidden">
                        <div className="flex items-center gap-1.5">
                            {kernelStatus === 'idle' && <Check className="w-3 h-3 text-white/40" />}
                            {kernelStatus === 'busy' && <Loader2 className="w-3 h-3 text-white/60 animate-spin" />}
                            {kernelStatus === 'starting' && <Loader2 className="w-3 h-3 text-white/40 animate-spin" />}
                            {kernelStatus === 'disconnected' && <X className="w-3 h-3 text-white/30" />}
                            <span>Jupyter API: <span className="text-white/80 capitalize">{kernelStatus}</span></span>
                        </div>
                        <div className="flex items-center gap-1.5">
                            <Terminal className="w-3 h-3" />
                            <span>Engine: <span className="text-foreground/80 uppercase">{engineMode}</span></span>
                        </div>
                        <div className="flex items-center gap-1.5">
                            <Server className="w-3 h-3" />
                            <span>
                                Cluster: <span className="text-foreground/80">{attachedClusterId || selectedClusterId || defaultClusterId}</span>
                            </span>
                        </div>
                        {attachedCluster && (
                            <div className="flex items-center gap-1.5 truncate">
                                <span>{clusterPowerLabel(attachedCluster)}</span>
                                {clusterShuffleLabel(attachedCluster) && (
                                    <span className="text-white/65">{clusterShuffleLabel(attachedCluster)}</span>
                                )}
                                {clusterShuffleWarningLabel(attachedCluster) === 'Shuffle Low' && (
                                    <span className="px-1 py-0.5 rounded border border-amber-400/35 text-amber-300/90 whitespace-nowrap">
                                        Shuffle Low
                                    </span>
                                )}
                                {clusterShuffleWarningLabel(attachedCluster) === 'Shuffle High' && (
                                    <span className="px-1 py-0.5 rounded border border-rose-400/35 text-rose-300/90 whitespace-nowrap">
                                        Shuffle High
                                    </span>
                                )}
                            </div>
                        )}
                        {!clusterApiAvailable && (
                            <div className="flex items-center gap-1.5 truncate">
                                <span>Cluster API: legacy backend (attach/observability disabled)</span>
                            </div>
                        )}
                        {clusterObservability?.workers && (
                            <div className="flex items-center gap-1.5 truncate">
                                <Activity className="w-3 h-3" />
                                <span>
                                    Workers {clusterObservability.workers.count} · Cores {clusterObservability.workers.cores_used}/{clusterObservability.workers.cores_total} · Mem {Math.round((clusterObservability.workers.memory_used_mb || 0) / 1024)}G/{Math.round((clusterObservability.workers.memory_total_mb || 0) / 1024)}G
                                </span>
                            </div>
                        )}
                        {clusterObservability?.logs?.[0] && (
                            <div className="flex items-center gap-1.5 truncate">
                                <span>
                                    Jobs {clusterObservability.logs[0].running_jobs} · Stages {clusterObservability.logs[0].active_stages}
                                </span>
                            </div>
                        )}
                        <div className="flex items-center gap-1.5">
                            <Code2 className="w-3 h-3" />
                            <span>{cells.length} cells</span>
                        </div>
                    </div>

                    <div className="flex items-center gap-3 font-mono">
                        <span>Ln 1, Col 1</span>
                        <span>UTF-8</span>
                        <span>{(cells.find(c => c.id === activeCell)?.language || defaultLanguage) === 'python' ? 'Python' : 'SQL'}</span>
                    </div>
                </div>
            </main>

            {uiNotice && (
                <div className="absolute right-4 bottom-12 z-[120] max-w-md">
                    <div
                        className={clsx(
                            "flex items-start gap-2 rounded-lg border px-3 py-2 shadow-2xl backdrop-blur-md",
                            uiNotice.tone === 'success' && "border-emerald-400/40 bg-emerald-500/15 text-emerald-300",
                            uiNotice.tone === 'error' && "border-rose-400/40 bg-rose-500/15 text-rose-300",
                            uiNotice.tone === 'info' && "border-cyan-400/40 bg-cyan-500/15 text-cyan-300",
                        )}
                    >
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

            {confirmDialog && (
                <div className="fixed inset-0 z-[130] flex items-center justify-center bg-black/65 backdrop-blur-sm" onClick={() => resolveConfirm(false)}>
                    <div
                        className="w-[440px] max-w-[92vw] rounded-xl border border-white/10 bg-[#151518] shadow-2xl"
                        onClick={(e) => e.stopPropagation()}
                    >
                        <div className="px-5 py-4 border-b border-white/10">
                            <h3 className="text-sm font-semibold text-white">{confirmDialog.title}</h3>
                            <p className="mt-1 text-xs text-white/65">{confirmDialog.message}</p>
                        </div>
                        <div className="px-5 py-3 flex items-center justify-end gap-2">
                            <button
                                onClick={() => resolveConfirm(false)}
                                className="px-3 py-1.5 text-xs rounded-md border border-white/10 text-white/70 hover:text-white hover:bg-white/5 transition-colors"
                            >
                                Cancel
                            </button>
                            <button
                                onClick={() => resolveConfirm(true)}
                                className={clsx(
                                    "px-3 py-1.5 text-xs rounded-md transition-colors",
                                    confirmDialog.tone === 'danger'
                                        ? "border border-rose-400/30 bg-rose-500/20 text-rose-200 hover:bg-rose-500/30"
                                        : "border border-cyan-400/30 bg-cyan-500/20 text-cyan-200 hover:bg-cyan-500/30"
                                )}
                            >
                                {confirmDialog.confirmLabel || 'Confirm'}
                            </button>
                        </div>
                    </div>
                </div>
            )}

            {/* ─── Keyboard Shortcuts Modal (Databricks-like) ─── */}
            {showShortcutsModal && (
                <div className="fixed inset-0 z-[100] flex items-center justify-center bg-black/60 backdrop-blur-sm" onClick={() => setShowShortcutsModal(false)}>
                    <div className="bg-[#1a1a1e] border border-white/10 rounded-xl shadow-2xl w-[520px] max-h-[70vh] overflow-auto" onClick={(e) => e.stopPropagation()}>
                        <div className="flex items-center justify-between px-5 py-3 border-b border-white/10">
                            <h2 className="text-white font-semibold text-sm tracking-wide">Keyboard Shortcuts</h2>
                            <button onClick={() => setShowShortcutsModal(false)} className="p-1 hover:bg-white/10 rounded transition-colors text-white/40 hover:text-white">
                                <X className="w-4 h-4" />
                            </button>
                        </div>
                        <div className="p-5 space-y-3 text-[12px]">
                            {[
                                { keys: '⇧ Enter', desc: 'Run cell and move to next' },
                                { keys: '⌘ Enter', desc: 'Run cell in place' },
                                { keys: '⌘ S', desc: 'Save notebook' },
                                { keys: '⌘ ⇧ Enter', desc: 'Run all cells' },
                                { keys: '⌘ ⇧ P', desc: 'Open command palette (Editor)' },
                                { keys: 'Esc', desc: 'Exit edit mode (focus cell)' },
                                { keys: '%sql', desc: 'Switch cell language to SQL (magic command)' },
                                { keys: '%python', desc: 'Switch cell language to Python (magic command)' },
                                { keys: '%md', desc: 'Switch cell to Markdown (magic command)' },
                            ].map((s, i) => (
                                <div key={i} className="flex items-center justify-between py-1.5 border-b border-white/5 last:border-0">
                                    <span className="text-white/70">{s.desc}</span>
                                    <kbd className="px-2 py-0.5 bg-white/5 border border-white/10 rounded text-white/60 font-mono text-[11px]">{s.keys}</kbd>
                                </div>
                            ))}
                        </div>
                    </div>
                </div>
            )}
        </div>
    );
}
