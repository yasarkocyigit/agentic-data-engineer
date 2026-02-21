
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
    StopCircle, RotateCw, EyeOff, PanelLeft, Eraser,
    type TypeIcon
} from 'lucide-react';
import Editor from '@monaco-editor/react';
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

// ─── Helpers ───
const genId = () => Math.random().toString(36).substring(2, 10);

const INITIAL_CELL: Cell = {
    id: genId(),
    cell_type: 'code',
    source: '',
    language: 'python',
    outputs: [],
    execution_count: null,
    running: false,
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
    if (data['text/html']) return <div className="px-4 py-2 bg-obsidian-bg" dangerouslySetInnerHTML={{ __html: data['text/html'] }} />;
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
}) {
    const editorRef = useRef<any>(null);
    const lineCount = cell.source.split('\n').length;
    const initialHeight = Math.max(40, Math.min(lineCount * 18 + 16, 500));
    const [editorHeight, setEditorHeight] = useState(initialHeight);
    const [showPreview, setShowPreview] = useState(false);

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
    };

    const handleEditorMount = (editor: any, monaco: any) => {
        editorRef.current = editor;
        monaco.editor.setTheme('obsidian');
        editor.onKeyDown((e: any) => {
            // Shift+Enter OR Cmd/Ctrl+Enter
            if ((e.shiftKey || e.metaKey || e.ctrlKey) && e.keyCode === 3) {
                e.preventDefault();
                onRun();
            }
        });
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
                        "flex-1 flex min-w-0 transition-all duration-150 rounded-lg border",
                        cell.cell_type === 'markdown' && isActive ? "flex-col" : "flex-row",
                        isActive ? "border-white/20 bg-white/[0.03] shadow-md" : "border-white/5 hover:border-white/10 bg-white/[0.01] hover:bg-white/[0.02]"
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
                                <button onClick={(e) => { e.stopPropagation(); alert('AI Generate requires an LLM API key configuration. Coming soon!'); }} className="w-6 h-6 flex items-center justify-center rounded hover:bg-white/10 text-[#d7aef2] hover:text-[#f3d0ff] transition-colors" title="AI Generate"><Sparkles className="w-3.5 h-3.5" /></button>
                                <button onClick={(e) => { e.stopPropagation(); alert('Expand mode coming soon!'); }} className="w-6 h-6 flex items-center justify-center rounded hover:bg-white/10 transition-colors" title="Expand"><Maximize2 className="w-3.5 h-3.5" /></button>
                                <button className="w-6 h-6 flex items-center justify-center rounded hover:bg-white/10 transition-colors" title="More options"><MoreHorizontal className="w-3.5 h-3.5" /></button>
                                <button onClick={(e) => { e.stopPropagation(); onDelete(); }} disabled={total <= 1} className="w-6 h-6 flex items-center justify-center rounded text-obsidian-danger/50 hover:text-obsidian-danger hover:bg-obsidian-danger/10 transition-colors disabled:opacity-20" title="Delete cell"><Trash2 className="w-4 h-4" /></button>
                            </div>
                        </div>
                    )}

                    <div className="flex-1 flex min-w-0 relative">
                        {/* Play Button - Minimal inline (Only for Code) */}
                        {cell.cell_type === 'code' && (
                            <div className="shrink-0 flex items-center pl-1.5 pr-0.5 relative z-10">
                                <button
                                    onClick={(e) => { e.stopPropagation(); onRun(); }}
                                    disabled={cell.running}
                                    className={clsx(
                                        "w-6 h-6 flex items-center justify-center rounded-full transition-all",
                                        cell.running
                                            ? "text-white/60"
                                            : "text-foreground/40 hover:text-white hover:bg-white/10"
                                    )}
                                    title="Run cell (⇧↵)"
                                >
                                    {cell.running ? <Loader2 className="w-3.5 h-3.5 animate-spin" /> : <Play className="w-3.5 h-3.5 fill-current" />}
                                </button>
                            </div>
                        )}

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

                        {/* Right Toolbar (Inside box) (Only for Code or inactive Markdown) */}
                        {!(cell.cell_type === 'markdown' && isActive) && (
                            <div className={clsx(
                                "absolute top-0 right-0 flex items-center pt-1.5 pr-1.5 gap-0.5 transition-opacity z-10 bg-gradient-to-l from-[#1e2025] via-[#1e2025]/95 to-transparent pl-6",
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
            {cell.cell_type === 'markdown' && isActive && (
                <div className="w-full flex-col items-center justify-center text-obsidian-muted/40 text-[11px] font-mono mt-8 leading-loose text-center mb-4 tracking-wide">
                    [Shift+Enter] to run and move to next cell<br />
                    [Cmd+Shift+P] to open the command palette<br />
                    [Esc H] to see all keyboard shortcuts
                </div>
            )}

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
        </div>
    );
}

// ─── Main Native Page ───
export default function NotebooksPage() {
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

    // ─── Kernel Management ───
    const startKernel = useCallback(async () => {
        setKernelStatus('starting');
        try {
            const resp = await fetch('/api/notebook/kernels', { method: 'POST' });
            const data = await resp.json();
            setKernelId(data.id);
            setEngineMode(data.mode);
            setKernelStatus('idle');
        } catch (e) {
            console.error('Failed to start kernel:', e);
            setKernelStatus('disconnected');
        }
    }, []);

    const restartKernel = useCallback(async () => {
        if (!kernelId) return;
        setKernelStatus('starting');
        try {
            await fetch(`/api/notebook/kernels/${kernelId}/restart`, { method: 'POST' });
            setKernelStatus('idle');
            setCells(prev => prev.map(c => ({ ...c, outputs: [], execution_count: null })));
        } catch (e) {
            console.error('Failed to restart kernel:', e);
            setKernelStatus('disconnected');
        }
    }, [kernelId]);

    const interruptKernel = useCallback(async () => {
        if (!kernelId) return;
        try {
            await fetch(`/api/notebook/kernels/${kernelId}/interrupt`, { method: 'POST' });
        } catch (e) {
            console.error('Failed to interrupt kernel:', e);
        }
    }, [kernelId]);

    useEffect(() => { startKernel(); }, [startKernel]);

    // ─── Save / Load ───
    const loadNotebookList = useCallback(async () => {
        try {
            const resp = await fetch('/api/notebook/files');
            const data = await resp.json();
            setSavedNotebooks(data.notebooks || []);
        } catch (e) {
            console.error('Failed to load notebooks:', e);
        }
    }, []);

    useEffect(() => { loadNotebookList(); }, [loadNotebookList]);

    const saveNotebook = useCallback(async () => {
        try {
            await fetch('/api/notebook/files', {
                method: 'POST',
                headers: { 'Content-Type': 'application/json' },
                body: JSON.stringify({
                    name: notebookName,
                    cells: cells.map(c => ({ id: c.id, cell_type: c.cell_type, source: c.source, language: c.language, outputs: c.outputs, execution_count: c.execution_count })),
                }),
            });
            loadNotebookList();
        } catch (e) {
            console.error('Failed to save:', e);
        }
    }, [notebookName, cells, loadNotebookList]);

    const createNewNotebook = useCallback(async () => {
        let i = 1;
        let newName = `Untitled-${i}`;
        while (savedNotebooks.some(nb => nb.name === newName)) {
            i++;
            newName = `Untitled-${i}`;
        }
        const newCells = [{ ...INITIAL_CELL, id: genId() }];

        try {
            await fetch('/api/notebook/files', {
                method: 'POST',
                headers: { 'Content-Type': 'application/json' },
                body: JSON.stringify({
                    name: newName,
                    cells: newCells.map(c => ({ id: c.id, cell_type: c.cell_type, source: c.source, language: c.language, outputs: c.outputs, execution_count: c.execution_count })),
                }),
            });

            await loadNotebookList();
            setNotebookName(newName);
            setCells(newCells);
            setActiveCell(newCells[0].id);
            setOpenTabs(prev => [...prev, newName]);
            setIsEditingName(false);
        } catch (e) {
            console.error('Failed to create new notebook:', e);
        }
    }, [savedNotebooks, loadNotebookList]);

    const loadNotebook = useCallback(async (name: string) => {
        try {
            const resp = await fetch(`/api/notebook/files/${name}`);
            const data = await resp.json();
            setNotebookName(data.name || name);
            setCells((data.cells || []).map((c: any) => ({ ...c, id: c.id || genId(), running: false, outputs: c.outputs || [] })));
            if (data.cells?.length > 0) setActiveCell(data.cells[0].id || genId());
            setOpenTabs(prev => prev.includes(data.name || name) ? prev : [...prev, data.name || name]);
            setIsEditingName(false);
        } catch (e) {
            console.error('Failed to load notebook:', e);
        }
    }, []);

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
    }, []);

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
        if (!confirm(`Are you sure you want to delete '${name}'?`)) return;

        try {
            await fetch(`/api/notebook/files/${name}`, { method: 'DELETE' });
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
            alert(`Failed to delete '${name}'`);
        }
    }, [loadNotebookList, notebookName, loadNotebook, openTabs]);

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

    const finalizeRename = useCallback(() => {
        setIsEditingName(false);
        if (originalName && originalName !== notebookName) {
            setOpenTabs(prev => prev.map(t => t === originalName ? notebookName : t));
        }
    }, [originalName, notebookName]);

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

        updateCell(id, { running: true, outputs: [] });
        setKernelStatus('busy');

        let codeToExecute = cell.source;
        if (cell.language === 'sql' && !codeToExecute.trimStart().startsWith('%%sql')) {
            codeToExecute = '%%sql\n' + codeToExecute;
        }

        try {
            const resp = await fetch(`/api/notebook/kernels/${kernelId}/execute`, {
                method: 'POST', headers: { 'Content-Type': 'application/json' }, body: JSON.stringify({ code: codeToExecute }),
            });
            const result = await resp.json();
            updateCell(id, { running: false, outputs: result.outputs || [], execution_count: result.execution_count });
        } catch (e) {
            updateCell(id, { running: false, outputs: [{ output_type: 'error', ename: 'ExecutionError', evalue: String(e), traceback: [] }] });
        }
        setKernelStatus('idle');
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
        if (!confirm('Restart kernel and run all cells?')) return;
        await restartKernel();
        await runAllCells();
    }, [restartKernel, runAllCells]);

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
            const resp = await fetch(`/api/notebook/files/${notebookName}`);
            const data = await resp.json();
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
            alert('Failed to export notebook');
        }
    }, [notebookName]);

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

    return (
        <div className="flex h-screen bg-[#09090b] text-foreground font-sans overflow-hidden">
            <Sidebar />

            {/* Background ambient light */}
            <div className="absolute inset-0 z-0 pointer-events-none overflow-hidden">
                <div className="absolute -top-[15%] -right-[10%] w-[45%] h-[45%] bg-obsidian-purple/[0.04] blur-[120px] rounded-full" />
                <div className="absolute bottom-[10%] -left-[10%] w-[40%] h-[40%] bg-obsidian-info/[0.03] blur-[100px] rounded-full" />
            </div>

            {/* ─── Left Explorer Sidebar (Premium Obsidian Design) ─── */}
            <div
                className={clsx(
                    "relative flex flex-col bg-black/20 backdrop-blur-md border-r border-white/5 z-20 overflow-hidden transition-[width] duration-300",
                    !showFilePanel && "w-0 border-r-0"
                )}
                style={{ width: showFilePanel ? 260 : 0 }}
            >
                <div className="h-10 bg-black/40 border-b border-white/5 flex items-center justify-between px-3 shrink-0">
                    <div className="flex items-center gap-2">
                        <BookOpen className="w-3.5 h-3.5 text-white/40" />
                        <span className="text-[11px] font-bold text-white tracking-widest uppercase">Notebooks</span>
                    </div>
                    <div className="flex gap-1">
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

            <main className="flex-1 flex flex-col min-w-0 overflow-hidden bg-transparent backdrop-blur-xl relative z-10">
                {/* ─── Top Navigation Bar (Premium Obsidian) ─── */}
                <div className="flex items-center px-4 justify-between shrink-0 h-10 bg-black/40 backdrop-blur-md border-b border-white/5 z-10 relative">
                    <div className="absolute top-0 left-0 right-0 h-[1px] bg-white/10" />
                    <div className="flex items-center gap-3">
                        {/* Toggle Sidebar Button */}
                        <button
                            onClick={() => setShowFilePanel(!showFilePanel)}
                            className="p-1.5 hover:bg-white/10 rounded-md text-obsidian-muted hover:text-white transition-all active:scale-95 border border-transparent hover:border-white/10"
                            title="Toggle Notebooks Explorer"
                        >
                            <LayoutPanelLeft className="w-4 h-4" />
                        </button>

                        <div className="w-[1px] h-4 bg-white/10 mx-1"></div>

                        <div className="flex items-center gap-0 text-[12px]">
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
                                        { label: 'Restart Kernel', shortcut: '', action: () => { if (confirm('Restart kernel? All variables will be lost.')) restartKernel(); }, icon: <RotateCw className="w-3.5 h-3.5" /> },
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
                                <div key={menu.label} className="relative">
                                    <button
                                        onClick={(e) => { e.stopPropagation(); setActiveMenu(activeMenu === menu.label ? null : menu.label); }}
                                        className={clsx(
                                            "px-2.5 py-1 rounded transition-colors tracking-wide",
                                            activeMenu === menu.label ? "text-white bg-white/10" : "text-white/50 hover:text-white hover:bg-white/5"
                                        )}
                                    >
                                        {menu.label}
                                    </button>
                                    {activeMenu === menu.label && (
                                        <div
                                            className="absolute top-full left-0 mt-1 w-56 bg-[#1a1a1e] border border-white/10 rounded-lg shadow-2xl z-50 overflow-hidden py-1 backdrop-blur-xl"
                                            onClick={(e) => e.stopPropagation()}
                                        >
                                            {(menu.items as any[]).map((item: any, i: number) =>
                                                item.divider ? (
                                                    <div key={i} className="h-px bg-white/10 my-1 mx-2" />
                                                ) : (
                                                    <button
                                                        key={i}
                                                        onClick={() => { item.action(); setActiveMenu(null); }}
                                                        disabled={item.disabled}
                                                        className="w-full flex items-center gap-2.5 px-3 py-1.5 text-[12px] text-white/70 hover:text-white hover:bg-white/10 transition-colors disabled:opacity-30 disabled:cursor-not-allowed"
                                                    >
                                                        <span className="text-white/40">{item.icon}</span>
                                                        <span className="flex-1 text-left">{item.label}</span>
                                                        {item.shortcut && <span className="text-[10px] text-white/30 font-mono">{item.shortcut}</span>}
                                                    </button>
                                                )
                                            )}
                                        </div>
                                    )}
                                </div>
                            ))}

                            <div className="w-px h-4 bg-white/10 mx-2" />

                            {/* ─── Functional Default Language Selector (Databricks-like) ─── */}
                            <div className="relative">
                                <button
                                    onClick={(e) => { e.stopPropagation(); setActiveMenu(activeMenu === 'lang' ? null : 'lang'); }}
                                    className={clsx(
                                        "flex items-center gap-1 px-2 py-1 rounded font-medium transition-colors",
                                        activeMenu === 'lang' ? "text-white bg-white/10" : "text-white/80 hover:bg-white/5"
                                    )}
                                >
                                    <span>{defaultLanguage === 'python' ? 'Python' : 'SQL'}</span>
                                    <ChevronDown className="w-3 h-3 text-white/40" />
                                </button>
                                {activeMenu === 'lang' && (
                                    <div
                                        className="absolute top-full left-0 mt-1 w-40 bg-[#1a1a1e] border border-white/10 rounded-lg shadow-2xl z-50 overflow-hidden py-1 backdrop-blur-xl"
                                        onClick={(e) => e.stopPropagation()}
                                    >
                                        {[{ id: 'python' as const, label: 'Python' }, { id: 'sql' as const, label: 'SQL' }].map(lang => (
                                            <button
                                                key={lang.id}
                                                onClick={() => { setDefaultLanguage(lang.id); setActiveMenu(null); }}
                                                className={clsx(
                                                    "w-full flex items-center gap-2.5 px-3 py-1.5 text-[12px] transition-colors",
                                                    defaultLanguage === lang.id ? "text-white bg-white/10" : "text-white/70 hover:text-white hover:bg-white/10"
                                                )}
                                            >
                                                {defaultLanguage === lang.id && <Check className="w-3 h-3 text-white/60" />}
                                                {defaultLanguage !== lang.id && <div className="w-3 h-3" />}
                                                <span>{lang.label}</span>
                                            </button>
                                        ))}
                                    </div>
                                )}
                            </div>
                        </div>
                    </div>

                    {/* Right side controls */}
                    <div className="flex items-center gap-3">
                        <button onClick={runAllCells} disabled={kernelStatus !== 'idle'}
                            className="flex items-center gap-1.5 px-3 py-1 rounded hover:bg-white/10 disabled:opacity-50 transition-colors text-white/70 hover:text-white font-medium text-[11px]">
                            <Play className="w-3.5 h-3.5" /> Run all
                        </button>

                        <div className="hidden md:flex items-center gap-1.5 px-3 py-1 rounded cursor-pointer transition-colors text-[11px]">
                            <div className="w-1.5 h-1.5 rounded-full bg-white/40" />
                            <span className="text-white/70 font-medium tracking-wide">Serverless</span>
                            <ChevronDown className="w-3 h-3 text-white/40" />
                        </div>

                        <div className="w-px h-4 bg-obsidian-border/50 mx-1" />

                        <button onClick={saveNotebook}
                            className="px-3 py-1 rounded hover:bg-white/10 transition-colors text-white/60 hover:text-white text-[11px] font-medium">
                            Save
                        </button>
                    </div>
                </div>

                {/* ─── Tab Bar (Premium Obsidian) ─── */}
                <div className="flex items-end overflow-x-auto shrink-0 bg-black/40 backdrop-blur-md h-[38px] w-full border-b border-white/5 custom-scrollbar">
                    {openTabs.map(tab => {
                        const isActive = isEditingName ? tab === originalName : tab === notebookName;
                        const displayTitle = isActive && isEditingName ? notebookName : tab;

                        return (
                            <div
                                key={tab}
                                onClick={() => { if (!isActive) loadNotebook(tab); }}
                                className={clsx(
                                    "relative flex items-center justify-between cursor-pointer select-none group h-[38px] min-w-[140px] max-w-[220px] px-3 transition-colors",
                                    isActive ? "bg-white/[0.04] text-white" : "text-white/40 hover:text-white/80 hover:bg-white/[0.02]"
                                )}
                                style={{
                                    borderTop: isActive ? '2px solid rgba(255,255,255,0.2)' : '2px solid transparent',
                                    borderRight: '1px solid rgba(255,255,255,0.05)',
                                    fontSize: '13px',
                                }}>
                                <div className="flex items-center gap-2.5 truncate">
                                    <FileText className={clsx("w-3.5 h-3.5 flex-shrink-0", isActive ? "text-white/50" : "text-white/30 group-hover:text-white/50")} />
                                    {isActive && isEditingName ? (
                                        <input
                                            autoFocus
                                            className="text-[13px] font-medium text-white bg-transparent outline-none w-32 tracking-wide"
                                            value={notebookName}
                                            onChange={(e) => setNotebookName(e.target.value)}
                                            onBlur={finalizeRename}
                                            onKeyDown={(e) => { if (e.key === 'Enter') finalizeRename(); }}
                                            onClick={(e) => e.stopPropagation()}
                                        />
                                    ) : (
                                        <span
                                            className="truncate font-medium tracking-wide"
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
                                <div className="flex items-center gap-1.5 ml-3">
                                    <X
                                        onClick={(e) => closeTab(tab, e)}
                                        className={clsx(
                                            "w-3.5 h-3.5 rounded-sm transition-all flex-shrink-0 hover:bg-white/20",
                                            isActive ? "opacity-50 hover:opacity-100" : "opacity-0 group-hover:opacity-50 hover:opacity-100"
                                        )}
                                    />
                                </div>
                            </div>
                        );
                    })}
                </div>

                {/* ─── Main Notebook Area ─── */}
                <div className="flex-1 flex flex-col overflow-auto bg-transparent relative z-0">
                    <div className="flex-1 py-4 px-6 flex flex-col gap-2 items-center w-full max-w-[1440px] mx-auto custom-scrollbar">
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
                    <div className="flex items-center gap-4">
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
