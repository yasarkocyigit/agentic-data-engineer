import React, { useState, useEffect, useCallback, Suspense } from 'react';
import { Sidebar } from '@/components/Sidebar';
import { Activity, CheckCircle, Cpu, Database, GitPullRequest, Bot, Terminal, ExternalLink, Search, Table, Bell, Play, Settings, X, FileCode, Maximize2, Minimize2, Eye, Code2, LayoutPanelLeft } from 'lucide-react';
import { useSearchParams, useNavigate } from 'react-router-dom';
import { Prism as SyntaxHighlighter } from 'react-syntax-highlighter';
import ReactMarkdown from 'react-markdown';
import clsx from 'clsx';
import remarkGfm from 'remark-gfm';

// ─── Material Obsidian Theme (matches /data Monaco editor) ───
const codeDarkTheme: { [key: string]: React.CSSProperties } = {
  'code[class*="language-"]': { color: '#eeffff', background: 'none', fontFamily: 'inherit', textAlign: 'left', whiteSpace: 'pre', wordSpacing: 'normal', wordBreak: 'normal', wordWrap: 'normal', lineHeight: '1.5', tabSize: 4, hyphens: 'none' },
  'pre[class*="language-"]': { color: '#eeffff', background: 'transparent', padding: '1em', margin: '0', overflow: 'auto' },
  // ── Comments ──
  'comment': { color: '#546e7a', fontStyle: 'italic' },
  'prolog': { color: '#546e7a' },
  'doctype': { color: '#546e7a' },
  'cdata': { color: '#546e7a' },
  // ── Punctuation ──
  'punctuation': { color: '#89ddff' },
  // ── Properties / Tags ──
  'property': { color: '#f07178' },
  'tag': { color: '#f07178' },
  'boolean': { color: '#f78c6c' },
  'number': { color: '#f78c6c' },
  'constant': { color: '#f78c6c' },
  'symbol': { color: '#f78c6c' },
  'deleted': { color: '#f07178' },
  // ── Strings ──
  'string': { color: '#c3e88d' },
  'char': { color: '#c3e88d' },
  'attr-value': { color: '#c3e88d' },
  'builtin': { color: '#ffcb6b' },
  'inserted': { color: '#c3e88d' },
  // ── Operators / URLs ──
  'operator': { color: '#89ddff' },
  'entity': { color: '#89ddff' },
  'url': { color: '#89ddff' },
  // ── Keywords ──
  'atrule': { color: '#c792ea' },
  'attr-name': { color: '#ffcb6b' },
  'keyword': { color: '#c792ea', fontStyle: 'italic' },
  'selector': { color: '#c792ea' },
  // ── Functions ──
  'function': { color: '#82aaff' },
  'class-name': { color: '#ffcb6b' },
  // ── Regex / Important ──
  'regex': { color: '#c3e88d' },
  'important': { color: '#c792ea', fontWeight: 'bold' },
  'variable': { color: '#f07178' },
  // ── Decorators ──
  'decorator': { color: '#ffcb6b' },
  'annotation': { color: '#ffcb6b' },
  'triple-quoted-string': { color: '#c3e88d', fontStyle: 'italic' },
  // ── YAML keys ──
  'key': { color: '#f07178' },
  'bold': { fontWeight: 'bold' },
  'italic': { fontStyle: 'italic' },
};

// ─── File Content Viewer ───
type OpenFile = {
  name: string;
  path: string;
  content: string;
  language: string;
  extension: string;
  size: number;
  lastModified: string;
  lineCount: number;
};

function HomeContent() {
  const [searchParams] = useSearchParams();
  const navigate = useNavigate();
  const filePath = searchParams.get('file');

  const [openFile, setOpenFile] = useState<OpenFile | null>(null);
  const [fileLoading, setFileLoading] = useState(false);
  const [fileError, setFileError] = useState<string | null>(null);
  const [fullscreen, setFullscreen] = useState(false);
  const [showRawMarkdown, setShowRawMarkdown] = useState(false);

  // Fetch file content when filePath changes
  useEffect(() => {
    if (filePath) {
      fetchFile(filePath);
    } else {
      setOpenFile(null);
    }
  }, [filePath]);

  async function fetchFile(path: string) {
    setFileLoading(true);
    setFileError(null);
    try {
      const res = await fetch(`/api/files?action=readFile&filePath=${encodeURIComponent(path)}`);
      if (!res.ok) {
        const data = await res.json();
        throw new Error(data.error || 'Failed to load file');
      }
      const data = await res.json();
      setOpenFile(data);
    } catch (e: any) {
      setFileError(e.message);
      setOpenFile(null);
    } finally {
      setFileLoading(false);
    }
  }

  function closeFile() {
    navigate('/');
    setOpenFile(null);
    setFullscreen(false);
    setShowRawMarkdown(false);
  }

  // Format bytes
  const formatBytes = (bytes: number) => {
    if (bytes < 1024) return `${bytes} B`;
    if (bytes < 1024 * 1024) return `${(bytes / 1024).toFixed(1)} KB`;
    return `${(bytes / (1024 * 1024)).toFixed(1)} MB`;
  };

  // Syntax highlighting colors based on language
  const getLanguageColor = (lang: string) => {
    const colors: Record<string, string> = {
      python: '#4e94c0', typescript: '#4b8ec2', javascript: '#b8a84e',
      sql: '#82aaff', yaml: '#8a6ea0', markdown: '#6ea8b0',
      shell: '#6ea870', html: '#c07a60', css: '#6e6ea0',
      json: '#8a9a6e', dockerfile: '#4e8a7e', plaintext: '#6c707e',
    };
    return colors[lang] || '#6c707e';
  };

  const [repoName, setRepoName] = useState('Workspace');

  // Fetch Gitea repo name
  useEffect(() => {
    async function fetchRepo() {
      try {
        const res = await fetch('/api/gitea/repos?limit=1');
        if (res.ok) {
          const data = await res.json();
          if (data.repos && data.repos.length > 0) {
            setRepoName(data.repos[0].full_name);
          }
        }
      } catch (e) {
        console.error('Failed to fetch repo name', e);
      }
    }
    fetchRepo();
  }, []);

  return (
    <div className="flex h-screen bg-[#09090b] text-foreground font-sans overflow-hidden relative" style={{ fontFamily: "'Inter', -apple-system, sans-serif" }}>
      {/* Ambient Lighting */}
      <div className="absolute top-0 left-0 w-[800px] h-[800px] bg-purple-500/5 rounded-full blur-[120px] pointer-events-none -translate-x-1/4 -translate-y-1/4 z-0" />
      <div className="absolute bottom-0 right-0 w-[600px] h-[600px] bg-sky-500/5 rounded-full blur-[100px] pointer-events-none translate-x-1/4 translate-y-1/4 z-0" />

      {/* Sidebar */}
      <div className="relative z-10 shrink-0">
        <Sidebar />
      </div>

      {/* Main Content */}
      <main className="flex-1 flex flex-col min-w-0 overflow-hidden relative z-10">

        {/* Top Toolbar */}
        <header className="flex items-center px-4 justify-between shrink-0 h-10 bg-black/40 backdrop-blur-md border-b border-white/5 z-10 w-full relative">
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
            <span className="font-bold text-foreground">{repoName}</span>
            <span className="text-obsidian-muted">/</span>
            <span className="text-foreground">{openFile ? openFile.name : 'Dashboard'}</span>
          </div>

          <div className="flex items-center space-x-3">
            <div className="flex items-center px-3 py-1 bg-black/20 border border-white/5 rounded-md text-[11px] font-medium text-foreground gap-2 transition-all">
              <span className="w-2 h-2 rounded-full bg-white/40 animate-pulse"></span>
              ClawdBot: Online
            </div>
            <button className="p-1.5 text-obsidian-muted hover:text-foreground hover:bg-white/5 rounded-md transition-all active:scale-95">
              <Bell className="w-4 h-4" />
            </button>
            <button className="p-1.5 text-obsidian-muted hover:text-foreground hover:bg-white/5 rounded-md transition-all active:scale-95">
              <Settings className="w-4 h-4" />
            </button>
          </div>
        </header>

        {/* File Viewer or Dashboard */}
        {openFile || fileLoading || fileError ? (
          <div className={`flex-1 flex flex-col min-h-0 ${fullscreen ? 'fixed inset-0 z-50 bg-[#09090b]' : ''}`}>
            {/* Tab Bar */}
            <div className="flex items-end overflow-x-auto shrink-0 bg-transparent h-[38px] w-full border-b border-white/5 px-2 gap-1 pt-1.5 z-0">
              <div className="flex items-center h-full bg-black/40 backdrop-blur-md rounded-t-lg mx-1 px-4 gap-2 relative group min-w-[140px] max-w-[220px] border-t border-x border-white/5 text-white">
                {/* Top Highlight Feature */}
                <div className="absolute top-0 left-0 right-0 h-[2px] bg-white/10 rounded-t-lg"></div>

                <FileCode className="w-3.5 h-3.5 drop-shadow-md" style={{ color: getLanguageColor(openFile?.language || 'plaintext') }} />
                <span className="text-[12px] font-medium text-foreground truncate flex-1 tracking-wide">{openFile?.name || 'Loading...'}</span>
                {openFile && (
                  <span className="text-[9px] text-obsidian-muted ml-1 opacity-0 group-hover:opacity-100 transition-opacity uppercase tracking-widest">{openFile.language}</span>
                )}
                <button onClick={closeFile} className="ml-2 text-obsidian-muted hover:text-white transition-all p-1 hover:bg-white/10 rounded-md active:scale-90 bg-black/20">
                  <X className="w-3 h-3" />
                </button>
              </div>
              <div className="flex-1" />
              <div className="flex items-center gap-2 px-3 pb-1">
                {openFile && (openFile.language === 'markdown' || openFile.language === 'html') && (
                  <button
                    onClick={() => setShowRawMarkdown(!showRawMarkdown)}
                    className="flex items-center gap-1.5 px-2.5 py-1 rounded-md text-[10px] uppercase tracking-wider font-bold text-obsidian-muted hover:text-foreground hover:bg-white/10 transition-all active:scale-95 border border-white/5 bg-black/20"
                    title={showRawMarkdown ? 'Preview' : 'Source'}
                  >
                    {showRawMarkdown ? <Eye className="w-3.5 h-3.5" /> : <Code2 className="w-3.5 h-3.5" />}
                    {showRawMarkdown ? 'Preview' : 'Source'}
                  </button>
                )}
                {openFile && (
                  <span className="text-[10px] text-obsidian-muted font-mono bg-black/20 px-2 py-0.5 rounded-md border border-white/5">
                    {openFile.lineCount}L · {formatBytes(openFile.size)}
                  </span>
                )}
                <button
                  onClick={() => setFullscreen(!fullscreen)}
                  className="text-obsidian-muted hover:text-white transition-all p-1.5 hover:bg-white/10 rounded-md active:scale-90"
                  title={fullscreen ? 'Exit Fullscreen' : 'Fullscreen'}
                >
                  {fullscreen ? <Minimize2 className="w-4 h-4" /> : <Maximize2 className="w-4 h-4" />}
                </button>
              </div>
            </div>

            {/* File Content */}
            <div className="flex-1 overflow-auto bg-black/20 backdrop-blur-xl custom-scrollbar relative z-0">
              {fileLoading && (
                <div className="flex items-center justify-center h-full">
                  <div className="text-obsidian-muted text-sm animate-pulse">Loading file...</div>
                </div>
              )}
              {fileError && (
                <div className="flex items-center justify-center h-full">
                  <div className="text-obsidian-danger text-sm">{fileError}</div>
                </div>
              )}
              {openFile && openFile.language === 'markdown' && !showRawMarkdown ? (
                /* ── Markdown Preview ── */
                <div className="p-8 max-w-4xl mx-auto markdown-preview">
                  <ReactMarkdown remarkPlugins={[remarkGfm]}>
                    {openFile.content}
                  </ReactMarkdown>
                </div>
              ) : openFile && openFile.language === 'html' && !showRawMarkdown ? (
                /* ── HTML Preview ── */
                <iframe
                  srcDoc={openFile.content}
                  className="w-full h-full border-0"
                  sandbox="allow-scripts allow-same-origin"
                  title={`Preview: ${openFile.name}`}
                  style={{ background: '#fff' }}
                />
              ) : openFile ? (
                <SyntaxHighlighter
                  language={openFile.language === 'shell' ? 'bash' : openFile.language === 'markdown' ? 'markdown' : openFile.language}
                  style={codeDarkTheme}
                  showLineNumbers={true}
                  wrapLongLines={false}
                  customStyle={{
                    margin: 0,
                    padding: '12px 0',
                    background: 'transparent',
                    fontSize: '13px',
                    lineHeight: '22px',
                    borderRadius: 0,
                    height: '100%',
                  }}
                  lineNumberStyle={{
                    minWidth: '3em',
                    paddingRight: '16px',
                    color: '#3a3a4a',
                    borderRight: '1px solid #2a2a30',
                    marginRight: '16px',
                    userSelect: 'none',
                  }}
                  codeTagProps={{
                    style: {
                      fontFamily: "'JetBrains Mono', 'Fira Code', 'Cascadia Code', 'Consolas', monospace",
                    }
                  }}
                >
                  {openFile.content}
                </SyntaxHighlighter>
              ) : null}
            </div>

            {/* Bottom Status Bar */}
            {openFile && (
              <div className="h-6 bg-black/40 backdrop-blur-md border-t border-white/5 flex items-center px-3 text-[10px] text-obsidian-muted gap-4 shrink-0 relative z-10">
                <span className="flex items-center gap-1">
                  <span className="w-1.5 h-1.5 rounded-full" style={{ backgroundColor: getLanguageColor(openFile.language) }} />
                  {openFile.language}
                </span>
                <span>UTF-8</span>
                <span>{openFile.lineCount} lines</span>
                <span className="ml-auto">{openFile.path}</span>
              </div>
            )}
          </div>
        ) : (
          /* Dashboard View */
          <div className="flex-1 flex flex-col p-0 overflow-hidden">

            {/* Top Section: stats */}
            <div className="h-[140px] border-b border-white/5 bg-black/20 backdrop-blur-xl flex shrink-0 relative">
              {/* Card 1: DAGs */}
              <div className="flex-1 border-r border-white/5 p-5 flex flex-col justify-between hover:bg-white/5 transition-colors z-10 relative">
                <div className="flex justify-between items-start">
                  <span className="text-[10px] text-obsidian-muted font-bold uppercase tracking-widest">Airflow DAGs</span>
                  <Activity className="w-4 h-4 text-white/40" />
                </div>
                <div>
                  <div className="text-3xl font-mono text-white tracking-tight">12 / 12</div>
                  <div className="flex items-center mt-1 text-[11px] text-white/50 font-medium">
                    <CheckCircle className="w-3.5 h-3.5 mr-1 text-white/40" /> All Operational
                  </div>
                </div>
              </div>

              {/* Card 2: Agent */}
              <div className="flex-1 border-r border-white/5 p-5 flex flex-col justify-between hover:bg-white/5 transition-colors z-10 relative">
                <div className="flex justify-between items-start">
                  <span className="text-[10px] text-obsidian-muted font-bold uppercase tracking-widest">Agent Status</span>
                  <Bot className="w-4 h-4 text-white/40" />
                </div>
                <div>
                  <div className="text-[22px] font-mono text-white tracking-tight truncate">Thinking...</div>
                  <div className="flex items-center mt-1 text-[11px] text-white/50 font-medium">
                    <GitPullRequest className="w-3.5 h-3.5 mr-1 text-white/40" /> Reviewing PR #42
                  </div>
                </div>
              </div>

              {/* Card 3: System Load */}
              <div className="flex-1 border-r border-white/5 p-5 flex flex-col justify-between hover:bg-white/5 transition-colors z-10 relative">
                <div className="flex justify-between items-start">
                  <span className="text-[10px] text-obsidian-muted font-bold uppercase tracking-widest">System Load</span>
                  <Cpu className="w-4 h-4 text-white/40" />
                </div>
                <div>
                  <div className="text-3xl font-mono text-white tracking-tight">34%</div>
                  <div className="w-full bg-black/40 border border-white/5 h-[3px] mt-2 rounded-full overflow-hidden">
                    <div className="bg-white/40 h-full w-[34%] rounded-full"></div>
                  </div>
                </div>
              </div>

              {/* Card 4: Data Volume */}
              <div className="flex-1 p-5 flex flex-col justify-between hover:bg-white/5 transition-colors z-10 relative">
                <div className="flex justify-between items-start">
                  <span className="text-[10px] text-obsidian-muted font-bold uppercase tracking-widest">Data Volume</span>
                  <Database className="w-4 h-4 text-white/40" />
                </div>
                <div>
                  <div className="text-3xl font-mono text-white tracking-tight">1.2 TB</div>
                  <div className="mt-1 text-[11px] text-obsidian-muted/90 font-medium">+12GB today</div>
                </div>
              </div>
            </div>

            {/* Bottom Section: Split Pane */}
            <div className="flex-1 flex min-h-0">
              {/* Left: Console / Output */}
              <div className="flex-[2] border-r border-white/5 flex flex-col bg-black/20 backdrop-blur-xl relative">
                <div className="h-8 bg-black/40 border-b border-white/5 flex items-center px-4 justify-between shrink-0 shadow-sm z-10 relative">
                  <div className="flex items-center gap-2">
                    <Terminal className="w-3.5 h-3.5 text-white/40" />
                    <span className="text-[10px] font-bold text-obsidian-muted uppercase tracking-widest">Agent Console Output</span>
                  </div>
                </div>
                <div className="flex-1 overflow-auto p-4 font-mono text-[11px] leading-6 custom-scrollbar relative z-10">
                  <div className="text-obsidian-muted group hover:bg-white/5 px-2 -mx-2 rounded transition-colors"><span className="text-white/30">[10:42:15]</span> <span className="text-white/60 font-bold">INFO...</span> Started metadata analysis of `medallion_pipeline.py`</div>
                  <div className="text-obsidian-muted group hover:bg-white/5 px-2 -mx-2 rounded transition-colors"><span className="text-white/30">[10:42:25]</span> <span className="text-white/40 font-bold">ACTION.</span> Executing `pip install pandas==2.1.0` in Docker container...</div>
                  <div className="text-obsidian-muted group hover:bg-white/5 px-2 -mx-2 rounded transition-colors"><span className="text-white/30">[10:43:02]</span> <span className="text-white/60 font-bold">SUCCESS</span> Dependencies resolved.</div>
                  <div className="text-obsidian-muted group hover:bg-white/5 px-2 -mx-2 rounded transition-colors"><span className="text-white/30">[10:43:10]</span> <span className="text-white/60 font-bold">PR OPEN</span> Created PR #43: "Fix dependency issue"</div>
                  <div className="flex items-center text-foreground mt-2 px-2 -mx-2">
                    <span className="mr-2 text-white/40">❯</span>
                    <span className="w-2 h-3.5 bg-white/40 animate-pulse"></span>
                  </div>
                </div>
              </div>

              {/* Right: Iceberg Catalog */}
              <div className="flex-1 flex flex-col bg-black/20 backdrop-blur-xl relative">
                <div className="h-8 bg-black/40 border-b border-white/5 flex items-center px-4 justify-between shrink-0 shadow-sm z-10 relative">
                  <span className="text-[10px] font-bold text-obsidian-muted uppercase tracking-widest">Iceberg Catalog</span>
                  <Search className="w-3.5 h-3.5 text-obsidian-muted" />
                </div>
                <div className="flex-1 overflow-auto p-2 custom-scrollbar relative z-10">
                  <table className="w-full text-left border-collapse">
                    <thead>
                      <tr>
                        <th className="p-2 px-3 pb-3 text-[10px] text-obsidian-muted font-bold uppercase tracking-wider">Table</th>
                        <th className="p-2 px-3 pb-3 text-[10px] text-obsidian-muted font-bold uppercase tracking-wider text-right">Modified</th>
                      </tr>
                    </thead>
                    <tbody className="text-[12px]">
                      {[
                        { name: 'orders_bronze', time: '2m', color: 'text-white/40' },
                        { name: 'customer_silver', time: '15m', color: 'text-white/40' },
                        { name: 'revenue_gold', time: '1h', color: 'text-white/40' }
                      ].map((item, i) => (
                        <tr key={i} className="group cursor-pointer">
                          <td className="p-2 px-3 border-b border-white/5 text-foreground flex items-center gap-2.5 transition-colors group-hover:bg-white/[0.02] rounded-l-md border-l-2 border-transparent group-hover:border-white/20">
                            <Table className={clsx("w-4 h-4", item.color)} />
                            <span className="group-hover:text-white transition-colors">{item.name}</span>
                          </td>
                          <td className="p-2 px-3 border-b border-white/5 text-obsidian-muted text-right transition-colors group-hover:bg-white/[0.02] rounded-r-md group-hover:text-foreground">
                            {item.time}
                          </td>
                        </tr>
                      ))}
                    </tbody>
                  </table>
                </div>
              </div>
            </div>
          </div>
        )}
      </main>
    </div>
  );
}

export default function Home() {
  return (
    <Suspense fallback={
      <div className="flex h-screen bg-obsidian-bg text-foreground items-center justify-center">
        <div className="animate-pulse text-obsidian-muted">Loading...</div>
      </div>
    }>
      <HomeContent />
    </Suspense>
  );
}
