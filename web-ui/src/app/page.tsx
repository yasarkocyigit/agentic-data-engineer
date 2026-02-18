import React, { useState, useEffect, useCallback, Suspense } from 'react';
import { Sidebar } from '@/components/Sidebar';
import { Activity, CheckCircle, Cpu, Database, GitPullRequest, Bot, Terminal, ExternalLink, Search, Table, Bell, Play, Settings, X, FileCode, Maximize2, Minimize2, Eye, Code2 } from 'lucide-react';
import { useSearchParams, useNavigate } from 'react-router-dom';
import { Prism as SyntaxHighlighter } from 'react-syntax-highlighter';
import ReactMarkdown from 'react-markdown';
import remarkGfm from 'remark-gfm';

// ─── One Dark Pro Theme ───
const codeDarkTheme: { [key: string]: React.CSSProperties } = {
  'code[class*="language-"]': { color: '#abb2bf', background: 'none', fontFamily: 'inherit', textAlign: 'left', whiteSpace: 'pre', wordSpacing: 'normal', wordBreak: 'normal', wordWrap: 'normal', lineHeight: '1.5', tabSize: 4, hyphens: 'none' },
  'pre[class*="language-"]': { color: '#abb2bf', background: '#1e1f22', padding: '1em', margin: '0', overflow: 'auto' },
  // ── Comments ──
  'comment': { color: '#7f848e', fontStyle: 'italic' },
  'prolog': { color: '#7f848e' },
  'doctype': { color: '#7f848e' },
  'cdata': { color: '#7f848e' },
  // ── Punctuation ──
  'punctuation': { color: '#abb2bf' },
  // ── Properties / Tags ──
  'property': { color: '#e06c75' },
  'tag': { color: '#e06c75' },
  'boolean': { color: '#d19a66' },
  'number': { color: '#d19a66' },
  'constant': { color: '#d19a66' },
  'symbol': { color: '#d19a66' },
  'deleted': { color: '#e06c75' },
  // ── Strings ──
  'string': { color: '#98c379' },
  'char': { color: '#98c379' },
  'attr-value': { color: '#98c379' },
  'builtin': { color: '#e5c07b' },
  'inserted': { color: '#98c379' },
  // ── Operators / URLs ──
  'operator': { color: '#56b6c2' },
  'entity': { color: '#56b6c2' },
  'url': { color: '#56b6c2' },
  // ── Keywords ──
  'atrule': { color: '#c678dd' },
  'attr-name': { color: '#d19a66' },
  'keyword': { color: '#c678dd' },
  'selector': { color: '#c678dd' },
  // ── Functions ──
  'function': { color: '#61afef' },
  'class-name': { color: '#e5c07b' },
  // ── Regex / Important ──
  'regex': { color: '#98c379' },
  'important': { color: '#c678dd', fontWeight: 'bold' },
  'variable': { color: '#e06c75' },
  // ── Decorators ──
  'decorator': { color: '#e5c07b' },
  'annotation': { color: '#e5c07b' },
  'triple-quoted-string': { color: '#98c379', fontStyle: 'italic' },
  // ── YAML keys ──
  'key': { color: '#e06c75' },
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
      python: '#4584b6', typescript: '#3178c6', javascript: '#f7df1e',
      sql: '#e38c00', yaml: '#cb4a32', markdown: '#519aba',
      shell: '#499c54', html: '#e34c26', css: '#264de4',
      json: '#5b5ea6', dockerfile: '#0db7ed', plaintext: '#6c707e',
    };
    return colors[lang] || '#6c707e';
  };

  return (
    <div className="flex h-screen bg-[#1e1f22] text-[#bcbec4] font-sans overflow-hidden">
      <Sidebar />

      {/* Main Content */}
      <main className="flex-1 flex flex-col min-w-0 overflow-hidden relative">

        {/* Top Toolbar */}
        <header className="h-9 bg-[#2b2d30] border-b border-[#393b40] flex items-center justify-between px-4 shrink-0">
          <div className="flex items-center gap-2 text-[12px]">
            <span className="font-bold text-[#bcbec4]">Workspace</span>
            <span className="text-[#6c707e]">/</span>
            <span className="text-[#bcbec4]">{openFile ? openFile.name : 'Dashboard'}</span>
          </div>

          <div className="flex items-center space-x-3">
            <div className="flex items-center px-2 py-0.5 bg-[#3c3f41] border border-[#555] rounded text-[11px] text-[#bcbec4] gap-2">
              <span className="w-1.5 h-1.5 rounded-full bg-[#499c54]"></span>
              ClawdBot: Online
            </div>
            <Bell className="w-3.5 h-3.5 text-[#6c707e] hover:text-[#bcbec4] cursor-pointer" />
            <Settings className="w-3.5 h-3.5 text-[#6c707e] hover:text-[#bcbec4] cursor-pointer" />
          </div>
        </header>

        {/* File Viewer or Dashboard */}
        {openFile || fileLoading || fileError ? (
          <div className={`flex-1 flex flex-col min-h-0 ${fullscreen ? 'fixed inset-0 z-50 bg-[#1e1f22]' : ''}`}>
            {/* Tab Bar */}
            <div className="h-8 bg-[#2b2d30] border-b border-[#393b40] flex items-center shrink-0">
              <div className="flex items-center h-full bg-[#1e1f22] border-r border-[#393b40] px-3 gap-2">
                <FileCode className="w-3.5 h-3.5" style={{ color: getLanguageColor(openFile?.language || 'plaintext') }} />
                <span className="text-[12px] text-[#bcbec4]">{openFile?.name || 'Loading...'}</span>
                {openFile && (
                  <span className="text-[9px] text-[#6c707e] ml-1">{openFile.language}</span>
                )}
                <button onClick={closeFile} className="ml-2 text-[#6c707e] hover:text-white transition-colors">
                  <X className="w-3 h-3" />
                </button>
              </div>
              <div className="flex-1" />
              <div className="flex items-center gap-2 px-3">
                {openFile && (openFile.language === 'markdown' || openFile.language === 'html') && (
                  <button
                    onClick={() => setShowRawMarkdown(!showRawMarkdown)}
                    className="flex items-center gap-1 px-2 py-0.5 rounded text-[10px] text-[#6c707e] hover:text-[#bcbec4] hover:bg-[#393b40] transition-colors"
                    title={showRawMarkdown ? 'Preview' : 'Source'}
                  >
                    {showRawMarkdown ? <Eye className="w-3 h-3" /> : <Code2 className="w-3 h-3" />}
                    {showRawMarkdown ? 'Preview' : 'Source'}
                  </button>
                )}
                {openFile && (
                  <span className="text-[10px] text-[#6c707e]">
                    {openFile.lineCount} lines · {formatBytes(openFile.size)}
                  </span>
                )}
                <button
                  onClick={() => setFullscreen(!fullscreen)}
                  className="text-[#6c707e] hover:text-white transition-colors"
                  title={fullscreen ? 'Exit Fullscreen' : 'Fullscreen'}
                >
                  {fullscreen ? <Minimize2 className="w-3.5 h-3.5" /> : <Maximize2 className="w-3.5 h-3.5" />}
                </button>
              </div>
            </div>

            {/* File Content */}
            <div className="flex-1 overflow-auto bg-[#1e1f22]">
              {fileLoading && (
                <div className="flex items-center justify-center h-full">
                  <div className="text-[#6c707e] text-sm animate-pulse">Loading file...</div>
                </div>
              )}
              {fileError && (
                <div className="flex items-center justify-center h-full">
                  <div className="text-[#e06c75] text-sm">{fileError}</div>
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
                    background: '#1e1f22',
                    fontSize: '13px',
                    lineHeight: '22px',
                    borderRadius: 0,
                    height: '100%',
                  }}
                  lineNumberStyle={{
                    minWidth: '3em',
                    paddingRight: '16px',
                    color: '#4b5263',
                    borderRight: '1px solid #393b40',
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
              <div className="h-6 bg-[#2b2d30] border-t border-[#393b40] flex items-center px-3 text-[10px] text-[#6c707e] gap-4 shrink-0">
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
            <div className="h-[140px] border-b border-[#393b40] bg-[#2b2d30] flex">
              <div className="flex-1 border-r border-[#393b40] p-3">
                <div className="flex justify-between items-start mb-2">
                  <span className="text-[11px] text-[#6c707e] font-bold uppercase">Airflow DAGs</span>
                  <Activity className="w-3.5 h-3.5 text-[#499c54]" />
                </div>
                <div className="text-2xl font-mono text-[#bcbec4]">12 / 12</div>
                <div className="flex items-center mt-2 text-[11px] text-[#499c54]">
                  <CheckCircle className="w-3 h-3 mr-1" /> All Operational
                </div>
              </div>

              <div className="flex-1 border-r border-[#393b40] p-3">
                <div className="flex justify-between items-start mb-2">
                  <span className="text-[11px] text-[#6c707e] font-bold uppercase">Agent Status</span>
                  <Bot className="w-3.5 h-3.5 text-[#3574f0]" />
                </div>
                <div className="text-xl font-mono text-[#bcbec4]">Thinking...</div>
                <div className="flex items-center mt-2 text-[11px] text-[#3574f0]">
                  <GitPullRequest className="w-3 h-3 mr-1" /> Reviewing PR #42
                </div>
              </div>

              <div className="flex-1 border-r border-[#393b40] p-3">
                <div className="flex justify-between items-start mb-2">
                  <span className="text-[11px] text-[#6c707e] font-bold uppercase">System Load</span>
                  <Cpu className="w-3.5 h-3.5 text-[#bcbec4]" />
                </div>
                <div className="text-2xl font-mono text-[#bcbec4]">34%</div>
                <div className="w-full bg-[#3c3f41] h-1 mt-3 rounded-full overflow-hidden">
                  <div className="bg-[#3574f0] h-full w-[34%]"></div>
                </div>
              </div>

              <div className="flex-1 p-3">
                <div className="flex justify-between items-start mb-2">
                  <span className="text-[11px] text-[#6c707e] font-bold uppercase">Data Volume</span>
                  <Database className="w-3.5 h-3.5 text-[#9aa7b0]" />
                </div>
                <div className="text-2xl font-mono text-[#bcbec4]">1.2 TB</div>
                <div className="mt-2 text-[11px] text-[#6c707e]">+12GB today</div>
              </div>
            </div>

            {/* Bottom Section: Split Pane */}
            <div className="flex-1 flex min-h-0">
              {/* Left: Console / Output */}
              <div className="flex-[2] border-r border-[#393b40] flex flex-col bg-[#1e1f22]">
                <div className="h-7 bg-[#3c3f41] border-b border-[#393b40] flex items-center px-3 justify-between">
                  <div className="flex items-center gap-2">
                    <Terminal className="w-3.5 h-3.5 text-[#6c707e]" />
                    <span className="text-[11px] font-bold text-[#bcbec4]">Agent Console Output</span>
                  </div>
                </div>
                <div className="flex-1 overflow-auto p-2 font-mono text-[11px] leading-5">
                  <div className="text-[#6c707e]">[10:42:15] <span className="text-[#3574f0]">INFO</span> Started metadata analysis of `medallion_pipeline.py`</div>
                  <div className="text-[#6c707e]">[10:42:25] <span className="text-[#9aa7b0]">ACTION</span> Executing `pip install pandas==2.1.0` in Docker container...</div>
                  <div className="text-[#6c707e]">[10:43:02] <span className="text-[#499c54]">SUCCESS</span> Dependencies resolved.</div>
                  <div className="text-[#6c707e]">[10:43:10] <span className="text-[#ffc66d]">PR OPEN</span> Created PR #43: &quot;Fix dependency issue&quot;</div>
                  <div className="flex items-center text-[#bcbec4] mt-1">
                    <span className="mr-1">{'>'}</span>
                    <span className="w-1.5 h-3 bg-[#bcbec4] animate-pulse"></span>
                  </div>
                </div>
              </div>

              {/* Right: Iceberg Catalog */}
              <div className="flex-1 flex flex-col bg-[#2b2d30]">
                <div className="h-7 bg-[#3c3f41] border-b border-[#393b40] flex items-center px-3 justify-between">
                  <span className="text-[11px] font-bold text-[#bcbec4]">Iceberg Catalog</span>
                  <Search className="w-3 h-3 text-[#6c707e]" />
                </div>
                <div className="flex-1 overflow-auto">
                  <table className="w-full text-left border-collapse">
                    <thead className="bg-[#3c3f41]">
                      <tr>
                        <th className="p-1 px-3 border-b border-[#393b40] text-[10px] text-[#6c707e] font-normal">Table</th>
                        <th className="p-1 px-3 border-b border-[#393b40] text-[10px] text-[#6c707e] font-normal text-right">Modified</th>
                      </tr>
                    </thead>
                    <tbody className="text-[11px]">
                      {[
                        { name: 'orders_bronze', time: '2m' },
                        { name: 'customer_silver', time: '15m' },
                        { name: 'revenue_gold', time: '1h' }
                      ].map((item, i) => (
                        <tr key={i} className="hover:bg-[#3c3f41]">
                          <td className="p-1 px-3 border-b border-[#393b40] text-[#a9b7c6] flex items-center gap-2">
                            <Table className="w-3 h-3 text-[#3574f0]" />
                            {item.name}
                          </td>
                          <td className="p-1 px-3 border-b border-[#393b40] text-[#6c707e] text-right">{item.time}</td>
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
      <div className="flex h-screen bg-[#1e1f22] text-[#bcbec4] items-center justify-center">
        <div className="animate-pulse text-[#6c707e]">Loading...</div>
      </div>
    }>
      <HomeContent />
    </Suspense>
  );
}
