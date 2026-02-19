import React, { useState, useEffect, useRef, Suspense } from 'react';
import { Database, Folder, Table as TableIcon, ChevronRight, Settings, Search, Maximize2, MoreHorizontal, Home, Archive, Terminal, Server, Columns, HardDrive, FileText, FileCode, File as FileIcon, Image as ImageIcon, GitBranch, GitPullRequest, BarChart3, Copy, Play, List, Type } from 'lucide-react';
import { Link, useLocation, useNavigate, useSearchParams } from 'react-router-dom';
import clsx from 'clsx';

type ContextMenuItem = { label: string; icon?: React.ReactNode; action: () => void; separator?: boolean };
type ContextMenuState = { x: number; y: number; items: ContextMenuItem[] } | null;

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

// ─── Activity Bar Items ───
const navItems = [
    { name: 'Home', icon: Home, path: '/' },
    { name: 'Database', icon: Database, path: '/data' },
    { name: 'Lineage', icon: GitBranch, path: '/lineage' },
    { name: 'Visualize', icon: BarChart3, path: '/visualize' },
    { name: 'Storage', icon: HardDrive, path: '/storage' },
    { name: 'Workflows', icon: Archive, path: '/workflows' },
    { name: 'CI/CD', icon: GitPullRequest, path: '/cicd' },
    { name: 'Agent', icon: Terminal, path: '/agent' },
    { name: 'Compute', icon: Server, path: '/compute' },
];

const Sidebar = () => {
    const { pathname } = useLocation();
    const navigate = useNavigate();
    const [searchParams] = useSearchParams();
    const activeFile = searchParams.get('file') || null;

    // ─── Data Explorer Logic ───
    const [dbEngine, setDbEngine] = useState<'trino' | 'postgres'>('trino');
    const [dbItems, setDbItems] = useState<any[]>([]);
    const [pgItems, setPgItems] = useState<any[]>([]);
    const [loadingNodes, setLoadingNodes] = useState<Set<string>>(new Set());
    const [expandedNodes, setExpandedNodes] = useState<Set<string>>(new Set());
    const [contextMenu, setContextMenu] = useState<ContextMenuState>(null);
    const contextMenuRef = useRef<HTMLDivElement>(null);

    // ─── Gitea Repos (CI/CD Sidebar) ───
    const [giteaRepos, setGiteaRepos] = useState<any[]>([]);
    const [giteaReposLoading, setGiteaReposLoading] = useState(false);

    // ─── Project File Tree ───
    const [fileTree, setFileTree] = useState<any[]>([]);
    const [fileTreeLoading, setFileTreeLoading] = useState(false);
    const [expandedFiles, setExpandedFiles] = useState<Set<string>>(new Set(['.']));

    // File icon helper
    const getFileTypeIcon = (name: string, ext?: string) => {
        if (!ext) return FileIcon;
        if (['py'].includes(ext)) return FileCode;
        if (['ts', 'tsx', 'js', 'jsx'].includes(ext)) return FileCode;
        if (['sql'].includes(ext)) return Database;
        if (['md', 'txt', 'csv'].includes(ext)) return FileText;
        if (['yml', 'yaml', 'json', 'toml'].includes(ext)) return FileCode;
        if (['sh', 'bash'].includes(ext)) return Terminal;
        if (['png', 'jpg', 'jpeg', 'gif', 'svg', 'webp'].includes(ext)) return ImageIcon;
        if (['html', 'css'].includes(ext)) return FileCode;
        if (['dockerfile'].includes(name.toLowerCase())) return Server;
        return FileIcon;
    };

    // Fetch Gitea repos for CI/CD sidebar
    useEffect(() => {
        if (pathname === '/cicd') {
            fetchGiteaRepos();
        }
    }, [pathname]);

    useEffect(() => {
        if (pathname !== '/cicd' || !giteaReposLoading) return;
        const t = window.setTimeout(() => setGiteaReposLoading(false), 12000);
        return () => window.clearTimeout(t);
    }, [pathname, giteaReposLoading]);

    async function fetchGiteaRepos() {
        setGiteaReposLoading(true);
        try {
            const data = await fetchJsonWithTimeout('/api/gitea/repos?limit=50', 10000);
            setGiteaRepos(data.repos || []);
        } catch (e) {
            console.error('Failed to fetch Gitea repos', e);
            setGiteaRepos([]);
        } finally {
            setGiteaReposLoading(false);
        }
    }

    // Fetch file tree
    useEffect(() => {
        if (pathname === '/') {
            fetchFileTree();
        }
    }, [pathname]);

    async function fetchFileTree() {
        if (fileTree.length > 0) return;
        setFileTreeLoading(true);
        try {
            const res = await fetch('/api/files?maxDepth=4');
            const data = await res.json();
            setFileTree(data.tree || []);
        } catch (e) {
            console.error('Failed to fetch file tree', e);
        } finally {
            setFileTreeLoading(false);
        }
    }

    const toggleFileNode = (path: string) => {
        setExpandedFiles(prev => {
            const next = new Set(prev);
            if (next.has(path)) next.delete(path);
            else next.add(path);
            return next;
        });
    };

    // File tree renderer
    const renderFileTree = (nodes: any[], level = 0) => {
        return nodes.map(node => {
            const isDir = node.type === 'directory';
            const isExpanded = expandedFiles.has(node.path);
            const Icon = isDir ? Folder : getFileTypeIcon(node.name, node.extension);

            // Format file size
            const formatSize = (bytes?: number) => {
                if (!bytes) return '';
                if (bytes < 1024) return `${bytes} B`;
                if (bytes < 1024 * 1024) return `${(bytes / 1024).toFixed(1)} KB`;
                return `${(bytes / (1024 * 1024)).toFixed(1)} MB`;
            };

            return (
                <div key={node.path}>
                    <div
                        className={clsx(
                            "flex items-center gap-1.5 py-[1px] px-2 cursor-pointer hover:bg-obsidian-panel-hover text-foreground select-none text-[13px]",
                            !isDir && activeFile === node.path && 'bg-obsidian-info text-white'
                        )}
                        onClick={() => {
                            if (isDir) {
                                toggleFileNode(node.path);
                            } else {
                                navigate(`/?file=${encodeURIComponent(node.path)}`);
                            }
                        }}
                        style={{ paddingLeft: `${(level * 12) + 8}px` }}
                    >
                        <div className="w-4 h-4 flex items-center justify-center flex-shrink-0">
                            {isDir ? (
                                <ChevronRight className={`w-3.5 h-3.5 text-obsidian-muted transition-transform ${isExpanded ? 'rotate-90' : ''}`} />
                            ) : (
                                <div className="w-4" />
                            )}
                        </div>
                        <Icon className={`w-4 h-4 flex-shrink-0 ${isDir ? 'text-obsidian-folder' :
                            node.extension === 'py' ? 'text-obsidian-python' :
                                ['ts', 'tsx'].includes(node.extension) ? 'text-obsidian-typescript' :
                                    node.extension === 'sql' ? 'text-obsidian-sql' :
                                        ['yml', 'yaml'].includes(node.extension) ? 'text-obsidian-yaml' :
                                            node.extension === 'md' ? 'text-obsidian-markdown' :
                                                ['sh', 'bash'].includes(node.extension) ? 'text-obsidian-success' :
                                                    'text-obsidian-muted'
                            }`} />
                        <span className="truncate">{node.name}</span>
                        {!isDir && node.size && (
                            <span className="ml-auto text-[10px] text-obsidian-muted opacity-0 group-hover:opacity-100 transition-opacity whitespace-nowrap">
                                {formatSize(node.size)}
                            </span>
                        )}
                    </div>
                    {isDir && isExpanded && node.children && (
                        <div>{renderFileTree(node.children, level + 1)}</div>
                    )}
                </div>
            );
        });
    };

    // Fetch Catalogs on Mount (Trino)
    useEffect(() => {
        if (pathname === '/data' && dbEngine === 'trino') {
            fetchCatalogs();
        }
    }, [pathname, dbEngine]);

    async function runQuery(query: string) {
        try {
            const res = await fetch('/api/trino', {
                method: 'POST',
                headers: { 'Content-Type': 'application/json' },
                body: JSON.stringify({ query })
            });
            const json = await res.json();
            return json.data || [];
        } catch (e) {
            console.error(e);
            return [];
        }
    }

    async function fetchCatalogs() {
        if (dbItems.length > 0) return;

        const rows = await runQuery('SHOW CATALOGS');
        const catalogs = rows.map((r: any) => ({
            id: r.Catalog,
            name: r.Catalog,
            type: 'database',
            children: [],
            loaded: false
        }));
        setDbItems(catalogs);
    }

    async function fetchPgDatabases() {
        if (pgItems.length > 0) return;
        try {
            const res = await fetch('/api/postgres', {
                method: 'POST',
                headers: { 'Content-Type': 'application/json' },
                body: JSON.stringify({ action: 'explore', type: 'databases' })
            });
            const json = await res.json();
            const databases = (json.data || []).map((r: any) => ({
                id: `pg:${r.name}`,
                name: r.name,
                type: 'database',
                engine: 'postgres',
                children: [],
                loaded: false
            }));
            setPgItems(databases);
        } catch (e) {
            console.error('Failed to fetch PG databases', e);
        }
    }

    async function togglePgNode(item: any) {
        const newExpanded = new Set(expandedNodes);
        if (newExpanded.has(item.id)) {
            newExpanded.delete(item.id);
            setExpandedNodes(newExpanded);
            return;
        }

        newExpanded.add(item.id);
        setExpandedNodes(newExpanded);

        if (!item.loaded) {
            setLoadingNodes(prev => new Set(prev).add(item.id));
            let children: any[] = [];

            try {
                if (item.type === 'database') {
                    const res = await fetch('/api/postgres', {
                        method: 'POST',
                        headers: { 'Content-Type': 'application/json' },
                        body: JSON.stringify({ action: 'explore', type: 'schemas', database: item.name })
                    });
                    const json = await res.json();
                    children = (json.data || []).map((r: any) => ({
                        id: `${item.id}.${r.name}`,
                        name: r.name,
                        type: 'schema',
                        engine: 'postgres',
                        parentId: item.id,
                        children: [],
                        loaded: false
                    }));
                } else if (item.type === 'schema') {
                    const res = await fetch('/api/postgres', {
                        method: 'POST',
                        headers: { 'Content-Type': 'application/json' },
                        body: JSON.stringify({ action: 'explore', type: 'tables', schema: item.name })
                    });
                    const json = await res.json();
                    children = (json.data || []).map((r: any) => ({
                        id: `${item.id}.${r.name}`,
                        name: r.name,
                        type: 'table',
                        engine: 'postgres',
                        parentId: item.id,
                        children: [],
                        loaded: false
                    }));
                } else if (item.type === 'table') {
                    const parts = item.id.split('.');
                    const schema = parts[parts.length - 2];
                    const tableName = parts[parts.length - 1];
                    const res = await fetch('/api/postgres', {
                        method: 'POST',
                        headers: { 'Content-Type': 'application/json' },
                        body: JSON.stringify({ action: 'explore', type: 'columns', schema, table: tableName })
                    });
                    const json = await res.json();
                    children = (json.data || []).map((r: any) => ({
                        id: `${item.id}.${r.name}`,
                        name: r.name,
                        dataType: r.data_type,
                        type: 'column',
                        engine: 'postgres',
                        parentId: item.id,
                        loaded: true
                    }));
                }
            } catch (err) {
                console.error('Failed to load PG children', err);
            }

            const updateTree = (nodes: any[]): any[] => {
                return nodes.map(node => {
                    if (node.id === item.id) return { ...node, children, loaded: true };
                    if (node.children) return { ...node, children: updateTree(node.children) };
                    return node;
                });
            };

            setPgItems(prev => updateTree(prev));
            setLoadingNodes(prev => {
                const next = new Set(prev);
                next.delete(item.id);
                return next;
            });
        }
    }

    useEffect(() => {
        if (pathname === '/data') {
            if (dbEngine === 'trino') {
                fetchCatalogs();
            } else {
                fetchPgDatabases();
            }
        }
    }, [pathname, dbEngine]);

    async function toggleNode(item: any) {
        const newExpanded = new Set(expandedNodes);
        if (newExpanded.has(item.id)) {
            newExpanded.delete(item.id);
            setExpandedNodes(newExpanded);
            return;
        }

        newExpanded.add(item.id);
        setExpandedNodes(newExpanded);

        if (!item.loaded) {
            setLoadingNodes(prev => new Set(prev).add(item.id));
            let children: any[] = [];

            try {
                if (item.type === 'database') {
                    const q = `SHOW SCHEMAS FROM "${item.name}"`;
                    const rows = await runQuery(q);
                    children = rows.map((r: any) => ({
                        id: `${item.id}.${r.Schema}`,
                        name: r.Schema,
                        type: 'schema',
                        parentId: item.id,
                        children: [],
                        loaded: false
                    }));
                } else if (item.type === 'schema') {
                    const [catalog, schema] = item.id.split('.');
                    const q = `SHOW TABLES FROM "${catalog}"."${schema}"`;
                    const rows = await runQuery(q);
                    children = rows.map((r: any) => ({
                        id: `${item.id}.${r.Table}`,
                        name: r.Table,
                        type: 'table',
                        parentId: item.id,
                        children: [],
                        loaded: false
                    }));
                } else if (item.type === 'table') {
                    const parts = item.id.split('.');
                    const catalog = parts[0];
                    const schema = parts[1];
                    const table = parts.slice(2).join('.');
                    const q = `SHOW COLUMNS FROM "${catalog}"."${schema}"."${table}"`;
                    const rows = await runQuery(q);
                    children = rows.map((r: any) => ({
                        id: `${item.id}.${r.Column}`,
                        name: r.Column,
                        dataType: r.Type,
                        type: 'column',
                        parentId: item.id,
                        loaded: true
                    }));
                }
            } catch (err) {
                console.error("Failed to load children", err);
            }

            const updateTree = (nodes: any[]): any[] => {
                return nodes.map(node => {
                    if (node.id === item.id) {
                        return { ...node, children, loaded: true };
                    }
                    if (node.children) {
                        return { ...node, children: updateTree(node.children) };
                    }
                    return node;
                });
            };

            setDbItems(prev => updateTree(prev));
            setLoadingNodes(prev => {
                const next = new Set(prev);
                next.delete(item.id);
                return next;
            });
        }
    }

    const getDataTypeColor = (type: string) => {
        const t = type.toLowerCase();
        if (t.includes('int') || t.includes('bigint') || t.includes('smallint') || t.includes('tinyint'))
            return { bg: 'bg-obsidian-info/15', text: 'text-[#5b9bd5]', label: t };
        if (t.includes('double') || t.includes('float') || t.includes('decimal') || t.includes('real') || t.includes('numeric'))
            return { bg: 'bg-obsidian-number/15', text: 'text-obsidian-number', label: t };
        if (t.includes('varchar') || t.includes('char') || t.includes('text') || t.includes('string'))
            return { bg: 'bg-obsidian-string/15', text: 'text-obsidian-string', label: t };
        if (t.includes('timestamp') || t.includes('date') || t.includes('time'))
            return { bg: 'bg-obsidian-date/15', text: 'text-obsidian-date', label: t };
        if (t.includes('bool'))
            return { bg: 'bg-obsidian-boolean/15', text: 'text-obsidian-boolean', label: t };
        if (t.includes('json') || t.includes('map') || t.includes('array') || t.includes('row'))
            return { bg: 'bg-obsidian-object/15', text: 'text-obsidian-object', label: t };
        if (t.includes('binary') || t.includes('blob') || t.includes('bytea') || t.includes('varbinary'))
            return { bg: 'bg-obsidian-binary/15', text: 'text-obsidian-binary', label: t };
        return { bg: 'bg-obsidian-muted/10', text: 'text-obsidian-muted', label: t };
    };

    const getDataTypeStyle = (type: string) => {
        return "text-obsidian-muted";
    };

    // ─── Context Menu Helpers ───
    const copyToClipboard = (text: string) => {
        navigator.clipboard.writeText(text).catch(() => { });
    };

    const buildContextMenuItems = (node: any, isPg: boolean): ContextMenuItem[] => {
        const items: ContextMenuItem[] = [];

        if (node.type === 'table') {
            const fullName = isPg
                ? (() => {
                    const parts = node.id.replace(/^pg:/, '').split('.');
                    return parts.length >= 2 ? `"${parts[parts.length - 2]}"."${parts[parts.length - 1]}"` : `"${node.name}"`;
                })()
                : (() => {
                    const parts = node.id.split('.');
                    return parts.length >= 3 ? `"${parts[0]}"."${parts[1]}"."${parts[2]}"` : `"${node.name}"`;
                })();

            items.push({
                label: 'SELECT TOP 100',
                icon: <Play className="w-3 h-3 text-[#6aab73]" />,
                action: () => {
                    const event = new CustomEvent('openclaw:run-query', {
                        detail: { query: `SELECT * FROM ${fullName} LIMIT 100`, engine: isPg ? 'postgres' : 'trino' }
                    });
                    window.dispatchEvent(event);
                }
            });
            items.push({
                label: 'Generate SELECT',
                icon: <List className="w-3 h-3 text-obsidian-info" />,
                action: () => {
                    const event = new CustomEvent('openclaw:insert-query', {
                        detail: { query: `SELECT\n    *\nFROM ${fullName}\nLIMIT 100;` }
                    });
                    window.dispatchEvent(event);
                }
            });
            items.push({
                label: 'SHOW COLUMNS',
                icon: <Columns className="w-3 h-3 text-[#b07cd8]" />,
                action: () => {
                    if (!isPg) {
                        const event = new CustomEvent('openclaw:run-query', {
                            detail: { query: `SHOW COLUMNS FROM ${fullName}`, engine: 'trino' }
                        });
                        window.dispatchEvent(event);
                    }
                }
            });
            items.push({ label: '', action: () => { }, separator: true });
        }

        items.push({
            label: 'Copy Name',
            icon: <Copy className="w-3 h-3 text-obsidian-muted" />,
            action: () => copyToClipboard(node.name)
        });

        if (node.type === 'column' && node.dataType) {
            items.push({
                label: `Copy Type: ${node.dataType}`,
                icon: <Type className="w-3 h-3 text-obsidian-muted" />,
                action: () => copyToClipboard(node.dataType)
            });
        }

        return items;
    };

    const handleContextMenu = (e: React.MouseEvent, node: any, isPg: boolean) => {
        e.preventDefault();
        e.stopPropagation();
        setContextMenu({
            x: e.clientX,
            y: e.clientY,
            items: buildContextMenuItems(node, isPg)
        });
    };

    const handleDoubleClick = (node: any, isPg: boolean) => {
        if (node.type !== 'table') return;
        const fullName = isPg
            ? (() => {
                const parts = node.id.replace(/^pg:/, '').split('.');
                return parts.length >= 2 ? `"${parts[parts.length - 2]}"."${parts[parts.length - 1]}"` : `"${node.name}"`;
            })()
            : (() => {
                const parts = node.id.split('.');
                return parts.length >= 3 ? `"${parts[0]}"."${parts[1]}"."${parts[2]}"` : `"${node.name}"`;
            })();
        const event = new CustomEvent('openclaw:run-query', {
            detail: { query: `SELECT * FROM ${fullName} LIMIT 100`, engine: isPg ? 'postgres' : 'trino' }
        });
        window.dispatchEvent(event);
    };

    // Close context menu on click outside
    useEffect(() => {
        const handleClick = () => setContextMenu(null);
        if (contextMenu) {
            window.addEventListener('click', handleClick);
            return () => window.removeEventListener('click', handleClick);
        }
    }, [contextMenu]);

    const renderTree = (nodes: any[], level = 0, isPg = false) => {
        return nodes.map(node => {
            const isExpanded = expandedNodes.has(node.id);
            const isLoading = loadingNodes.has(node.id);
            const Icon = node.type === 'database' ? Database :
                node.type === 'schema' ? Folder :
                    node.type === 'table' ? TableIcon :
                        Columns;

            return (
                <div key={node.id}>
                    <div
                        className={clsx(
                            "flex items-center gap-1.5 py-[1px] px-2 cursor-pointer hover:bg-obsidian-panel-hover text-foreground select-none text-[13px] group",
                            level > 0 && "ml-3"
                        )}
                        onClick={() => isPg ? togglePgNode(node) : toggleNode(node)}
                        onDoubleClick={() => handleDoubleClick(node, isPg)}
                        onContextMenu={(e) => handleContextMenu(e, node, isPg)}
                        style={{ paddingLeft: level > 0 ? `${(level * 12) + 8}px` : '8px' }}
                    >
                        <div className="w-4 h-4 flex items-center justify-center flex-shrink-0">
                            {node.children && node.children.length > 0 ? (
                                <ChevronRight className={clsx("w-3.5 h-3.5 text-obsidian-muted transition-transform", isExpanded && "rotate-90")} />
                            ) : (
                                <div className="w-4" />
                            )}
                        </div>

                        <Icon className={clsx("w-4 h-4 flex-shrink-0 transition-colors",
                            node.type === 'database' ? (isPg ? "text-obsidian-postgres" : "text-obsidian-trino") :
                                node.type === 'schema' ? "text-obsidian-folder" :
                                    node.type === 'table' ? "text-[#5b9bd5]" : "text-[#a9b7c6]"
                        )} />

                        <span className={clsx("truncate", node.type === 'column' ? "opacity-90" : "")}>{node.name}</span>

                        {node.dataType && (() => {
                            const dc = getDataTypeColor(node.dataType);
                            return (
                                <span className={clsx(
                                    "ml-auto text-[9px] px-1.5 py-[1px] rounded-[3px] font-medium font-mono tracking-wide transition-opacity",
                                    dc.bg, dc.text
                                )}>
                                    {dc.label}
                                </span>
                            );
                        })()}

                        {isLoading && <span className="ml-auto text-[9px] text-obsidian-muted animate-pulse">...</span>}
                    </div>
                    {isExpanded && node.children && (
                        <div>{renderTree(node.children, level + 1, isPg)}</div>
                    )}
                </div>
            );
        });
    };

    return (
        <div className="flex h-full select-none">
            {/* Activity Bar */}
            <div className="w-12 bg-obsidian-panel border-r border-obsidian-border flex flex-col items-center py-2 space-y-4 z-10 shrink-0">
                {navItems.map((item) => {
                    const Icon = item.icon;
                    const isActive = pathname === item.path || (item.path !== '/' && pathname.startsWith(item.path));
                    return (
                        <Link to={item.path} key={item.path} title={item.name}>
                            <div className={clsx(
                                "p-2 rounded cursor-pointer transition-colors relative group",
                                isActive ? "bg-[#4c5052]" : "hover:bg-obsidian-panel-hover"
                            )}>
                                <Icon className={clsx(
                                    "w-5 h-5",
                                    isActive ? "text-foreground" : "text-obsidian-muted group-hover:text-foreground"
                                )} />
                                {isActive && <div className="absolute left-0 top-0 bottom-0 w-[3px] bg-obsidian-info rounded-r"></div>}
                            </div>
                        </Link>
                    );
                })}
                <div className="mt-auto pb-2">
                    <div className="p-2 cursor-pointer hover:bg-obsidian-panel-hover rounded text-obsidian-muted hover:text-foreground">
                        <Settings className="w-5 h-5" />
                    </div>
                </div>
            </div>

            {/* Sidebar Content */}
            <div className="w-[250px] bg-obsidian-panel border-r border-obsidian-border flex flex-col h-full text-foreground text-[12px] font-sans select-none">
                {/* Header */}
                <div className="h-9 flex items-center px-3 border-b border-obsidian-border bg-obsidian-panel justify-between shrink-0">
                    <span className="font-bold tracking-tight text-foreground">
                        {pathname === '/data' ? 'Database Explorer' :
                            pathname === '/workflows' ? 'Project Structure' :
                                pathname === '/lineage' ? 'Data Lineage' :
                                    pathname === '/visualize' ? 'Dashboards' :
                                        pathname === '/compute' ? 'Infrastructure' :
                                            pathname === '/cicd' ? 'Repositories' : 'Explorer'}
                    </span>
                    <div className="flex gap-1">
                        <Search className="w-3.5 h-3.5 text-obsidian-muted cursor-pointer hover:text-foreground" />
                        <Maximize2 className="w-3.5 h-3.5 text-obsidian-muted cursor-pointer hover:text-foreground" />
                    </div>
                </div>

                {/* Tree View */}
                <div className="flex-1 overflow-y-auto p-1 py-2">
                    {pathname === '/data' ? (
                        <>
                            {/* Engine Tabs */}
                            <div className="flex items-center gap-1 px-2 mb-2">
                                <button
                                    onClick={() => setDbEngine('trino')}
                                    className={clsx(
                                        "flex-1 px-2 py-1 rounded text-[10px] font-medium transition-all",
                                        dbEngine === 'trino'
                                            ? "bg-obsidian-info/20 text-obsidian-info border border-[#3574f0]/30"
                                            : "text-obsidian-muted hover:text-foreground hover:bg-obsidian-panel-hover"
                                    )}
                                >
                                    Trino
                                </button>
                                <button
                                    onClick={() => setDbEngine('postgres')}
                                    className={clsx(
                                        "flex-1 px-2 py-1 rounded text-[10px] font-medium transition-all",
                                        dbEngine === 'postgres'
                                            ? "bg-[#336791]/20 text-[#5b9bd5] border border-[#336791]/30"
                                            : "text-obsidian-muted hover:text-foreground hover:bg-obsidian-panel-hover"
                                    )}
                                >
                                    PostgreSQL
                                </button>
                            </div>
                            {dbEngine === 'trino' ? (
                                <>
                                    {dbItems.length === 0 && (
                                        <div className="ml-4 text-obsidian-muted text-[10px] animate-pulse">Fetching Catalogs...</div>
                                    )}
                                    {renderTree(dbItems)}
                                </>
                            ) : (
                                <>
                                    {pgItems.length === 0 && (
                                        <div className="ml-4 text-obsidian-muted text-[10px] animate-pulse">Fetching Databases...</div>
                                    )}
                                    {renderTree(pgItems, 0, true)}
                                </>
                            )}
                        </>
                    ) : pathname === '/cicd' ? (
                        <>
                            {giteaReposLoading ? (
                                <div className="ml-4 text-obsidian-muted text-[10px] animate-pulse">Loading repos...</div>
                            ) : giteaRepos.length === 0 ? (
                                <div className="ml-4 text-obsidian-muted text-[10px]">No repos found</div>
                            ) : (
                                giteaRepos.map((repo: any) => (
                                    <div
                                        key={repo.id}
                                        className="flex items-center gap-1.5 py-[2px] px-2 cursor-pointer hover:bg-obsidian-panel-hover text-foreground select-none text-[13px]"
                                        style={{ paddingLeft: '8px' }}
                                        onClick={() => {
                                            window.dispatchEvent(new CustomEvent('openclaw:select-repo', {
                                                detail: { repo: repo.full_name }
                                            }));
                                        }}
                                    >
                                        <GitPullRequest className="w-4 h-4 text-obsidian-danger flex-shrink-0" />
                                        <span className="truncate">{repo.name}</span>
                                        {repo.language && (
                                            <span className="ml-auto text-[9px] text-obsidian-muted">{repo.language}</span>
                                        )}
                                    </div>
                                ))
                            )}
                        </>
                    ) : pathname === '/' ? (
                        <>
                            {fileTreeLoading ? (
                                <div className="ml-4 text-obsidian-muted text-[10px] animate-pulse">Loading project structure...</div>
                            ) : fileTree.length === 0 ? (
                                <div className="ml-4 text-obsidian-muted text-[10px]">No files found</div>
                            ) : (
                                renderFileTree(fileTree)
                            )}
                        </>
                    ) : (
                        <div className="px-4 py-2 text-obsidian-muted italic">
                            {pathname === '/workflows' && "DAGs Explorer..."}
                        </div>
                    )}
                </div>

                {/* Bottom Panel (Services/Status) */}
                <div className="h-[200px] border-t border-obsidian-border bg-obsidian-panel flex flex-col shrink-0">
                    <div className="h-7 flex items-center px-3 border-b border-obsidian-border gap-2">
                        <span className="font-semibold text-foreground">Services</span>
                        <div className="ml-auto flex gap-1">
                            <MoreHorizontal className="w-3.5 h-3.5 text-obsidian-muted" />
                        </div>
                    </div>
                    <div className="flex-1 overflow-y-auto p-2 space-y-1">
                        <ServiceItem name="Airflow (API)" status="running" />
                        <ServiceItem name="Trino (Coordinator)" status="running" />
                        <ServiceItem name="Spark Master" status="running" />
                        <ServiceItem name="MinIO (S3)" status="running" />
                        <ServiceItem name="Marquez (API)" status="running" />
                        <ServiceItem name="Superset" status="running" />
                        <ServiceItem name="Gitea" status="running" />
                    </div>
                </div>
            </div>

            {/* Context Menu Overlay */}
            {contextMenu && (
                <div
                    ref={contextMenuRef}
                    className="fixed z-[9999] bg-obsidian-panel border border-obsidian-border rounded-md shadow-2xl py-1 min-w-[180px]"
                    style={{ left: contextMenu.x, top: contextMenu.y }}
                    onClick={(e) => e.stopPropagation()}
                >
                    {contextMenu.items.map((item, idx) =>
                        item.separator ? (
                            <div key={idx} className="h-[1px] bg-obsidian-border my-1" />
                        ) : (
                            <button
                                key={idx}
                                className="w-full px-3 py-1.5 text-left text-[11px] text-foreground hover:bg-obsidian-info/20 flex items-center gap-2 transition-colors"
                                onClick={() => {
                                    item.action();
                                    setContextMenu(null);
                                }}
                            >
                                {item.icon}
                                {item.label}
                            </button>
                        )
                    )}
                </div>
            )}
        </div>
    );
};

const ServiceItem = ({ name, status }: { name: string, status: string }) => (
    <div className="flex items-center py-[1px] hover:bg-obsidian-panel-hover rounded cursor-pointer px-1">
        <div className={clsx("w-2 h-2 rounded-full mr-2", status === 'running' ? "bg-green-500" : "bg-red-500")}></div>
        <span className="text-foreground">{name}</span>
    </div>
);

const SidebarWrapper = () => (
    <Suspense fallback={<div className="flex h-full bg-obsidian-panel" />}>
        <Sidebar />
    </Suspense>
);

export { SidebarWrapper as Sidebar };
