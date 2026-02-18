import { NextResponse } from 'next/server';
import fs from 'fs';
import path from 'path';

// ─── Project Root ───
const PROJECT_ROOT = path.resolve(process.cwd(), '..');

// ─── Directories & files to exclude ───
const EXCLUDE_DIRS = new Set([
    '.git', 'node_modules', '__pycache__', '.next', '.vscode',
    'logs', '.DS_Store', 'extra-jars', 'jars', 'plugins',
    '.system_generated', '.gemini', 'log.txt', '.env',
]);

const EXCLUDE_EXTENSIONS = new Set([
    '.pyc', '.pyo', '.class', '.o', '.so', '.dylib',
]);

// ─── Types ───
type FileNode = {
    name: string;
    path: string;       // relative to project root
    type: 'file' | 'directory';
    extension?: string;
    size?: number;
    children?: FileNode[];
};

// ─── Recursive Directory Reader ───
function readDirectory(dirPath: string, relativePath: string, depth: number, maxDepth: number): FileNode[] {
    if (depth > maxDepth) return [];

    try {
        const entries = fs.readdirSync(dirPath, { withFileTypes: true });
        const nodes: FileNode[] = [];

        // Sort: directories first, then files, alphabetically
        const sorted = entries.sort((a, b) => {
            if (a.isDirectory() && !b.isDirectory()) return -1;
            if (!a.isDirectory() && b.isDirectory()) return 1;
            return a.name.localeCompare(b.name);
        });

        for (const entry of sorted) {
            if (EXCLUDE_DIRS.has(entry.name)) continue;
            if (entry.name.startsWith('.') && entry.name !== '.env.example' && entry.name !== '.gitignore') continue;

            const fullPath = path.join(dirPath, entry.name);
            const relPath = path.join(relativePath, entry.name);
            const ext = path.extname(entry.name).toLowerCase();

            if (EXCLUDE_EXTENSIONS.has(ext)) continue;

            if (entry.isDirectory()) {
                const children = readDirectory(fullPath, relPath, depth + 1, maxDepth);
                nodes.push({
                    name: entry.name,
                    path: relPath,
                    type: 'directory',
                    children,
                });
            } else if (entry.isFile()) {
                try {
                    const stat = fs.statSync(fullPath);
                    nodes.push({
                        name: entry.name,
                        path: relPath,
                        type: 'file',
                        extension: ext.replace('.', '') || undefined,
                        size: stat.size,
                    });
                } catch {
                    nodes.push({
                        name: entry.name,
                        path: relPath,
                        type: 'file',
                        extension: ext.replace('.', '') || undefined,
                    });
                }
            }
        }

        return nodes;
    } catch {
        return [];
    }
}

// ─── GET Handler ───
export async function GET(request: Request) {
    const { searchParams } = new URL(request.url);
    const action = searchParams.get('action') || 'tree';
    const subPath = searchParams.get('path') || '';
    const maxDepth = parseInt(searchParams.get('maxDepth') || '3', 10);

    try {
        // ── Read File Content ──
        if (action === 'readFile') {
            const filePath = searchParams.get('filePath');
            if (!filePath) {
                return NextResponse.json({ error: 'Missing filePath parameter' }, { status: 400 });
            }

            // Normalize: strip leading './' if present
            const cleanPath = filePath.replace(/^\.\//, '');
            const targetPath = path.join(PROJECT_ROOT, cleanPath);

            // Security: prevent traversal above project root
            if (!targetPath.startsWith(PROJECT_ROOT)) {
                return NextResponse.json({ error: 'Invalid path' }, { status: 400 });
            }

            if (!fs.existsSync(targetPath) || !fs.statSync(targetPath).isFile()) {
                return NextResponse.json({ error: 'File not found' }, { status: 404 });
            }

            const stat = fs.statSync(targetPath);
            const ext = path.extname(targetPath).toLowerCase().replace('.', '');

            // Language mapping
            const langMap: Record<string, string> = {
                py: 'python', ts: 'typescript', tsx: 'typescript', js: 'javascript', jsx: 'javascript',
                sql: 'sql', md: 'markdown', json: 'json', yml: 'yaml', yaml: 'yaml',
                sh: 'shell', bash: 'shell', html: 'html', css: 'css', xml: 'xml',
                toml: 'toml', csv: 'plaintext', txt: 'plaintext', cfg: 'ini',
                properties: 'ini', conf: 'ini', dockerfile: 'dockerfile',
            };

            // Only preview text files under 2MB
            const MAX_SIZE = 2 * 1024 * 1024;
            if (stat.size > MAX_SIZE) {
                return NextResponse.json({ error: 'File too large for preview', size: stat.size }, { status: 413 });
            }

            const content = fs.readFileSync(targetPath, 'utf-8');
            const fileName = path.basename(targetPath);
            const language = fileName.toLowerCase() === 'dockerfile' ? 'dockerfile' : (langMap[ext] || 'plaintext');

            return NextResponse.json({
                name: fileName,
                path: cleanPath,
                content,
                language,
                extension: ext,
                size: stat.size,
                lastModified: stat.mtime.toISOString(),
                lineCount: content.split('\n').length,
            });
        }

        // ── List Directory Tree ──
        const targetPath = path.join(PROJECT_ROOT, subPath);

        // Security: prevent traversal above project root
        if (!targetPath.startsWith(PROJECT_ROOT)) {
            return NextResponse.json({ error: 'Invalid path' }, { status: 400 });
        }

        const tree = readDirectory(targetPath, subPath || '.', 0, maxDepth);

        return NextResponse.json({
            root: path.basename(PROJECT_ROOT),
            path: subPath || '.',
            tree,
        });
    } catch (error: any) {
        console.error('[Files API Error]', error);
        return NextResponse.json(
            { error: error.message || 'Failed to read project files' },
            { status: 500 }
        );
    }
}
