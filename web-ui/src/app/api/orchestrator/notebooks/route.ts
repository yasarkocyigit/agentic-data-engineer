import { NextResponse } from 'next/server';
import { readFile, readdir, stat } from 'fs/promises';
import { existsSync } from 'fs';
import path from 'path';

// ─── Dynamic Notebook Resolution ───
// Instead of a hardcoded map, this route dynamically resolves the notebook
// for a given task_id by:
//   1. Parsing all DAG files for SparkSubmitOperator(task_id='...', application='...')
//   2. Mapping the Docker path (/opt/airflow/notebooks/...) to local (../notebooks/...)
//   3. Falling back to scanning notebooks/ directory for matching filenames

const PROJECT_ROOT = path.resolve(process.cwd(), '..');
const NOTEBOOKS_DIR = path.join(PROJECT_ROOT, 'notebooks');
const DAGS_DIR = path.join(PROJECT_ROOT, 'dags');

// Docker mount prefix to strip when resolving to local path
const DOCKER_NOTEBOOK_PREFIX = '/opt/airflow/notebooks/';

/**
 * Parse all DAG files to build a dynamic task_id → notebook path mapping.
 * Searches for patterns like: task_id='run_bronze_ingestion' ... application='/opt/airflow/notebooks/bronze/ingestion.py'
 */
async function buildTaskNotebookMap(): Promise<Record<string, string>> {
    const map: Record<string, string> = {};

    if (!existsSync(DAGS_DIR)) return map;

    const dagFiles = (await readdir(DAGS_DIR)).filter(f => f.endsWith('.py'));

    for (const dagFile of dagFiles) {
        const content = await readFile(path.join(DAGS_DIR, dagFile), 'utf-8');

        // Match SparkSubmitOperator blocks: find task_id and application pairs
        // Pattern: SparkSubmitOperator(\n  task_id='xxx', ... application='yyy', ...
        const operatorRegex = /SparkSubmitOperator\s*\(([\s\S]*?)\)/g;
        let match;
        while ((match = operatorRegex.exec(content)) !== null) {
            const block = match[1];

            // Extract task_id
            const taskIdMatch = block.match(/task_id\s*=\s*['"]([^'"]+)['"]/);
            // Extract application path
            const appMatch = block.match(/application\s*=\s*['"]([^'"]+)['"]/);

            if (taskIdMatch && appMatch) {
                const taskId = taskIdMatch[1];
                let appPath = appMatch[1];

                // Convert Docker path to local path
                if (appPath.startsWith(DOCKER_NOTEBOOK_PREFIX)) {
                    appPath = appPath.substring(DOCKER_NOTEBOOK_PREFIX.length);
                }

                map[taskId] = appPath;
            }
        }
    }

    return map;
}

/**
 * Recursively scan notebooks directory for all .py files.
 * Returns a map of filename → relative path for fallback matching.
 */
async function scanNotebooks(dir: string, prefix = ''): Promise<Record<string, string>> {
    const result: Record<string, string> = {};
    if (!existsSync(dir)) return result;

    const entries = await readdir(dir);
    for (const entry of entries) {
        if (entry.startsWith('_') || entry.startsWith('.') || entry === '__pycache__') continue;
        const fullPath = path.join(dir, entry);
        const relativePath = prefix ? `${prefix}/${entry}` : entry;
        const stats = await stat(fullPath);
        if (stats.isDirectory()) {
            Object.assign(result, await scanNotebooks(fullPath, relativePath));
        } else if (entry.endsWith('.py')) {
            result[entry] = relativePath;
            // Also map without extension
            result[entry.replace('.py', '')] = relativePath;
        }
    }
    return result;
}

// POST /api/orchestrator/notebooks — get notebook source for a task
export async function POST(request: Request) {
    try {
        const body = await request.json();
        const { task_id } = body;

        if (!task_id) {
            return NextResponse.json({ error: 'Missing task_id' }, { status: 400 });
        }

        // Step 1: Parse DAG files to find the application path for this task
        const taskMap = await buildTaskNotebookMap();
        let relativePath = taskMap[task_id];

        // Step 2: Fallback — scan notebooks directory and try to match by name convention
        // e.g. task "run_bronze_ingestion" might map to "ingestion.py" in bronze/
        if (!relativePath) {
            const notebookFiles = await scanNotebooks(NOTEBOOKS_DIR);

            // Try direct filename match (e.g. task_id = "run_bronze_ingestion" → look for "ingestion.py")
            const possibleNames = [
                task_id,                                    // exact match
                task_id.replace(/^run_/, ''),               // strip "run_" prefix
                task_id.replace(/^run_\w+_/, ''),           // strip "run_<layer>_" prefix
            ];

            for (const name of possibleNames) {
                if (notebookFiles[name] || notebookFiles[`${name}.py`]) {
                    relativePath = notebookFiles[name] || notebookFiles[`${name}.py`];
                    break;
                }
            }
        }

        if (!relativePath) {
            return NextResponse.json(
                { error: `No notebook found for task: ${task_id}. Available mappings: ${Object.keys(taskMap).join(', ')}` },
                { status: 404 }
            );
        }

        const notebookPath = path.join(NOTEBOOKS_DIR, relativePath);

        if (!existsSync(notebookPath)) {
            return NextResponse.json({ error: `Notebook file not found: ${relativePath}` }, { status: 404 });
        }

        const content = await readFile(notebookPath, 'utf-8');

        return NextResponse.json({
            task_id,
            filename: path.basename(notebookPath),
            path: `notebooks/${relativePath}`,
            content,
        });
    } catch (error: unknown) {
        const message = error instanceof Error ? error.message : 'Unknown error';
        console.error('[Orchestrator] POST /notebooks error:', message);
        return NextResponse.json({ error: message }, { status: 500 });
    }
}
