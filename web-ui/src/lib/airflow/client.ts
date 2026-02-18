// ─── Airflow REST API Client ───
// All communication with Airflow goes through this module.
// Uses JWT service account auth, typed responses.

import { getAirflowToken } from './auth';
import type {
    AirflowDAG,
    AirflowDAGRun,
    AirflowTask,
    AirflowTaskInstance,
    AirflowHealth,
    PipelineDAG,
    PipelineRun,
    PipelineGraph,
    GraphNode,
    GraphEdge,
    HealthStatus,
} from './types';

const AIRFLOW_BASE = process.env.AIRFLOW_API_URL || 'http://localhost:8081';
const API_PREFIX = '/api/v2';

// ─── Core fetch helper ───

async function airflowFetch<T>(path: string, options?: RequestInit): Promise<T> {
    const token = await getAirflowToken();
    const url = `${AIRFLOW_BASE}${API_PREFIX}${path}`;

    const res = await fetch(url, {
        ...options,
        headers: {
            Authorization: `Bearer ${token}`,
            'Content-Type': 'application/json',
            ...options?.headers,
        },
    });

    if (!res.ok) {
        let errMsg = `Airflow ${res.status}`;
        try {
            const errBody = await res.json();
            // Airflow 3.x returns { detail: [...] } or { detail: "string" }
            if (errBody.detail) {
                if (Array.isArray(errBody.detail)) {
                    errMsg += ': ' + errBody.detail.map((d: any) => d.msg || JSON.stringify(d)).join('; ');
                } else {
                    errMsg += ': ' + String(errBody.detail);
                }
            } else {
                errMsg += ': ' + JSON.stringify(errBody);
            }
        } catch {
            const text = await res.text().catch(() => 'Unknown');
            errMsg += ': ' + text;
        }
        throw new Error(errMsg);
    }

    // Some endpoints return plain text (logs)
    const contentType = res.headers.get('content-type') || '';
    if (contentType.includes('application/json')) {
        return res.json();
    }
    return res.text() as unknown as T;
}

// ─── DAGs ───

export async function listDAGs(params?: {
    limit?: number;
    offset?: number;
    paused?: boolean;
    dagIdPattern?: string;
    tags?: string[];
}): Promise<{ dags: PipelineDAG[]; total: number }> {
    const searchParams = new URLSearchParams();
    searchParams.set('limit', String(params?.limit || 100));
    searchParams.set('offset', String(params?.offset || 0));
    if (params?.paused !== undefined) searchParams.set('paused', String(params.paused));
    if (params?.dagIdPattern) searchParams.set('dag_id_pattern', params.dagIdPattern);
    if (params?.tags?.length) params.tags.forEach((t) => searchParams.append('tags', t));

    const data = await airflowFetch<{ dags: AirflowDAG[]; total_entries: number }>(
        `/dags?${searchParams.toString()}`
    );

    // Fetch last run for each DAG concurrently
    const dagsWithRuns = await Promise.all(
        data.dags.map(async (dag) => {
            let lastRun: PipelineRun | null = null;
            try {
                const runs = await airflowFetch<{ dag_runs: AirflowDAGRun[]; total_entries: number }>(
                    `/dags/${dag.dag_id}/dagRuns?order_by=-start_date&limit=1`
                );
                if (runs.dag_runs.length > 0) {
                    lastRun = transformRun(runs.dag_runs[0]);
                }
            } catch {
                // DAG may have no runs yet
            }

            return transformDAG(dag, lastRun);
        })
    );

    return { dags: dagsWithRuns, total: data.total_entries };
}

export async function getDAG(dagId: string): Promise<PipelineDAG> {
    const dag = await airflowFetch<AirflowDAG>(`/dags/${dagId}`);
    let lastRun: PipelineRun | null = null;
    try {
        const runs = await airflowFetch<{ dag_runs: AirflowDAGRun[] }>(
            `/dags/${dagId}/dagRuns?order_by=-start_date&limit=1`
        );
        if (runs.dag_runs.length > 0) lastRun = transformRun(runs.dag_runs[0]);
    } catch { /* no runs */ }
    return transformDAG(dag, lastRun);
}

export async function toggleDAGPause(dagId: string, isPaused: boolean): Promise<{ dag_id: string; is_paused: boolean }> {
    const result = await airflowFetch<AirflowDAG>(`/dags/${dagId}`, {
        method: 'PATCH',
        body: JSON.stringify({ is_paused: isPaused }),
    });
    return { dag_id: result.dag_id, is_paused: result.is_paused };
}

// ─── Runs ───

export async function listRuns(dagId: string, params?: {
    limit?: number;
    offset?: number;
    state?: string;
}): Promise<{ runs: PipelineRun[]; total: number }> {
    const searchParams = new URLSearchParams();
    searchParams.set('order_by', '-start_date');
    searchParams.set('limit', String(params?.limit || 25));
    searchParams.set('offset', String(params?.offset || 0));
    if (params?.state) searchParams.set('state', params.state);

    const data = await airflowFetch<{ dag_runs: AirflowDAGRun[]; total_entries: number }>(
        `/dags/${dagId}/dagRuns?${searchParams.toString()}`
    );

    return {
        runs: data.dag_runs.map(transformRun),
        total: data.total_entries,
    };
}

export async function triggerRun(dagId: string, conf?: Record<string, unknown>): Promise<PipelineRun> {
    const result = await airflowFetch<AirflowDAGRun>(`/dags/${dagId}/dagRuns`, {
        method: 'POST',
        body: JSON.stringify({
            logical_date: new Date().toISOString(),
            conf: conf || {},
        }),
    });
    return transformRun(result);
}

export async function getRun(dagId: string, runId: string): Promise<PipelineRun> {
    const result = await airflowFetch<AirflowDAGRun>(
        `/dags/${dagId}/dagRuns/${encodeURIComponent(runId)}`
    );
    return transformRun(result);
}

export async function cancelRun(dagId: string, runId: string): Promise<PipelineRun> {
    const result = await airflowFetch<AirflowDAGRun>(
        `/dags/${dagId}/dagRuns/${encodeURIComponent(runId)}`,
        {
            method: 'PATCH',
            body: JSON.stringify({ state: 'failed' }),
        }
    );
    return transformRun(result);
}

// ─── DAG Source Code ───

export async function getDagSource(dagId: string): Promise<{ content: string; version: number }> {
    const result = await airflowFetch<{ content: string; dag_id: string; version_number: number }>(
        `/dagSources/${dagId}`
    );
    return { content: result.content, version: result.version_number };
}

// ─── Graph Structure ───

export async function getDAGStructure(dagId: string): Promise<PipelineGraph> {
    const data = await airflowFetch<{ tasks: AirflowTask[] }>(`/dags/${dagId}/tasks`);

    const nodes: GraphNode[] = data.tasks.map((t) => ({
        id: t.task_id,
        label: t.task_display_name,
        operator: t.operator_name,
        color: t.ui_color,
        fgColor: t.ui_fgcolor,
        triggerRule: t.trigger_rule,
        isMapped: t.is_mapped,
    }));

    const edges: GraphEdge[] = [];
    for (const task of data.tasks) {
        for (const downstream of task.downstream_task_ids) {
            edges.push({ source: task.task_id, target: downstream });
        }
    }

    return { nodes, edges };
}

// ─── Task Instances ───

export async function getTaskInstances(dagId: string, runId: string): Promise<AirflowTaskInstance[]> {
    const data = await airflowFetch<{ task_instances: AirflowTaskInstance[] }>(
        `/dags/${dagId}/dagRuns/${encodeURIComponent(runId)}/taskInstances`
    );
    return data.task_instances;
}

// ─── Logs ───

export async function getTaskLogs(
    dagId: string,
    runId: string,
    taskId: string,
    tryNumber: number = 1,
    mapIndex: number = -1,
): Promise<string> {
    const params = new URLSearchParams();
    if (mapIndex >= 0) params.set('map_index', String(mapIndex));

    const result = await airflowFetch<string>(
        `/dags/${dagId}/dagRuns/${encodeURIComponent(runId)}/taskInstances/${taskId}/logs/${tryNumber}?${params.toString()}`
    );
    return typeof result === 'string' ? result : JSON.stringify(result);
}

// ─── Health ───

export async function getHealth(): Promise<HealthStatus> {
    const data = await airflowFetch<AirflowHealth>('/monitor/health');
    return {
        scheduler: {
            status: data.scheduler.status || 'unknown',
            heartbeat: data.scheduler.latest_scheduler_heartbeat,
        },
        database: { status: data.metadatabase.status },
        dag_processor: {
            status: data.dag_processor.status || 'unknown',
            heartbeat: data.dag_processor.latest_dag_processor_heartbeat,
        },
    };
}

// ─── Transform Helpers ───

function transformDAG(dag: AirflowDAG, lastRun: PipelineRun | null): PipelineDAG {
    return {
        dag_id: dag.dag_id,
        display_name: dag.dag_display_name,
        description: dag.description,
        is_paused: dag.is_paused,
        schedule: dag.timetable_summary,
        schedule_description: dag.timetable_description,
        owners: dag.owners,
        tags: dag.tags.map((t) => t.name),
        next_run: dag.next_dagrun_logical_date,
        max_active_runs: dag.max_active_runs,
        file_path: dag.relative_fileloc,
        has_import_errors: dag.has_import_errors,
        last_run: lastRun,
    };
}

function transformRun(run: AirflowDAGRun): PipelineRun {
    let durationSeconds: number | null = null;
    if (run.start_date && run.end_date) {
        durationSeconds = (new Date(run.end_date).getTime() - new Date(run.start_date).getTime()) / 1000;
    }
    return {
        run_id: run.dag_run_id,
        state: run.state,
        run_type: run.run_type,
        triggered_by: run.triggered_by,
        queued_at: run.queued_at,
        start_date: run.start_date,
        end_date: run.end_date,
        duration_seconds: durationSeconds,
        conf: run.conf,
        note: run.note,
    };
}
