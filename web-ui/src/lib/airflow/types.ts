// ─── Airflow 3.x REST API Type Definitions ───
// Based on live probing of /api/v2 endpoints

export interface AirflowDAG {
    dag_id: string;
    dag_display_name: string;
    is_paused: boolean;
    is_stale: boolean;
    description: string | null;
    timetable_summary: string | null;
    timetable_description: string | null;
    tags: { name: string; dag_id: string }[];
    owners: string[];
    max_active_runs: number;
    max_active_tasks: number;
    fileloc: string;
    relative_fileloc: string;
    bundle_name: string;
    last_parsed_time: string | null;
    next_dagrun_logical_date: string | null;
    next_dagrun_data_interval_start: string | null;
    next_dagrun_data_interval_end: string | null;
    has_import_errors: boolean;
    file_token: string;
}

export interface AirflowDAGRun {
    dag_run_id: string;
    dag_id: string;
    logical_date: string | null;
    queued_at: string;
    start_date: string | null;
    end_date: string | null;
    data_interval_start: string | null;
    data_interval_end: string | null;
    run_after: string;
    last_scheduling_decision: string | null;
    run_type: 'manual' | 'scheduled' | 'backfill';
    state: DAGRunState;
    triggered_by: string;
    conf: Record<string, unknown>;
    note: string | null;
}

export type DAGRunState = 'queued' | 'running' | 'success' | 'failed';

export interface AirflowTask {
    task_id: string;
    task_display_name: string;
    owner: string;
    trigger_rule: string;
    depends_on_past: boolean;
    retries: number;
    pool: string;
    queue: string;
    operator_name: string;
    ui_color: string;
    ui_fgcolor: string;
    downstream_task_ids: string[];
    is_mapped: boolean;
    class_ref: { module_path: string; class_name: string };
}

export interface AirflowTaskInstance {
    id: string;
    task_id: string;
    dag_id: string;
    dag_run_id: string;
    map_index: number;
    start_date: string | null;
    end_date: string | null;
    duration: number | null;
    state: TaskInstanceState | null;
    try_number: number;
    max_tries: number;
    task_display_name: string;
    operator: string;
    pool: string;
    pool_slots: number;
    queue: string;
    priority_weight: number;
    hostname: string;
    note: string | null;
    rendered_map_index: string | null;
}

export type TaskInstanceState =
    | 'success'
    | 'failed'
    | 'running'
    | 'queued'
    | 'scheduled'
    | 'skipped'
    | 'up_for_retry'
    | 'up_for_reschedule'
    | 'upstream_failed'
    | 'removed'
    | 'deferred'
    | null;

export interface AirflowHealth {
    metadatabase: { status: 'healthy' | 'unhealthy' };
    scheduler: { status: 'healthy' | 'unhealthy' | null; latest_scheduler_heartbeat: string | null };
    dag_processor: { status: 'healthy' | 'unhealthy' | null; latest_dag_processor_heartbeat: string | null };
    triggerer: { status: 'healthy' | 'unhealthy' | null; latest_triggerer_heartbeat: string | null };
}

// ─── Orchestrator UI Types (transformed for frontend) ───

export interface PipelineDAG {
    dag_id: string;
    display_name: string;
    description: string | null;
    is_paused: boolean;
    schedule: string | null;
    schedule_description: string | null;
    owners: string[];
    tags: string[];
    next_run: string | null;
    max_active_runs: number;
    file_path: string;
    has_import_errors: boolean;
    last_run: PipelineRun | null;
}

export interface PipelineRun {
    run_id: string;
    logical_date?: string | null;
    run_after?: string | null;
    state: DAGRunState;
    run_type: string;
    triggered_by: string;
    queued_at: string;
    start_date: string | null;
    end_date: string | null;
    duration_seconds: number | null;
    conf: Record<string, unknown>;
    note: string | null;
}

export interface GraphNode {
    id: string;
    label: string;
    operator: string;
    color: string;
    fgColor: string;
    triggerRule: string;
    isMapped: boolean;
    // Overlaid per-run
    state?: TaskInstanceState;
    duration?: number | null;
    mapIndex?: number;
}

export interface GraphEdge {
    source: string;
    target: string;
}

export interface PipelineGraph {
    nodes: GraphNode[];
    edges: GraphEdge[];
}

export interface HealthStatus {
    scheduler: { status: string; heartbeat: string | null };
    database: { status: string };
    dag_processor: { status: string; heartbeat: string | null };
}

// ─── State color mapping ───

export const STATE_COLORS: Record<string, { bg: string; text: string; label: string }> = {
    success: { bg: 'rgba(73, 156, 84, 0.15)', text: '#499c54', label: 'Success' },
    failed: { bg: 'rgba(255, 82, 97, 0.15)', text: '#ff5261', label: 'Failed' },
    running: { bg: 'rgba(53, 116, 240, 0.15)', text: '#3574f0', label: 'Running' },
    queued: { bg: 'rgba(188, 190, 196, 0.15)', text: '#bcbec4', label: 'Queued' },
    scheduled: { bg: 'rgba(188, 190, 196, 0.10)', text: '#8c8e9e', label: 'Scheduled' },
    skipped: { bg: 'rgba(229, 192, 123, 0.15)', text: '#e5c07b', label: 'Skipped' },
    up_for_retry: { bg: 'rgba(229, 192, 123, 0.15)', text: '#e5c07b', label: 'Retry' },
    upstream_failed: { bg: 'rgba(255, 82, 97, 0.10)', text: '#ff7b86', label: 'Upstream Failed' },
    removed: { bg: 'rgba(108, 112, 126, 0.15)', text: '#6c707e', label: 'Removed' },
    deferred: { bg: 'rgba(198, 120, 221, 0.15)', text: '#c678dd', label: 'Deferred' },
    no_status: { bg: 'rgba(108, 112, 126, 0.10)', text: '#6c707e', label: 'No Status' },
};

export function getStateColor(state: string | null | undefined) {
    return STATE_COLORS[state || 'no_status'] || STATE_COLORS.no_status;
}
