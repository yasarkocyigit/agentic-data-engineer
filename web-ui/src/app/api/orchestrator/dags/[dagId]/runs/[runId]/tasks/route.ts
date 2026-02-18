import { NextResponse } from 'next/server';
import { getTaskInstances } from '@/lib/airflow/client';

// GET /api/orchestrator/dags/[dagId]/runs/[runId]/tasks
export async function GET(
    request: Request,
    { params }: { params: Promise<{ dagId: string; runId: string }> }
) {
    try {
        const { dagId, runId } = await params;
        const taskInstances = await getTaskInstances(dagId, runId);
        return NextResponse.json({ task_instances: taskInstances });
    } catch (error: unknown) {
        const message = error instanceof Error ? error.message : 'Unknown error';
        console.error('[Orchestrator] GET /tasks error:', message);
        return NextResponse.json({ error: message }, { status: 500 });
    }
}
