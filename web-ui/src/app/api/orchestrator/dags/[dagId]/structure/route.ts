import { NextResponse } from 'next/server';
import { getDAGStructure, getTaskInstances } from '@/lib/airflow/client';

// GET /api/orchestrator/dags/[dagId]/structure?run_id=...
export async function GET(
    request: Request,
    { params }: { params: Promise<{ dagId: string }> }
) {
    try {
        const { dagId } = await params;
        const { searchParams } = new URL(request.url);
        const runId = searchParams.get('run_id');

        // Get static graph structure
        const graph = await getDAGStructure(dagId);

        // Overlay task instance state if a run_id is provided
        if (runId) {
            const taskInstances = await getTaskInstances(dagId, runId);
            const stateMap = new Map(taskInstances.map((ti) => [ti.task_id, ti]));

            graph.nodes = graph.nodes.map((node) => {
                const ti = stateMap.get(node.id);
                return {
                    ...node,
                    state: ti?.state,
                    duration: ti?.duration,
                    mapIndex: ti?.map_index,
                };
            });
        }

        return NextResponse.json(graph);
    } catch (error: unknown) {
        const message = error instanceof Error ? error.message : 'Unknown error';
        console.error('[Orchestrator] GET /structure error:', message);
        return NextResponse.json({ error: message }, { status: 500 });
    }
}
