import { NextResponse } from 'next/server';
import { getDAG, listRuns } from '@/lib/airflow/client';

// GET /api/orchestrator/dags/[dagId]
export async function GET(
    request: Request,
    { params }: { params: Promise<{ dagId: string }> }
) {
    try {
        const { dagId } = await params;
        const { searchParams } = new URL(request.url);
        const includeRuns = searchParams.get('include_runs') === 'true';

        const dag = await getDAG(dagId);

        let runs = undefined;
        if (includeRuns) {
            const limit = parseInt(searchParams.get('runs_limit') || '25');
            const result = await listRuns(dagId, { limit });
            runs = result.runs;
        }

        return NextResponse.json({ dag, runs });
    } catch (error: unknown) {
        const message = error instanceof Error ? error.message : 'Unknown error';
        console.error('[Orchestrator] GET /dags/[dagId] error:', message);
        return NextResponse.json({ error: message }, { status: 500 });
    }
}
