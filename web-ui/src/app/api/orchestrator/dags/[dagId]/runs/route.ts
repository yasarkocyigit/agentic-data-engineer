import { NextResponse } from 'next/server';
import { listRuns, triggerRun } from '@/lib/airflow/client';

// GET /api/orchestrator/dags/[dagId]/runs
export async function GET(
    request: Request,
    { params }: { params: Promise<{ dagId: string }> }
) {
    try {
        const { dagId } = await params;
        const { searchParams } = new URL(request.url);
        const limit = parseInt(searchParams.get('limit') || '25');
        const offset = parseInt(searchParams.get('offset') || '0');
        const state = searchParams.get('state') || undefined;

        const result = await listRuns(dagId, { limit, offset, state });
        return NextResponse.json(result);
    } catch (error: unknown) {
        const message = error instanceof Error ? error.message : 'Unknown error';
        console.error('[Orchestrator] GET /runs error:', message);
        return NextResponse.json({ error: message }, { status: 500 });
    }
}

// POST /api/orchestrator/dags/[dagId]/runs — trigger a new run
export async function POST(
    request: Request,
    { params }: { params: Promise<{ dagId: string }> }
) {
    try {
        const { dagId } = await params;
        const body = await request.json().catch(() => ({}));
        const run = await triggerRun(dagId, body.conf);
        return NextResponse.json({ message: 'Run triggered', run });
    } catch (error: unknown) {
        const message = error instanceof Error ? error.message : 'Unknown error';
        console.error('[Orchestrator] POST /runs error:', message);
        return NextResponse.json({ error: message }, { status: 500 });
    }
}
