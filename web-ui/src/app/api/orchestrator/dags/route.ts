import { NextResponse } from 'next/server';
import { listDAGs, toggleDAGPause, triggerRun, cancelRun, getDagSource } from '@/lib/airflow/client';

// GET /api/orchestrator/dags
export async function GET(request: Request) {
    try {
        const { searchParams } = new URL(request.url);
        const limit = parseInt(searchParams.get('limit') || '100');
        const offset = parseInt(searchParams.get('offset') || '0');
        const search = searchParams.get('search') || undefined;
        const paused = searchParams.has('paused') ? searchParams.get('paused') === 'true' : undefined;
        const tags = searchParams.getAll('tags');

        const result = await listDAGs({
            limit,
            offset,
            dagIdPattern: search,
            paused,
            tags: tags.length > 0 ? tags : undefined,
        });

        return NextResponse.json(result);
    } catch (error: unknown) {
        const message = error instanceof Error ? error.message : 'Unknown error';
        console.error('[Orchestrator] GET /dags error:', message);
        return NextResponse.json({ error: message }, { status: 500 });
    }
}

// POST /api/orchestrator/dags — actions: trigger, pause, unpause
export async function POST(request: Request) {
    try {
        const body = await request.json();
        const { dag_id, action, conf } = body;

        if (!dag_id || !action) {
            return NextResponse.json({ error: 'Missing dag_id or action' }, { status: 400 });
        }

        if (action === 'trigger') {
            const run = await triggerRun(dag_id, conf);
            return NextResponse.json({ message: 'Run triggered', run });
        }

        if (action === 'pause') {
            const result = await toggleDAGPause(dag_id, true);
            return NextResponse.json({ message: 'DAG paused', ...result });
        }

        if (action === 'unpause') {
            const result = await toggleDAGPause(dag_id, false);
            return NextResponse.json({ message: 'DAG unpaused', ...result });
        }

        if (action === 'cancel') {
            const { run_id } = body;
            if (!run_id) return NextResponse.json({ error: 'Missing run_id for cancel' }, { status: 400 });
            const result = await cancelRun(dag_id, run_id);
            return NextResponse.json({ message: 'Run cancelled', run: result });
        }

        if (action === 'source') {
            const result = await getDagSource(dag_id);
            return NextResponse.json(result);
        }

        return NextResponse.json({ error: 'Invalid action' }, { status: 400 });
    } catch (error: unknown) {
        const message = error instanceof Error ? error.message : 'Unknown error';
        console.error('[Orchestrator] POST /dags error:', message);
        return NextResponse.json({ error: message }, { status: 500 });
    }
}
