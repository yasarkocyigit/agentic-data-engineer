import { NextResponse } from 'next/server';
import { getHealth } from '@/lib/airflow/client';

// GET /api/orchestrator/health
export async function GET() {
    try {
        const health = await getHealth();
        return NextResponse.json(health);
    } catch (error: unknown) {
        const message = error instanceof Error ? error.message : 'Unknown error';
        return NextResponse.json(
            {
                scheduler: { status: 'unreachable', heartbeat: null },
                database: { status: 'unreachable' },
                dag_processor: { status: 'unreachable', heartbeat: null },
                error: message,
            },
            { status: 503 }
        );
    }
}
