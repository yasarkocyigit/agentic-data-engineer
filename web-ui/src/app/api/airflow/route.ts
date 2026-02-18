import { NextResponse } from 'next/server';
import { exec } from 'child_process';
import util from 'util';

const execPromise = util.promisify(exec);

// Execute Airflow CLI commands via Docker
async function runAirflowCLI(command: string): Promise<string> {
    const fullCommand = `docker exec airflow_webserver airflow ${command}`;
    console.log(`[Airflow CLI] ${fullCommand}`);
    const { stdout, stderr } = await execPromise(fullCommand, { timeout: 15000 });
    if (stderr && !stdout) {
        console.error('[Airflow CLI] stderr:', stderr);
    }
    return stdout.trim();
}

// Track last auto-recovery to avoid restart loops (max once per 60s)
let lastRecoveryTime = 0;
const RECOVERY_COOLDOWN_MS = 60_000;

// GET /api/airflow?action=list_dags
export async function GET(request: Request) {
    try {
        const { searchParams } = new URL(request.url);
        const action = searchParams.get('action') || 'list_dags';

        if (action === 'list_dags') {
            const output = await runAirflowCLI('dags list -o json');
            let cliDags = [];
            try {
                cliDags = JSON.parse(output);
            } catch {
                console.error('[Airflow CLI] Failed to parse JSON:', output);
                return NextResponse.json({ error: 'Failed to parse Airflow response' }, { status: 500 });
            }

            // ── Auto-Recovery: restart scheduler & dag-processor when 0 DAGs ──
            if (cliDags.length === 0) {
                const now = Date.now();
                if (now - lastRecoveryTime > RECOVERY_COOLDOWN_MS) {
                    lastRecoveryTime = now;
                    console.warn('[Airflow Auto-Recovery] 0 DAGs detected — restarting scheduler & dag-processor...');
                    try {
                        await execPromise(
                            'docker compose restart airflow-scheduler airflow-dag-processor',
                            { timeout: 30000, cwd: process.env.PROJECT_ROOT || '/app' }
                        );
                        // Wait for services to stabilize
                        await new Promise(resolve => setTimeout(resolve, 8000));
                        // Retry DAG list
                        const retryOutput = await runAirflowCLI('dags list -o json');
                        try {
                            cliDags = JSON.parse(retryOutput);
                            console.log(`[Airflow Auto-Recovery] ✅ Recovery succeeded — ${cliDags.length} DAG(s) found`);
                        } catch {
                            console.error('[Airflow Auto-Recovery] Retry parse failed:', retryOutput);
                        }
                    } catch (recoveryErr: any) {
                        console.error('[Airflow Auto-Recovery] Recovery failed:', recoveryErr.message);
                    }
                } else {
                    console.warn('[Airflow Auto-Recovery] Cooldown active — skipping restart');
                }
            }

            const dags = cliDags.map((dag: any) => ({
                dag_id: dag.dag_id,
                owners: Array.isArray(dag.owners) ? dag.owners : [dag.owners],
                is_paused: dag.is_paused === 'True' || dag.is_paused === true,
                schedule_interval: null,
                fileloc: dag.fileloc,
            }));

            return NextResponse.json({ dags, total_entries: dags.length });
        }

        return NextResponse.json({ error: 'Unknown action' }, { status: 400 });
    } catch (error: any) {
        console.error('[Airflow CLI] GET Error:', error);
        return NextResponse.json({ error: error.message || 'Airflow CLI failed' }, { status: 500 });
    }
}

// POST /api/airflow  { dag_id, action: "trigger" | "pause" | "unpause" }
export async function POST(request: Request) {
    try {
        const body = await request.json();
        const { dag_id, action } = body;

        if (!dag_id || !action) {
            return NextResponse.json({ error: 'Missing dag_id or action' }, { status: 400 });
        }

        let output = '';

        if (action === 'trigger') {
            output = await runAirflowCLI(`dags trigger ${dag_id}`);
        } else if (action === 'pause') {
            output = await runAirflowCLI(`dags pause ${dag_id}`);
        } else if (action === 'unpause') {
            output = await runAirflowCLI(`dags unpause ${dag_id}`);
        } else {
            return NextResponse.json({ error: 'Invalid action' }, { status: 400 });
        }

        return NextResponse.json({ message: `${action} completed`, output });
    } catch (error: any) {
        console.error('[Airflow CLI] POST Error:', error);
        return NextResponse.json({ error: error.message || 'Airflow CLI failed' }, { status: 500 });
    }
}
