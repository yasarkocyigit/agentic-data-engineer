import { NextResponse } from 'next/server';
import { Pool } from 'pg';

// ─── Connection Pool ───
// Next.js runs on the host machine, so always connect to localhost
// (POSTGRES_HOST from .env is 'host.docker.internal' which is for Docker containers)
const pool = new Pool({
    host: 'localhost',
    port: parseInt(process.env.POSTGRES_PORT || '5433'),
    user: process.env.POSTGRES_USER || 'postgres',
    password: process.env.POSTGRES_PASSWORD || process.env.PG_PASSWORD,
    database: process.env.POSTGRES_DB || 'controldb',
    max: 5,
    idleTimeoutMillis: 30000,
    connectionTimeoutMillis: 5000,
});

export async function POST(request: Request) {
    try {
        const body = await request.json();
        const { action, query, database, schema, table } = body;

        if (action === 'query') {
            // ─── Execute SQL Query ───
            if (!query) {
                return NextResponse.json({ error: 'Query is required' }, { status: 400 });
            }

            const startTime = Date.now();
            const result = await pool.query(query);
            const endTime = Date.now();

            const columns = result.fields.map((f: { name: string }) => f.name);
            const data = result.rows;

            return NextResponse.json({
                columns,
                data,
                stats: {
                    rowCount: result.rowCount,
                    duration: endTime - startTime,
                    command: result.command,
                }
            });

        } else if (action === 'explore') {
            // ─── Schema Exploration ───
            const { type } = body;

            if (type === 'databases') {
                const result = await pool.query(
                    "SELECT datname AS name FROM pg_database WHERE datistemplate = false ORDER BY datname"
                );
                return NextResponse.json({ data: result.rows });

            } else if (type === 'schemas') {
                if (!database) {
                    return NextResponse.json({ error: 'database is required' }, { status: 400 });
                }
                // For controldb (default), list schemas directly
                const result = await pool.query(
                    "SELECT schema_name AS name FROM information_schema.schemata WHERE schema_name NOT IN ('pg_toast', 'pg_catalog', 'information_schema') ORDER BY schema_name"
                );
                return NextResponse.json({ data: result.rows });

            } else if (type === 'tables') {
                if (!schema) {
                    return NextResponse.json({ error: 'schema is required' }, { status: 400 });
                }
                const result = await pool.query(
                    "SELECT table_name AS name, table_type FROM information_schema.tables WHERE table_schema = $1 ORDER BY table_name",
                    [schema]
                );
                return NextResponse.json({ data: result.rows });

            } else if (type === 'columns') {
                if (!schema || !table) {
                    return NextResponse.json({ error: 'schema and table are required' }, { status: 400 });
                }
                const result = await pool.query(
                    `SELECT column_name AS name, data_type, is_nullable, column_default
                     FROM information_schema.columns
                     WHERE table_schema = $1 AND table_name = $2
                     ORDER BY ordinal_position`,
                    [schema, table]
                );
                return NextResponse.json({ data: result.rows });

            } else {
                return NextResponse.json({ error: 'Invalid explore type' }, { status: 400 });
            }

        } else {
            return NextResponse.json({ error: 'Invalid action. Use "query" or "explore"' }, { status: 400 });
        }

    } catch (e: unknown) {
        const msg = e instanceof Error ? e.message : 'PostgreSQL error';
        console.error('PostgreSQL API Error:', msg);
        return NextResponse.json({ error: msg }, { status: 500 });
    }
}
