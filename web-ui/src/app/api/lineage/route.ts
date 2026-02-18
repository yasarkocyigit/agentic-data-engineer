import { NextResponse } from 'next/server';

const MARQUEZ_URL = process.env.MARQUEZ_URL || 'http://localhost:5002';

export async function GET(request: Request) {
    const { searchParams } = new URL(request.url);
    const action = searchParams.get('action');

    try {
        let endpoint = '';

        switch (action) {
            case 'namespaces':
                endpoint = '/api/v1/namespaces';
                break;
            case 'jobs': {
                const ns = searchParams.get('ns');
                if (!ns) return NextResponse.json({ error: 'ns (namespace) is required' }, { status: 400 });
                const jobLimit = searchParams.get('limit') || '100';
                endpoint = `/api/v1/namespaces/${encodeURIComponent(ns)}/jobs?limit=${jobLimit}`;
                break;
            }
            case 'datasets': {
                const ns = searchParams.get('ns');
                if (!ns) return NextResponse.json({ error: 'ns (namespace) is required' }, { status: 400 });
                const dsLimit = searchParams.get('limit') || '100';
                endpoint = `/api/v1/namespaces/${encodeURIComponent(ns)}/datasets?limit=${dsLimit}`;
                break;
            }
            case 'lineage': {
                const nodeId = searchParams.get('nodeId');
                if (!nodeId) return NextResponse.json({ error: 'nodeId is required' }, { status: 400 });
                const depth = searchParams.get('depth') || '5';
                endpoint = `/api/v1/lineage?nodeId=${encodeURIComponent(nodeId)}&depth=${depth}`;
                break;
            }
            case 'dataset': {
                const ns = searchParams.get('ns');
                const name = searchParams.get('name');
                if (!ns || !name) return NextResponse.json({ error: 'ns and name required' }, { status: 400 });
                endpoint = `/api/v1/namespaces/${encodeURIComponent(ns)}/datasets/${encodeURIComponent(name)}`;
                break;
            }
            case 'job': {
                const ns = searchParams.get('ns');
                const name = searchParams.get('name');
                if (!ns || !name) return NextResponse.json({ error: 'ns and name required' }, { status: 400 });
                endpoint = `/api/v1/namespaces/${encodeURIComponent(ns)}/jobs/${encodeURIComponent(name)}`;
                break;
            }
            default:
                return NextResponse.json({ error: 'Invalid action. Use: namespaces, jobs, datasets, lineage, dataset, job' }, { status: 400 });
        }

        const res = await fetch(`${MARQUEZ_URL}${endpoint}`, {
            headers: { 'Accept': 'application/json' },
        });

        if (!res.ok) {
            const text = await res.text();
            return NextResponse.json({ error: `Marquez API error: ${res.status} ${text}` }, { status: res.status });
        }

        const data = await res.json();
        return NextResponse.json(data);

    } catch (e: unknown) {
        const msg = e instanceof Error ? e.message : 'Lineage API error';
        console.error('Lineage API Error:', msg);
        return NextResponse.json({ error: msg }, { status: 500 });
    }
}
