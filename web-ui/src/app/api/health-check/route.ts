import { NextResponse } from 'next/server';

export async function POST(request: Request) {
    try {
        const { url } = await request.json();

        if (!url) {
            return NextResponse.json({ error: 'URL is required' }, { status: 400 });
        }

        const controller = new AbortController();
        const timeout = setTimeout(() => controller.abort(), 5000); // 5s timeout

        try {
            const response = await fetch(url, {
                method: 'GET',
                signal: controller.signal,
                headers: {
                    'Accept': 'application/json, text/html, */*',
                },
            });
            clearTimeout(timeout);

            return NextResponse.json({
                healthy: response.ok,
                status: response.status,
                statusText: response.statusText,
            });
        } catch (fetchError: any) {
            clearTimeout(timeout);

            if (fetchError.name === 'AbortError') {
                return NextResponse.json({
                    healthy: false,
                    status: 0,
                    statusText: 'Timeout (5s)',
                });
            }

            return NextResponse.json({
                healthy: false,
                status: 0,
                statusText: fetchError.message || 'Connection refused',
            });
        }
    } catch (e: any) {
        return NextResponse.json({ error: e.message }, { status: 500 });
    }
}
