import { NextResponse } from 'next/server';

export async function POST(request: Request) {
    try {
        const { query } = await request.json();

        if (!query) {
            return NextResponse.json({ error: 'Query is required' }, { status: 400 });
        }

        // 1. Initial Request
        const trinoUrl = 'http://localhost:8083/v1/statement';

        // Trino API v1/statement does not support trailing semicolons
        const sanitizedQuery = query.trim().replace(/;$/, '');

        let response = await fetch(trinoUrl, {
            method: 'POST',
            body: sanitizedQuery,
            headers: {
                'X-Trino-User': 'admin',
                'Content-Type': 'text/plain', // Trino expects plain text body for the query
            },
        });

        if (!response.ok) {
            const errorText = await response.text();
            return NextResponse.json({ error: `Trino connection failed: ${response.status} ${errorText}` }, { status: 502 });
        }

        let result = await response.json();

        // Accumulate data
        let allData: any[] = [];
        let columns: any[] = [];
        let stats: any = {};
        let error: any = null;

        // 2. Loop through pages
        let pageCount = 0;

        // Loop must handle the checks AFTER updating result
        // We use a true loop and break internally
        while (true) {
            pageCount++;

            // Debug Log
            // console.log(`[Trino] Page ${pageCount}:`, { 
            //    nextUri: result.nextUri, 
            //    hasData: !!result.data, 
            //    dataLen: result.data?.length 
            // });

            // Check for error in response body
            if (result.error) {
                error = result.error;
                console.error('[Trino] Error in response:', result.error);
                break;
            }

            // Collect columns (usually appears in the first few responses)
            if (result.columns && columns.length === 0) {
                columns = result.columns;
            }

            // Collect data
            if (result.data && result.data.length > 0) {
                allData = allData.concat(result.data);
            }

            // Update stats
            if (result.stats) {
                stats = result.stats;
            }

            // Fetch next page if available, otherwise DONE.
            if (!result.nextUri) {
                break;
            }

            response = await fetch(result.nextUri, {
                headers: { 'X-Trino-User': 'admin' }
            });

            if (!response.ok) {
                console.error('[Trino] NextURI fetch failed:', response.status);
                break;
            }
            result = await response.json();

            // Safety break
            if (pageCount > 500) break;
        }

        console.log('[Trino] Finished fetching.', {
            totalRows: allData.length,
            columns: columns.length
        });

        if (error) {
            const errorMsg = typeof error.message === 'string' ? error.message : JSON.stringify(error);
            return NextResponse.json({
                error: errorMsg,
                details: error
            }, { status: 400 });
        }

        // Format columns for frontend
        const formattedColumns = columns.map((col: any) => col.name);

        // Format data: Trino returns array of arrays (values matches column order)
        // We need array of objects: { colName: value, ... }
        const formattedData = allData.map((row: any[]) => {
            const rowObj: any = {};
            formattedColumns.forEach((colName: string, index: number) => {
                rowObj[colName] = row[index];
            });
            return rowObj;
        });

        return NextResponse.json({
            columns: formattedColumns,
            data: formattedData,
            stats: stats
        });

    } catch (e: any) {
        console.error('Trino API Error:', e);
        return NextResponse.json({ error: e.message || String(e) }, { status: 500 });
    }
}
