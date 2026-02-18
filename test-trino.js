// Native fetch is available in Node 18+

async function run() {
    try {
        const query = 'SELECT * FROM iceberg.silver.clean_orders LIMIT 5';
        console.log('EXECUTING:', query);

        // 1. Initial Request
        const trinoUrl = 'http://localhost:8083/v1/statement';
        let response = await fetch(trinoUrl, {
            method: 'POST',
            body: query,
            headers: {
                'X-Trino-User': 'admin',
                'Content-Type': 'text/plain',
            },
        });

        if (!response.ok) {
            console.error('Initial fetch failed:', response.status, await response.text());
            return;
        }

        let result = await response.json();
        let allData = [];
        let columns = [];
        let pageCount = 0;

        console.log('--- STARTING LOOP ---');

        // 2. Loop through pages
        while ((result.nextUri || result.data) && pageCount < 20) {
            pageCount++;
            console.log(`Page ${pageCount}:`, {
                nextUri: result.nextUri,
                hasData: !!result.data,
                dataLen: result.data ? result.data.length : 0,
                hasColumns: !!result.columns
            });

            if (result.error) {
                console.error('Trino Error:', result.error);
                break;
            }

            if (result.columns && columns.length === 0) {
                columns = result.columns;
                console.log('COLUMNS FOUND:', columns.map(c => c.name));
            }

            if (result.data && result.data.length > 0) {
                allData = allData.concat(result.data);
            }

            if (result.nextUri) {
                response = await fetch(result.nextUri, {
                    headers: { 'X-Trino-User': 'admin' }
                });
                if (!response.ok) {
                    console.error('Next fetch failed');
                    break;
                }
                result = await response.json();
            } else {
                break;
            }
        }

        console.log('--- FINISHED ---');
        console.log('Total Rows:', allData.length);
        if (allData.length > 0) {
            console.log('First Row Sample:', allData[0]);
        }

    } catch (e) {
        console.error('Script Error:', e);
    }
}

run();
