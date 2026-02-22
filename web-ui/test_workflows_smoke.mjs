import { chromium } from 'playwright';

(async () => {
    const browser = await chromium.launch();
    const page = await browser.newPage({ viewport: { width: 1600, height: 900 } });
    page.on('console', (msg) => console.log('BROWSER:', msg.text()));

    await page.goto('http://localhost:3010/workflows', { waitUntil: 'domcontentloaded' });
    await page.waitForTimeout(1500);

    const hasHeader = await page.getByText('Orchestration').first().isVisible().catch(() => false);
    console.log('Header visible:', hasHeader);

    const rows = page.locator('tbody tr');
    const rowCount = await rows.count();
    console.log('Pipeline rows:', rowCount);

    if (rowCount > 0) {
        await rows.first().click();
        await page.waitForTimeout(600);

        const runsTab = page.getByRole('button', { name: /Runs/i }).first();
        if (await runsTab.isVisible().catch(() => false)) {
            await runsTab.click();
            await page.waitForTimeout(400);
        }

        for (const label of ['calendar', 'gantt', 'task', 'list']) {
            const btn = page.getByRole('button', { name: new RegExp(`^${label}$`, 'i') }).first();
            if (await btn.isVisible().catch(() => false)) {
                await btn.click();
                await page.waitForTimeout(250);
            }
        }

        // Expand first run if available to expose task controls.
        const runRows = page.locator('div.border-b.border-white\\/5 >> div.px-4.py-2\\.5.cursor-pointer.transition-colors');
        if (await runRows.count() > 0) {
            await runRows.first().click();
            await page.waitForTimeout(400);
            const mappedBtn = page.getByRole('button', { name: /mapped only/i }).first();
            if (await mappedBtn.count() > 0) {
                await mappedBtn.click({ timeout: 1000 }).catch(() => {});
                await page.waitForTimeout(250);
                await mappedBtn.click({ timeout: 1000 }).catch(() => {});
            }
        }

        const runConfBtn = page.getByRole('button', { name: /Run \+ Conf/i }).first();
        if (await runConfBtn.isVisible().catch(() => false)) {
            await runConfBtn.click();
            await page.waitForTimeout(400);
            const modalVisible = await page.getByText('Trigger DAG with JSON conf').first().isVisible().catch(() => false);
            console.log('Conf modal visible:', modalVisible);
            await page.keyboard.press('Escape').catch(() => {});
            await page.mouse.click(20, 20).catch(() => {});
        }
    }

    await page.screenshot({ path: 'workflows_smoke.png', fullPage: true });
    await browser.close();
})();
