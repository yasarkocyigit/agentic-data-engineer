import { chromium } from 'playwright';

(async () => {
    const browser = await chromium.launch();
    const page = await browser.newPage();
    page.on('console', msg => console.log('BROWSER CONSOLE:', msg.text()));

    await page.goto('http://localhost:3010/cicd');

    // Wait for repos and click the first one
    await page.waitForSelector('div.truncate', { timeout: 10000 });
    await page.evaluate(() => {
        const repoHeaders = document.querySelectorAll('div.truncate');
        if (repoHeaders.length > 0) {
            repoHeaders[0].click();
        }
    });
    console.log('Repo clicked.');

    // Wait for row elements to appear indicating the tree has loaded
    try {
        await page.waitForSelector('tr.group.cursor-pointer', { timeout: 10000 });
        console.log('Tree loaded.');
    } catch (e) {
        console.log('Tree did not load in time.');
    }

    // Click on a file
    const fileClicked = await page.evaluate(() => {
        const trs = document.querySelectorAll('tr.group.cursor-pointer');
        for (const tr of trs) {
            if (tr.textContent.includes('README.md') || tr.textContent.includes('package.json') || tr.textContent.includes('.py')) {
                tr.click();
                return tr.textContent.trim().split('\n')[0];
            }
        }
        if (trs.length > 0) {
            trs[trs.length - 1].click();
            return trs[trs.length - 1].textContent.trim().split('\n')[0];
        }
        return false;
    });

    console.log('File Clicked:', fileClicked);
    await page.waitForTimeout(3000);

    // Snapshot dimensions of editor components
    const rendering = await page.evaluate(() => {
        const prose = document.querySelector('.prose');
        const editor = document.querySelector('.monaco-editor');
        const container = document.querySelector('.monaco-editor')?.parentElement;
        return {
            proseExists: !!prose,
            proseHeight: prose ? prose.offsetHeight : 0,
            editorExists: !!editor,
            editorHeight: editor ? editor.offsetHeight : 0,
            containerMinHeight: container?.style?.minHeight || (container ? window.getComputedStyle(container).minHeight : null)
        };
    });

    console.log('Rendering state:', rendering);
    await page.screenshot({ path: 'frontend_file_view_test.png' });

    await browser.close();
})();
