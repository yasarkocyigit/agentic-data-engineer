const { chromium } = require('playwright');
(async () => {
    const browser = await chromium.launch();
    const page = await browser.newPage();
    page.on('console', msg => console.log('BROWSER CONSOLE:', msg.text()));
    page.on('pageerror', error => console.log('BROWSER ERROR:', error.message));
    
    await page.goto('http://localhost:3010/cicd');
    await page.waitForTimeout(2000);
    
    // Check if openclaw:select-repo works
    await page.evaluate(() => {
        window.dispatchEvent(new CustomEvent('openclaw:select-repo', { detail: { repo: 'yasarkocyigit/agentic-data-engineer' } }));
    });
    
    await page.waitForTimeout(2000);
    
    // Click on a file
    const fileClicked = await page.evaluate(() => {
        const trs = document.querySelectorAll('tr.group.cursor-pointer');
        for (const tr of trs) {
            // Find a file to click
            if (tr.textContent.includes('README.md') || tr.textContent.includes('main.py')) {
                tr.click();
                return true;
            }
        }
        // Just click the last TR if no specific file found
        if (trs.length > 0) {
            trs[trs.length - 1].click();
            return true;
        }
        return false;
    });
    
    console.log('File Clicked:', fileClicked);
    await page.waitForTimeout(2000);
    
    // Check if editor or markdown rendered
    const rendering = await page.evaluate(() => {
        return {
            prose: !!document.querySelector('.prose'),
            monaco: !!document.querySelector('.monaco-editor')
        };
    });
    
    console.log('Rendering state:', rendering);
    
    await browser.close();
})();
