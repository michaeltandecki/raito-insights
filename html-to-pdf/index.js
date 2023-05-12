const puppeteer = require('puppeteer');
const fs = require('fs');
require('dotenv').config();

(async () => {

  const browser = await puppeteer.launch();
  const page = await browser.newPage();

  const website_url = process.env.URL; 
  await page.goto(website_url, { waitUntil: 'networkidle0' }); 

//   const html = fs.readFileSync('report_230211.html', 'utf-8');
//   await page.setContent(html, { waitUntil: 'domcontentloaded' });

  await page.emulateMediaType('screen');
  const pdf = await page.pdf({
    path: 'result.pdf',
    margin: { top: '100px', right: '50px', bottom: '100px', left: '50px' },
    printBackground: true,
    format: 'A4',
  });

  await browser.close();  
})();