const fs=require('fs');
const html=fs.readFileSync('public/index.html','utf8');
const required=[
  'hm-global-mobile-page-shell-v1',
  'body.app-page-active:not(.main-menu-page-active) .page.active .crm-dashboard-mini-sidebar',
  'body.app-page-active:not(.main-menu-page-active) .page.active .hm-dashboard-ref .sidebar',
  'function crmGetMobileHeaderPageTitle(pageId)',
  "'page-main-menu':'Dashboard'",
  "'page-photo-files':'Photos'",
  "'page-company-docs':'Documents'",
  "'page-crm-pipeline':'Pipeline'",
  "if (typeof crmSyncMobilePageChrome === 'function') crmSyncMobilePageChrome(pageId);",
  'Phone-friendly field placement. A tap uses the field',
  'id="tpl-company-doc-help"',
  "edit.textContent='Edit Fields'",
  'ensureCompanyDocPdfJs',
  "showPage('page-template-builder');requestAnimationFrame"
];
for(const snippet of required){if(!html.includes(snippet))throw new Error('Mobile/page sync missing: '+snippet);}
if(html.includes('hm-global-mobile-page-title-v1'))throw new Error('Duplicate mobile title controller detected.');
console.log('Mobile page titles, collapsed rails, and document field editor verified.');
