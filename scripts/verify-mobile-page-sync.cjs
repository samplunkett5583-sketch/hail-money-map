const fs=require('fs');
const html=fs.readFileSync('public/index.html','utf8');
const required=['hm-global-mobile-page-shell-v1','hm-global-mobile-page-title-v1','body.app-page-active:not(.main-menu-page-active) .page.active .crm-dashboard-mini-sidebar','page-crm-pipeline.active .hm-dashboard-ref .wrap',"'page-photo-files':'Photos'",'data-crm-header-title="Document Editor"'];
for(const x of required){if(!html.includes(x))throw new Error('Mobile page sync missing: '+x);}
const m=html.match(/<script id="hm-global-mobile-page-title-v1">([\s\S]*?)<\/script>/);if(!m)throw new Error('Mobile title script missing');new Function(m[1]);
console.log('Global mobile page collapse and titles verified.');
