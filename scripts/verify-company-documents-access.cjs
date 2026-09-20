const fs = require('fs');
const html = fs.readFileSync('public/index.html', 'utf8');
const required = [
  "c.hmOrganizationId",
  "collection('hmEmployees').doc(u.uid)",
  "d.organizationId||d.hmOrganizationId",
  "/@hailmoney\\.test$/i",
  "return'yopro'",
  "edit.textContent='Edit'",
  "edit.onclick=function(){editDoc(d.id)}",
  "window.hmCompanyDocumentTemplateEdit",
  "save.textContent='Save Document Fields'",
  "templateFields:fields",
  "stopImmediatePropagation"
];
for (const snippet of required) {
  if (!html.includes(snippet)) throw new Error('Company Documents workspace/editor missing: ' + snippet);
}
const old = "async function orgId(){return typeof crmResolveFirestoreOrgId==='function'?await crmResolveFirestoreOrgId():''}";
if (html.includes(old)) throw new Error('Company Documents reverted to the single-source workspace resolver.');
console.log('Company Documents workspace access and field editor verified.');
