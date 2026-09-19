const fs = require('fs');
const html = fs.readFileSync('public/index.html', 'utf8');
const required = [
  "c.hmOrganizationId",
  "collection('hmEmployees').doc(u.uid)",
  "d.organizationId||d.hmOrganizationId",
  "/@hailmoney\\.test$/i",
  "return'yopro'"
];
for (const snippet of required) {
  if (!html.includes(snippet)) throw new Error('Company Documents workspace resolver missing: ' + snippet);
}
const old = "async function orgId(){return typeof crmResolveFirestoreOrgId==='function'?await crmResolveFirestoreOrgId():''}";
if (html.includes(old)) throw new Error('Company Documents reverted to the single-source workspace resolver.');
console.log('Company Documents workspace access verified.');
