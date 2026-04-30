const url = process.env.SUPABASE_URL + "/rest/v1/storm_lsr_raw?select=event_date,event_type&limit=100000";
const headers = { apikey: process.env.SUPABASE_ANON_KEY, Authorization: 'Bearer ' + process.env.SUPABASE_ANON_KEY };
const res = await fetch(url, { headers });
if (!res.ok) throw new Error('Supabase query failed: ' + res.status + ' ' + await res.text());
const rows = await res.json();
const counts = {};
rows.forEach(r => {
  const yr = (r.event_date || '').slice(0,4);
  if (!yr) return;
  const et = r.event_type || 'unknown';
  const key = yr + '|' + et;
  counts[key] = (counts[key] || 0) + 1;
});
const sorted = Object.keys(counts).sort((a,b)=> {
  const [ya,ea] = a.split('|');
  const [yb,eb] = b.split('|');
  if (ya !== yb) return Number(yb) - Number(ya);
  return ea.localeCompare(eb);
});
sorted.forEach(k => {
  const [yr,et] = k.split('|');
  console.log(yr + '\t' + et + '\t' + counts[k]);
});
console.log('TOTAL_ROWS=' + rows.length);
