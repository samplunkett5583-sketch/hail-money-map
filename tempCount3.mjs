import { createClient } from '@supabase/supabase-js';
const url = process.env.SUPABASE_URL;
const key = process.env.SUPABASE_SERVICE_ROLE_KEY || process.env.SUPABASE_ANON_KEY;
if (!url || !key) throw new Error('Missing SUPABASE_URL or key in env');
const supabase = createClient(url, key, { auth: { persistSession: false } });
const counts = {};
const pageSize = 1000;
let start = 0;
let total = 0;
while (true) {
  const { data, error } = await supabase.from('storm_lsr_raw').select('event_date,event_type').range(start, start + pageSize - 1);
  if (error) throw error;
  if (!data || data.length === 0) break;
  data.forEach(r => {
    const yr = (r.event_date || '').slice(0,4);
    if (!yr) return;
    const et = r.event_type || 'unknown';
    const key = yr + '|' + et;
    counts[key] = (counts[key] || 0) + 1;
  });
  total += data.length;
  if (data.length < pageSize) break;
  start += pageSize;
}
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
console.log('TOTAL_ROWS=' + total);
