import fs from 'node:fs';
import path from 'node:path';
import pg from 'pg';

const root = path.resolve(path.dirname(new URL(import.meta.url).pathname.replace(/^\/(?:[A-Za-z]:)/, m => m.slice(1))), '..');
const envPath = path.join(root, '.env.neon.local');
const envText = fs.readFileSync(envPath, 'utf8');
for (const line of envText.split(/\r?\n/)) {
  const m = line.match(/^([A-Z0-9_]+)=(.*)$/);
  if (!m) continue;
  let value = m[2].trim();
  if ((value.startsWith('"') && value.endsWith('"')) || (value.startsWith("'") && value.endsWith("'"))) value = value.slice(1, -1);
  process.env[m[1]] = value;
}
if (!process.env.DATABASE_URL) throw new Error('DATABASE_URL is missing.');
const sql = fs.readFileSync(path.join(root, 'neon', 'migrations', '001_hail_money_cloud.sql'), 'utf8');
const client = new pg.Client({ connectionString: process.env.DATABASE_URL });
await client.connect();
try {
  await client.query(sql);
  const r = await client.query("select to_regclass('public.hm_app_state') as state_table, to_regclass('public.hm_files') as files_table, to_regclass('public.hm_audit_events') as audit_table");
  console.log(JSON.stringify(r.rows[0]));
} finally {
  await client.end();
}
