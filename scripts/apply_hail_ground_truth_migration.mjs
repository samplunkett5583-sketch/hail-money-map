#!/usr/bin/env node

import fs from "node:fs/promises";

const SUPABASE_URL = process.env.SUPABASE_URL;
const SUPABASE_ACCESS_TOKEN = process.env.SUPABASE_ACCESS_TOKEN;

if (!SUPABASE_URL || !SUPABASE_ACCESS_TOKEN) {
  console.error("Missing SUPABASE_URL or SUPABASE_ACCESS_TOKEN");
  process.exit(1);
}

const projectRef = new URL(SUPABASE_URL).hostname.split(".")[0];
const sql = await fs.readFile(
  new URL("../supabase/migrations/20260723_create_hail_ground_truth.sql", import.meta.url),
  "utf8",
);

const response = await fetch(
  `https://api.supabase.com/v1/projects/${projectRef}/database/query`,
  {
    method: "POST",
    headers: {
      Authorization: `Bearer ${SUPABASE_ACCESS_TOKEN}`,
      "Content-Type": "application/json",
    },
    body: JSON.stringify({ query: sql }),
  },
);
const body = await response.text();
if (!response.ok) {
  throw new Error(`Supabase migration ${response.status}: ${body.slice(0, 1000)}`);
}
console.log("Hail ground-truth schema is ready.");

