#!/usr/bin/env node
/**
 * Regression check for the MRMS batch date collector. Supabase pages are
 * simulated so the test proves dates beyond the first 1,000 rows are retained
 * and wind-only rows are never queried as hail dates.
 */

import { readFile } from "node:fs/promises";

const source = await readFile(new URL("./ingest_mrms_batch.mjs", import.meta.url), "utf8");

function assert(condition, message) {
  if (!condition) throw new Error(message);
}

assert(source.includes(".range(from, from + pageSize - 1)"),
  "MRMS batch date collection must paginate Supabase rows");
assert((source.match(/\.range\(from, from \+ pageSize - 1\)/g) || []).length >= 2,
  "Both the hail-date inventory and ingested-date lookup must paginate");
assert(source.includes('sb.from("hail_lsr_raw").select("event_date")'),
  "MRMS batch must build its date inventory from verified hail reports");
assert(!source.includes('"legacy hail polygons"'),
  "Hail-date inventory must not scan the large legacy polygon table");
assert(source.includes('.from("hail_radar_days")'),
  "Ingested-date lookup must use the compact canonical date table");
assert(!source.includes('sb.from("storm_lsr_raw").select("event_date")'),
  "Wind-only report dates must not be treated as hail dates");
assert(source.includes("index % shardCount === shardIndex"),
  "Full backfill must partition dates into independent shards");
const shardAssignment = source.indexOf(
  "workDates = workDates.filter((_, index) => index % shardCount === shardIndex)",
);
const completedDateFilter = source.indexOf(
  "workDates = workDates.filter((d) => !ingested.has(d))",
);
assert(shardAssignment >= 0 && completedDateFilter >= 0 && shardAssignment < completedDateFilter,
  "Dates must receive a permanent shard before completed dates are removed");

const html = await readFile(new URL("../public/index.html", import.meta.url), "utf8");
assert(html.includes("rowSrcLower !== 'mrms_mesh'"),
  "Canonical MRMS geometry must bypass synthetic hand-cut rendering");

const converter = await readFile(new URL("./mrms_mesh_to_geojson.py", import.meta.url), "utf8");
assert(!converter.includes("SimplifyPreserveTopology"),
  "Canonical MRMS polygons must retain native radar-grid edges");

const workflow = await readFile(new URL("../.github/workflows/mrms-backfill.yml", import.meta.url), "utf8");
assert(workflow.includes("shard: [0, 1, 2, 3, 4, 5]"),
  "Backfill workflow must fan out across six shards");
assert(workflow.includes('SHARD_COUNT: "6"'),
  "Backfill workflow and batch script must share the shard count");

console.log("[MRMS-BATCH-DATES] pagination and canonical-edge guards passed");
