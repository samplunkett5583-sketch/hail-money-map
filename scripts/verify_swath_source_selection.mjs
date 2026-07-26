#!/usr/bin/env node
/**
 * Regression checks for the production hail-product selector embedded in
 * public/index.html. These scenarios represent the recurring failure modes:
 * canonical MRMS plus legacy tracks, multiple legacy products, and one
 * multi-band product.
 */

import { readFile } from "node:fs/promises";
import vm from "node:vm";

const html = await readFile(new URL("../public/index.html", import.meta.url), "utf8");
const start = html.indexOf("function mapsSelectHailCoverageRows(");
if (start < 0) throw new Error("mapsSelectHailCoverageRows not found");

let brace = html.indexOf("{", start);
let depth = 0;
let quote = null;
let escaped = false;
let end = -1;
for (let index = brace; index < html.length; index++) {
  const char = html[index];
  if (quote) {
    if (escaped) escaped = false;
    else if (char === "\\") escaped = true;
    else if (char === quote) quote = null;
    continue;
  }
  if (char === "'" || char === '"' || char === "`") {
    quote = char;
    continue;
  }
  if (char === "{") depth++;
  if (char === "}" && --depth === 0) {
    end = index + 1;
    break;
  }
}
if (end < 0) throw new Error("mapsSelectHailCoverageRows closing brace not found");

const context = {
  console,
  mapsState: {
    showDerivedCorridors: false,
    showQuarantinedGeometry: false,
    hailShadowSummaryByDate: {},
  },
  mapsNormalizeDate: (value) => String(value || "").slice(0, 10),
  mapsHailRowProvenance: (row) => row && row.provenance || null,
  mapsIsSwathDebugMode: () => false,
  mapsAcceptedOnlyFeatureEnabled: () => false,
};
vm.createContext(context);
vm.runInContext(html.slice(start, end), context);

const row = (source, product, band = 0.5, priority = 1) => ({
  storm_type: "hail",
  source,
  source_product: product,
  band_min: band,
  source_priority: priority,
  polygon_geojson: { type: "Polygon", coordinates: [] },
});

function assert(condition, message) {
  if (!condition) throw new Error(message);
}

const june18 = [
  row("nexrad_iem", "iem_nexrad_track", 0.5, 1),
  row("nexrad_iem", "iem_nexrad_track", 1.5, 1),
  row("mrms_mesh", "MESH_Max_1440min", 0.5, 1),
  row("mrms_mesh", "MESH_Max_1440min", 1.25, 1),
];
const june18Selected = context.mapsSelectHailCoverageRows(june18, "2026-06-18");
assert(june18Selected.length === 2, "June 18 must select one canonical product");
assert(june18Selected.every((item) => item.source === "mrms_mesh"),
  "June 18 must not stack NEXRAD tracks over MRMS");

const legacyDate = [
  row("nexrad_iem", "iem_nexrad_track", 0.5, 1),
  row("nexrad_iem", "iem_nexrad_track", 1.5, 1),
  row("swdi", "legacy_polygon", 0.5, 2),
];
const legacySelected = context.mapsSelectHailCoverageRows(legacyDate, "2026-04-14");
assert(legacySelected.length === 2, "A legacy date must retain every band from one product");
assert(legacySelected.every((item) => item.source === "nexrad_iem"),
  "A legacy date must not stack competing product families");

const may3 = [
  row("manual", "reviewed_2026_05_03", 0.5, 1),
  row("manual", "reviewed_2026_05_03", 1.5, 1),
  row("nexrad_iem", "iem_nexrad_track", 0.5, 1),
];
const may3Selected = context.mapsSelectHailCoverageRows(may3, "2026-05-03");
assert(may3Selected.length === 2 && may3Selected.every((item) => item.source === "manual"),
  "May 3 reviewed geometry must remain the selected product");

console.log("[SWATH-SELECTION] 3 multi-date regression scenarios passed");
