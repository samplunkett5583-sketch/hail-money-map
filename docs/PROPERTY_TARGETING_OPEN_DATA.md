# Hail Money open property targeting

Campaign Mode uses cached open data. Pressing **Apply** queries Supabase/PostGIS only; it never calls RentCast or another paid property provider.

## Model `hm-open-value-v1.0.0`

The first version is intentionally a targeting/ranking estimate, not an appraisal:

```text
currentLocalBaseline = ACS B25077_001E × FHFA current index / FHFA ACS-vintage index
sizeRatio = building footprint square feet / block-group median residential footprint square feet
sizeAdjustment = clamp(sizeRatio ^ 0.55, 0.70, 1.65)
Hail Money estimate = round(currentLocalBaseline × sizeAdjustment)
```

The footprint exponent and bounds live together in `supabase/functions/_shared/hail-money-value-model.js`. The range combines ACS margin-of-error, a 15% model component, and a 5% FHFA component and is bounded to 18–45% on each side. A modeled record is **Low** rather than **Modeled** when residential classification is weak or the ACS margin of error is more than 35% of the median.

Property-level overrides have precedence. Official market/fair-cash values and authorized sales/appraisals are **High** confidence. Official assessed values become **Medium** only when the import includes a verified jurisdiction assessment ratio. No production value is hard-coded for Fairmount.

Limitations: a building footprint is not living area; block-group medians cannot reproduce renovations, interior quality, lots or property condition; FHFA MSA movement is an area trend, not a house appraisal; Overture Addresses is an Alpha theme and coverage varies. Cards therefore say “Hail Money estimate” and “Targeting estimate—not an appraisal.”

## Free sources, license and attribution

- **Overture Maps Foundation Addresses and Buildings**, downloaded for only the requested bounding box from the official monthly GeoParquet release with the official `overturemaps` CLI. Overture publishes data under the **Community Data License Agreement – Permissive 2.0**, except source datasets that retain their own licenses. OpenStreetMap-derived data remains under **ODbL 1.0**. The importer preserves release IDs, feature source attributes and the required visible attribution: `© OpenStreetMap contributors, Overture Maps Foundation`. Follow the per-feature `sources` metadata and Overture attribution guide: <https://docs.overturemaps.org/attribution/>.
- **U.S. Census Bureau ACS 5-year**, variable `B25077_001E` (median value of owner-occupied housing units) and `B25077_001M` (margin of error), at block-group geography, plus TIGERweb block-group geometry. Census Bureau statistical and geographic data are U.S. federal government works and are reusable; the app preserves the ACS vintage, GEOID, variables and source endpoints. Census API terms: <https://www.census.gov/data/developers/about/terms-of-service.html>.
- **FHFA House Price Index**, the public `traditional / all-transactions / quarterly / NSA` series. The Madison County adapter uses the St. Louis, MO-IL MSA (`41180`) because it is the most local reliable quarterly series in FHFA's current public master JSON for the validation county. The app preserves series, geography, baseline/current periods, indexes and factor. FHFA public datasets: <https://www.fhfa.gov/data/hpi>.
- **Official assessor/GIS or authorized sale/appraisal override**, imported only from a lawfully obtained admin file. The source and its own terms remain attached to each override. The importer never scrapes Zillow, Realtor.com, Redfin or a county website.

## Tables and security

Migration `20260821120000_add_open_property_targeting.sql` adds owner-free structures, area values, current/history estimates, auditable overrides, coverage and import jobs. Spatial and value indexes support exact swath intersection and inclusive minimum filtering. Writes and search RPCs are service-role-only. Browser code cannot read the base tables or see a service key.

## Import and refresh workflow

1. The campaign Edge Function checks complete cached coverage.
2. Missing coverage creates one deduplicated `property_import_jobs` row for the rounded swath bounds. The request returns a truthful partial-coverage status; the browser never downloads source data.
3. An admin worker runs the importer for that queued bounding box. It downloads only missing Overture data, fetches ACS/TIGER/FHFA, associates addresses with primary non-accessory buildings, calculates estimates and writes them in chunks.
4. Coverage becomes complete only after structures, area values and estimates all succeed. A release/vintage key prevents an unchanged source release being presented as new coverage.
5. Later overlapping swaths reuse the cached rows. A new Overture/ACS/FHFA vintage is a deliberate refresh, not an Apply-time redownload.

Install the official Overture client once:

```powershell
py -m pip install overturemaps
```

Then, from the project folder, import the Madison County validation adapter (replace the box with the queued job's `progress_metadata.bounds` when importing another storm):

```powershell
$env:SUPABASE_URL='https://YOUR_PROJECT.supabase.co'
$env:SUPABASE_SECRET_KEY='load-securely-from-your-secret-manager'
$env:CENSUS_API_KEY='your-free-census-key'
npm run import:open-properties -- --jurisdiction madison-county-il --bbox=-90.28,38.72,-89.58,39.22
```

`SUPABASE_SECRET_KEY` must be a server-only `sb_secret_` key. Never put it in browser code, commit it, print it, or send it as a Bearer token. Administrative requests send it only in the `apikey` header.

Use `--skip-download --work-dir C:\path\to\existing\download` to resume already downloaded GeoJSON files. The work directory must contain `building.geojson` and `address.geojson`.

## Official/authorized overrides

CSV columns are:

```text
source_building_id,normalized_address,override_type,raw_field_name,raw_value,normalized_value,low_estimate,high_estimate,assessment_ratio,source_name,source_url,source_date,calculation_method
```

Use one stable building ID or one exact normalized address. For an official assessed value, supply a verified `assessment_ratio`; otherwise the row is rejected.

```powershell
npm run import:property-overrides -- --file C:\secure\authorized-property-values.csv --imported-by admin
```

No owner name or owner mailing address is accepted or stored.

## Fairmount validation

Fairmount Drive, Logan Road, Danforth Road, The Crossways Drive and Pondway Drive are validation locations only. A successful live validation requires an imported Overture release and cached ACS/FHFA rows covering the test swath. The automated fixture verifies that larger primary residential footprints rank above otherwise identical local houses, exact-threshold houses are included, outside/accessory structures are excluded, overlaps are deduplicated, and no paid request is made. It does not fabricate real Fairmount market values.
