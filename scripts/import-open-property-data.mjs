#!/usr/bin/env node
import fs from 'node:fs/promises'
import os from 'node:os'
import path from 'node:path'
import { pathToFileURL } from 'node:url'
import { spawnSync } from 'node:child_process'
import { calculateModeledEstimate, parseAcsMedianValueRow } from '../supabase/functions/_shared/hail-money-value-model.js'
import {
  associateAddressesToBuildings, bboxPolygon, classifyBuilding, geometryWkt,
  median, normalizeAddress, pointInGeometry, selectFhfaSeries, stableId,
} from './property-open-data-tools.mjs'
import { jurisdictionAdapter } from './property-jurisdiction-adapters.mjs'
import { getSupabaseServerKey, supabaseServerHeaders } from './supabase-server-auth.mjs'

const OVERTURE_CATALOG_URL = 'https://stac.overturemaps.org/catalog.json'
const FHFA_MASTER_URL = 'https://www.fhfa.gov/hpi/download/monthly/hpi_master.json'
const ACS_URL = 'https://api.census.gov/data/2024/acs/acs5'
const TIGER_BLOCK_GROUP_URL = 'https://tigerweb.geo.census.gov/arcgis/rest/services/TIGERweb/tigerWMS_ACS2024/MapServer/10/query'

function argsOf(argv) {
  const out = {}
  for (let i = 0; i < argv.length; i++) {
    if (!argv[i].startsWith('--')) continue
    const [key, inline] = argv[i].slice(2).split('=', 2)
    out[key] = inline ?? (argv[i + 1] && !argv[i + 1].startsWith('--') ? argv[++i] : true)
  }
  return out
}

function sanitizeError(error) {
  return String(error?.message || error || 'import failed')
    .replace(/((?:api[-_ ]?key|authorization|bearer|token|service_role)\s*[:=]?\s*)[^\s,;]+/gi, '$1[redacted]')
    .slice(0, 700)
}

function requireEnv(name) {
  const value = process.env[name]
  if (!value) throw new Error(name + ' is required')
  return value
}

async function readGeoJson(file) {
  const text = await fs.readFile(file, 'utf8')
  try {
    const parsed = JSON.parse(text)
    if (parsed.type === 'FeatureCollection') return parsed.features || []
    if (parsed.type === 'Feature') return [parsed]
  } catch (_) {}
  return text.split(/\r?\n/).filter(Boolean).map((line) => JSON.parse(line))
}

async function latestOvertureRelease() {
  const response = await fetch(OVERTURE_CATALOG_URL)
  if (!response.ok) throw new Error('Overture release catalog failed with HTTP ' + response.status)
  const catalog = await response.json()
  const children = (catalog.links || []).filter((link) => link.rel === 'child').map((link) => String(link.href || ''))
  const release = children.map((href) => {
    const cleaned = href.replace(/[?#].*$/, '').replace(/\/?catalog\.json$/i, '').replace(/\/$/, '')
    const segment = cleaned.split('/').at(-1) || ''
    return /^\d{4}-\d{2}-\d{2}(?:\.\d+)?$/.test(segment) ? segment : null
  }).filter(Boolean).sort().at(-1)
  return release || 'latest'
}

function runOvertureDownload(binary, bbox, type, output) {
  const bboxText = bbox.join(',')
  const attempts = [
    [binary, ['download', '--bbox=' + bboxText, '-f', 'geojson', '--type=' + type, '-o', output]],
    [process.env.PYTHON || 'python', ['-m', 'overturemaps', 'download', '--bbox=' + bboxText, '-f', 'geojson', '--type=' + type, '-o', output]],
  ]
  for (const [command, commandArgs] of attempts) {
    const result = spawnSync(command, commandArgs, { stdio: 'inherit', shell: false })
    if (!result.error && result.status === 0) return
  }
  throw new Error('The official overturemaps CLI is required. Install it with: python -m pip install overturemaps')
}

async function fetchAcs(adapter, year, censusKey) {
  const url = new URL('https://api.census.gov/data/' + year + '/acs/acs5')
  url.searchParams.set('get', 'NAME,B25077_001E,B25077_001M')
  url.searchParams.set('for', 'block group:*')
  url.searchParams.append('in', 'state:' + adapter.stateFips)
  url.searchParams.append('in', 'county:' + adapter.countyFips)
  url.searchParams.append('in', 'tract:*')
  if (censusKey) url.searchParams.set('key', censusKey)
  const response = await fetch(url)
  if (!response.ok) throw new Error('ACS request failed with HTTP ' + response.status)
  const rows = await response.json(); const headers = rows.shift()
  return rows.map((row) => parseAcsMedianValueRow(headers, row)).filter(Boolean)
}

async function fetchBlockGroups(adapter) {
  const url = new URL(TIGER_BLOCK_GROUP_URL)
  url.searchParams.set('where', `STATE='${adapter.stateFips}' AND COUNTY='${adapter.countyFips}'`)
  url.searchParams.set('outFields', 'GEOID,STATE,COUNTY,TRACT,BLKGRP,NAME')
  url.searchParams.set('returnGeometry', 'true')
  url.searchParams.set('outSR', '4326')
  url.searchParams.set('f', 'geojson')
  const response = await fetch(url)
  if (!response.ok) throw new Error('TIGERweb block-group request failed with HTTP ' + response.status)
  const body = await response.json()
  if (!Array.isArray(body.features)) throw new Error('TIGERweb returned no block-group feature collection')
  return body.features
}

async function fetchFhfa(adapter, acsYear) {
  const response = await fetch(FHFA_MASTER_URL)
  if (!response.ok) throw new Error('FHFA HPI request failed with HTTP ' + response.status)
  const series = selectFhfaSeries(await response.json(), adapter, acsYear)
  if (!series) throw new Error('No reliable FHFA HPI series was found for ' + adapter.label)
  return series
}

function addressParts(properties = {}) {
  return {
    number: String(properties.number ?? properties.house_number ?? ''),
    street: String(properties.street ?? properties.street_name ?? ''),
    locality: String(properties.locality ?? properties.city ?? ''),
    postcode: String(properties.postcode ?? properties.postal_code ?? ''),
  }
}

function findBlockGroup(point, blockGroups) {
  return blockGroups.find((feature) => pointInGeometry(point, feature.geometry)) || null
}

async function supabaseRequest(database, route, options = {}) {
  const response = await fetch(database.url + '/rest/v1/' + route, {
    method: options.method || 'GET',
    headers: {
      ...supabaseServerHeaders(database.key),
      'Content-Type': 'application/json', Prefer: options.prefer || 'return=representation',
    },
    body: options.body === undefined ? undefined : JSON.stringify(options.body),
  })
  const text = await response.text(); let data = null
  try { data = text ? JSON.parse(text) : null } catch (_) { data = null }
  if (!response.ok) throw new Error((options.label || route) + ' failed with HTTP ' + response.status + ': ' + String(data?.message || text).slice(0, 400))
  return data
}

async function upsertChunks(database, table, rows, conflict, chunkSize = 500, select = '') {
  const selected = []
  for (let offset = 0; offset < rows.length; offset += chunkSize) {
    const query = new URLSearchParams({ on_conflict: conflict }); if (select) query.set('select', select)
    const data = await supabaseRequest(database, table + '?' + query, {
      method: 'POST', body: rows.slice(offset, offset + chunkSize),
      prefer: 'resolution=merge-duplicates,return=representation', label: table + ' upsert',
    })
    if (Array.isArray(data)) selected.push(...data)
  }
  return selected
}

export async function runImport(options = {}) {
  const adapter = jurisdictionAdapter(options.jurisdiction || 'madison-county-il')
  const bbox = String(options.bbox || adapter.defaultBbox.join(',')).split(',').map(Number)
  if (bbox.length !== 4 || !bbox.every(Number.isFinite)) throw new Error('--bbox must be west,south,east,north')
  const acsYear = Number(options['acs-year'] || 2024)
  const sourceRelease = String(options.release || await latestOvertureRelease())
  const workDir = path.resolve(String(options['work-dir'] || path.join(os.tmpdir(), 'hail-money-open-property-' + sourceRelease)))
  await fs.mkdir(workDir, { recursive: true })
  const buildingFile = path.join(workDir, 'building.geojson')
  const addressFile = path.join(workDir, 'address.geojson')
  if (!options['skip-download']) {
    runOvertureDownload(String(options['overture-bin'] || process.env.OVERTUREMAPS_BIN || 'overturemaps'), bbox, 'building', buildingFile)
    runOvertureDownload(String(options['overture-bin'] || process.env.OVERTUREMAPS_BIN || 'overturemaps'), bbox, 'address', addressFile)
  }
  const database = { url: requireEnv('SUPABASE_URL').replace(/\/$/, ''), key: getSupabaseServerKey() }
  // Matches the Edge Function's rounded swath-bounds key so a worker advances
  // the exact queued job the user sees in Campaign Mode.
  const requestKey = `open-property:${bbox.map((n) => n.toFixed(3)).join(':')}`
  const initialJob = {
    request_key: requestKey, jurisdiction: adapter.id, source_name: 'overture_acs_fhfa',
    source_release: sourceRelease, status: 'running', started_at: new Date().toISOString(),
    completed_at: null, batches_processed: 0, records_received: 0, records_inserted: 0,
    records_updated: 0, duplicates_removed: 0, records_rejected: 0, sanitized_error: null,
    progress_metadata: { bbox, acsYear, stage: 'reading open sources' },
  }
  const jobs = await supabaseRequest(database, 'property_import_jobs?on_conflict=request_key&select=id', { method: 'POST', body: initialJob, prefer: 'resolution=merge-duplicates,return=representation', label: 'create import job' })
  const job = jobs?.[0]; if (!job?.id) throw new Error('Unable to create import job')
  try {
    const [buildings, addresses, acsRows, blockGroups, fhfa] = await Promise.all([
      readGeoJson(buildingFile), readGeoJson(addressFile),
      fetchAcs(adapter, acsYear, process.env.CENSUS_API_KEY || ''), fetchBlockGroups(adapter), fetchFhfa(adapter, acsYear),
    ])
    const acsByGeoid = new Map(acsRows.map((row) => [row.geoid, row]))
    const matches = associateAddressesToBuildings(addresses, buildings)
    const candidates = []
    let rejected = 0
    for (const match of matches) {
      const classification = classifyBuilding(match.building.properties || {}, true, match.areaSqft)
      if (!classification.residential) { rejected++; continue }
      const blockGroup = findBlockGroup(match.point, blockGroups)
      const geoid = String(blockGroup?.properties?.GEOID || '')
      const acs = acsByGeoid.get(geoid)
      if (!blockGroup || !acs) { rejected++; continue }
      candidates.push({ ...match, classification, blockGroup, geoid, acs })
    }
    const footprintByGeoid = new Map()
    candidates.forEach((row) => {
      if (!footprintByGeoid.has(row.geoid)) footprintByGeoid.set(row.geoid, [])
      footprintByGeoid.get(row.geoid).push(row.areaSqft)
    })
    const structureRows = candidates.map((row) => {
      const address = addressParts(row.address.properties || {})
      const buildingId = String(row.building.id || row.building.properties?.id || stableId('overture-building', JSON.stringify(row.building.geometry)))
      const addressId = String(row.address.id || row.address.properties?.id || stableId('overture-address', row.addressText + '|' + row.point.join(',')))
      return {
        jurisdiction: adapter.id, state_code: adapter.stateCode, county_name: adapter.countyName,
        source_name: 'Overture Maps', source_release: sourceRelease, source_building_id: buildingId,
        source_address_id: addressId, normalized_address: row.addressText || normalizeAddress(row.address.properties),
        address_number: address.number, street_name: address.street, locality: address.locality, postcode: address.postcode,
        point_geometry: geometryWkt({ type: 'Point', coordinates: row.point }), building_geometry: geometryWkt(row.building.geometry),
        footprint_area_sqft: Math.round(row.areaSqft), building_classification: row.classification.classification,
        is_residential: true, residential_confidence: row.classification.confidence,
        residential_reason: row.classification.reason,
        source_attributes: { building: row.building.properties || {}, address: row.address.properties || {}, censusGeoid: row.geoid },
        source_url: 'https://overturemaps.org/',
        source_attribution: '© OpenStreetMap contributors, Overture Maps Foundation; Overture source attributions retained in source attributes',
        source_updated_at: new Date().toISOString(), updated_at: new Date().toISOString(),
      }
    })
    const storedStructures = await upsertChunks(database, 'property_structures', structureRows, 'source_name,source_building_id', 300, 'id,source_building_id,source_attributes,footprint_area_sqft,residential_confidence')
    const uniqueBlockGroups = [...new Map(candidates.map((row) => [row.geoid, row])).values()]
    const areaRows = uniqueBlockGroups.map((row) => {
      const adjustment = fhfa.currentIndex / fhfa.baselineIndex
      return {
        census_geoid: row.geoid, geography_type: 'ACS 5-year block group', state_code: adapter.stateCode, county_name: adapter.countyName,
        acs_variable: 'B25077_001E', acs_median_value: row.acs.medianValue, acs_margin_of_error: row.acs.marginOfError,
        acs_vintage: acsYear, fhfa_geography_type: fhfa.level, fhfa_geography_id: fhfa.placeId, fhfa_series: fhfa.series,
        fhfa_baseline_period: fhfa.baselinePeriod, fhfa_current_period: fhfa.currentPeriod,
        fhfa_baseline_index: fhfa.baselineIndex, fhfa_current_index: fhfa.currentIndex, fhfa_adjustment_factor: adjustment,
        adjusted_current_baseline: Math.round(row.acs.medianValue * adjustment),
        median_residential_footprint_sqft: Math.round(median(footprintByGeoid.get(row.geoid)) || 0),
        coverage_geometry: geometryWkt(row.blockGroup.geometry),
        source_metadata: {
          acsName: row.acs.name, acsApi: ACS_URL, tigerwebLayer: TIGER_BLOCK_GROUP_URL,
          fhfaSource: FHFA_MASTER_URL, fhfaPeriod: fhfa.currentPeriod,
        }, updated_at: new Date().toISOString(),
      }
    })
    const storedAreas = await upsertChunks(database, 'property_area_values', areaRows, 'census_geoid,acs_vintage,fhfa_current_period', 250, 'id,census_geoid,acs_median_value,acs_margin_of_error,median_residential_footprint_sqft,fhfa_baseline_index,fhfa_current_index,acs_vintage,fhfa_current_period')
    const areaByGeoid = new Map(storedAreas.map((row) => [row.census_geoid, row]))
    const estimateRows = storedStructures.map((structure) => {
      const geoid = String(structure.source_attributes?.censusGeoid || '')
      const area = areaByGeoid.get(geoid)
      const estimate = calculateModeledEstimate({
        acsMedianValue: area?.acs_median_value, acsMarginOfError: area?.acs_margin_of_error,
        fhfaBaselineIndex: area?.fhfa_baseline_index, fhfaCurrentIndex: area?.fhfa_current_index,
        footprintAreaSqft: structure.footprint_area_sqft,
        localMedianFootprintSqft: area?.median_residential_footprint_sqft,
        residentialConfidence: structure.residential_confidence,
      })
      if (!estimate.valid) return null
      return {
        property_id: structure.id, area_value_id: area.id, estimated_value: estimate.estimatedValue,
        low_estimate: estimate.lowEstimate, high_estimate: estimate.highEstimate, confidence: estimate.confidence,
        model_version: estimate.modelVersion, valuation_method: estimate.valuationMethod,
        calculation_components: estimate.components,
        input_sources: [
          { source: 'Overture Maps', release: sourceRelease },
          { source: 'U.S. Census ACS 5-year', vintage: acsYear, geography: geoid, variable: 'B25077_001E' },
          { source: 'FHFA HPI', geography: fhfa.placeId, period: fhfa.currentPeriod, series: fhfa.series },
        ], calculation_date: new Date().toISOString(), updated_at: new Date().toISOString(),
      }
    }).filter(Boolean)
    await upsertChunks(database, 'property_value_estimates', estimateRows, 'property_id', 300)
    const coverageGeometry = geometryWkt(bboxPolygon(bbox))
    await supabaseRequest(database, 'property_data_coverage?on_conflict=jurisdiction,source_name,source_release,coverage_key', { method: 'POST', prefer: 'resolution=merge-duplicates,return=minimal', body: {
      jurisdiction: adapter.id, source_name: 'hail_money_estimates', source_release: sourceRelease,
      coverage_key: requestKey,
      coverage_geometry: coverageGeometry, status: 'complete', complete: true,
      record_count: estimateRows.length, source_vintage: `${sourceRelease}|ACS${acsYear}|FHFA${fhfa.currentPeriod}`,
      imported_at: new Date().toISOString(), checked_at: new Date().toISOString(), error_details: null,
      source_metadata: { bbox, overtureRelease: sourceRelease, acsYear, fhfa, rejected, modelVersion: estimateRows[0]?.model_version || null },
    } })
    await supabaseRequest(database, 'property_import_jobs?id=eq.' + encodeURIComponent(job.id), { method: 'PATCH', prefer: 'return=minimal', body: {
      status: 'complete', completed_at: new Date().toISOString(), records_received: matches.length,
      records_inserted: estimateRows.length, records_rejected: rejected,
      progress_metadata: { bbox, stage: 'complete', buildings: buildings.length, addresses: addresses.length, matched: matches.length },
    } })
    return { jurisdiction: adapter.id, release: sourceRelease, buildings: buildings.length, addresses: addresses.length, estimates: estimateRows.length, rejected, workDir }
  } catch (error) {
    await supabaseRequest(database, 'property_import_jobs?id=eq.' + encodeURIComponent(job.id), { method: 'PATCH', prefer: 'return=minimal', body: { status: 'failed', completed_at: new Date().toISOString(), sanitized_error: sanitizeError(error) } }).catch(() => {})
    throw error
  }
}

if (process.argv[1] && import.meta.url === pathToFileURL(path.resolve(process.argv[1])).href) {
  runImport(argsOf(process.argv.slice(2))).then((result) => console.log(JSON.stringify(result, null, 2))).catch((error) => {
    console.error(sanitizeError(error)); process.exitCode = 1
  })
}
