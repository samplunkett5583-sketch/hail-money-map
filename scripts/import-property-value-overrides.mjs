#!/usr/bin/env node
import fs from 'node:fs/promises'
import path from 'node:path'
import { pathToFileURL } from 'node:url'
import { getSupabaseServerKey, supabaseServerHeaders } from './supabase-server-auth.mjs'

const ALLOWED_TYPES = new Set(['official_market_value', 'official_assessed_value', 'authorized_sale', 'authorized_appraisal'])

function argsOf(argv) {
  const out = {}
  for (let i = 0; i < argv.length; i++) if (argv[i].startsWith('--')) {
    const key = argv[i].slice(2); out[key] = argv[i + 1] && !argv[i + 1].startsWith('--') ? argv[++i] : true
  }
  return out
}

function positive(value) {
  const number = Number(String(value ?? '').replace(/[$,]/g, ''))
  return Number.isFinite(number) && number > 0 ? number : null
}

function parseCsv(text) {
  const records = []; let row = []; let field = ''; let quoted = false
  for (let i = 0; i <= text.length; i++) {
    const character = text[i] ?? '\n'
    if (quoted && character === '"' && text[i + 1] === '"') { field += '"'; i++; continue }
    if (character === '"') { quoted = !quoted; continue }
    if (!quoted && (character === ',' || character === '\n' || character === '\r')) {
      if (character === '\r' && text[i + 1] === '\n') continue
      row.push(field.trim()); field = ''
      if (character !== ',') { if (row.some(Boolean)) records.push(row); row = [] }
      continue
    }
    field += character
  }
  const headers = records.shift() || []
  return records.map((values) => Object.fromEntries(headers.map((header, index) => [header, values[index] || ''])))
}

async function databaseRequest(database, route, options = {}) {
  const response = await fetch(database.url + '/rest/v1/' + route, {
    method: options.method || 'GET',
    headers: supabaseServerHeaders(database.key, { 'Content-Type': 'application/json', Prefer: options.prefer || 'return=representation' }),
    body: options.body === undefined ? undefined : JSON.stringify(options.body),
  })
  const text = await response.text(); let data = null
  try { data = text ? JSON.parse(text) : null } catch (_) { data = null }
  if (!response.ok) throw new Error(String(data?.message || text || 'Supabase request failed').slice(0, 400))
  return data
}

export async function importOverrides(file, options = {}) {
  if (!file) throw new Error('--file is required')
  const url = process.env.SUPABASE_URL; const key = getSupabaseServerKey()
  if (!url) throw new Error('SUPABASE_URL and SUPABASE_SECRET_KEY are required')
  const rows = parseCsv(await fs.readFile(path.resolve(file), 'utf8'))
  const database = { url: url.replace(/\/$/, ''), key }
  let imported = 0; const errors = []
  for (const [index, row] of rows.entries()) {
    try {
      const overrideType = String(row.override_type || '')
      if (!ALLOWED_TYPES.has(overrideType)) throw new Error('unsupported override_type')
      const query = new URLSearchParams({ select: 'id', limit: '2' })
      if (row.source_building_id) query.set('source_building_id', 'eq.' + row.source_building_id)
      else if (row.normalized_address) query.set('normalized_address', 'eq.' + String(row.normalized_address).toUpperCase())
      else throw new Error('source_building_id or normalized_address is required')
      const properties = await databaseRequest(database, 'property_structures?' + query)
      if (!properties?.length) throw new Error('property was not found')
      if (properties.length > 1) throw new Error('property lookup was ambiguous')
      const rawValue = positive(row.raw_value); const assessmentRatio = positive(row.assessment_ratio)
      const normalizedValue = positive(row.normalized_value) ||
        (overrideType === 'official_assessed_value' && rawValue && assessmentRatio ? rawValue / assessmentRatio : rawValue)
      if (!rawValue || !normalizedValue) throw new Error('raw_value and a calculable normalized value are required')
      if (overrideType === 'official_assessed_value' && !assessmentRatio) throw new Error('official assessed values require a verified assessment_ratio')
      const propertyId = properties[0].id
      await databaseRequest(database, 'property_value_overrides?property_id=eq.' + encodeURIComponent(propertyId) + '&active=eq.true', { method: 'PATCH', body: { active: false, updated_at: new Date().toISOString() }, prefer: 'return=minimal' })
      const confidence = overrideType === 'official_assessed_value' ? 'Medium' : 'High'
      await databaseRequest(database, 'property_value_overrides', { method: 'POST', body: {
        property_id: propertyId, override_type: overrideType,
        raw_field_name: row.raw_field_name || 'imported_value', raw_value: rawValue,
        normalized_value: normalizedValue, low_estimate: positive(row.low_estimate), high_estimate: positive(row.high_estimate),
        confidence, source_name: row.source_name || options['source-name'] || 'authorized admin import',
        source_url: row.source_url || null, source_date: row.source_date || null, assessment_ratio: assessmentRatio,
        calculation_method: row.calculation_method || (assessmentRatio ? `assessed value divided by verified ratio ${assessmentRatio}` : 'authorized property-level value'),
        audit_metadata: { importFile: path.basename(file), row: index + 2 }, active: true,
        imported_by: options['imported-by'] || 'admin import',
      }, prefer: 'return=minimal' })
      imported++
    } catch (error) {
      errors.push({ row: index + 2, message: String(error.message || error).slice(0, 240) })
    }
  }
  return { rows: rows.length, imported, rejected: errors.length, errors }
}

if (process.argv[1] && import.meta.url === pathToFileURL(path.resolve(process.argv[1])).href) {
  const args = argsOf(process.argv.slice(2))
  importOverrides(args.file, args).then((result) => {
    console.log(JSON.stringify(result, null, 2)); if (result.rejected) process.exitCode = 2
  }).catch((error) => { console.error(String(error.message || error)); process.exitCode = 1 })
}
