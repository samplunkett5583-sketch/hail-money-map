const corsHeaders = {
  'Access-Control-Allow-Origin': '*',
  'Access-Control-Allow-Headers': 'authorization, x-client-info, apikey, content-type',
}

import {
  collectAllPropertyPages,
  coverageRequestKey,
  normalizeCampaignSwaths,
  swathBounds,
} from '../_shared/property-campaign-search-core.js'
import {
  DEVELOPMENT_PROVIDER_ID,
  PROPERTY_PROVIDER_MODES,
  PropertyProviderError,
  evaluatePropertyAddonEntitlement,
  jwtSubject,
  normalizeProviderMode,
  redactProviderSecrets,
  runCachedPropertySearch,
  selectPropertyProvider,
} from '../_shared/property-data-provider.js'
import { getSupabaseServerKey, supabaseServerHeaders } from '../_shared/supabase-server-auth.ts'

type JsonRecord = Record<string, unknown>

function json(body: unknown, status = 200) {
  return new Response(JSON.stringify(body), {
    status,
    headers: { ...corsHeaders, 'Content-Type': 'application/json' },
  })
}

function databaseHeaders(key: string) {
  return supabaseServerHeaders(key, { 'Content-Type': 'application/json' })
}

async function databaseRequest(url: string, key: string, path: string, init: RequestInit = {}) {
  const response = await fetch(url + path, {
    ...init,
    headers: { ...databaseHeaders(key), ...(init.headers || {}) },
  })
  const text = await response.text()
  let parsed: unknown = null
  try { parsed = text ? JSON.parse(text) : null } catch (_) { parsed = null }
  if (!response.ok) {
    const detail = (parsed as JsonRecord)?.message || (parsed as JsonRecord)?.error || text
    throw new PropertyProviderError(
      'Database operation failed with HTTP ' + response.status + ': ' + redactProviderSecrets(detail),
      'PROPERTY_DATABASE_ERROR',
      502,
    )
  }
  return parsed
}

async function rpc(url: string, key: string, name: string, body: JsonRecord) {
  return databaseRequest(url, key, '/rest/v1/rpc/' + name, {
    method: 'POST', body: JSON.stringify(body),
  })
}

async function findMembershipAndEntitlement(url: string, key: string, userExternalId: string) {
  const memberships = await databaseRequest(
    url,
    key,
    '/rest/v1/company_memberships?select=company_id,role,active&active=eq.true&user_external_id=eq.'
      + encodeURIComponent(userExternalId) + '&limit=1',
  )
  const membership = Array.isArray(memberships) ? memberships[0] || null : null
  if (!membership) {
    throw new PropertyProviderError('An active company membership is required', 'COMPANY_MEMBERSHIP_REQUIRED', 403)
  }
  const entitlements = await databaseRequest(
    url,
    key,
    '/rest/v1/company_property_addon_entitlements?select=*&company_id=eq.'
      + encodeURIComponent(String(membership.company_id)) + '&limit=1',
  )
  return {
    membership,
    entitlement: Array.isArray(entitlements) ? entitlements[0] || null : null,
  }
}

async function queueFreeCoverageImport(url: string, key: string, swaths: Array<{geometry: unknown}>) {
  const bounds = swathBounds(swaths)
  if (!bounds) return null
  const importRequestKey = coverageRequestKey(bounds)
  const response = await fetch(url + '/rest/v1/property_import_jobs?on_conflict=request_key', {
    method: 'POST',
    headers: { ...databaseHeaders(key), Prefer: 'resolution=ignore-duplicates,return=minimal' },
    body: JSON.stringify({
      request_key: importRequestKey,
      jurisdiction: 'open-data coverage requested by active hail swath',
      source_name: 'overture_acs_fhfa',
      source_release: 'latest',
      status: 'queued',
      progress_metadata: {
        bounds,
        reason: 'Campaign Mode requested missing free open-property coverage',
        paidProviderAllowed: false,
      },
    }),
  })
  if (!response.ok && response.status !== 409) {
    console.warn('[property-campaign-search] free coverage queue unavailable status=' + response.status)
  }
  const rows = await databaseRequest(
    url,
    key,
    '/rest/v1/property_import_jobs?select=status,batches_processed,records_received,records_inserted,records_rejected,progress_metadata,created_at,started_at,completed_at&request_key=eq.'
      + encodeURIComponent(importRequestKey) + '&limit=1',
  )
  return Array.isArray(rows) ? rows[0] || null : null
}

function createDevelopmentProvider(url: string, key: string) {
  return {
    id: DEVELOPMENT_PROVIDER_ID,
    mode: PROPERTY_PROVIDER_MODES.DEVELOPMENT,
    displayName: 'Hail Money open-data estimates',
    paid: false,
    persistentCache: true,
    completePagination: true,
    stableIdField: 'property_id',
    costMetadata: { currency: 'USD', perRequestMicros: 0, paidRequestsDisabled: true },
    allowsApplyTimeImport: true,
    coverageForSwaths(swaths: Array<{geometry: unknown}>) {
      return rpc(url, key, 'hail_money_property_coverage', { p_swaths: swaths })
    },
    async searchCachedBySwaths({ swaths, minimumValue, pageSize }: {
      swaths: Array<{geometry: unknown}>, minimumValue: number, pageSize: number
    }) {
      const collected = await collectAllPropertyPages(({ afterId, pageSize: currentPageSize }) => rpc(
        url,
        key,
        'search_hail_money_properties',
        { p_swaths: swaths, p_minimum: minimumValue, p_after_id: afterId, p_limit: currentPageSize },
      ), pageSize)
      return {
        pages: collected.pages,
        properties: collected.properties.map((row: JsonRecord) => ({
          id: row.property_id,
          stablePropertyId: row.property_id,
          sourceBuildingId: row.source_building_id,
          address: row.normalized_address || 'Residential property',
          normalizedAddress: row.normalized_address,
          latitude: row.latitude,
          longitude: row.longitude,
          geometry: null,
          residentialPropertyType: 'residential',
          marketValue: row.estimated_value,
          lowEstimate: row.low_estimate,
          highEstimate: row.high_estimate,
          source: 'Hail Money open-data estimate',
          effectiveDate: row.calculation_date,
          confidence: row.confidence,
          valuationMethod: row.valuation_method,
          datasetVersion: row.model_version,
          containingSwathId: row.containing_swath_id,
          sourceSummary: row.source_summary,
        })),
      }
    },
    queueFreeCoverageImport(swaths: Array<{geometry: unknown}>) {
      return queueFreeCoverageImport(url, key, swaths)
    },
  }
}

async function recordZeroCostUsage(
  url: string,
  key: string,
  companyId: string,
  providerId: string,
  recordsReturned: number,
  coverageComplete: boolean,
) {
  try {
    await databaseRequest(url, key, '/rest/v1/property_provider_usage_ledger', {
      method: 'POST',
      headers: { Prefer: 'return=minimal' },
      body: JSON.stringify({
        company_id: companyId,
        provider_id: providerId,
        operation: 'campaign_cached_search',
        cache_hit: true,
        provider_requests: 0,
        records_returned: recordsReturned,
        estimated_cost_micros: 0,
        billable: false,
        status: 'success',
        request_metadata: { coverageComplete, paidApiRequests: 0 },
      }),
    })
  } catch (_) {
    console.warn('[property-campaign-search] zero-cost usage ledger unavailable')
  }
}

export async function handleRequest(request: Request) {
  if (request.method === 'OPTIONS') return new Response('ok', { headers: corsHeaders })
  if (request.method !== 'POST') return json({ error: 'POST is required', operation: 'validate-method' }, 405)

  let body: JsonRecord
  try { body = await request.json() } catch (_) {
    return json({ error: 'Request body must be valid JSON', operation: 'parse-request' }, 400)
  }
  const swaths = normalizeCampaignSwaths(body.swaths)
  const minimum = Math.max(0, Number(body.minimum_value) || 0)
  const requestedMode = normalizeProviderMode(body.provider_mode)
  if (!swaths.length) return json({ error: 'No valid active hail-swath geometry was supplied', operation: 'validate-swaths' }, 400)

  const userExternalId = jwtSubject(request.headers.get('authorization'))
  if (!userExternalId) return json({ error: 'An authenticated company user is required', operation: 'authenticate-company-user' }, 401)

  const supabaseUrl = Deno.env.get('SUPABASE_URL') || ''
  const serviceKey = getSupabaseServerKey()
  if (!supabaseUrl || !serviceKey) {
    return json({ error: 'Open property database connection is not configured', operation: 'load-database-secret' }, 500)
  }

  try {
    const { membership, entitlement } = await findMembershipAndEntitlement(supabaseUrl, serviceKey, userExternalId)
    const authorized = evaluatePropertyAddonEntitlement({ mode: requestedMode, membership, entitlement })
    const provider = selectPropertyProvider({
      mode: requestedMode,
      environment: {
        PROPERTY_PRODUCTION_PROVIDER_ENABLED: Deno.env.get('PROPERTY_PRODUCTION_PROVIDER_ENABLED') || 'false',
        PROPERTY_PRODUCTION_PROVIDER_ID: Deno.env.get('PROPERTY_PRODUCTION_PROVIDER_ID') || '',
      },
      developmentProvider: createDevelopmentProvider(supabaseUrl, serviceKey),
      productionProviders: {}, // No production provider is approved or contracted.
    })
    const operationAuthorization = await rpc(
      supabaseUrl,
      serviceKey,
      'authorize_property_provider_operation',
      {
        target_company_id: authorized.companyId,
        target_provider_id: provider.id,
        requested_provider_requests: 0,
        requested_records: 0,
        requested_cost_micros: 0,
      },
    ) as JsonRecord
    if (operationAuthorization?.allowed !== true) {
      throw new PropertyProviderError(
        'Property provider operation was blocked by the company usage policy',
        'PROPERTY_USAGE_LIMIT_BLOCKED',
        403,
      )
    }
    const result = await runCachedPropertySearch({
      provider,
      swaths,
      minimumValue: minimum,
      pageSize: Number(body.page_size) || 750,
      entitlement: authorized,
    })
    const complete = (result.coverage as JsonRecord)?.complete === true
    await recordZeroCostUsage(
      supabaseUrl,
      serviceKey,
      authorized.companyId,
      provider.id,
      result.properties.length,
      complete,
    )
    const properties = result.properties.map((record) => ({
      id: record.id,
      stablePropertyId: record.stablePropertyId,
      address: record.address,
      normalizedAddress: record.normalizedAddress,
      latitude: record.latitude,
      longitude: record.longitude,
      propertyGeometry: record.geometry,
      residentialPropertyType: record.residentialPropertyType,
      hailMoneyEstimatedValue: record.marketValue,
      estimatedMarketValue: record.marketValue,
      lowEstimate: record.lowEstimate,
      highEstimate: record.highEstimate,
      source: record.source,
      effectiveDate: record.effectiveDate,
      confidence: record.confidence,
      modelVersion: record.datasetVersion,
      datasetVersion: record.datasetVersion,
      valuationMethod: record.valuationMethod,
      calculationDate: record.effectiveDate,
      containingSwathId: record.containingSwathId,
      sourceSummary: record.sourceSummary,
    }))

    return json({
      success: true,
      mode: provider.mode,
      complete,
      partialReason: complete ? null : 'Open property-data coverage is still being prepared for part of the active swaths.',
      developmentNotice: provider.mode === PROPERTY_PROVIDER_MODES.DEVELOPMENT
        ? 'Development property data — open-data estimates'
        : null,
      productionAvailable: false,
      provider: {
        id: provider.id,
        displayName: provider.displayName,
        datasetVersion: properties[0]?.datasetVersion || null,
        paid: false,
        cached: true,
      },
      source: 'Cached Overture Maps, U.S. Census ACS, FHFA HPI, and authorized official overrides',
      minimumValue: minimum,
      properties,
      coverage: result.coverage,
      importJob: result.importJob,
      entitlement: {
        companyId: authorized.companyId,
        providerAccessLevel: authorized.providerAccessLevel,
        billingEnabled: authorized.billingEnabled,
      },
      diagnostics: {
        databasePages: result.pages,
        uniqueQualifyingProperties: properties.length,
        cacheHit: true,
        providerRequests: 0,
        paidApiRequests: 0,
        estimatedCostMicros: 0,
      },
    })
  } catch (error) {
    const providerError = error instanceof PropertyProviderError ? error : null
    const message = redactProviderSecrets(error instanceof Error ? error.message : error)
    console.error('[property-campaign-search] status=failed code=' + (providerError?.code || 'UNEXPECTED_ERROR') + ' message=' + message)
    return json({
      error: message,
      code: providerError?.code || 'UNEXPECTED_ERROR',
      operation: requestedMode === PROPERTY_PROVIDER_MODES.PRODUCTION
        ? 'production-provider-authorization'
        : 'development-open-property-search',
      productionAvailable: false,
      diagnostics: { paidApiRequests: 0, providerRequests: 0, estimatedCostMicros: 0 },
    }, providerError?.status || 502)
  }
}

if (typeof Deno !== 'undefined' && typeof Deno.serve === 'function') Deno.serve(handleRequest)
