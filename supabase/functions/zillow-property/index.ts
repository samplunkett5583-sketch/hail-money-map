import { getSupabaseServerKey, supabaseServerHeaders } from '../_shared/supabase-server-auth.ts'

const corsHeaders = {
  'Access-Control-Allow-Origin': '*',
  'Access-Control-Allow-Headers': 'authorization, x-client-info, apikey, content-type',
}

const RENTCAST_ROLLING_LIMIT = 45
const RENTCAST_MAX_RADIUS_MILES = 5
const DEFAULT_PAGE_SIZE = 500
const MADISON_COUNTY_ASSESSMENT_RATIO = 0.3333

type JsonRecord = Record<string, unknown>
type Position = [number, number]
type PolygonCoordinates = Position[][]
type NormalizedSwath = {
  id: string
  date: string | null
  source: string | null
  sourceProduct: string | null
  sourceRowIds: unknown[]
  layer: string | null
  polygons: PolygonCoordinates[]
}
type QueryArea = { latitude: number; longitude: number; radius: number }
type CoordinateBounds = { minLat: number; maxLat: number; minLng: number; maxLng: number }

class UsageLimitError extends Error {
  used: number
  limit: number
  constructor(used: number, limit: number) {
    super('RentCast free-request safety limit reached (' + used + '/' + limit + '). No additional request was sent and no overage charge was created.')
    this.name = 'UsageLimitError'
    this.used = used
    this.limit = limit
  }
}

function pickFirst(...values: unknown[]) {
  for (const value of values) {
    if (value !== undefined && value !== null && value !== '') return value
  }
  return null
}

function finitePositive(value: unknown) {
  const numberValue = Number(value)
  return Number.isFinite(numberValue) && numberValue > 0 ? numberValue : null
}

function safeErrorMessage(value: unknown) {
  return String(value || 'Unknown property-data error')
    .replace(/((?:api[-_ ]?key|authorization|bearer|token)\s*[:=]?\s*)[^\s,;]+/gi, '$1[redacted]')
    .slice(0, 420)
}

function jsonResponse(body: unknown, status = 200) {
  return new Response(JSON.stringify(body), {
    status,
    headers: { ...corsHeaders, 'Content-Type': 'application/json' },
  })
}

function parseJsonOrThrow(text: string, label: string) {
  if (!text) return null
  try {
    return JSON.parse(text)
  } catch (error) {
    throw new Error('Failed to parse ' + label + ' JSON: ' + safeErrorMessage(error instanceof Error ? error.message : error))
  }
}

async function callUsageGuard(functionName: string, body: JsonRecord) {
  const supabaseUrl = Deno.env.get('SUPABASE_URL')
  const serverSecretKey = getSupabaseServerKey()
  if (!supabaseUrl || !serverSecretKey) throw new Error('RentCast safety guard is not configured')
  const response = await fetch(supabaseUrl + '/rest/v1/rpc/' + functionName, {
    method: 'POST',
    headers: supabaseServerHeaders(serverSecretKey, { 'Content-Type': 'application/json' }),
    body: JSON.stringify(body),
  })
  if (!response.ok) throw new Error('RentCast safety guard failed with status ' + response.status)
  return response
}

async function reserveRentCastRequest() {
  const response = await callUsageGuard('reserve_external_api_request', {
    p_provider: 'rentcast', p_limit: RENTCAST_ROLLING_LIMIT, p_window_days: 31,
  })
  const reservation = await response.json()
  if (!reservation?.allowed || !reservation?.token) {
    throw new UsageLimitError(Number(reservation?.used) || RENTCAST_ROLLING_LIMIT, Number(reservation?.limit) || RENTCAST_ROLLING_LIMIT)
  }
  return reservation
}

async function finalizeRentCastRequest(token: string, success: boolean) {
  try {
    await callUsageGuard('finalize_external_api_request', { p_token: token, p_success: success })
  } catch (error) {
    console.error('[zillow-property] operation=usage-guard-finalize message=' + safeErrorMessage(error instanceof Error ? error.message : error))
  }
}

async function availableRentCastRequests() {
  const reservation = await reserveRentCastRequest()
  const available = Math.max(0, Number(reservation.remaining) + 1)
  await finalizeRentCastRequest(reservation.token, false)
  return available
}

async function guardedRentCastFetch(url: URL | string, init: RequestInit) {
  const reservation = await reserveRentCastRequest()
  try {
    const response = await fetch(url, init)
    await finalizeRentCastRequest(reservation.token, response.ok)
    return { response, remaining: Number(reservation.remaining) }
  } catch (error) {
    await finalizeRentCastRequest(reservation.token, false)
    throw error
  }
}

function geometryPolygons(value: unknown): PolygonCoordinates[] {
  if (!value || typeof value !== 'object') return []
  const geometry = value as JsonRecord
  if (geometry.type === 'Feature') return geometryPolygons(geometry.geometry)
  if (geometry.type === 'FeatureCollection') {
    return (Array.isArray(geometry.features) ? geometry.features : []).flatMap(geometryPolygons)
  }
  if (geometry.type === 'Polygon') return normalizePolygonCoordinates(geometry.coordinates)
  if (geometry.type === 'MultiPolygon' && Array.isArray(geometry.coordinates)) {
    return geometry.coordinates.flatMap(normalizePolygonCoordinates)
  }
  return []
}

function normalizePolygonCoordinates(value: unknown): PolygonCoordinates[] {
  if (!Array.isArray(value) || !value.length) return []
  const rings = value.map((ring) => {
    if (!Array.isArray(ring)) return []
    return ring.map((coordinate) => {
      if (!Array.isArray(coordinate) || coordinate.length < 2) return null
      const longitude = Number(coordinate[0])
      const latitude = Number(coordinate[1])
      if (!Number.isFinite(longitude) || !Number.isFinite(latitude) || Math.abs(longitude) > 180 || Math.abs(latitude) > 90) return null
      return [longitude, latitude] as Position
    }).filter(Boolean) as Position[]
  }).filter((ring) => ring.length >= 3)
  return rings.length ? [rings] : []
}

export function normalizeSwaths(rawSwaths: unknown): NormalizedSwath[] {
  if (!Array.isArray(rawSwaths)) return []
  return rawSwaths.map((raw, index) => {
    const record = raw && typeof raw === 'object' ? raw as JsonRecord : {}
    return {
      id: String(record.id || 'active-swath-' + (index + 1)),
      date: record.date ? String(record.date) : null,
      source: record.source ? String(record.source) : null,
      sourceProduct: record.sourceProduct ? String(record.sourceProduct) : null,
      sourceRowIds: Array.isArray(record.sourceRowIds) ? record.sourceRowIds : [],
      layer: record.layer ? String(record.layer) : null,
      polygons: geometryPolygons(record.geometry),
    }
  }).filter((swath) => swath.polygons.length > 0)
}

function pointInRing(longitude: number, latitude: number, ring: Position[]) {
  let inside = false
  for (let index = 0, previous = ring.length - 1; index < ring.length; previous = index++) {
    const [x, y] = ring[index]
    const [previousX, previousY] = ring[previous]
    const intersects = ((y > latitude) !== (previousY > latitude)) &&
      (longitude < (previousX - x) * (latitude - y) / ((previousY - y) || 1e-12) + x)
    if (intersects) inside = !inside
  }
  return inside
}

export function pointInPolygon(longitude: number, latitude: number, polygon: PolygonCoordinates) {
  if (!polygon.length || !pointInRing(longitude, latitude, polygon[0])) return false
  for (let holeIndex = 1; holeIndex < polygon.length; holeIndex++) {
    if (pointInRing(longitude, latitude, polygon[holeIndex])) return false
  }
  return true
}

function containingSwath(swaths: NormalizedSwath[], longitude: number, latitude: number) {
  for (const swath of swaths) {
    if (swath.polygons.some((polygon) => pointInPolygon(longitude, latitude, polygon))) return swath
  }
  return null
}

function polygonBounds(polygon: PolygonCoordinates): CoordinateBounds {
  const bounds = { minLat: Infinity, maxLat: -Infinity, minLng: Infinity, maxLng: -Infinity }
  for (const ring of polygon) {
    for (const [longitude, latitude] of ring) {
      bounds.minLat = Math.min(bounds.minLat, latitude)
      bounds.maxLat = Math.max(bounds.maxLat, latitude)
      bounds.minLng = Math.min(bounds.minLng, longitude)
      bounds.maxLng = Math.max(bounds.maxLng, longitude)
    }
  }
  return bounds
}

function pointInBounds([longitude, latitude]: Position, bounds: CoordinateBounds) {
  return longitude >= bounds.minLng && longitude <= bounds.maxLng &&
    latitude >= bounds.minLat && latitude <= bounds.maxLat
}

function segmentIntersectsSegment(a: Position, b: Position, c: Position, d: Position) {
  function orientation(p: Position, q: Position, r: Position) {
    const value = (q[1] - p[1]) * (r[0] - q[0]) - (q[0] - p[0]) * (r[1] - q[1])
    return Math.abs(value) < 1e-12 ? 0 : (value > 0 ? 1 : 2)
  }
  function onSegment(p: Position, q: Position, r: Position) {
    return q[0] <= Math.max(p[0], r[0]) + 1e-12 && q[0] >= Math.min(p[0], r[0]) - 1e-12 &&
      q[1] <= Math.max(p[1], r[1]) + 1e-12 && q[1] >= Math.min(p[1], r[1]) - 1e-12
  }
  const o1 = orientation(a, b, c), o2 = orientation(a, b, d)
  const o3 = orientation(c, d, a), o4 = orientation(c, d, b)
  if (o1 !== o2 && o3 !== o4) return true
  return (o1 === 0 && onSegment(a, c, b)) || (o2 === 0 && onSegment(a, d, b)) ||
    (o3 === 0 && onSegment(c, a, d)) || (o4 === 0 && onSegment(c, b, d))
}

function polygonIntersectsBounds(polygon: PolygonCoordinates, bounds: CoordinateBounds) {
  const corners: Position[] = [
    [bounds.minLng, bounds.minLat], [bounds.maxLng, bounds.minLat],
    [bounds.maxLng, bounds.maxLat], [bounds.minLng, bounds.maxLat],
  ]
  if (corners.some(([longitude, latitude]) => pointInPolygon(longitude, latitude, polygon))) return true
  const edges: Array<[Position, Position]> = [
    [corners[0], corners[1]], [corners[1], corners[2]],
    [corners[2], corners[3]], [corners[3], corners[0]],
  ]
  for (const ring of polygon) {
    if (ring.some((point) => pointInBounds(point, bounds))) return true
    for (let index = 0, previous = ring.length - 1; index < ring.length; previous = index++) {
      if (edges.some(([start, end]) => segmentIntersectsSegment(ring[previous], ring[index], start, end))) return true
    }
  }
  return false
}

function polygonAreaSqMiles(polygon: PolygonCoordinates) {
  const ring = polygon[0] || []
  if (ring.length < 3) return 0
  const meanLat = ring.reduce((sum, coordinate) => sum + coordinate[1], 0) / ring.length
  const milesPerLng = 69.172 * Math.cos(meanLat * Math.PI / 180)
  let twiceArea = 0
  for (let index = 0, previous = ring.length - 1; index < ring.length; previous = index++) {
    const x1 = ring[previous][0] * milesPerLng
    const y1 = ring[previous][1] * 69.0
    const x2 = ring[index][0] * milesPerLng
    const y2 = ring[index][1] * 69.0
    twiceArea += x1 * y2 - x2 * y1
  }
  return Math.abs(twiceArea) / 2
}

export function queryAreasForSwaths(swaths: NormalizedSwath[]) {
  const areas: QueryArea[] = []
  const diagonalStep = RENTCAST_MAX_RADIUS_MILES * Math.SQRT2 * 0.92
  for (const swath of swaths) {
    for (const polygon of swath.polygons) {
      const bounds = polygonBounds(polygon)
      const centerLat = (bounds.minLat + bounds.maxLat) / 2
      const milesPerLng = Math.max(20, 69.172 * Math.cos(centerLat * Math.PI / 180))
      const heightMiles = Math.max(0.01, (bounds.maxLat - bounds.minLat) * 69.0)
      const widthMiles = Math.max(0.01, (bounds.maxLng - bounds.minLng) * milesPerLng)
      const latitudeCells = Math.max(1, Math.ceil(heightMiles / diagonalStep))
      const longitudeCells = Math.max(1, Math.ceil(widthMiles / diagonalStep))
      for (let latitudeIndex = 0; latitudeIndex < latitudeCells; latitudeIndex++) {
        for (let longitudeIndex = 0; longitudeIndex < longitudeCells; longitudeIndex++) {
          const latitude = bounds.minLat + (latitudeIndex + 0.5) * (bounds.maxLat - bounds.minLat) / latitudeCells
          const longitude = bounds.minLng + (longitudeIndex + 0.5) * (bounds.maxLng - bounds.minLng) / longitudeCells
          const cellHeight = heightMiles / latitudeCells
          const cellWidth = widthMiles / longitudeCells
          const cellBounds: CoordinateBounds = {
            minLat: bounds.minLat + latitudeIndex * (bounds.maxLat - bounds.minLat) / latitudeCells,
            maxLat: bounds.minLat + (latitudeIndex + 1) * (bounds.maxLat - bounds.minLat) / latitudeCells,
            minLng: bounds.minLng + longitudeIndex * (bounds.maxLng - bounds.minLng) / longitudeCells,
            maxLng: bounds.minLng + (longitudeIndex + 1) * (bounds.maxLng - bounds.minLng) / longitudeCells,
          }
          if (!polygonIntersectsBounds(polygon, cellBounds)) continue
          const radius = Math.min(RENTCAST_MAX_RADIUS_MILES, Math.max(0.05, Math.hypot(cellHeight / 2, cellWidth / 2) + 0.15))
          const duplicate = areas.some((area) => {
            const y = (area.latitude - latitude) * 69.0
            const x = (area.longitude - longitude) * milesPerLng
            return Math.hypot(x, y) < Math.min(area.radius, radius) * 0.30
          })
          if (!duplicate) areas.push({ latitude, longitude, radius })
        }
      }
    }
  }
  return areas
}

export function normalizeAddress(value: unknown) {
  return String(value || '').toUpperCase()
    .replace(/\bDRIVE\b/g, 'DR').replace(/\bROAD\b/g, 'RD').replace(/\bSTREET\b/g, 'ST')
    .replace(/\bSAINT\b/g, 'ST').replace(/\bUSA\b/g, '')
    .replace(/\bNORTH\b/g, 'N').replace(/\bSOUTH\b/g, 'S').replace(/\bEAST\b/g, 'E').replace(/\bWEST\b/g, 'W')
    .replace(/[^A-Z0-9]+/g, ' ').trim().replace(/\s+/g, ' ')
}

function stablePropertyKey(record: JsonRecord) {
  const stableId = pickFirst(record.id, record.assessorID, record.assessorId, record.parcelNumber, record.parcel_number)
  if (stableId) return 'id:' + String(stableId).trim().toLowerCase()
  const address = normalizeAddress(pickFirst(record.formattedAddress, record.addressLine1))
  return address ? 'address:' + address : 'coordinate:' + Number(record.latitude).toFixed(6) + ',' + Number(record.longitude).toFixed(6)
}

export function isResidentialProperty(record: JsonRecord) {
  const propertyType = String(pickFirst(record.propertyType, record.property_type) || '').toLowerCase()
  return /single.family|condo|townhouse|manufactured|mobile|multi.family|apartment|duplex|triplex|quadruplex|residential/.test(propertyType)
}

function latestAssessment(record: JsonRecord) {
  const assessments = record.taxAssessments
  if (!assessments || typeof assessments !== 'object') return null
  const entries = Object.entries(assessments as JsonRecord).sort(([left], [right]) => Number(right) - Number(left))
  for (const [year, raw] of entries) {
    if (!raw || typeof raw !== 'object') continue
    const value = finitePositive((raw as JsonRecord).value)
    if (value) return { value, year, rawField: 'taxAssessments.' + year + '.value' }
  }
  return null
}

export function normalizeMarketValue(record: JsonRecord) {
  const directFields: Array<[string, unknown]> = [
    ['estimatedValue', record.estimatedValue], ['estimated_value', record.estimated_value],
    ['marketValue', record.marketValue], ['market_value', record.market_value],
    ['avmValue', record.avmValue], ['avm_value', record.avm_value],
    ['currentMarketValue', record.currentMarketValue], ['current_market_value', record.current_market_value],
  ]
  for (const [rawField, rawValue] of directFields) {
    const value = finitePositive(rawValue)
    if (value) return {
      estimatedMarketValue: value, rawValue: value, rawField, valueType: 'market_value',
      valueSource: 'RentCast property record', calculationMethod: 'direct market-value field', valueDate: null,
    }
  }
  const assessment = latestAssessment(record)
  const state = String(pickFirst(record.state, record.stateAbbreviation, record.state_code) || '').trim().toUpperCase()
  const county = String(pickFirst(record.county, record.countyName, record.county_name) || '').trim().toUpperCase()
  if (assessment && state === 'IL' && county.includes('MADISON')) {
    return {
      estimatedMarketValue: assessment.value / MADISON_COUNTY_ASSESSMENT_RATIO,
      rawValue: assessment.value,
      rawField: assessment.rawField,
      valueType: 'assessment_derived_market_estimate',
      valueSource: 'RentCast property record / Illinois statutory assessment ratio',
      calculationMethod: 'assessment / ' + MADISON_COUNTY_ASSESSMENT_RATIO.toFixed(6),
      valueDate: assessment.year,
    }
  }
  return {
    estimatedMarketValue: null, rawValue: assessment?.value || null,
    rawField: assessment?.rawField || null,
    valueType: assessment ? 'assessment_not_normalized_outside_supported_jurisdiction' : 'unavailable',
    valueSource: 'RentCast property record', calculationMethod: null, valueDate: assessment?.year || null,
  }
}

export function propertyDiagnosticRow(record: JsonRecord, swaths: NormalizedSwath[], minimumValue: number) {
  const latitude = Number(record.latitude)
  const longitude = Number(record.longitude)
  const address = String(pickFirst(record.formattedAddress, record.addressLine1) || '')
  const id = pickFirst(record.id, record.assessorID, record.assessorId, record.parcelNumber, record.parcel_number)
  const residential = isResidentialProperty(record)
  const swath = Number.isFinite(latitude) && Number.isFinite(longitude) ? containingSwath(swaths, longitude, latitude) : null
  const value = normalizeMarketValue(record)
  let exclusionReason: string | null = null
  if (!residential) exclusionReason = 'non-residential property type'
  else if (!Number.isFinite(latitude) || !Number.isFinite(longitude)) exclusionReason = 'invalid or unavailable coordinates'
  else if (!swath) exclusionReason = 'outside every active legitimate hail swath'
  else if (value.estimatedMarketValue == null || !Number.isFinite(Number(value.estimatedMarketValue))) exclusionReason = 'estimated current market value unavailable'
  else if (Number(value.estimatedMarketValue) < minimumValue) exclusionReason = 'estimated current market value below minimum'
  return {
    id: id || null,
    address,
    normalizedAddress: normalizeAddress(address),
    propertyType: pickFirst(record.propertyType, record.property_type),
    latitude: Number.isFinite(latitude) ? latitude : null,
    longitude: Number.isFinite(longitude) ? longitude : null,
    containingSwathId: swath?.id || null,
    containingSwathSource: swath?.source || null,
    included: exclusionReason === null,
    exclusionReason,
    ...value,
  }
}

function diagnosticAddressResults(addresses: unknown, diagnosticRows: JsonRecord[], complete: boolean) {
  if (!Array.isArray(addresses)) return []
  return addresses.map((requestedAddress) => {
    const normalizedRequested = normalizeAddress(requestedAddress)
    const exact = diagnosticRows.find((row) => row.normalizedAddress === normalizedRequested)
    return exact ? { requestedAddress, providerReturns: true, ...exact } : {
      requestedAddress,
      providerReturns: false,
      normalizedAddress: null,
      rawValue: null,
      rawField: null,
      valueType: null,
      estimatedMarketValue: null,
      latitude: null,
      longitude: null,
      containingSwathId: null,
      included: false,
      exclusionReason: complete ? 'provider did not return this address in the active-swath search areas' : 'search stopped before provider coverage was complete',
    }
  })
}

async function runSwathSearch(requestBody: JsonRecord, rentcastApiKey: string) {
  const swaths = normalizeSwaths(requestBody.swaths)
  const minimumValue = Math.max(0, Number(requestBody.minimum_value) || 0)
  const pageSize = Math.min(DEFAULT_PAGE_SIZE, Math.max(1, Number(requestBody.page_size) || DEFAULT_PAGE_SIZE))
  const debug = requestBody.debug === true
  if (!swaths.length) return jsonResponse({ error: 'No valid active hail-swath geometry was supplied', operation: 'validate-active-swaths' }, 400)
  const queryAreas = queryAreasForSwaths(swaths)
  if (!queryAreas.length) return jsonResponse({ error: 'Active hail-swath geometry could not be converted into provider search areas', operation: 'build-query-areas' }, 400)

  const seenCandidates = new Set<string>()
  const diagnosticRows: JsonRecord[] = []
  const properties: JsonRecord[] = []
  const sourceSummary = swaths.map((swath) => ({
    id: swath.id, date: swath.date, source: swath.source,
    sourceProduct: swath.sourceProduct, sourceRowIds: swath.sourceRowIds, layer: swath.layer,
  }))
  const counts = {
    totalActiveLegitimateSwaths: swaths.length,
    totalSwathAreaSearchedSqMi: Number(swaths.reduce((sum, swath) => sum + swath.polygons.reduce((polygonSum, polygon) => polygonSum + polygonAreaSqMiles(polygon), 0), 0).toFixed(2)),
    queryAreasTotal: queryAreas.length,
    queryAreasCompleted: 0,
    pagesRequested: 0,
    candidateRecordsReceived: 0,
    uniqueCandidateRecords: 0,
    residentialPropertiesExamined: 0,
    rejectedOutsideAllSwaths: 0,
    rejectedNonResidential: 0,
    rejectedInvalidCoordinates: 0,
    rejectedValueUnavailable: 0,
    rejectedBelowMinimum: 0,
    duplicateRecordsRemoved: 0,
    uniqueQualifyingProperties: 0,
  }
  let complete = true
  let partialReason: string | null = null
  let safetyRemaining: number | null = null

  safetyRemaining = await availableRentCastRequests()
  if (requestBody.preflight_only === true) {
    complete = false
    partialReason = 'Preflight only: no RentCast property request was sent.'
    return jsonResponse({
      success: true,
      complete,
      partialReason,
      source: 'RentCast property records',
      minimumValue,
      properties,
      swathSources: sourceSummary,
      diagnostics: {
        ...counts,
        providerSafetyRemaining: safetyRemaining,
        preflightOnly: true,
        minimumProviderRequestsBeforePagination: queryAreas.length,
        capacitySufficientBeforePagination: queryAreas.length <= safetyRemaining,
      },
      diagnosticTable: debug ? diagnosticRows : [],
      diagnosticAddresses: diagnosticAddressResults(requestBody.diagnostic_addresses, diagnosticRows, complete),
    })
  }
  if (queryAreas.length > safetyRemaining) {
    complete = false
    partialReason = 'The complete swath search requires at least ' + queryAreas.length +
      ' RentCast requests before pagination, but only ' + safetyRemaining +
      ' protected free-plan requests remain. No RentCast property request was sent.'
    return jsonResponse({
      success: true,
      complete,
      partialReason,
      source: 'RentCast property records',
      minimumValue,
      properties,
      swathSources: sourceSummary,
      diagnostics: { ...counts, providerSafetyRemaining: safetyRemaining, preflightBlocked: true },
      diagnosticTable: debug ? diagnosticRows : [],
      diagnosticAddresses: diagnosticAddressResults(requestBody.diagnostic_addresses, diagnosticRows, complete),
    })
  }

  areaLoop: for (let areaIndex = 0; areaIndex < queryAreas.length; areaIndex++) {
    const area = queryAreas[areaIndex]
    let offset = 0
    let areaTotalCount: number | null = null
    let areaRecordsReceived = 0
    let pageNumber = 0
    while (true) {
      pageNumber++
      const areaUrl = new URL('https://api.rentcast.io/v1/properties')
      areaUrl.search = new URLSearchParams({
        latitude: String(area.latitude), longitude: String(area.longitude), radius: String(area.radius),
        limit: String(pageSize), offset: String(offset), includeTotalCount: offset === 0 ? 'true' : 'false',
      }).toString()
      let guarded
      try {
        guarded = await guardedRentCastFetch(areaUrl, { headers: { 'X-Api-Key': rentcastApiKey, Accept: 'application/json' } })
      } catch (error) {
        if (error instanceof UsageLimitError) {
          complete = false
          partialReason = error.message
          break areaLoop
        }
        throw error
      }
      counts.pagesRequested++
      safetyRemaining = Number.isFinite(guarded.remaining) ? guarded.remaining : safetyRemaining
      const response = guarded.response
      if (!response.ok) {
        const upstreamMessage = safeErrorMessage(await response.text())
        if (response.status === 402 || response.status === 429) {
          complete = false
          partialReason = 'RentCast stopped the search at area ' + (areaIndex + 1) + ', page ' + pageNumber + ' with HTTP ' + response.status + ': ' + upstreamMessage
          break areaLoop
        }
        return jsonResponse({
          error: 'RentCast property page failed with HTTP ' + response.status + ': ' + upstreamMessage,
          operation: 'rentcast-properties-page', batch: areaIndex + 1, page: pageNumber,
        }, 502)
      }
      if (offset === 0) areaTotalCount = finitePositive(response.headers.get('x-total-count'))
      const page = parseJsonOrThrow(await response.text(), 'RentCast area ' + (areaIndex + 1) + ' page ' + pageNumber)
      if (!Array.isArray(page)) {
        return jsonResponse({ error: 'RentCast returned a non-array property page', operation: 'parse-rentcast-page', batch: areaIndex + 1, page: pageNumber }, 502)
      }
      counts.candidateRecordsReceived += page.length
      areaRecordsReceived += page.length
      for (const raw of page) {
        const record = raw && typeof raw === 'object' ? raw as JsonRecord : {}
        const key = stablePropertyKey(record)
        if (seenCandidates.has(key)) { counts.duplicateRecordsRemoved++; continue }
        seenCandidates.add(key)
        counts.uniqueCandidateRecords++
        const diagnostic = propertyDiagnosticRow(record, swaths, minimumValue)
        diagnosticRows.push(diagnostic)
        if (!isResidentialProperty(record)) counts.rejectedNonResidential++
        else {
          counts.residentialPropertiesExamined++
          if (diagnostic.exclusionReason === 'invalid or unavailable coordinates') counts.rejectedInvalidCoordinates++
          else if (diagnostic.exclusionReason === 'outside every active legitimate hail swath') counts.rejectedOutsideAllSwaths++
          else if (diagnostic.exclusionReason === 'estimated current market value unavailable') counts.rejectedValueUnavailable++
          else if (diagnostic.exclusionReason === 'estimated current market value below minimum') counts.rejectedBelowMinimum++
        }
        if (diagnostic.included) properties.push({
          id: diagnostic.id,
          address: diagnostic.address,
          normalizedAddress: diagnostic.normalizedAddress,
          latitude: diagnostic.latitude,
          longitude: diagnostic.longitude,
          propertyType: diagnostic.propertyType,
          estimatedMarketValue: diagnostic.estimatedMarketValue,
          qualifyingValue: diagnostic.estimatedMarketValue,
          rawValue: diagnostic.rawValue,
          rawField: diagnostic.rawField,
          valueType: diagnostic.valueType,
          valueSource: diagnostic.valueSource,
          calculationMethod: diagnostic.calculationMethod,
          valueDate: diagnostic.valueDate,
          containingSwathId: diagnostic.containingSwathId,
          containingSwathSource: diagnostic.containingSwathSource,
        })
      }
      if (page.length < pageSize || (areaTotalCount !== null && areaRecordsReceived >= areaTotalCount)) break
      offset += page.length
    }
    counts.queryAreasCompleted++
  }

  counts.uniqueQualifyingProperties = properties.length
  if (counts.rejectedValueUnavailable > 0) {
    complete = false
    const valueLimitation = counts.rejectedValueUnavailable +
      ' residential candidate' + (counts.rejectedValueUnavailable === 1 ? '' : 's') +
      ' had no current market-value field and no supported assessment-to-market conversion. ' +
      'Per-property AVM requests were not made because they would exceed the protected free-request allowance.'
    partialReason = partialReason ? partialReason + ' ' + valueLimitation : valueLimitation
  }
  console.info('[zillow-property] operation=swath-search status=' + (complete ? 'complete' : 'partial') +
    ' pages=' + counts.pagesRequested + ' candidates=' + counts.candidateRecordsReceived +
    ' qualifying=' + counts.uniqueQualifyingProperties + ' queryAreas=' + counts.queryAreasCompleted + '/' + counts.queryAreasTotal)
  return jsonResponse({
    success: true,
    complete,
    partialReason,
    source: 'RentCast property records',
    minimumValue,
    properties,
    swathSources: sourceSummary,
    diagnostics: { ...counts, providerSafetyRemaining: safetyRemaining },
    diagnosticTable: debug ? diagnosticRows : [],
    diagnosticAddresses: diagnosticAddressResults(requestBody.diagnostic_addresses, diagnosticRows, complete),
  })
}

export async function handleRequest(request: Request) {
  if (request.method === 'OPTIONS') return new Response('ok', { headers: corsHeaders })
  let requestBody: JsonRecord = {}
  let propertyAddress = ''
  try {
    if (request.method === 'GET') {
      const url = new URL(request.url)
      requestBody.mode = url.searchParams.get('mode') || 'detail'
      propertyAddress = url.searchParams.get('address') || ''
    } else {
      requestBody = await request.json()
      propertyAddress = String(requestBody.property_address || '')
    }
  } catch (_) {
    return jsonResponse({ error: 'Request body must be valid JSON', operation: 'parse-request' }, 400)
  }

  if (requestBody.mode === 'swaths') {
    return jsonResponse({
      error: 'Bulk and automatic RentCast property search is disabled. Campaign Mode uses cached open property data.',
      operation: 'paid-bulk-disabled',
      paidApiRequests: 0,
    }, 410)
  }
  if (requestBody.mode && requestBody.mode !== 'detail') {
    return jsonResponse({ error: 'Invalid mode. Use detail or swaths', operation: 'validate-mode' }, 400)
  }

  const detailEnabled = String(Deno.env.get('RENTCAST_SINGLE_PROPERTY_VERIFICATION_ENABLED') || '').toLowerCase() === 'true'
  const expectedAdminSecret = Deno.env.get('PROPERTY_VERIFICATION_ADMIN_SECRET') || ''
  const suppliedAdminSecret = request.headers.get('x-hail-money-admin-secret') || ''
  if (!detailEnabled || !expectedAdminSecret || suppliedAdminSecret !== expectedAdminSecret) {
    return jsonResponse({
      error: 'Single-property paid verification is disabled or requires server-side administrator authorization.',
      operation: 'paid-detail-disabled',
      paidApiRequests: 0,
    }, 403)
  }
  const rentcastApiKey = Deno.env.get('RENTCAST_API_KEY')
  if (!rentcastApiKey) return jsonResponse({ error: 'RENTCAST_API_KEY is not configured', operation: 'load-secret' }, 500)

  const normalizedPropertyAddress = String(propertyAddress || '').replace(/,\s*USA$/i, '').trim()
  if (!normalizedPropertyAddress) return jsonResponse({ error: 'property_address is required', operation: 'validate-address' }, 400)
  const baseHeaders = { 'X-Api-Key': rentcastApiKey, Accept: 'application/json' }
  try {
    const propertyUrl = 'https://api.rentcast.io/v1/properties?address=' + encodeURIComponent(normalizedPropertyAddress) + '&limit=1'
    const propertyFetch = await guardedRentCastFetch(propertyUrl, { method: 'GET', headers: baseHeaders })
    if (!propertyFetch.response.ok) throw new Error('RentCast properties request failed with status ' + propertyFetch.response.status)
    const propertyResult = parseJsonOrThrow(await propertyFetch.response.text(), 'property detail')
    const propertyRecord = Array.isArray(propertyResult) ? (propertyResult[0] || null) : propertyResult
    const property = propertyRecord && typeof propertyRecord === 'object' ? propertyRecord as JsonRecord : {}

    let avmResult: JsonRecord = {}
    const directMarketValue = normalizeMarketValue(property)
    if (directMarketValue.valueType !== 'market_value') {
      const avmUrl = 'https://api.rentcast.io/v1/avm/value?address=' + encodeURIComponent(normalizedPropertyAddress)
      try {
        const avmFetch = await guardedRentCastFetch(avmUrl, { method: 'GET', headers: baseHeaders })
        if (avmFetch.response.ok) {
          const parsed = parseJsonOrThrow(await avmFetch.response.text(), 'AVM detail')
          avmResult = parsed && typeof parsed === 'object' ? parsed as JsonRecord : {}
        }
      } catch (error) {
        if (!(error instanceof UsageLimitError)) throw error
      }
    }
    const avmPrice = finitePositive(pickFirst(avmResult.price, avmResult.estimatedValue, avmResult.estimated_value, avmResult.value))
    const normalizedValue = avmPrice ? {
      estimatedMarketValue: avmPrice,
      rawValue: avmPrice,
      rawField: avmResult.price != null ? 'price' : (avmResult.estimatedValue != null ? 'estimatedValue' : 'value'),
      valueType: 'avm_market_value',
      valueSource: 'RentCast AVM',
      calculationMethod: 'direct AVM value',
      valueDate: pickFirst(avmResult.date, avmResult.valuationDate),
    } : directMarketValue

    return jsonResponse({
      success: true,
      source: 'RentCast',
      address: pickFirst(property.formattedAddress, property.addressLine1, normalizedPropertyAddress),
      normalizedAddress: normalizeAddress(pickFirst(property.formattedAddress, property.addressLine1, normalizedPropertyAddress)),
      latitude: Number.isFinite(Number(property.latitude)) ? Number(property.latitude) : null,
      longitude: Number.isFinite(Number(property.longitude)) ? Number(property.longitude) : null,
      city: pickFirst(property.city),
      state: pickFirst(property.state, property.stateAbbreviation, property.state_code),
      county: pickFirst(property.county, property.countyName, property.county_name),
      ownerName: pickFirst(property.ownerName, property.owner_name),
      ownerOccupied: pickFirst(property.ownerOccupied, property.owner_occupied),
      mailingAddress: pickFirst(property.mailingAddress, property.mailing_address),
      bedrooms: pickFirst(property.bedrooms, property.beds),
      bathrooms: pickFirst(property.bathrooms, property.baths),
      squareFootage: pickFirst(property.squareFootage, property.square_footage, property.livingArea, property.living_area, property.sqft),
      lotSize: pickFirst(property.lotSize, property.lot_size, property.lotArea),
      yearBuilt: pickFirst(property.yearBuilt, property.year_built),
      propertyType: pickFirst(property.propertyType, property.property_type),
      lastSalePrice: pickFirst(property.lastSalePrice, property.last_sale_price),
      lastSaleDate: pickFirst(property.lastSaleDate, property.last_sale_date),
      assessedValue: latestAssessment(property)?.value || null,
      estimatedValue: normalizedValue.estimatedMarketValue,
      ...normalizedValue,
      raw_property: propertyResult,
      raw_avm: Object.keys(avmResult).length ? avmResult : null,
    })
  } catch (error) {
    const message = safeErrorMessage(error instanceof Error ? error.message : error)
    console.error('[zillow-property] operation=property-detail status=failed message=' + message)
    return jsonResponse({ error: message, operation: 'property-detail' }, error instanceof UsageLimitError ? 429 : 502)
  }
}

if (typeof Deno !== 'undefined' && typeof Deno.serve === 'function') {
  Deno.serve(handleRequest)
}
