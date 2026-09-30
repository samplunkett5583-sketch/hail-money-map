import { serve } from 'https://deno.land/std@0.168.0/http/server.ts'

const corsHeaders = {
  'Access-Control-Allow-Origin': '*',
  'Access-Control-Allow-Headers': 'authorization, x-client-info, apikey, content-type',
}

function pickFirst(...values: unknown[]) {
  for (const v of values) {
    if (v !== undefined && v !== null && v !== '') return v
  }
  return null
}

function parseJsonOrThrow(text: string, label: string) {
  if (!text) return null
  try {
    return JSON.parse(text)
  } catch (err) {
    throw new Error(`[zillow-property] Failed to parse ${label} JSON: ${err instanceof Error ? err.message : String(err)}`)
  }
}

serve(async (req) => {
  if (req.method === 'OPTIONS') {
    return new Response('ok', { headers: corsHeaders })
  }

  const rentcastApiKey = Deno.env.get('RENTCAST_API_KEY')
  if (!rentcastApiKey) {
    return new Response(JSON.stringify({ error: 'RENTCAST_API_KEY is not configured' }), {
      headers: { ...corsHeaders, 'Content-Type': 'application/json' },
      status: 500,
    })
  }

  let property_address = ''
  if (req.method === 'GET') {
    const url = new URL(req.url)
    const mode = url.searchParams.get('mode') || 'detail'
    if (mode !== 'detail') {
      return new Response(JSON.stringify({ error: 'Invalid mode. Use detail' }), {
        headers: { ...corsHeaders, 'Content-Type': 'application/json' },
        status: 400,
      })
    }
    property_address = url.searchParams.get('address') || ''
  } else {
    const body = await req.json()
    property_address = body?.property_address || ''
  }

  const normalized_property_address = String(property_address || '').replace(/,\s*USA$/i, '').trim()

  console.log('[zillow-property] incoming property_address:', property_address)
  console.log('[zillow-property] normalized property_address:', normalized_property_address)

  if (!normalized_property_address) {
    return new Response(JSON.stringify({ error: 'property_address is required' }), {
      headers: { ...corsHeaders, 'Content-Type': 'application/json' },
      status: 400,
    })
  }

  const propertyUrl = `https://api.rentcast.io/v1/properties?address=${encodeURIComponent(normalized_property_address)}&limit=1`
  const baseHeaders = {
    'X-Api-Key': rentcastApiKey,
    'Content-Type': 'application/json',
  }

  try {
    const propertyResp = await fetch(propertyUrl, {
      method: 'GET',
      headers: baseHeaders,
    })
    console.log('[zillow-property] upstream HTTP status:', JSON.stringify({ properties: propertyResp.status }))
    if (!propertyResp.ok) {
      throw new Error(`RentCast properties request failed with status ${propertyResp.status}`)
    }

    const propertyRawText = await propertyResp.text()
    console.log('[zillow-property] raw upstream response text:', JSON.stringify({ properties: propertyRawText }))
    const propertyResult = parseJsonOrThrow(propertyRawText, 'properties')
    const propertyRecord = Array.isArray(propertyResult) ? (propertyResult[0] || null) : propertyResult

    let avmResult: unknown = null
    let shouldCallAvm = true
    if (propertyRecord && typeof propertyRecord === 'object') {
      const pr = propertyRecord as Record<string, unknown>
      shouldCallAvm = !(pickFirst(pr.assessedValue, pr.assessed_value) != null || pickFirst(pr.lastSalePrice, pr.last_sale_price) != null)
    }

    if (shouldCallAvm) {
      const avmUrl = `https://api.rentcast.io/v1/avm/value?address=${encodeURIComponent(normalized_property_address)}`
      const avmResp = await fetch(avmUrl, {
        method: 'GET',
        headers: baseHeaders,
      })
      console.log('[zillow-property] upstream HTTP status:', JSON.stringify({ avm: avmResp.status }))
      if (!avmResp.ok) {
        throw new Error(`RentCast AVM request failed with status ${avmResp.status}`)
      }
      const avmRawText = await avmResp.text()
      console.log('[zillow-property] raw upstream response text:', JSON.stringify({ avm: avmRawText }))
      avmResult = parseJsonOrThrow(avmRawText, 'avm')
    }

    const p = propertyRecord && typeof propertyRecord === 'object' ? propertyRecord as Record<string, unknown> : {}
    const a = avmResult && typeof avmResult === 'object' ? avmResult as Record<string, unknown> : {}

    const responsePayload = {
      success: true,
      source: 'RentCast',
      address: normalized_property_address,
      ownerName: pickFirst(p.ownerName, p.owner_name),
      ownerOccupied: pickFirst(p.ownerOccupied, p.owner_occupied),
      mailingAddress: pickFirst(p.mailingAddress, p.mailing_address),
      bedrooms: pickFirst(p.bedrooms, p.beds),
      bathrooms: pickFirst(p.bathrooms, p.baths),
      squareFootage: pickFirst(p.squareFootage, p.square_footage, p.livingArea, p.living_area, p.sqft),
      lotSize: pickFirst(p.lotSize, p.lot_size, p.lotArea),
      yearBuilt: pickFirst(p.yearBuilt, p.year_built),
      propertyType: pickFirst(p.propertyType, p.property_type),
      lastSalePrice: pickFirst(p.lastSalePrice, p.last_sale_price),
      lastSaleDate: pickFirst(p.lastSaleDate, p.last_sale_date),
      assessedValue: pickFirst(p.assessedValue, p.assessed_value),
      taxAmount: pickFirst(p.taxAmount, p.tax_amount),
      estimatedValue: pickFirst(a.estimatedValue, a.estimated_value, a.value),
      raw_property: propertyResult,
      raw_avm: avmResult ?? null,
    }

    const mappedFields = Object.keys(responsePayload).filter((k) => {
      if (k === 'raw_property' || k === 'raw_avm') return false
      const v = (responsePayload as Record<string, unknown>)[k]
      return v !== null && v !== undefined && v !== ''
    })
    console.log('[zillow-property] mapped fields:', JSON.stringify(mappedFields))

    return new Response(JSON.stringify(responsePayload), {
      headers: { ...corsHeaders, 'Content-Type': 'application/json' },
      status: 200,
    })
  } catch (error) {
    console.log('[zillow-property] caught exception:', error instanceof Error ? error.message : String(error))
    return new Response(JSON.stringify({
      error: error instanceof Error ? error.message : String(error),
    }), {
      headers: { ...corsHeaders, 'Content-Type': 'application/json' },
      status: 502,
    })
  }
})