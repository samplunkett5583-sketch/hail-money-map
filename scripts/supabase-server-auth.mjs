export function getSupabaseServerKey(environment = process.env) {
  const key = String(environment.HM_SUPABASE_SERVER_KEY || environment.SUPABASE_SECRET_KEY || '').trim()
  if (!key.startsWith('sb_secret_')) {
    throw new Error('A server-side sb_secret key is required')
  }
  return key
}

export function supabaseServerHeaders(key, additions = {}) {
  if (!String(key || '').startsWith('sb_secret_')) {
    throw new Error('A server-side sb_secret key is required')
  }
  return { apikey: key, ...additions }
}

export function supabaseServerFetch(key, fetchImplementation = globalThis.fetch) {
  if (!String(key || '').startsWith('sb_secret_')) {
    throw new Error('A server-side sb_secret key is required')
  }
  return (input, init = {}) => {
    const headers = new Headers(init.headers || {})
    if (headers.get('Authorization') === 'Bearer ' + key) headers.delete('Authorization')
    headers.set('apikey', key)
    return fetchImplementation(input, { ...init, headers })
  }
}
