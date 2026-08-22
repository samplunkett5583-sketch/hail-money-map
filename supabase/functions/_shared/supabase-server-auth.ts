type EnvironmentReader = (name: string) => string | undefined

declare const Deno: { env: { get(name: string): string | undefined } }

function environmentValue(readEnvironment: EnvironmentReader, name: string) {
  return String(readEnvironment(name) || '').trim()
}

export function getSupabaseServerKey(
  readEnvironment: EnvironmentReader = (name) => Deno.env.get(name),
) {
  const deployedSecret = environmentValue(readEnvironment, 'HM_SUPABASE_SERVER_KEY')
  if (deployedSecret.startsWith('sb_secret_')) return deployedSecret

  const directSecret = environmentValue(readEnvironment, 'SUPABASE_SECRET_KEY')
  if (directSecret.startsWith('sb_secret_')) return directSecret

  const secretMap = environmentValue(readEnvironment, 'SUPABASE_SECRET_KEYS')
  if (secretMap) {
    try {
      const parsed = JSON.parse(secretMap)
      const preferredName = environmentValue(readEnvironment, 'HM_SUPABASE_SECRET_NAME') || 'default'
      const preferred = parsed?.[preferredName]
      if (typeof preferred === 'string' && preferred.startsWith('sb_secret_')) return preferred
      const candidates = Object.values(parsed || {}).filter(
        (value): value is string => typeof value === 'string' && value.startsWith('sb_secret_'),
      )
      if (candidates.length === 1) return candidates[0]
    } catch (_) {
      return ''
    }
  }
  return ''
}

export function supabaseServerHeaders(
  key: string,
  additions: Record<string, string> = {},
) {
  return { apikey: key, ...additions }
}

export function supabaseServerFetch(
  key: string,
  fetchImplementation: typeof fetch = fetch,
) {
  if (!key.startsWith('sb_secret_')) throw new Error('A server-side sb_secret key is required')
  return (input: RequestInfo | URL, init: RequestInit = {}) => {
    const headers = new Headers(init.headers || {})
    if (headers.get('Authorization') === 'Bearer ' + key) headers.delete('Authorization')
    headers.set('apikey', key)
    return fetchImplementation(input, { ...init, headers })
  }
}
