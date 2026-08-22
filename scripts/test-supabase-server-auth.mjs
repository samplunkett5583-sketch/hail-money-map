import assert from 'node:assert/strict'
import { execFileSync } from 'node:child_process'
import { readFileSync } from 'node:fs'
import { getSupabaseServerKey, supabaseServerFetch, supabaseServerHeaders } from './supabase-server-auth.mjs'

const fakeSecret = ['sb', 'secret', 'unit', 'test'].join('_')
assert.equal(getSupabaseServerKey({ SUPABASE_SECRET_KEY: fakeSecret }), fakeSecret)
assert.throws(() => getSupabaseServerKey({ SUPABASE_SECRET_KEY: 'legacy.jwt.value' }))

const headers = supabaseServerHeaders(fakeSecret, { 'Content-Type': 'application/json' })
assert.equal(headers.apikey, fakeSecret)
assert.equal(headers.Authorization, undefined)
assert.equal(headers['Content-Type'], 'application/json')

let capturedHeaders = null
const guardedFetch = supabaseServerFetch(fakeSecret, async (_input, init) => {
  capturedHeaders = new Headers(init.headers)
  return new Response('{}', { status: 200 })
})
await guardedFetch('https://example.invalid', {
  headers: { Authorization: 'Bearer ' + fakeSecret },
})
assert.equal(capturedHeaders.get('apikey'), fakeSecret)
assert.equal(capturedHeaders.get('Authorization'), null)

await guardedFetch('https://example.invalid', {
  headers: { Authorization: 'Bearer legitimate-user-jwt' },
})
assert.equal(capturedHeaders.get('Authorization'), 'Bearer legitimate-user-jwt')

const edgeHelper = readFileSync('supabase/functions/_shared/supabase-server-auth.ts', 'utf8')
assert.match(edgeHelper, /SUPABASE_SECRET_KEYS/)
assert.match(edgeHelper, /HM_SUPABASE_SECRET_NAME/)
assert.match(edgeHelper, /headers\.delete\('Authorization'\)/)

const operationalFiles = execFileSync('git', ['ls-files', '-z'], { encoding: 'utf8' })
  .split('\0')
  .filter((file) => file && file !== 'scripts/check-secret-redaction.mjs')
const legacyVariablePattern = new RegExp('\\bSUPABASE_' + 'SERVICE_' + 'ROLE_' + 'KEY|\\bSERVICE_' + 'ROLE_' + 'KEY\\b')
for (const file of operationalFiles) {
  const text = readFileSync(file, 'utf8')
  assert.doesNotMatch(text, legacyVariablePattern, file)
  if (text.includes('getSupabaseServerKey') && text.includes('createClient(')) {
    assert.match(text, /supabaseServerFetch/, file + ' must strip generated secret-key Bearer auth')
  }
}

console.log('Supabase server-auth migration checks passed.')
