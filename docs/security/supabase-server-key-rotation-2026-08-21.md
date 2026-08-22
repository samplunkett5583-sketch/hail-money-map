# Supabase server-key rotation — 2026-08-21

Project: `ehegjhlkadtpnkborlbz`

Compromised-key fingerprint: SHA-256 prefix `24c6d047c6a742be`

This record intentionally contains no credential values, prefixes, JWTs, API-key-list output, or screenshots.

## Cutover requirements

- New server key is an `sb_secret_` key and is never exposed to browser code.
- Edge Functions resolve the key from Supabase's automatic secret-key map.
- Local and CI callers use `SUPABASE_SECRET_KEY` and send it only through `apikey`.
- Repository secret scanning covers UTF-8, UTF-16, legacy variable names, logging, and Bearer misuse.
- The compromised legacy key is revoked only after production callers pass with the replacement.
- Shared Git history is not rewritten; revocation neutralizes the historical credential.
