import { execFileSync } from 'node:child_process';
import { readFileSync } from 'node:fs';

const includeUntracked = process.argv.includes('--include-untracked');
const scanArgs = includeUntracked
  ? ['ls-files', '-z', '--cached', '--others', '--exclude-standard']
  : ['ls-files', '-z'];
const trackedFiles = execFileSync('git', scanArgs, {
  encoding: 'utf8',
  maxBuffer: 64 * 1024 * 1024,
})
  .split('\0')
  .filter(Boolean);

const findings = [];

function lineNumber(text, offset) {
  return text.slice(0, offset).split('\n').length;
}

function report(file, text, offset, kind) {
  findings.push({ file, line: lineNumber(text, offset), kind });
}

function decodeJwtPayload(token) {
  const parts = token.split('.');
  if (parts.length !== 3) return null;
  try {
    return JSON.parse(Buffer.from(parts[1], 'base64url').toString('utf8'));
  } catch {
    return null;
  }
}

for (const file of trackedFiles) {
  let buffer;
  try {
    buffer = readFileSync(file);
  } catch {
    continue;
  }

  let text;
  if (buffer[0] === 0xff && buffer[1] === 0xfe) {
    text = buffer.toString('utf16le');
  } else if (buffer[0] === 0xfe && buffer[1] === 0xff) {
    const swapped = Buffer.from(buffer);
    swapped.swap16();
    text = swapped.toString('utf16le');
  } else if (buffer.includes(0)) {
    // A few tracked historical HTML fixtures are UTF-16 without a reliable MIME
    // signal. Decode text-like UTF-16LE; skip unrelated binary artifacts.
    const sample = buffer.subarray(0, Math.min(buffer.length, 4096));
    const zeroBytes = [...sample].filter((byte) => byte === 0).length;
    if (zeroBytes / sample.length < 0.20) continue;
    text = buffer.toString('utf16le');
  } else {
    text = buffer.toString('utf8');
  }

  for (const match of text.matchAll(/\bsb_secret_[A-Za-z0-9_-]{20,}\b/g)) {
    report(file, text, match.index, 'hard-coded Supabase secret key');
  }

  for (const match of text.matchAll(/\beyJ[A-Za-z0-9_-]{8,}\.[A-Za-z0-9_-]{8,}\.[A-Za-z0-9_-]{8,}\b/g)) {
    const payload = decodeJwtPayload(match[0]);
    if (payload?.role === 'service_role') {
      report(file, text, match.index, 'hard-coded Supabase service_role JWT');
    }
  }

  for (const match of text.matchAll(/console\.(?:log|info|debug|warn|error)\s*\(\s*(?:process\.env\.)?(?:serviceRoleKey|service_role|SUPABASE_SERVICE_ROLE_KEY|secretKey)\b/gi)) {
    report(file, text, match.index, 'possible credential logging');
  }

  if (file !== 'scripts/check-secret-redaction.mjs') {
    for (const match of text.matchAll(/\b(?:SUPABASE_SERVICE_ROLE_KEY|SERVICE_ROLE_KEY)\b/g)) {
      report(file, text, match.index, 'legacy Supabase server-key variable');
    }
  }

  for (const match of text.matchAll(/console\.(?:log|info|debug|warn|error)\s*\(\s*(?:process\.env\.)?(?:SUPABASE_SECRET_KEY|SUPABASE_SECRET_KEYS|HM_SUPABASE_SECRET_NAME)\b/gi)) {
    report(file, text, match.index, 'possible new credential logging');
  }

  if (file !== 'scripts/check-secret-redaction.mjs') {
    for (const match of text.matchAll(/authorization[^\n]{0,100}bearer[^\n]{0,100}\b(?:SUPABASE_SECRET_KEY|SUPABASE_KEY|database\.key)\b/gi)) {
      report(file, text, match.index, 'server secret incorrectly used as a Bearer token');
    }
  }
}

if (findings.length) {
  console.error('Secret-redaction check failed. No secret values are shown.');
  for (const finding of findings) {
    console.error(`${finding.file}:${finding.line} - ${finding.kind}`);
  }
  process.exit(1);
}

console.log(`Secret-redaction check passed for ${trackedFiles.length} ${includeUntracked ? 'tracked and untracked' : 'tracked'} files.`);
