const { spawnSync } = require('child_process');

const BASELINE_BRANCH = 'APPROVED_FULL_APP_LOCK_2026-09-19';
const BASELINE_REF = `refs/remotes/origin/${BASELINE_BRANCH}`;

const protectedPaths = [
  'public',
  'functions',
  'scripts',
  '.github/workflows',
  '.claude',
  '.firebaserc',
  'firebase.json',
  'firestore.rules',
  'firestore.indexes.json',
  'storage.rules',
  'package.json',
  'package-lock.json',
  'AGENTS.md',
  'CODEX-INSTRUCTIONS.txt',
  'FIX-INSTRUCTIONS.txt'
];

function runGit(args) {
  const result = spawnSync('git', args, { encoding: 'utf8' });
  if (result.status !== 0) {
    process.stderr.write(result.stdout || '');
    process.stderr.write(result.stderr || '');
    process.exit(result.status || 1);
  }
  return (result.stdout || '').trim();
}

runGit([
  'fetch',
  '--no-tags',
  'origin',
  `${BASELINE_BRANCH}:${BASELINE_REF}`
]);

const changed = runGit([
  'diff',
  '--name-only',
  BASELINE_REF,
  'HEAD',
  '--',
  ...protectedPaths
]);

if (changed) {
  console.error('BLOCKED: protected Hail Money source differs from the approved full-app baseline.');
  console.error('Changed protected files:');
  console.error(changed);
  console.error('Do not deploy until the user explicitly approves the exact change and the approved baseline is moved to that approved commit.');
  process.exit(1);
}

console.log(`Approved full application lock verified against ${BASELINE_BRANCH}.`);
