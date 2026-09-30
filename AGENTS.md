# Active Hail Money Source

- User-designated canonical project: `C:\dev\HailMoney`.
- Canonical branch: `master`.
- Live application: `https://www.hail.money`.
- Active web source: `C:\dev\HailMoney\public`.
- Entry file: `C:\dev\HailMoney\public\index.html`.
- Current production lock: `PRODUCTION_LOCKED_2026-10-01`.
- Do not edit or deploy from backups, root HTML copies, previous builds, staging folders, stale branches, or other Hail Money worktrees.
- For Hosting changes, deploy from the canonical project only and verify `www.hail.money` after deployment.

# Approved CRM UI Baseline

- The entire CRM is frozen except for the exact change explicitly requested by the user.
- The universal header layout is frozen across all main and nested pages.
- Header background, height, logo sizes, title position, centered Hail Money brand, profile placement, and stage-row placement cannot change without explicit user authorization.
- Do not alter CRM layout, styling, navigation, watermark, Activity Feed, Dashboard, Pipeline, Contacts, Documents, Job Folder, nested pages, forms, authentication, or shared-shell behavior unless the user explicitly requests that exact change.
- All future Maps work must be limited to `page-map` markup, Maps-scoped CSS, and map-specific JavaScript/functions.
- Maps work may not change shared CRM renderers, shared headers, shared navigation, Dashboard, Pipeline, Contacts, Documents, Job Folder, nested pages, Activity Feed, watermark, forms, or authentication.
- Maps, storm data, estimating, and backend work must preserve the approved CRM UI and universal header system.
- The repository intentionally contains one canonical production snapshot; the current application lock is `PRODUCTION_LOCKED_2026-10-01`.
- Do not switch branches, reset, restore, merge, or cherry-pick without verifying that the production baseline remains present.
- Never broadly rewrite `public/index.html` for targeted work.
- Do not make unrelated UI changes.



