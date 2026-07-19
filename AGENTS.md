# Approved CRM UI Baseline

- The entire CRM is frozen.
- The universal header layout is frozen across all main and nested pages.
- Header background, height, logo sizes, title position, centered Hail Money brand, profile placement, and stage-row placement cannot change without explicit user authorization.
- Do not alter CRM layout, styling, navigation, watermark, Activity Feed, Dashboard, Pipeline, Contacts, Documents, Job Folder, nested pages, forms, authentication, or shared-shell behavior unless the user explicitly requests that exact change.
- All future Maps work must be limited to `page-map` markup, Maps-scoped CSS, and map-specific JavaScript/functions.
- Maps work may not change shared CRM renderers, shared headers, shared navigation, Dashboard, Pipeline, Contacts, Documents, Job Folder, nested pages, Activity Feed, watermark, forms, or authentication.
- Before and after every Maps task, compare all protected non-Maps code against the locked tag `APPROVED_APP_UI_LOCKED_2026-07-19`.
- If a Maps repair requires shared code, stop and request explicit permission before editing it.
- Maps, storm data, estimating, and backend work must preserve the approved CRM UI and universal header system.
- The previous approved baseline remains `APPROVED_CRM_UI_LOCKED_2026-07-18`; the current application lock is `APPROVED_APP_UI_LOCKED_2026-07-19`.
- Do not switch branches, reset, restore, merge, or cherry-pick without verifying that the approved CRM baseline remains present.
- Never broadly rewrite `public/index.html` for targeted work.
- Do not make unrelated UI changes.
