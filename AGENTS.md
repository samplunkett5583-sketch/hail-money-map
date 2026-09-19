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

## Permanent Fix Rule

- A fix is not complete until the final working change is committed to the active GitHub branch that production deploys from.
- Never leave an approved fix only in a local file, backup, temporary worktree, generated artifact, or deployment-time patch.
- Deployment scripts must not silently replace or undo previously approved source behavior.
- For critical approved behavior, add or update a deploy-time contract check that fails the deployment if the behavior regresses.
- Before pushing a new change, compare it against the current production-source branch and preserve every unrelated approved fix.
- If a local working copy contains a newer approved fix than GitHub, merge that exact fix into GitHub before any further production deployment.
- Do not call a change finished merely because it works locally or in a transient deployment runner.
