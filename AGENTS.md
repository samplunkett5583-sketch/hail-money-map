# Hail Money repository rules

## Protected CRM surface

- The existing CRM shell, header, navigation, lead details, dashboard, and non-Maps behavior are frozen unless the user explicitly asks to change them.
- Money Maps changes must stay inside the Money Maps markup, Money Maps CSS, Money Maps JavaScript, its focused tests, and its server-side storm-data producers.
- Compare protected non-Maps UI against `APPROVED_APP_UI_LOCKED_2026-07-19` before committing Maps work.
- Never use a broad rewrite of `public/index.html`; make narrow, reviewable edits.

## Money Maps swath no-regression workflow

Before changing hail swath geometry, source selection, display styling, date lifecycle, marker toggles, opacity, Campaign Mode containment, or the weather ticker:

1. Trace the complete data path from source observation to stored geometry to visible overlay and Campaign Mode consumption.
2. Inspect the relevant commit history and identify the exact behavior change before editing.
3. Establish a reproducible failing fixture or live diagnostic using authoritative observations. Never invent a path, hand-draw a correction, hard-code a location, or widen/move geometry to make a screenshot pass.
4. Keep one authoritative geometry selector shared by Money Maps, area search, property containment, and Campaign Mode. Hail Spotter Reports are markers only and must never add, remove, recolor, resize, or replace swath geometry.
5. Require provenance on report-derived geometry: event date, source table/provider, generation method, observation count, observed hail range, and generated timestamp when available. Cached geometry without positive observation evidence is not customer-visible.
6. Verify the affected date plus a different date; confirm previous and pinned overlays are removed, opacity affects the same active geometry, and Campaign Mode receives the exact visible geometry.
7. Run the focused Money Maps authority tests, existing Money Maps regression tests, Campaign provider tests, and secret scans before commit or deployment.
8. Use the in-app browser for final live verification and record source/product/row IDs and geometry fingerprints in non-secret diagnostics.

## Data integrity

- Do not display synthetic, mock, reference-image-derived, or browser-authored hail swaths as real storm coverage.
- Ground observations can support report-derived coverage estimates, but the UI and diagnostics must label them honestly; they are not radar measurements or property-level damage verification.
- Do not make paid API calls unless the user explicitly authorizes them. Campaign development mode must remain cached/open-data only and production paid-provider mode must fail closed.
