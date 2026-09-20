# Company Document Field Editor

Implementation target: separate feature branch from `APPROVED_FULL_APP_LOCK_2026-09-19`.

Admin/Owner behavior:
- Company Documents keeps the existing Upload, Open, and Delete actions.
- Every supported PDF/image document also gets an Edit action.
- Edit opens a visual document editor on the actual uploaded page.
- Palette includes plain text fields for Homeowner Name, Property Address, and Phone Number.
- Admin selects a field and clicks the exact location on the page to place it, then can drag and resize it.
- Each field stores page-relative normalized coordinates (`pageIndex`, `xPct`, `yPct`, `wPct`, `hPct`) rather than viewport pixels.
- Coordinates are therefore stable across screen sizes and later PDF/print rendering.
- Saved field mappings live with the shared company-document metadata so they work on every device for that company.
- Non-admin users can open documents but cannot edit mappings.

Print/sign rendering contract:
- Render each text value against the original PDF page coordinate system using the saved normalized coordinates.
- Do not derive final print placement from browser scroll offsets, page zoom, or CSS viewport pixels.
- Field boxes are editor-only guides and are not printed.
