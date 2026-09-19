from pathlib import Path

html = Path("public/index.html").read_text(encoding="utf-8")

start = html.find("    #page-lead-detail::before {")
end = html.find("    .jf-tab-panel { display:none; }", start)
if start < 0 or end < 0:
    raise SystemExit("Lead Details CSS block is missing.")

lead = html[start:end]
required = [
    "grid-template-columns: repeat(4, minmax(0, 1fr));",
    "min-height: 42px;",
    "border: 1px solid rgba(123, 82, 5, 0.32);",
    "grid-template-columns: minmax(0, 1fr);",
    "#page-lead-detail .jf-folder-wrap { padding: 16px 24px 28px; }",
    "#page-lead-detail .jf-folder-wrap { padding: 12px 18px 24px; }",
]
for rule in required:
    if rule not in lead:
        raise SystemExit(f"Permanent Lead Details rule missing: {rule}")

forbidden = [
    "grid-template-columns: repeat(7, minmax(0, 1fr));",
    "border: 7px solid #080808;",
    "grid-template-columns: minmax(668px, 690px) minmax(96px, 1fr) minmax(280px, 360px);",
]
for rule in forbidden:
    if rule in lead:
        raise SystemExit(f"Regressed Lead Details rule detected: {rule}")

print("Permanent UI contracts verified.")
