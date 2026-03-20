# Run this from your project root: c:\dev\hail map leader
# It finds the LAST </style> tag and inserts the CSS patch just before it

$htmlPath = "public\index.html"
$cssPath  = "hmm-css-patch.css"

$html = Get-Content $htmlPath -Raw -Encoding UTF8
$css  = Get-Content $cssPath  -Raw -Encoding UTF8

# Remove the comment header from the CSS patch so it doesn't show as text
$css = $css -replace '/\*[\s\S]*?closing </style> tag in the main app file[\s\S]*?\*/', ''
$css = $css.Trim()

# Find the LAST </style> and insert before it
$lastStyleClose = $html.LastIndexOf('</style>')

if ($lastStyleClose -lt 0) {
    Write-Host "ERROR: Could not find </style> in $htmlPath"
    exit 1
}

$newHtml = $html.Substring(0, $lastStyleClose) + "`n" + $css + "`n" + $html.Substring($lastStyleClose)

[System.IO.File]::WriteAllText((Resolve-Path $htmlPath), $newHtml, [System.Text.Encoding]::UTF8)

Write-Host "SUCCESS: CSS injected before </style> in $htmlPath"
