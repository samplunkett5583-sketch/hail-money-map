$ErrorActionPreference = "Stop"

$port = 5500
$prefix = "http://localhost:$port/"

$listener = New-Object System.Net.HttpListener
$listener.Prefixes.Add($prefix)
$listener.Start()

Write-Host "Serving from: $PSScriptRoot"
Write-Host "Open: $prefix"
Write-Host "Press Ctrl+C to stop."

function Get-ContentType($path) {
    switch ([IO.Path]::GetExtension($path).ToLower()) {
        ".html" { "text/html; charset=utf-8" }
        ".js"   { "application/javascript; charset=utf-8" }
        ".css"  { "text/css; charset=utf-8" }
        ".json" { "application/json; charset=utf-8" }
        ".png"  { "image/png" }
        ".jpg"  { "image/jpeg" }
        ".jpeg" { "image/jpeg" }
        ".svg"  { "image/svg+xml" }
        ".ico"  { "image/x-icon" }
        default { "application/octet-stream" }
    }
}

try {
    while ($listener.IsListening) {
        $context = $listener.GetContext()
        $req = $context.Request
        $res = $context.Response

        $path = $req.Url.LocalPath.TrimStart("/")
        if ($path -eq "") { $path = "index.html" }

        $file = Join-Path $PSScriptRoot "public\$path"
        if (-not (Test-Path $file)) {
            $file = Join-Path $PSScriptRoot "public\index.html"
        }

        if (Test-Path $file) {
            $bytes = [IO.File]::ReadAllBytes($file)
            $res.ContentType = Get-ContentType $file
            $res.ContentLength64 = $bytes.Length
            $res.OutputStream.Write($bytes, 0, $bytes.Length)
        } else {
            $msg = "404 Not Found"
            $bytes = [Text.Encoding]::UTF8.GetBytes($msg)
            $res.StatusCode = 404
            $res.OutputStream.Write($bytes, 0, $bytes.Length)
        }

        $res.OutputStream.Close()
    }
}
finally {
    $listener.Stop()
    $listener.Close()
}
