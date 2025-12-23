@echo off
cd /d "%~dp0"

echo Starting web server at http://localhost:5500
echo Leave this window open while using the app.
echo.

powershell -command "Start-Process powershell -ArgumentList '-NoExit','-Command','cd ''%cd%''; $H=New-Object System.Net.HttpListener; $H.Prefixes.Add(\"http://*:5500/\"); $H.Start(); Write-Host \"Serving http://localhost:5500\"; while ($true) { $C=$H.GetContext(); $R=$C.Response; $P=$C.Request.Url.LocalPath.TrimStart(''/''); if ($P -eq \"\") { $P=\"index.html\" }; if (Test-Path $P) { $Bytes=[System.IO.File]::ReadAllBytes($P); $R.ContentType=\"text/html\"; $R.OutputStream.Write($Bytes,0,$Bytes.Length) } else { $R.StatusCode=404 }; $R.Close() }'"

pause
