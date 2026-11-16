@echo off
set "PLAYWRIGHT_BROWSERS_PATH=C:\Users\Sergio\AppData\Local\ms-playwright"
cd /d "C:\Users\Sergio\Desktop\tv-stream-server"
powershell -NoProfile -Command "for($i=0;$i -lt 60;$i++){ try { Invoke-WebRequest http://localhost:3000/ -UseBasicParsing -TimeoutSec 2 | Out-Null; exit 0 } catch { Start-Sleep 2 } }; exit 1"
"C:\Program Files\nodejs\node.exe" server.js > "C:\Users\Sergio\Desktop\tv-stream-server\logs\server.log" 2>&1
