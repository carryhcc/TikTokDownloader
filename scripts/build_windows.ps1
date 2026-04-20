Param(
  [Parameter(Mandatory = $true)]
  [string]$ArchTag
)

$ErrorActionPreference = "Stop"

Write-Host "==> Build start for $ArchTag"

python -m pip install --upgrade pip
python -m pip install -r requirements.txt
python -m pip install pyinstaller

pyinstaller --noconfirm --clean --noconsole --onedir `
  --name DouKWebUI `
  --add-data "static;static" `
  --add-data "locale;locale" `
  --add-data "docs;docs" `
  main_webui.py

$outDir = "release\DouKWebUI-$ArchTag"
if (Test-Path $outDir) {
  Remove-Item -Recurse -Force $outDir
}
New-Item -ItemType Directory -Force -Path $outDir | Out-Null

Copy-Item -Recurse -Force "dist\DouKWebUI\*" $outDir
Copy-Item -Force "README.md" "$outDir\README.md"

$zipName = "DouKWebUI-$ArchTag.zip"
if (Test-Path $zipName) {
  Remove-Item -Force $zipName
}
Compress-Archive -Path "$outDir\*" -DestinationPath $zipName

Write-Host "==> Build done: $zipName"
