# Start the mock site, run API/HTML collection, print stats, then stop the mock site.
param(
    [int]$Port = 8010
)

$ErrorActionPreference = "Stop"
$RepoRoot = Split-Path -Parent $PSScriptRoot
Set-Location $RepoRoot

$job = Start-Job -ScriptBlock {
    param($p, $root)
    Set-Location $root
    crawl-flow serve-mock --port $p
} -ArgumentList $Port, $RepoRoot

try {
    Start-Sleep -Seconds 2
    crawl-flow run --source demo_api --limit 5
    if ($LASTEXITCODE -ne 0) { throw "demo_api run failed" }
    crawl-flow run --source demo_html --limit 5
    if ($LASTEXITCODE -ne 0) { throw "demo_html run failed" }
    crawl-flow db stats
}
finally {
    Stop-Job $job -ErrorAction SilentlyContinue
    Remove-Job $job -Force -ErrorAction SilentlyContinue
}
