# Primary startup command (Windows PowerShell). Run from repo root: .\scripts\up.ps1
if (-not (Test-Path .env)) { Copy-Item .env.example .env }
docker compose -f infra/compose.yaml --env-file .env up -d --build
docker compose -f infra/compose.yaml ps
