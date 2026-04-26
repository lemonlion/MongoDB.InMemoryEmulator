<#
.SYNOPSIS
    Starts MongoDB in Docker as a single-node replica set and waits for readiness.
    Required for transaction support (transactions need a replica set).
.PARAMETER Image
    Docker image. Default 'mongo:7.0.14'.
.PARAMETER ContainerName
    Container name. Default 'mongodb-test'.
.PARAMETER Port
    Host port. Default 27017.
.PARAMETER TimeoutSeconds
    Max seconds to wait. Default 60.
#>
param(
    [string]$Image = 'mongo:7.0.14',
    [string]$ContainerName = 'mongodb-test',
    [int]$Port = 27017,
    [int]$TimeoutSeconds = 60
)

$ErrorActionPreference = 'Stop'

# Check if already running
$existing = docker ps --filter "name=$ContainerName" --format '{{.Names}}' 2>$null
if ($existing -eq $ContainerName) {
    Write-Host "MongoDB container '$ContainerName' is already running." -ForegroundColor Yellow
    return
}

try { docker rm -f -v $ContainerName 2>$null | Out-Null } catch {}

Write-Host "Starting MongoDB ($Image) as replica set..." -ForegroundColor Cyan
docker run --detach --name $ContainerName `
    --publish "${Port}:27017" `
    $Image `
    mongod --replSet rs0 | Out-Null

# Wait for mongod to accept connections
$elapsed = 0
while ($elapsed -lt $TimeoutSeconds) {
    try {
        $result = docker exec $ContainerName mongosh --quiet --eval "db.runCommand({ping:1}).ok" 2>$null
        if ($result -match '1') { break }
    } catch {}
    Start-Sleep -Seconds 1
    $elapsed++
    Write-Host "  Waiting for MongoDB... ($elapsed s)" -ForegroundColor DarkGray
}

if ($elapsed -ge $TimeoutSeconds) {
    Write-Error "MongoDB did not become ready within ${TimeoutSeconds}s"
    exit 1
}

# Initialize single-node replica set (required for transactions)
Write-Host "Initializing replica set..." -ForegroundColor Cyan
docker exec $ContainerName mongosh --quiet --eval @"
rs.initiate({
  _id: 'rs0',
  members: [{ _id: 0, host: 'localhost:27017' }]
})
"@ | Out-Null

# Wait for primary election
$elapsed = 0
while ($elapsed -lt 30) {
    $isPrimary = docker exec $ContainerName mongosh --quiet --eval "rs.status().myState" 2>$null
    if ($isPrimary -match '1') { break }
    Start-Sleep -Seconds 1
    $elapsed++
}

Write-Host "MongoDB ready on port $Port (replica set rs0)" -ForegroundColor Green
