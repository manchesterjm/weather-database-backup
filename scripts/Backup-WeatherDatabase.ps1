# Backup-WeatherDatabase.ps1
# Backs up weather.db to GitHub with force push (no history)

$ErrorActionPreference = "Continue"

$RepoUrl = "https://github.com/manchesterjm/weather-database-backup.git"
$BackupDir = "D:\Scripts\weather_database_backup"
$DatabasePath = "D:\Scripts\weather_data\weather.db"
$LogFile = "D:\Scripts\weather_backup.log"

Write-Host "Weather Database Backup" -ForegroundColor Cyan
Write-Host "========================" -ForegroundColor Cyan

# Create backup directory if it doesn't exist
if (-not (Test-Path $BackupDir)) {
    Write-Host "Creating backup directory..."
    New-Item -ItemType Directory -Path $BackupDir | Out-Null
    Set-Location $BackupDir
    git init -b main
    git remote add origin $RepoUrl
} else {
    Set-Location $BackupDir
}

# Copy database to backup directory
Write-Host "Copying database..."
Copy-Item $DatabasePath -Destination "$BackupDir\weather.db" -Force

# Sync weather scripts to backup directory
Write-Host "Syncing scripts..."
$ScriptsDir = "$BackupDir\scripts"
if (-not (Test-Path $ScriptsDir)) {
    New-Item -ItemType Directory -Path $ScriptsDir | Out-Null
}

# Copy all weather-related scripts
$ScriptsToCopy = @(
    "weather_logger.py",
    "weather_accuracy.py",
    "weather_spaghetti.py",
    "capture_pws_data.py",
    "capture_wundermap.sh",
    "Backup-WeatherDatabase.ps1",
    "Create-WeatherDBBackupTask.ps1",
    "Create-WeatherLoggerTask.ps1",
    "Create-PWSDataTask.ps1",
    "Create-WunderMapTask.ps1"
)
foreach ($script in $ScriptsToCopy) {
    $srcPath = "D:\Scripts\$script"
    if (Test-Path $srcPath) {
        Copy-Item $srcPath -Destination "$ScriptsDir\$script" -Force
    }
}

# Get database size
$size = (Get-Item "$BackupDir\weather.db").Length / 1MB
Write-Host "Database size: $([math]::Round($size, 2)) MB"

# Create/update README
$timestamp = Get-Date -Format "yyyy-MM-dd HH:mm:ss"
$readme = @"
# Weather Database Backup

Colorado Springs area weather data collected from NWS, METAR, NBM, GFS, and CPC sources.

**Last backup:** $timestamp

**Database size:** $([math]::Round($size, 2)) MB

## Data Sources

- NWS Forecast API (forecast_snapshots, digital_forecast, hourly_snapshots)
- METAR observations (KCOS, KFLY, KFCS, KAFF)
- National Blend of Models (nbm_forecasts)
- GFS model data (gfs_forecasts)
- CPC outlooks (cpc_outlooks)

## Note

This repo uses force-push to maintain only the current backup (no history).
"@
$readme | Out-File -FilePath "$BackupDir\README.md" -Encoding UTF8

# Git add, commit, force push
Write-Host "Committing..."
git add weather.db README.md scripts/
$commitOutput = git commit -m "Backup $timestamp" 2>&1
Write-Host $commitOutput

Write-Host "Pushing to GitHub..."
$pushOutput = git push --force -u origin main 2>&1
Write-Host $pushOutput

# Log the backup
$logTimestamp = Get-Date -Format "yyyy_MM_dd_HH_mm_ss"
$logEntry = "$logTimestamp | Size: $([math]::Round($size, 2)) MB | weather.db backed up to GitHub"
Add-Content -Path $LogFile -Value $logEntry -ErrorAction SilentlyContinue
Write-Host "Logged to: $LogFile"

Write-Host ""
Write-Host "Backup complete!" -ForegroundColor Green
Write-Host "Repo: https://github.com/manchesterjm/weather-database-backup"

# Pause so user can see output
Write-Host ""
Write-Host "Press Enter to close..." -ForegroundColor Yellow
Read-Host
