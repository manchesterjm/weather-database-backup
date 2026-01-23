# Create-WunderMapTask.ps1
# Creates a scheduled task to capture WunderMap screenshots hourly
# Run this script as Administrator

$TaskName = "WunderMap Screenshot Capture"
$Description = "Captures hourly screenshots of Weather Underground WunderMap for Colorado Springs"

# Check if running as admin
$isAdmin = ([Security.Principal.WindowsPrincipal] [Security.Principal.WindowsIdentity]::GetCurrent()).IsInRole([Security.Principal.WindowsBuiltInRole]::Administrator)
if (-not $isAdmin) {
    Write-Host "ERROR: This script must be run as Administrator" -ForegroundColor Red
    Write-Host "Right-click PowerShell and select 'Run as Administrator'" -ForegroundColor Yellow
    exit 1
}

# Remove existing task if present
$existingTask = Get-ScheduledTask -TaskName $TaskName -ErrorAction SilentlyContinue
if ($existingTask) {
    Write-Host "Removing existing task..." -ForegroundColor Yellow
    Unregister-ScheduledTask -TaskName $TaskName -Confirm:$false
}

# Create action - run WSL bash script
$Action = New-ScheduledTaskAction -Execute "wsl.exe" -Argument "-e /mnt/d/Scripts/capture_wundermap.sh"

# Create trigger - every hour on the hour
$Trigger = New-ScheduledTaskTrigger -Once -At (Get-Date -Minute 0 -Second 0).AddHours(1) -RepetitionInterval (New-TimeSpan -Hours 1) -RepetitionDuration (New-TimeSpan -Days 3650)

# Create settings
$Settings = New-ScheduledTaskSettingsSet -AllowStartIfOnBatteries -DontStopIfGoingOnBatteries -StartWhenAvailable -RunOnlyIfNetworkAvailable

# Create principal (run as current user)
$Principal = New-ScheduledTaskPrincipal -UserId $env:USERNAME -LogonType Interactive -RunLevel Limited

# Register the task
Register-ScheduledTask -TaskName $TaskName -Action $Action -Trigger $Trigger -Settings $Settings -Principal $Principal -Description $Description

Write-Host ""
Write-Host "SUCCESS: Task '$TaskName' created!" -ForegroundColor Green
Write-Host ""
Write-Host "Details:" -ForegroundColor Cyan
Write-Host "  - Runs every hour on the hour"
Write-Host "  - Screenshots saved to: D:\Pictures\Screenshots\WunderMap\"
Write-Host "  - Old screenshots (>7 days) automatically deleted"
Write-Host "  - Log file: D:\Pictures\Screenshots\WunderMap\capture.log"
Write-Host ""
Write-Host "To test now, run:" -ForegroundColor Yellow
Write-Host "  wsl -e /mnt/d/Scripts/capture_wundermap.sh"
