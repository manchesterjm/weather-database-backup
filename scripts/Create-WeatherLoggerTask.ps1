# Create-WeatherLoggerTask.ps1
# Run this script as Administrator to create the Weather Logger scheduled task
# The task will run every 3 hours to fetch and log weather forecasts

$TaskName = "Weather Forecast Logger"
$Description = "Fetches NWS weather forecasts for Colorado Springs every 3 hours and stores in SQLite database for analysis"
$PythonPath = "D:\Python313\python.exe"
$ScriptPath = "D:\Scripts\weather_logger.py"
$WorkingDir = "D:\Scripts"

# Check if running as admin
$IsAdmin = ([Security.Principal.WindowsPrincipal] [Security.Principal.WindowsIdentity]::GetCurrent()).IsInRole([Security.Principal.WindowsBuiltInRole]::Administrator)
if (-not $IsAdmin) {
    Write-Host "ERROR: This script must be run as Administrator" -ForegroundColor Red
    Write-Host "Right-click PowerShell and select 'Run as administrator'" -ForegroundColor Yellow
    exit 1
}

# Remove existing task if it exists
$ExistingTask = Get-ScheduledTask -TaskName $TaskName -ErrorAction SilentlyContinue
if ($ExistingTask) {
    Write-Host "Removing existing task..." -ForegroundColor Yellow
    Unregister-ScheduledTask -TaskName $TaskName -Confirm:$false
}

# Create the action (run truly hidden via VBScript)
$Action = New-ScheduledTaskAction -Execute "wscript.exe" -Argument "`"D:\Scripts\run_hidden.vbs`" `"$PythonPath $ScriptPath`"" -WorkingDirectory $WorkingDir

# Create triggers for every 3 hours (offset by 5 minutes to avoid race condition with metar_logger)
$Triggers = @(
    New-ScheduledTaskTrigger -Daily -At "00:05"
    New-ScheduledTaskTrigger -Daily -At "03:05"
    New-ScheduledTaskTrigger -Daily -At "06:05"
    New-ScheduledTaskTrigger -Daily -At "09:05"
    New-ScheduledTaskTrigger -Daily -At "12:05"
    New-ScheduledTaskTrigger -Daily -At "15:05"
    New-ScheduledTaskTrigger -Daily -At "18:05"
    New-ScheduledTaskTrigger -Daily -At "21:05"
)

# Task settings
$Settings = New-ScheduledTaskSettingsSet -AllowStartIfOnBatteries -DontStopIfGoingOnBatteries -StartWhenAvailable -RunOnlyIfNetworkAvailable

# Create principal (run whether user is logged in or not, with highest privileges)
$Principal = New-ScheduledTaskPrincipal -UserId $env:USERNAME -LogonType S4U -RunLevel Highest

# Register the task
try {
    Register-ScheduledTask -TaskName $TaskName -Description $Description -Action $Action -Trigger $Triggers -Settings $Settings -Principal $Principal
    Write-Host ""
    Write-Host "SUCCESS: Scheduled task '$TaskName' created!" -ForegroundColor Green
    Write-Host ""
    Write-Host "Schedule: Every 3 hours at :05 (00:05, 03:05, 06:05, 09:05, 12:05, 15:05, 18:05, 21:05)" -ForegroundColor Cyan
    Write-Host "Script: $ScriptPath" -ForegroundColor Cyan
    Write-Host "Database: D:\Scripts\weather_data\weather.db" -ForegroundColor Cyan
    Write-Host "Log: D:\Scripts\weather_logger.log" -ForegroundColor Cyan
    Write-Host ""
    Write-Host "To run immediately: schtasks /run /tn `"$TaskName`"" -ForegroundColor Yellow
    Write-Host "To view task: taskschd.msc" -ForegroundColor Yellow
}
catch {
    Write-Host "ERROR: Failed to create scheduled task" -ForegroundColor Red
    Write-Host $_.Exception.Message -ForegroundColor Red
    exit 1
}
