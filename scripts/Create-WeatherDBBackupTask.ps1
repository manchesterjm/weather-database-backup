# Create-WeatherDBBackupTask.ps1
# Creates a scheduled task to backup weather.db daily at 2 AM
# Run this script as Administrator

$TaskName = "Weather Database Backup"
$Description = "Daily backup of weather.db with 7-day rolling retention"
$ScriptPath = "D:\Scripts\Backup-WeatherDB.ps1"

# Remove existing task if it exists
Unregister-ScheduledTask -TaskName $TaskName -Confirm:$false -ErrorAction SilentlyContinue

# Create trigger - run daily at 2:00 AM
$Trigger = New-ScheduledTaskTrigger -Daily -At "2:00 AM"

# Create action - run PowerShell with the backup script
$Action = New-ScheduledTaskAction -Execute "powershell.exe" -Argument "-NoProfile -ExecutionPolicy Bypass -File `"$ScriptPath`""

# Create principal (run whether logged in or not)
$Principal = New-ScheduledTaskPrincipal -UserId "$env:USERDOMAIN\$env:USERNAME" -LogonType S4U -RunLevel Highest

# Create settings
$Settings = New-ScheduledTaskSettingsSet -AllowStartIfOnBatteries -DontStopIfGoingOnBatteries -StartWhenAvailable -ExecutionTimeLimit (New-TimeSpan -Minutes 10)

# Register the task
Register-ScheduledTask -TaskName $TaskName -Description $Description -Trigger $Trigger -Action $Action -Principal $Principal -Settings $Settings

Write-Host "Task '$TaskName' created successfully" -ForegroundColor Green
Write-Host "Schedule: Daily at 2:00 AM" -ForegroundColor Cyan
Write-Host "Backup location: D:\Scripts\weather_data\backups\" -ForegroundColor Cyan
Write-Host "Retention: 7 days" -ForegroundColor Cyan
