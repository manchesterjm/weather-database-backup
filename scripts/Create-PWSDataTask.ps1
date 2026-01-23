# Create-PWSDataTask.ps1
# Creates a scheduled task to capture PWS data hourly
# Run this script as Administrator

$TaskName = "PWS Data Capture"
$Description = "Captures crowdsourced weather station data from WunderMap hourly"

# Remove existing task if present
Unregister-ScheduledTask -TaskName $TaskName -Confirm:$false -ErrorAction SilentlyContinue

# Action: Run the PWS capture script via WSL
$Action = New-ScheduledTaskAction -Execute "wsl.exe" `
    -Argument "-e python3 /mnt/d/Scripts/capture_pws_data.py"

# Trigger: Every hour at 5 minutes past the hour (offset from screenshot capture)
$Trigger = New-ScheduledTaskTrigger -Once -At (Get-Date).Date.AddHours((Get-Date).Hour).AddMinutes(5) `
    -RepetitionInterval (New-TimeSpan -Hours 1)

# Settings
$Settings = New-ScheduledTaskSettingsSet `
    -AllowStartIfOnBatteries `
    -DontStopIfGoingOnBatteries `
    -StartWhenAvailable `
    -ExecutionTimeLimit (New-TimeSpan -Minutes 5)

# Principal: Run as current user
$Principal = New-ScheduledTaskPrincipal -UserId $env:USERNAME -LogonType Interactive

# Register the task
Register-ScheduledTask -TaskName $TaskName `
    -Action $Action `
    -Trigger $Trigger `
    -Settings $Settings `
    -Principal $Principal `
    -Description $Description

Write-Host ""
Write-Host "Scheduled task '$TaskName' created successfully!" -ForegroundColor Green
Write-Host "Task will run every hour at 5 minutes past the hour."
Write-Host ""
Write-Host "To run immediately: Start-ScheduledTask -TaskName '$TaskName'"
Write-Host "To check status: Get-ScheduledTask -TaskName '$TaskName'"
