# Weather Database Backup

Colorado Springs area weather data collected from NWS, METAR, NBM, GFS, CPC, and PWS sources.

**Last backup:** 2026-01-23

## Data Sources

- NWS Forecast API (forecast_snapshots, digital_forecast, hourly_snapshots)
- METAR observations (KCOS, KFLY, KFCS, KAFF)
- National Blend of Models (nbm_forecasts)
- GFS model data (gfs_forecasts)
- CPC outlooks (cpc_outlooks)
- PWS crowdsourced data from WunderMap (pws_observations)

## Scripts

| Script | Purpose |
|--------|---------|
| `weather_logger.py` | Main data collection (NWS, METAR, NBM, GFS, CPC) |
| `capture_pws_data.py` | PWS data extraction from WunderMap |
| `weather_accuracy.py` | Forecast accuracy analysis |
| `weather_spaghetti.py` | Spaghetti plot generation |
| `capture_wundermap.sh` | WunderMap screenshot capture |
| `Backup-WeatherDatabase.ps1` | Database backup to GitHub |
| `Create-*.ps1` | Scheduled task setup scripts |

## Database Tables

- `forecast_snapshots` - NWS text forecasts
- `digital_forecast` - NWS hourly digital forecast
- `hourly_snapshots` - Hourly observation snapshots
- `metar` - METAR observations
- `nbm_forecasts` - National Blend of Models
- `gfs_forecasts` - GFS model data
- `cpc_outlooks` - Climate Prediction Center outlooks
- `pws_observations` - Crowdsourced PWS temperatures
- `alerts` - NWS weather alerts
- `actual_daily_climate` - Actual observed climate data
- `actual_snowfall` - Snowfall measurements

## Note

This repo uses force-push to maintain only the current backup (no history).
Scripts are synced from `D:\Scripts\` on each backup.
