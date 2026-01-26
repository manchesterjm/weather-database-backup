# Weather Data Reference

> **Purpose:** Documentation of parseable NWS/NOAA weather data sources for potential app/program development
> **Created:** 2026-01-01

---

## Local Forecasts (weather.gov)

Base URL: `https://forecast.weather.gov/MapClick.php`

### Colorado Springs Coordinates
- **Latitude:** 38.9194
- **Longitude:** -104.7509
- Location: ~4 miles N of Colorado Springs

### Available Formats

| Format | Parameter | Description |
|--------|-----------|-------------|
| **Digital (Hourly)** | `FcstType=digital` | Tabular hourly data - best for parsing |
| **Text** | `FcstType=text` | 7-day narrative forecast |
| **Graphical** | (default) | HTML with images - not easily parseable |

### Digital Forecast URL (Recommended for Apps)
```
https://forecast.weather.gov/MapClick.php?w0=t&w1=td&w2=hi&w3=sfcwind&w4=sky&w5=pop&w6=rh&w7=rain&pqpfhr=3&w15=pqpf0&w16=pqpf1&w17=pqpf2&w18=pqpf3&psnwhr=3&AheadHour=0&Submit=Submit&&FcstType=digital&textField1=38.9194&textField2=-104.7509&site=all
```

**Data columns available:**
- `w0=t` - Temperature
- `w1=td` - Dewpoint
- `w2=hi` - Heat Index
- `w3=sfcwind` - Surface Wind
- `w4=sky` - Sky Cover
- `w5=pop` - Probability of Precipitation
- `w6=rh` - Relative Humidity
- `w7=rain` - Rain amount

### Text Forecast URL
```
https://forecast.weather.gov/MapClick.php?lat=38.9194&lon=-104.7509&FcstType=text
```

### To Use Different Location
Replace `textField1` (lat) and `textField2` (lon) with desired coordinates.

---

## NWS Pueblo Office (Local Products)

**Office:** NWS Pueblo, CO (PUB) - covers Colorado Springs area
**Homepage:** https://www.weather.gov/pub

### Text Products (Parseable)

| Product | URL | Description |
|---------|-----|-------------|
| **Area Forecast Discussion (AFD)** | `.../product.php?site=PUB&issuedby=PUB&product=AFD` | Meteorologist reasoning - best insight into forecast confidence |
| **Hazardous Weather Outlook (HWO)** | `.../product.php?site=PUB&issuedby=PUB&product=HWO` | Local hazards summary for next 7 days |
| **Fire Weather Planning (FWF)** | `.../product.php?site=PUB&issuedby=PUB&product=FWF` | Fire weather conditions |
| **Winter Weather** | `.../product.php?site=PUB&issuedby=PUB&product=WSW` | Winter storm warnings/watches |

### Direct URLs

```
# Area Forecast Discussion (most useful - explains forecaster thinking)
https://forecast.weather.gov/product.php?site=PUB&issuedby=PUB&product=AFD

# Hazardous Weather Outlook
https://forecast.weather.gov/product.php?site=PUB&issuedby=PUB&product=HWO

# Fire Weather Planning Forecast
https://forecast.weather.gov/product.php?site=PUB&issuedby=PUB&product=FWF

# Winter Storm Products
https://forecast.weather.gov/product.php?site=PUB&issuedby=PUB&product=WSW
```

### Other Local Products

| Product | Code | Notes |
|---------|------|-------|
| Zone Forecast | ZFP | Detailed zone-by-zone forecast |
| Short Term Forecast | NOW | Next few hours |
| Special Weather Statement | SPS | Non-warning hazards |
| Climate Report | CLI | Daily climate data |
| Regional Weather Roundup | RWR | Multi-state summary |

### Why AFD is Useful

The Area Forecast Discussion is where meteorologists explain:
- Why they chose certain forecast values
- Confidence levels in the forecast
- Model disagreements they're weighing
- Timing uncertainty for weather events

More useful than consumer forecasts for understanding "how sure are they about this?"

---

## Extended Outlooks (CPC - Climate Prediction Center)

Base URL: `https://www.cpc.ncep.noaa.gov/products/predictions/`

### Text Discussions (Parseable)

| Outlook | URL | Content |
|---------|-----|---------|
| **6-10 Day** | `.../610day/fxus06.html` | Days 6-10 temp/precip outlook |
| **8-14 Day** | `.../610day/fxus06.html` | Included in same file as 6-10 |
| **30-Day (Monthly)** | `.../long_range/fxus07.html` | Monthly temp/precip outlook |
| **90-Day (Seasonal)** | `.../long_range/fxus05.html` | Seasonal outlook + ENSO status |

### Full URLs
```
https://www.cpc.ncep.noaa.gov/products/predictions/610day/fxus06.html
https://www.cpc.ncep.noaa.gov/products/predictions/long_range/fxus07.html
https://www.cpc.ncep.noaa.gov/products/predictions/long_range/fxus05.html
```

### Data Available in Text Discussions
- Temperature outlook (above/below/near normal)
- Precipitation outlook
- ENSO status (El Nino/La Nina/Neutral)
- Confidence level (1-5 scale)
- Regional breakdowns
- Forecast reasoning

---

## CPC Expert Assessments

Hub page: `https://www.cpc.ncep.noaa.gov/products/expert_assessment/`

### ENSO Diagnostic Discussion

```
https://www.cpc.ncep.noaa.gov/products/analysis_monitoring/enso_advisory/ensodisc.shtml
```

**Updated:** Weekly (Thursdays)

**Data available:**
- Current ENSO status (El Niño / La Niña / Neutral)
- Niño-3.4 index value (°C anomaly)
- Sea surface temperature anomalies
- Forecast probabilities by season
- Expected transition timeline
- Atmospheric indicators (winds, convection)

**Example data:**
```
Status: La Niña Advisory
Niño-3.4: -0.5°C (weak La Niña)
Forecast: 68% chance neutral by Jan-Mar 2026
```

### Global Tropics Hazards Outlook

```
https://www.cpc.ncep.noaa.gov/products/precip/CWlink/ghazards/
```

**Updated:** Weekly

**Data available:**
- Active tropical cyclones (name, category, location, track)
- Tropical cyclone development probabilities by region
- Week 2-3 hazard forecasts
- Temperature hazards (extreme heat)
- Precipitation patterns

**Regions covered:** Indian Ocean, Pacific, Atlantic, Australia, global tropics

### US Drought Information

```
https://www.cpc.ncep.noaa.gov/products/Drought/
```

**Products available:**
- US Drought Monitor (weekly)
- Monthly Drought Outlook
- Seasonal Drought Outlook (3rd Thursday monthly)
- Drought indices (SPI, Palmer, EDDI, soil moisture)
- Alaska-specific data

### Additional Resources (PDF/PPT only)

| Product | URL |
|---------|-----|
| ENSO Evolution Slides | `.../lanina/enso_evolution-status-fcsts-web.pdf` |
| MJO Weekly Update | `.../MJO/mjoupdate.pdf` |
| Global Ocean Briefing | `.../ocean_briefing_gif/global_ocean_monitoring_current.ppt` |

---

## Week-2 Hazards Outlook

### Main Page (Text Summary)
```
https://www.cpc.ncep.noaa.gov/products/predictions/threats/threats.php
```

**Hazard types covered:**
- Heavy Precipitation
- Heavy Snow
- High Winds
- Extreme Heat
- Rapid Onset Drought

### KML Files (Geographic Data)

Base URL: `https://www.cpc.ncep.noaa.gov/products/predictions/threats/`

| Hazard | Categorical KML | Probabilistic KML |
|--------|-----------------|-------------------|
| Temperature | `temp_D8_14.kml` | `temp_prob_D8_14.kml` |
| Precipitation | `prcp_D8_14.kml` | `prcp_prob_D8_14.kml` |
| Snow | `snow_D8_14.kml` | `snow_prob_D8_14.kml` |
| Wind | `wind_D8_14.kml` | `wind_prob_D8_14.kml` |
| Extreme Heat | - | `excess_heat_prob_D8_14.kml` |
| Drought | `soils_D8_14.kml` | - |

**Note:** The `*_prob_*.kml` files contain actual polygon data. The non-prob versions may be empty layer configs.

### KML Structure
```xml
<Document>
  <Folder>
    <Placemark>
      <name>Slight Risk of Heavy Snow</name>
      <description>1/9/2026 to 1/12/2026</description>
      <Style>...</Style>
      <Polygon>
        <outerBoundaryIs>
          <LinearRing>
            <coordinates>lon,lat,0 lon,lat,0 ...</coordinates>
          </LinearRing>
        </outerBoundaryIs>
      </Polygon>
    </Placemark>
  </Folder>
</Document>
```

### Checking Location Against Hazards
Colorado bounding box:
- **Latitude:** 37°N to 41°N
- **Longitude:** -109°W to -102°W

To check if a location is in a hazard zone:
1. Parse KML polygon coordinates
2. Use point-in-polygon algorithm
3. Check if target lat/lon falls within polygon boundary

---

## Data Refresh Schedule

| Product | Update Frequency | Time (ET) |
|---------|------------------|-----------|
| **NWS API** | | |
| Hourly Forecast | Every hour | - |
| 7-Day Forecast | Twice daily | ~6am, 6pm |
| Alerts | Real-time | - |
| **CPC Outlooks** | | |
| 6-10 Day | Daily | 8:30am |
| Week-2 Hazards | Daily | Morning |
| 30-Day Monthly | Once monthly | Mid-month, 8:30am |
| 90-Day Seasonal | Once monthly | Mid-month, 8:30am |
| **CPC Expert Assessments** | | |
| ENSO Discussion | Weekly | Thursday |
| Global Tropics Hazards | Weekly | - |
| Drought Monitor | Weekly | Thursday |
| Drought Outlook (Monthly) | Monthly | Last day of month |
| Drought Outlook (Seasonal) | Monthly | 3rd Thursday |

---

## Potential App Architecture

### Data Sources Layer
```
NWS API
├── Local Forecast (digital/text)
└── Alerts

CPC API
├── 6-10 Day Outlook
├── 8-14 Day Outlook
├── 30-Day Monthly
├── 90-Day Seasonal
└── Week-2 Hazards
    ├── Text Summary (.php)
    └── Geographic Data (.kml)
```

### Parsing Approach

| Source | Format | Parser |
|--------|--------|--------|
| Digital Forecast | HTML table | BeautifulSoup / lxml |
| Text Forecast | HTML paragraphs | BeautifulSoup / regex |
| CPC Discussions | HTML text | BeautifulSoup / regex |
| KML Hazards | XML | xml.etree / lxml |

### Python Libraries
```python
import requests          # HTTP fetching
from bs4 import BeautifulSoup  # HTML parsing
import xml.etree.ElementTree as ET  # KML/XML parsing
from shapely.geometry import Point, Polygon  # Point-in-polygon checks
```

### Example: Check if Location in Hazard Zone
```python
import requests
import xml.etree.ElementTree as ET
from shapely.geometry import Point, Polygon

def check_hazard(lat, lon, kml_url):
    response = requests.get(kml_url)
    root = ET.fromstring(response.content)

    # KML namespace
    ns = {'kml': 'http://www.opengis.net/kml/2.2'}

    point = Point(lon, lat)

    for placemark in root.findall('.//kml:Placemark', ns):
        name = placemark.find('kml:name', ns).text
        coords_text = placemark.find('.//kml:coordinates', ns).text

        # Parse coordinates (lon,lat,alt format)
        coords = []
        for coord in coords_text.strip().split():
            parts = coord.split(',')
            coords.append((float(parts[0]), float(parts[1])))

        polygon = Polygon(coords)
        if polygon.contains(point):
            return name  # Return hazard name

    return None  # No hazard

# Check Colorado Springs
result = check_hazard(38.9194, -104.7509,
    'https://www.cpc.ncep.noaa.gov/products/predictions/threats/wind_prob_D8_14.kml')
print(f"Hazard: {result}")
```

---

## NWS API (JSON) - Recommended

**No API key required!** Returns structured JSON - much better than scraping HTML.

Base URL: `https://api.weather.gov`

### Colorado Springs Endpoints

| Endpoint | URL |
|----------|-----|
| **Point Lookup** | `https://api.weather.gov/points/38.9194,-104.7509` |
| **7-Day Forecast** | `https://api.weather.gov/gridpoints/PUB/82,107/forecast` |
| **Hourly Forecast** | `https://api.weather.gov/gridpoints/PUB/82,107/forecast/hourly` |
| **Raw Grid Data** | `https://api.weather.gov/gridpoints/PUB/82,107` |
| **Active Alerts** | `https://api.weather.gov/alerts/active?point=38.9194,-104.7509` |
| **Nearby Stations** | `https://api.weather.gov/gridpoints/PUB/82,107/stations` |

**Note:** Grid coordinates (PUB/82,107) come from the point lookup response.

### All Available Endpoints

| Endpoint | Purpose |
|----------|---------|
| `/points/{lat},{lon}` | Get grid coords, zones, office for location |
| `/gridpoints/{office}/{x},{y}/forecast` | 7-day forecast (14 periods) |
| `/gridpoints/{office}/{x},{y}/forecast/hourly` | 156 hours of forecasts |
| `/gridpoints/{office}/{x},{y}` | Raw grid data (all measurements) |
| `/gridpoints/{office}/{x},{y}/stations` | Nearby observation stations |
| `/alerts/active` | All active alerts nationwide |
| `/alerts/active?area={state}` | Active alerts for state (e.g., `CO`) |
| `/alerts/active?point={lat},{lon}` | Active alerts for location |
| `/alerts/types` | List of alert type codes |
| `/stations` | All observation stations |
| `/stations/{id}` | Station details |
| `/stations/{id}/observations/latest` | Current conditions |
| `/zones/{type}/{zoneId}/forecast` | Zone forecast text |
| `/openapi.json` | Full API specification |

### 7-Day Forecast Response Fields

Each of 14 periods (day/night alternating):

```json
{
  "number": 1,
  "name": "Tonight",
  "startTime": "2026-01-01T18:00:00-07:00",
  "endTime": "2026-01-02T06:00:00-07:00",
  "isDaytime": false,
  "temperature": 28,
  "temperatureUnit": "F",
  "temperatureTrend": null,
  "probabilityOfPrecipitation": {
    "unitCode": "wmoUnit:percent",
    "value": 20
  },
  "windSpeed": "8 to 12 mph",
  "windDirection": "SW",
  "icon": "https://api.weather.gov/icons/land/night/few?size=medium",
  "shortForecast": "Mostly Clear",
  "detailedForecast": "Mostly clear, with a low around 28..."
}
```

### Hourly Forecast Response Fields

Each of 156 hours:

```json
{
  "number": 1,
  "startTime": "2026-01-01T19:00:00-07:00",
  "endTime": "2026-01-01T20:00:00-07:00",
  "isDaytime": false,
  "temperature": 35,
  "temperatureUnit": "F",
  "dewpoint": {
    "unitCode": "wmoUnit:degC",
    "value": -5.5
  },
  "relativeHumidity": {
    "unitCode": "wmoUnit:percent",
    "value": 45
  },
  "probabilityOfPrecipitation": {
    "unitCode": "wmoUnit:percent",
    "value": 0
  },
  "windSpeed": "8 mph",
  "windDirection": "SSW",
  "shortForecast": "Partly Cloudy"
}
```

### Raw Grid Data Fields

The `/gridpoints/{office}/{x},{y}` endpoint returns time-series arrays for:

| Category | Measurements |
|----------|--------------|
| **Temperature** | temperature, dewpoint, apparentTemperature, windChill, heatIndex, wetBulbGlobeTemperature |
| **Wind** | windSpeed, windGust, windDirection, transportWindSpeed, transportWindDirection |
| **Precipitation** | probabilityOfPrecipitation, quantitativePrecipitation, snowfallAmount, iceAccumulation |
| **Sky** | skyCover, visibility, ceilingHeight |
| **Other** | relativeHumidity, mixingHeight, snowLevel, probabilityOfThunder |
| **Daily** | maxTemperature, minTemperature |

### Alert Response Fields

```json
{
  "id": "https://api.weather.gov/alerts/urn:oid:...",
  "areaDesc": "El Paso County",
  "geocode": { "SAME": ["008041"], "UGC": ["COZ085"] },
  "sent": "2026-01-01T10:00:00-07:00",
  "effective": "2026-01-01T10:00:00-07:00",
  "expires": "2026-01-02T10:00:00-07:00",
  "status": "Actual",
  "messageType": "Alert",
  "severity": "Moderate",
  "certainty": "Likely",
  "urgency": "Expected",
  "event": "Wind Advisory",
  "headline": "Wind Advisory issued...",
  "description": "Southwest winds 25 to 35 mph...",
  "instruction": "Secure outdoor objects..."
}
```

### Python Example: Fetch All Weather Data

```python
import requests

BASE = "https://api.weather.gov"
LAT, LON = 38.9194, -104.7509

# Step 1: Get grid coordinates from point
point = requests.get(f"{BASE}/points/{LAT},{LON}").json()
forecast_url = point["properties"]["forecast"]
hourly_url = point["properties"]["forecastHourly"]
grid_url = point["properties"]["forecastGridData"]

# Step 2: Get forecasts
forecast = requests.get(forecast_url).json()
hourly = requests.get(hourly_url).json()
grid = requests.get(grid_url).json()

# Step 3: Get alerts
alerts = requests.get(f"{BASE}/alerts/active?point={LAT},{LON}").json()

# Access data
for period in forecast["properties"]["periods"]:
    print(f"{period['name']}: {period['temperature']}°F - {period['shortForecast']}")

for alert in alerts["features"]:
    print(f"ALERT: {alert['properties']['event']} - {alert['properties']['headline']}")
```

### Response Formats

The API supports multiple formats via `Accept` header:

| Format | Accept Header |
|--------|---------------|
| GeoJSON (default) | `application/geo+json` |
| JSON-LD | `application/ld+json` |
| DWML | `application/vnd.noaa.dwml+xml` |
| OXML | `application/vnd.noaa.obs+xml` |
| CAP | `application/cap+xml` |
| ATOM | `application/atom+xml` |

---

## Updated App Architecture

### Data Sources (Recommended Priority)

```
1. NWS JSON API (Primary - cleanest data)
   ├── /points/{lat},{lon}     → Grid lookup
   ├── /gridpoints/.../forecast → 7-day
   ├── /gridpoints/.../forecast/hourly → 156 hours
   ├── /gridpoints/...         → Raw measurements
   └── /alerts/active?point=   → Current alerts

2. CPC Extended Outlooks (HTML scraping)
   ├── 6-10 Day (fxus06.html)
   ├── 30-Day (fxus07.html)
   ├── 90-Day (fxus05.html)
   └── Week-2 Hazards
       ├── Text (.php)
       └── KML polygons

3. CPC Expert Assessments (HTML scraping)
   ├── ENSO Discussion (ensodisc.shtml)
   ├── Global Tropics Hazards (ghazards/)
   └── Drought Portal (Drought/)

4. HTML Fallback (if API unavailable)
   └── forecast.weather.gov with FcstType=digital
```

### Python Libraries

```python
import requests              # HTTP fetching (works for JSON API)
import xml.etree.ElementTree as ET  # KML parsing
from shapely.geometry import Point, Polygon  # Point-in-polygon
# BeautifulSoup only needed for CPC HTML pages
```

---

## Scheduled Task Hidden Window Wrapper

All weather logger scheduled tasks use a VBScript wrapper to run completely hidden (no window pop-ups during games or fullscreen apps).

**File:** `D:\Scripts\run_hidden.vbs`

**How it works:**
- Tasks call `wscript.exe run_hidden.vbs "command"` instead of batch files directly
- VBScript's `WScript.Shell.Run` with window style 0 = truly invisible
- No window flash, even briefly (unlike PowerShell `-WindowStyle Hidden` which briefly flashes)

**Task action format:**
```
Execute: wscript.exe
Arguments: "D:\Scripts\run_hidden.vbs" "D:\Scripts\run_gfs_logger.bat"
```

**To recreate all tasks with hidden windows:**
```powershell
# Run as Administrator
D:\Scripts\Create-GFSLoggerTask.ps1
D:\Scripts\Create-NBMLoggerTask.ps1
D:\Scripts\Create-CPCLoggerTask.ps1
D:\Scripts\Create-METARLoggerTask.ps1
D:\Scripts\Create-WeatherLoggerTask.ps1
```

---

## Weather Forecast Logger (Automated Data Collection)

**Purpose:** Collects NWS forecast data every 6 hours for historical analysis, spaghetti charts, and forecast accuracy tracking.

### Files

| File | Purpose |
|------|---------|
| `D:\Scripts\weather_logger.py` | Python script that fetches and stores data |
| `D:\Scripts\weather_data\weather.db` | SQLite database with all collected data |
| `D:\Scripts\weather_logger.log` | Log file for debugging |
| `D:\Scripts\Create-WeatherLoggerTask.ps1` | Admin script to create scheduled task |

### Schedule

Runs every 3 hours via Windows Task Scheduler (offset to :05 to avoid race conditions):
- 00:05, 03:05, 06:05, 09:05, 12:05, 15:05, 18:05, 21:05

### Database Schema

**digital_forecast** - Hourly from HTML scrape (often more accurate)
- `fetch_time` - When data was fetched
- `forecast_date`, `forecast_hour` - When forecast is for
- `temperature`, `dewpoint`, `heat_index`, `wind_speed`, `wind_direction`
- `sky_cover`, `precip_probability`, `relative_humidity`

**Note:** JSON API and digital forecast can differ significantly (up to 10-15°F observed). Digital forecast appears more accurate.

**forecast_snapshots** - 7-day forecast (14 periods)
- `fetch_time` - When data was fetched
- `forecast_time` - When forecast is valid for
- `period_name`, `temperature`, `wind_speed`, `wind_direction`
- `precipitation_probability`, `short_forecast`, `detailed_forecast`

**hourly_snapshots** - 156-hour forecast
- Same structure with `dewpoint`, `humidity`

**alerts** - Active weather alerts
- `event`, `severity`, `headline`, `description`, `effective`, `expires`

**actual_snowfall** - NOAA NCEI daily snowfall measurements
- `station_id` - GHCN station ID (e.g., USW00093037)
- `station_name` - Station name (e.g., "COLORADO SPRINGS MUNICIPAL AP")
- `observation_date` - Date of measurement
- `snowfall_inches` - Snowfall amount (NULL for missing "M", 0.001 for trace "T")
- `fetch_time` - When data was fetched from NOAA
- Data source: `https://www.ncei.noaa.gov/access/monitoring/daily-snow/CO-snowfall-YYYYMM.csv`
- Filters for El Paso county stations (35 stations total)
- Primary station: Colorado Springs Municipal AP (USW00093037)
- Uses INSERT OR REPLACE to update existing station/date records
- Note: NOAA data has 1-2 day delay (yesterday's snowfall appears today)

**observations** - Actual weather observations from KCOS (Colorado Springs Municipal Airport)
- `station_id` - Weather station ID (KCOS)
- `observation_time` - When observation was taken (every 5 minutes)
- `temperature_c`, `temperature_f` - Air temperature
- `dewpoint_c`, `dewpoint_f` - Dewpoint temperature
- `wind_speed_ms`, `wind_speed_mph` - Wind speed
- `wind_direction_deg` - Wind direction (degrees)
- `wind_gust_ms`, `wind_gust_mph` - Wind gust speed
- `barometric_pressure_pa`, `sea_level_pressure_pa` - Pressure readings
- `visibility_m`, `visibility_mi` - Visibility distance
- `relative_humidity` - Humidity percentage
- `wind_chill_c`, `wind_chill_f` - Wind chill (if applicable)
- `heat_index_c`, `heat_index_f` - Heat index (if applicable)
- `cloud_coverage` - Cloud layer descriptions
- `text_description` - Weather conditions text
- `raw_message` - Raw METAR message
- Data source: `https://api.weather.gov/stations/KCOS/observations`
- Updates every 5 minutes from KCOS
- Uses INSERT OR IGNORE to skip duplicate observations
- Essential for forecast verification - compare predictions vs actual conditions

**metar** - Parsed METAR observations from AirNav (KCOS)
- `observation_time` - When the METAR was issued (UTC)
- `fetch_time` - When data was scraped from AirNav
- `raw_metar` - Complete raw METAR string
- `wind_direction_deg` - Wind direction (degrees, NULL for variable)
- `wind_speed_kt` - Wind speed (knots)
- `wind_gust_kt` - Wind gust (knots, NULL if no gusts)
- `visibility_sm` - Visibility (statute miles)
- `temperature_c` - Temperature (Celsius)
- `dewpoint_c` - Dewpoint (Celsius)
- `altimeter_inhg` - Altimeter setting (inches of mercury)
- `ceiling_ft` - Lowest broken/overcast layer (feet AGL, NULL if clear/few/scattered)
- `flight_category` - VFR, MVFR, IFR, or LIFR based on ceiling and visibility
- `sky_conditions` - Sky cover layers (e.g., "FEW004 OVC009")
- `weather_phenomena` - Present weather codes (e.g., "-SN BR" for light snow and mist)
- Data source: `https://www.airnav.com/airport/KCOS`
- Uses INSERT OR IGNORE to skip duplicate observations
- Flight categories: LIFR (<200ft or <0.5SM), IFR (<500ft or <1SM), MVFR (<1000ft or <3SM), VFR (≥1000ft and ≥3SM)

### Spaghetti Chart Queries

```sql
-- How did the forecast for Jan 10 evolve over time?
SELECT fetch_time, temperature, precipitation_probability, short_forecast
FROM forecast_snapshots
WHERE date(forecast_time) = '2026-01-10' AND is_daytime = 1
ORDER BY fetch_time;

-- Temperature forecast accuracy (compare to actuals)
SELECT date(forecast_time) as date,
       MIN(fetch_time) as first_forecast,
       MAX(fetch_time) as last_forecast,
       AVG(temperature) as avg_predicted_temp
FROM forecast_snapshots
WHERE is_daytime = 1
GROUP BY date(forecast_time);

-- All forecasts made for a specific day
SELECT fetch_time, temperature, short_forecast
FROM forecast_snapshots
WHERE forecast_time LIKE '2026-01-10%'
ORDER BY fetch_time, forecast_time;
```

### Snowfall Data Queries

```sql
-- Get actual snowfall for Colorado Springs airport
SELECT observation_date, snowfall_inches
FROM actual_snowfall
WHERE station_id = 'USW00093037'
ORDER BY observation_date DESC;

-- All El Paso county stations with snowfall on a specific date
SELECT station_name, snowfall_inches
FROM actual_snowfall
WHERE observation_date = '2026-01-08'
  AND snowfall_inches > 0
ORDER BY snowfall_inches DESC;

-- Compare forecast vs actual (join example)
SELECT
    a.observation_date,
    a.snowfall_inches as actual,
    fs.short_forecast as forecast_text
FROM actual_snowfall a
LEFT JOIN forecast_snapshots fs ON date(fs.forecast_time) = a.observation_date
WHERE a.station_id = 'USW00093037'
  AND fs.is_daytime = 1
GROUP BY a.observation_date;
```

### Observation Queries

```sql
-- Recent observations from KCOS
SELECT observation_time, temperature_f, wind_speed_mph, text_description
FROM observations
ORDER BY observation_time DESC
LIMIT 24;

-- Compare forecast vs actual temperature
SELECT
    date(o.observation_time) as date,
    AVG(o.temperature_f) as actual_temp,
    f.temperature as forecast_temp,
    ABS(AVG(o.temperature_f) - f.temperature) as error
FROM observations o
JOIN forecast_snapshots f ON date(o.observation_time) = date(f.forecast_time)
WHERE f.is_daytime = 1
GROUP BY date(o.observation_time);

-- Hourly observation summary for a specific day
SELECT
    strftime('%H:00', observation_time) as hour,
    temperature_f,
    wind_speed_mph,
    wind_direction_deg,
    text_description
FROM observations
WHERE date(observation_time) = '2026-01-08'
ORDER BY observation_time;
```

### METAR Queries

```sql
-- Recent METARs from KCOS
SELECT observation_time, flight_category, temperature_c, visibility_sm,
       ceiling_ft, weather_phenomena
FROM metar
ORDER BY observation_time DESC
LIMIT 24;

-- Flight category history for a specific day
SELECT observation_time, flight_category, ceiling_ft, visibility_sm, raw_metar
FROM metar
WHERE date(observation_time) = '2026-01-08'
ORDER BY observation_time;

-- Weather phenomena summary (snow, rain, fog events)
SELECT observation_time, weather_phenomena, visibility_sm, ceiling_ft
FROM metar
WHERE weather_phenomena IS NOT NULL AND weather_phenomena != ''
ORDER BY observation_time DESC
LIMIT 50;

-- Compare METAR temp vs NWS observation temp
SELECT m.observation_time,
       m.temperature_c as metar_temp_c,
       o.temperature_c as nws_temp_c,
       m.flight_category
FROM metar m
JOIN observations o ON strftime('%Y-%m-%d %H', m.observation_time) =
                       strftime('%Y-%m-%d %H', o.observation_time)
ORDER BY m.observation_time DESC
LIMIT 24;
```

### Querying the Database

**From WSL:** Use Python (sqlite3 command not installed):
```bash
python3 -c "
import sqlite3
conn = sqlite3.connect('/mnt/d/Scripts/weather_data/weather.db')
cur = conn.cursor()
cur.execute('SELECT datetime(fetch_time, \"localtime\"), COUNT(*) FROM forecast_snapshots GROUP BY strftime(\"%Y-%m-%d %H\", fetch_time) ORDER BY fetch_time DESC LIMIT 10')
for row in cur.fetchall(): print(row)
conn.close()
"
```

**From PowerShell:** Use Python with Windows path:
```powershell
D:\Python313\python.exe -c "import sqlite3; conn = sqlite3.connect('D:\\Scripts\\weather_data\\weather.db'); print(conn.execute('SELECT COUNT(*) FROM forecast_snapshots').fetchone()[0]); conn.close()"
```

**Note:** WSL does not have `sqlite3` CLI installed. Always use Python's sqlite3 module to query the database.

### Manual Run

```powershell
D:\Python313\python.exe D:\Scripts\weather_logger.py
```

### Setup (First Time)

Run as Administrator:
```powershell
D:\Scripts\Create-WeatherLoggerTask.ps1
```

---

## Spaghetti Chart Visualization

**Script:** `D:\Scripts\weather_spaghetti.py`

Generates spaghetti charts showing how forecasts evolved over time - revealing forecast uncertainty and model convergence/divergence as target dates approach.

### Usage

```bash
python D:\Scripts\weather_spaghetti.py                    # Temperature chart (default)
python D:\Scripts\weather_spaghetti.py --metric snow     # Snow accumulation chart
python D:\Scripts\weather_spaghetti.py --metric precip   # Precipitation probability chart
python D:\Scripts\weather_spaghetti.py --save            # Save to file (auto-named)
python D:\Scripts\weather_spaghetti.py --save output.png # Save to specific file
```

### Available Metrics

| Metric | Description |
|--------|-------------|
| `temp` (default) | High/low temperature forecasts - two-panel chart |
| `snow` | Snow accumulation with uncertainty bands |
| `precip` | Precipitation probability evolution |

### Output

- **Display mode (default):** Opens matplotlib window
- **Save mode:** Saves to `D:\Scripts\weather_data\spaghetti_{metric}_{timestamp}.png`

### Chart Features

- Each line represents a different target date
- X-axis: When the forecast was made
- Y-axis: Predicted value
- Snow chart includes shaded uncertainty bands (low to high range)
- Annotations show current predicted ranges

### Example Insights

From 5 days of data during January 2026 storm:
- Snow forecast started at 1-2" (Jan 3-4)
- Dropped to ~1" (models backed off)
- Ramped dramatically to 8-11" (Jan 7)
- Current: 4-8" Thursday, 5-10" Friday

---

## NOAA GFS Raw Model Data (AWS Open Data)

### Overview

The Global Forecast System (GFS) is NOAA's primary global weather prediction model. Raw model output is freely available via AWS S3 bucket with no authentication required.

**Registry:** https://registry.opendata.aws/noaa-gfs-bdp-pds/
**S3 Bucket:** `s3://noaa-gfs-bdp-pds/` (public, no credentials needed)

### What GFS Provides vs NWS API

| Data Source | Pros | Cons |
|-------------|------|------|
| **NWS API** | Bias-corrected, interpreted forecasts; easy to use JSON | Already processed; may not include all raw parameters |
| **GFS Raw** | 743 parameters per forecast hour; raw model output; global 0.25° grid | Large files (~500 MB); needs GRIB2 decoding; not bias-corrected |

**When to use GFS raw data:**
- Need parameters not in NWS API (CAPE, visibility, categorical precip types, etc.)
- Want to compare raw model vs bias-corrected forecasts
- Research/analysis of model performance
- Global data outside US

### S3 Bucket Structure

```
s3://noaa-gfs-bdp-pds/
├── gfs.YYYYMMDD/               # Model run date
│   ├── 00/                     # 00Z cycle (midnight UTC)
│   │   ├── atmos/              # Atmospheric products
│   │   │   ├── gfs.t00z.pgrb2.0p25.f000   # Analysis (513 MB)
│   │   │   ├── gfs.t00z.pgrb2.0p25.f001   # +1 hour forecast
│   │   │   ├── gfs.t00z.pgrb2.0p25.f024   # +24 hour forecast
│   │   │   └── ... up to f384             # +16 days
│   │   └── wave/               # Wave model data
│   ├── 06/                     # 06Z cycle
│   ├── 12/                     # 12Z cycle
│   └── 18/                     # 18Z cycle
└── sst.YYYYMMDD/               # Sea surface temperature
```

### Model Run Schedule

| Cycle | Model Run Time (UTC) | Data Available (approx) |
|-------|---------------------|------------------------|
| 00Z | Midnight UTC | ~03:30 UTC |
| 06Z | 6:00 AM UTC | ~09:30 UTC |
| 12Z | Noon UTC | ~15:30 UTC |
| 18Z | 6:00 PM UTC | ~21:30 UTC |

### File Sizes and Forecast Resolution

| Forecast Range | Time Step | File Size |
|----------------|-----------|-----------|
| f000-f120 | Hourly | ~500-540 MB each |
| f123-f384 | 3-hourly | ~500-540 MB each |

**Total forecast horizon:** 384 hours (16 days)
**Grid resolution:** 0.25° × 0.25° (~28 km at equator)

### Available Parameters (743 total in pgrb2 files)

Key surface/near-surface parameters:

| Parameter | Description | Units |
|-----------|-------------|-------|
| 2m Temperature | Air temperature at 2 meters | K |
| 2m Dewpoint | Dewpoint temperature at 2 meters | K |
| 2m Relative Humidity | Humidity at 2 meters | % |
| Apparent Temperature | "Feels like" temperature | K |
| Max/Min Temperature | 6-hour max/min | K |
| Total Precipitation | Accumulated precipitation | kg/m² (mm) |
| Snow Depth | Accumulated snow depth | m |
| Snow Water Equivalent | Water content of snow | kg/m² |
| Categorical Snow | Yes/No snow flag | 0 or 1 |
| Categorical Rain | Yes/No rain flag | 0 or 1 |
| Categorical Freezing Rain | Yes/No freezing rain | 0 or 1 |
| Percent Frozen Precip | Percentage frozen | % |
| 10m U/V Wind | Wind components at 10m | m/s |
| Wind Gust | Maximum gust speed | m/s |
| Total Cloud Cover | Percentage cloud cover | % |
| Visibility | Horizontal visibility | m |
| MSLP | Mean sea level pressure | Pa |
| CAPE | Convective Available Potential Energy | J/kg |

### GFS Extractor Script

**Location:** `D:\Scripts\gfs_extractor.py`

Extracts Colorado Springs weather data from GFS GRIB2 files.

**Requirements:**
- AWS CLI v2 (installed via official installer)
- pygrib library (in virtual environment)
- numpy

**Usage:**
```bash
# Activate virtual environment (if using one)
source /tmp/grib-env/bin/activate

# Latest model run, 24-hour forecast
python D:\Scripts\gfs_extractor.py

# Multiple forecast hours
python D:\Scripts\gfs_extractor.py --hours 0 24 48 72

# Specific model run
python D:\Scripts\gfs_extractor.py --date 20260108 --cycle 00

# List available model runs
python D:\Scripts\gfs_extractor.py --list

# Keep downloaded files (default: auto-delete)
python D:\Scripts\gfs_extractor.py --keep
```

**Example Output:**
```
GFS Forecast for Colorado Springs, CO
Coordinates: 38.9194°N, 104.7509°W
Model Run: 20260108 00Z
Forecast Hour: +24h
Valid Time: 2026-01-09 00:00 UTC
======================================================================

Temperature:
  2m Temperature           :    29.46 °F
  2m Dewpoint              :    29.00 °F
  Apparent Temperature     :    24.16 °F
  Max Temperature          :    32.14 °F
  Min Temperature          :    29.44 °F
  2m Relative Humidity     :    97.90 %

Precipitation:
  Total Precipitation      :     0.25 in
  Snow Depth               :     1.45 in
  Categorical Snow         : Yes
  Percent Frozen Precip    :   100.00 %

Wind:
  10m Wind Speed           :     3.91 mph
  10m Wind Direction       : 87° (E)
  Wind Gust                :     8.96 mph

Other:
  Total Cloud Cover        :   100.00 %
  Visibility               :     0.05 mi
  Sea Level Pressure       :  1005.94 mb
  CAPE                     :     9.00 J/kg
```

### AWS CLI Installation (WSL/Linux)

Ubuntu Noble doesn't have awscli in apt repositories. Use official installer:

```bash
# Download and install AWS CLI v2
curl "https://awscli.amazonaws.com/awscli-exe-linux-x86_64.zip" -o "/tmp/awscliv2.zip"
unzip -o /tmp/awscliv2.zip -d /tmp
sudo /tmp/aws/install

# Verify
aws --version  # aws-cli/2.32.31 ...
```

### pygrib Installation (WSL/Linux)

Ubuntu's PEP 668 prevents pip install to system Python. Use virtual environment:

```bash
python3 -m venv /tmp/grib-env
source /tmp/grib-env/bin/activate
pip install pygrib numpy
```

### Raw AWS Commands

```bash
# List available model runs (most recent)
aws s3 ls s3://noaa-gfs-bdp-pds/ --no-sign-request | tail -10

# List files for a specific run
aws s3 ls s3://noaa-gfs-bdp-pds/gfs.20260108/00/atmos/ --no-sign-request

# Download a specific forecast file
aws s3 cp s3://noaa-gfs-bdp-pds/gfs.20260108/00/atmos/gfs.t00z.pgrb2.0p25.f024 /tmp/ --no-sign-request
```

### Comparison: GFS vs NWS API for Colorado Springs (Jan 8, 2026)

| Parameter | GFS Raw | NWS API | Notes |
|-----------|---------|---------|-------|
| Temperature | 29°F | 24°F | GFS ~5° higher |
| Cloud Cover | 100% | 100% | Agreement |
| Precip Type | Categorical Snow = Yes | "Snow" | Same |
| Visibility | 0.05 mi | Not provided | GFS advantage |
| CAPE | 9 J/kg | Not provided | GFS advantage |

**Key insight:** NWS applies local bias corrections to GFS output. Raw GFS may differ by 5-10°F from NWS forecasts, especially in complex terrain like Colorado Springs.

---

## GFS Forecast Logger (Automated Data Collection)

**Purpose:** Collects raw GFS model data 4x daily for Colorado Springs. Provides additional forecast parameters not available in NWS API (visibility, CAPE, categorical precipitation types).

### Files

| File | Purpose |
|------|---------|
| `D:\Scripts\gfs_logger.py` | Python script that downloads GRIB2 files and extracts data |
| `D:\Scripts\run_gfs_logger.bat` | Batch wrapper that sets up conda environment |
| `D:\Scripts\weather_data\weather.db` | SQLite database (same as weather_logger.py) |
| `D:\Scripts\gfs_logger.log` | Log file for debugging |
| `D:\Scripts\Create-GFSLoggerTask.ps1` | Admin script to create scheduled task |

### Schedule

Runs 4x daily via Windows Task Scheduler, aligned with GFS model availability (offset to :30 to avoid race conditions):
| Local (MST) | GFS Run |
|-------------|---------|
| 21:30 | 00Z |
| 03:30 | 06Z |
| 09:30 | 12Z |
| 15:30 | 18Z |

GFS data is available ~3.5-4 hours after each model run.

### Requirements

- **pygrib library** (requires conda environment due to native dependencies)
- **Conda environment:** `C:\Users\manch\.conda\envs\gfs-logger`

```bash
# Setup conda environment (one-time)
conda create -n gfs-logger python=3.12 pygrib numpy requests -c conda-forge -y
```

The scheduled task uses `run_gfs_logger.bat` which sets up the conda PATH automatically.

### Database Schema

**gfs_forecasts** - Raw GFS model output for Colorado Springs
- `fetch_time` - When data was fetched
- `model_run_date` - GFS model date (YYYYMMDD)
- `model_run_cycle` - GFS cycle (00, 06, 12, 18)
- `forecast_hour` - Hours from model run (6, 12, 24, 48, 72, 120, 168)
- `valid_time` - When forecast is valid for
- Temperature: `temp_2m_k`, `dewpoint_2m_k`, `apparent_temp_k`
- Humidity: `rh_2m`
- Precipitation: `total_precip_mm`, `snow_depth_m`, `cat_snow`, `cat_rain`, `cat_freezing_rain`, `pct_frozen`
- Wind: `u_wind_10m_ms`, `v_wind_10m_ms`, `wind_speed_ms`, `wind_direction_deg`, `wind_gust_ms`
- Other: `cloud_cover_pct`, `visibility_m`, `mslp_pa`, `cape_jkg`

**Note:** f000 (analysis) files are skipped - they have a different GRIB structure that causes invalid data extraction.

### Forecast Hours Collected

| Hour | Lead Time |
|------|-----------|
| f006 | 6 hours |
| f012 | 12 hours |
| f024 | 1 day |
| f048 | 2 days |
| f072 | 3 days |
| f120 | 5 days |
| f168 | 7 days |

### Example Queries

```sql
-- Latest GFS forecast for Colorado Springs
SELECT valid_time,
       (temp_2m_k - 273.15) * 9/5 + 32 as temp_f,
       wind_speed_ms * 2.237 as wind_mph,
       cloud_cover_pct,
       cat_snow
FROM gfs_forecasts
WHERE model_run_date = (SELECT MAX(model_run_date) FROM gfs_forecasts)
ORDER BY forecast_hour;

-- Compare GFS vs NWS for same valid time
SELECT g.valid_time,
       (g.temp_2m_k - 273.15) * 9/5 + 32 as gfs_temp_f,
       f.temperature as nws_temp_f,
       g.cat_snow as gfs_snow,
       f.short_forecast as nws_forecast
FROM gfs_forecasts g
JOIN forecast_snapshots f ON datetime(g.valid_time) = datetime(f.forecast_time)
WHERE g.model_run_date = (SELECT MAX(model_run_date) FROM gfs_forecasts);
```

### Manual Run

```powershell
# From PowerShell - use the batch wrapper
D:\Scripts\run_gfs_logger.bat
```

### Setup (First Time)

Run as Administrator in PowerShell:
```powershell
D:\Scripts\Create-GFSLoggerTask.ps1
```

---

## NBM Forecast Logger (National Blend of Models)

**Purpose:** Collects NBM model data 4x daily for Colorado Springs. NBM statistically blends multiple models (GFS, HRRR, RAP, NAM, ECMWF, and more) into a single high-accuracy forecast at 2.5km resolution.

### Why NBM?

NBM is considered one of the most accurate operational forecasts because it:
- Combines strengths of multiple models while compensating for their weaknesses
- Uses statistical post-processing to reduce systematic biases
- Provides probabilistic forecasts (confidence levels)
- Has 2.5km resolution (vs 25km for GFS)

### Files

| File | Purpose |
|------|---------|
| `D:\Scripts\nbm_logger.py` | Python script that downloads GRIB2 files and extracts data |
| `D:\Scripts\run_nbm_logger.bat` | Batch wrapper that sets up conda environment |
| `D:\Scripts\weather_data\weather.db` | SQLite database (same as other loggers) |
| `D:\Scripts\nbm_logger.log` | Log file for debugging |
| `D:\Scripts\Create-NBMLoggerTask.ps1` | Admin script to create scheduled task |

### Schedule

Runs 4x daily via Windows Task Scheduler (offset to :10 to avoid METAR conflict at :00):
| Local (MST) | NBM Run |
|-------------|---------|
| 00:10 | 23Z |
| 06:10 | 05Z |
| 12:10 | 11Z |
| 18:10 | 17Z |

NBM runs hourly but we only need 4x daily to capture key forecasts.

### Requirements

- **pygrib library** (uses same conda environment as GFS logger)
- **Conda environment:** `C:\Users\manch\.conda\envs\gfs-logger`
- **Storage note:** NBM files are large (100-170 MB each), ~1 GB total per run

### Database Schema

**nbm_forecasts** - National Blend of Models output for Colorado Springs
- `fetch_time` - When data was fetched
- `model_run_date` - NBM model date (YYYYMMDD)
- `model_run_cycle` - NBM cycle (00-23)
- `forecast_hour` - Hours from model run (1, 6, 12, 24, 36, 48, 72, 96, 120, 168)
- `valid_time` - When forecast is valid for
- Temperature: `temp_2m_k`, `dewpoint_2m_k`, `apparent_temp_k`, `max_temp_k`, `min_temp_k`
- Humidity: `rh_2m`
- Precipitation: `total_precip_mm`, `snow_amt_mm`, `prob_precip_pct`, `prob_snow_pct`
- Wind: `u_wind_10m_ms`, `v_wind_10m_ms`, `wind_speed_ms`, `wind_direction_deg`, `wind_gust_ms`
- Other: `sky_cover_pct`, `visibility_m`, `ceiling_m`

### Forecast Hours Collected

| Hour | Lead Time |
|------|-----------|
| f001 | 1 hour |
| f006 | 6 hours |
| f012 | 12 hours |
| f024 | 1 day |
| f036 | 1.5 days |
| f048 | 2 days |
| f072 | 3 days |
| f096 | 4 days |
| f120 | 5 days |
| f168 | 7 days |

### Example Queries

```sql
-- Latest NBM forecast for Colorado Springs
SELECT valid_time,
       (temp_2m_k - 273.15) * 9/5 + 32 as temp_f,
       sky_cover_pct,
       prob_precip_pct
FROM nbm_forecasts
WHERE model_run_date = (SELECT MAX(model_run_date) FROM nbm_forecasts)
ORDER BY forecast_hour;

-- Compare NBM vs GFS for same valid time
SELECT n.valid_time,
       (n.temp_2m_k - 273.15) * 9/5 + 32 as nbm_temp_f,
       (g.temp_2m_k - 273.15) * 9/5 + 32 as gfs_temp_f,
       n.sky_cover_pct as nbm_sky,
       g.cloud_cover_pct as gfs_sky
FROM nbm_forecasts n
JOIN gfs_forecasts g ON datetime(n.valid_time) = datetime(g.valid_time)
WHERE n.model_run_date = (SELECT MAX(model_run_date) FROM nbm_forecasts);
```

### Manual Run

```powershell
# From PowerShell - use the batch wrapper
D:\Scripts\run_nbm_logger.bat
```

### Setup (First Time)

Run as Administrator in PowerShell:
```powershell
D:\Scripts\Create-NBMLoggerTask.ps1
```

---

## CPC Outlook Logger (8-14 Day + Monthly)

**Purpose:** Collects CPC extended outlooks for long-range forecast data not available in NWS API. These outlooks provide temperature and precipitation probabilities for 8-14 days and the coming month.

### Files

| File | Purpose |
|------|---------|
| `D:\Scripts\cpc_logger.py` | Python script that fetches and parses CPC outlook pages |
| `D:\Scripts\run_cpc_logger.bat` | Batch wrapper script |
| `D:\Scripts\weather_data\weather.db` | SQLite database (shared with other loggers) |
| `D:\Scripts\cpc_logger.log` | Log file for debugging |
| `D:\Scripts\Create-CPCLoggerTask.ps1` | Admin script to create scheduled task |

### Schedule

Runs daily at 4:00 PM local time (CPC outlooks are issued around 3 PM EST daily).
Also runs at logon to catch up on missed updates.

### Data Sources

| Outlook | URL | Valid Period |
|---------|-----|--------------|
| **8-14 Day** | `https://www.cpc.ncep.noaa.gov/products/predictions/610day/fxus06.html` | Days 8-14 |
| **Monthly** | `https://www.cpc.ncep.noaa.gov/products/predictions/long_range/fxus07.html` | 30 days |

### Database Schema

**cpc_outlooks** - CPC extended outlook discussions
- `fetch_time` - When data was fetched
- `outlook_type` - "8_14_day" or "monthly"
- `issued_date` - When CPC issued the outlook
- `valid_start` - Start of valid period
- `valid_end` - End of valid period
- `discussion` - Full text of the outlook discussion

### Example Queries

```sql
-- Latest 8-14 day outlook
SELECT issued_date, valid_start, valid_end, discussion
FROM cpc_outlooks
WHERE outlook_type = '8_14_day'
ORDER BY issued_date DESC
LIMIT 1;

-- All outlooks for the past week
SELECT outlook_type, issued_date, valid_start, valid_end,
       length(discussion) as chars
FROM cpc_outlooks
WHERE issued_date >= date('now', '-7 days')
ORDER BY issued_date DESC;
```

### Manual Run

```powershell
D:\Python313\python.exe D:\Scripts\cpc_logger.py
```

### Setup (First Time)

Run as Administrator:
```powershell
D:\Scripts\Create-CPCLoggerTask.ps1
```

---

## Hourly Weather Logger (METAR + Digital Forecast)

**Purpose:** Collects hourly data that updates more frequently than the 3-hour weather_logger:
- METAR from multiple Colorado Springs area airports
- NWS Digital Forecast (hourly tabular data)

### Files

| File | Purpose |
|------|---------|
| `D:\Scripts\metar_logger.py` | Python script for hourly collection |
| `D:\Scripts\metar_logger.log` | Log file for debugging |
| `D:\Scripts\Create-METARLoggerTask.ps1` | Admin script to create scheduled task |
| `D:\Scripts\weather_data\weather.db` | SQLite database (shared with weather_logger) |

### Data Collected

**METAR Stations:**
| Station | Airport | Notes |
|---------|---------|-------|
| **KCOS** | Colorado Springs Municipal Airport | Primary station |
| **KFLY** | Meadow Lake Airport | North Colorado Springs |
| **KAFF** | USAF Academy Airfield | North Colorado Springs |
| **KFCS** | Fort Carson | South Colorado Springs |
| **KAPA** | Centennial Airport | Denver area (~50 mi N) |
| **KPUB** | Pueblo Memorial Airport | ~40 mi S |

**Digital Forecast:** NWS hourly tabular forecast (24 hours of temperature, wind, precip probability, etc.)

### Schedule

Runs every hour at :00 via Windows Task Scheduler ("Hourly Weather Logger" task).

**Note:** Other weather scripts are staggered to avoid race conditions (weather_logger at :05, gfs_logger at :15).

### Queries

```sql
-- Latest METAR from all stations
SELECT station_id, observation_time, temperature_c, visibility_sm,
       flight_category, weather_phenomena
FROM metar
WHERE observation_time > datetime('now', '-2 hours')
ORDER BY station_id, observation_time DESC;

-- Compare conditions across stations
SELECT station_id, temperature_c, wind_speed_kt, ceiling_ft, flight_category
FROM metar m
WHERE observation_time = (
    SELECT MAX(observation_time) FROM metar WHERE station_id = m.station_id
)
ORDER BY station_id;

-- Hourly digital forecast evolution
SELECT fetch_time, forecast_date, forecast_hour, temperature, precip_probability
FROM digital_forecast
WHERE forecast_date = '2026-01-09'
ORDER BY fetch_time, forecast_hour;
```

### Manual Run

```powershell
D:\Python313\python.exe D:\Scripts\metar_logger.py
```

### Setup (First Time)

Run as Administrator:
```powershell
D:\Scripts\Create-METARLoggerTask.ps1
```

---

## Centralized Database Utilities (db_utils.py)

All weather scripts use a shared `db_utils.py` module for database access. This ensures consistent configuration, retry logic, and path handling across all scripts.

### Module Location

`D:\Scripts\db_utils.py`

### Key Functions

| Function | Purpose |
|----------|---------|
| `get_connection(db_path=None)` | Creates connection with WAL mode, busy timeout |
| `execute_with_retry(operation, conn, description)` | Retries on "database is locked" and "disk I/O error" |
| `commit_with_retry(conn, description)` | Commit wrapper with retry logic |
| `check_database_accessible(db_path, timeout)` | Tests if database is accessible |
| `wait_for_database(db_path, max_wait, interval)` | Waits for database to become available |

### Path Constants

```python
SCRIPTS_DIR = Path("D:/Scripts")          # or /mnt/d/Scripts in WSL
DATA_DIR = SCRIPTS_DIR / "weather_data"
DB_PATH = DATA_DIR / "weather.db"
```

### Retry Settings

| Setting | Value | Purpose |
|---------|-------|---------|
| `DB_BUSY_TIMEOUT_MS` | 30000 (30s) | SQLite busy timeout |
| `DB_MAX_RETRIES` | 3 | Max retry attempts |
| `DB_RETRY_DELAY_SEC` | 10 | Delay between retries |

### Scripts Using db_utils

All 10 weather scripts import and use db_utils:
- `weather_logger.py`, `metar_logger.py`, `cpc_logger.py`
- `gfs_logger.py`, `nbm_logger.py`, `capture_pws_data.py`
- `script_metrics.py`, `check_weather_status.py`
- `weather_accuracy.py`, `weather_spaghetti.py`

### Usage Example

```python
import db_utils

# Get connection
conn = db_utils.get_connection()

# Execute with retry
def do_insert(c):
    cursor = c.cursor()
    cursor.execute("INSERT INTO ...")

db_utils.execute_with_retry(do_insert, conn, "inserting data")
db_utils.commit_with_retry(conn, "committing data")
conn.close()
```

---

## Database Corruption Protection

### WAL Mode

All weather logging scripts use SQLite WAL (Write-Ahead Logging) mode for crash resilience via `db_utils.get_connection()`.

**WAL mode provides:**
- **Crash resilience:** Database survives unexpected shutdowns (power loss, crashes)
- **Concurrent access:** Multiple readers can access while one writer is active
- **Better performance:** Reduces disk I/O for typical workloads

**PRAGMA settings applied (in db_utils.py):**
```python
conn.execute("PRAGMA journal_mode=WAL")      # Enable WAL mode
conn.execute("PRAGMA synchronous=NORMAL")    # Safe for WAL mode
conn.execute("PRAGMA wal_autocheckpoint=1000")  # Checkpoint every ~4MB
conn.execute("PRAGMA busy_timeout=30000")    # Wait 30s if locked
```

**WAL files:** When using WAL mode, SQLite creates two additional files:
- `weather.db-wal` - Write-ahead log (uncommitted transactions)
- `weather.db-shm` - Shared memory file (index for WAL)

These files are normal and should **not** be deleted while the database is in use.

### Staggered Schedules

Scripts are offset to prevent database lock conflicts:

| Script | Schedule | Frequency |
|--------|----------|-----------|
| METAR Logger | :00 | Hourly |
| PWS Data Capture | :05 | Hourly |
| Weather Forecast Logger | :05 | Every 3h |
| NBM Forecast Logger | :10 | 4x daily |
| GFS Forecast Logger | :30 | 4x daily |
| CPC Logger | 16:00 | Daily |
| Database Backup | 02:00 | Daily |

**Database Lock Handling:** Scripts include retry logic to handle concurrent access:
- **Busy timeout:** 30 seconds (PRAGMA busy_timeout=30000)
- **Retry logic:** 3 attempts with 10-second delays
- **Scripts with retry:** `nbm_logger.py`, `gfs_logger.py`, `capture_pws_data.py`

**WSL Note:** Scripts using WSL (like `capture_pws_data.py` with shot-scraper) may experience intermittent "disk I/O error" when Windows processes are actively writing to the database. The retry logic handles this.

### Daily Backups

| File | Purpose |
|------|---------|
| `D:\Scripts\Backup-WeatherDB.ps1` | Daily backup script |
| `D:\Scripts\Create-WeatherDBBackupTask.ps1` | Scheduled task creator |
| `D:\Scripts\weather_db_backup.log` | Backup log file |
| `D:\Scripts\weather_data\backups\` | Backup storage location |

**Schedule:** Daily at 2:00 AM (via "Weather Database Backup" scheduled task)

**Retention:** 7 days of rolling backups

**Backup process:**
1. Checkpoints WAL file to ensure consistency
2. Copies database to timestamped backup file
3. Deletes backups older than 7 days

### Manual Backup

```powershell
D:\Scripts\Backup-WeatherDB.ps1
```

### Database Recovery

If database becomes corrupted:

1. **Check backup directory:** `D:\Scripts\weather_data\backups\`
2. **Replace corrupted database:**
   ```bash
   cp /mnt/d/Scripts/weather_data/backups/weather_YYYYMMDD_HHMMSS.db /mnt/d/Scripts/weather_data/weather.db
   ```
3. **Refill missing data:**
   ```bash
   D:\Python313\python.exe D:\Scripts\weather_logger.py
   ```
   (NWS API provides historical observations automatically)

---

## GOES-19 Satellite Imagery (2026-01-14)

NOAA's GOES-East satellite provides full-disk Earth imagery updated every 10 minutes.

### Web Interface
```
https://www.star.nesdis.noaa.gov/GOES/fulldisk.php?sat=G19
```

### CDN Direct Access
```
https://cdn.star.nesdis.noaa.gov/GOES19/ABI/FD/GEOCOLOR/
```

### Available Products
| Product | Description |
|---------|-------------|
| **GeoColor** | True color day / IR night composite |
| **GLM Lightning** | Flash extent density |
| **Air Mass RGB** | Atmospheric analysis |
| **Fire Temperature RGB** | Wildfire detection |
| **Dust RGB** | Dust/sand storm detection |
| **Individual Bands** | All 16 spectral bands (visible through IR) |

### Image Resolutions (GeoColor)
| Resolution | File Size | Notes |
|------------|-----------|-------|
| 339 × 339 | ~115 KB | Thumbnail |
| 678 × 678 | ~391 KB | Small |
| 1808 × 1808 | ~2 MB | Medium |
| 5424 × 5424 | ~13 MB | Large |
| 10848 × 10848 | ~13 MB | Very Large |
| 21696 × 21696 | ~40 MB | Full resolution |

### Filename Format
```
YYYYDDDHHMM_GOES19-ABI-FD-GEOCOLOR-10848x10848.jpg
```
- YYYY = Year
- DDD = Day of year (001-366)
- HHMM = Time in UTC

### Data Retention
- CDN retains ~10 days of imagery
- Images posted every 10 minutes
- ~1,008 images per week at each resolution

### Download Script
Location: `D:\Scripts\download_goes_images.py`

```powershell
# Download all 10848x10848 images
python D:\Scripts\download_goes_images.py

# Dry run
python D:\Scripts\download_goes_images.py --dry-run

# Different resolution
python D:\Scripts\download_goes_images.py --resolution 5424

# Archive existing complete weeks
python D:\Scripts\download_goes_images.py --archive-now
```

**Features:**
- Multi-threaded downloads (default 4 threads)
- Auto-archives complete weeks to zip (90% threshold)
- Resumable - skips already-downloaded files
- Storage: `D:\Pictures\GOES_Images\`
- Archives: `D:\Pictures\GOES_Images\archive\`

### Scheduled Task
- **Task Name:** GOES-19 Image Downloader
- **Triggers:** Daily at 6:00 AM + At Logon
- **Setup script:** `D:\Scripts\Create-GOESDownloaderTask.ps1` (run as Admin)

**Recovery:** CDN retains ~10 days of images. If script doesn't run for a few days, next run catches up automatically (skips already-downloaded files). Only risk is 10+ days without running.

### Animation Loops
12-hour animation of GeoColor:
```
https://www.star.nesdis.noaa.gov/GOES/fulldisk_band.php?sat=G19&band=GEOCOLOR&length=12
```

---

## Future Enhancements to Research

- [x] weather.gov JSON API structure
- [x] NWS Alerts API
- [x] Automated forecast data collection (weather_logger.py)
- [x] Visualization script for spaghetti charts
- [x] Radar/satellite image sources - GOES-19 GeoColor downloader
- [x] Historical climate data (NOAA NCEI) - actual_snowfall table
- [x] GFS raw model data (AWS Open Data) - gfs_extractor.py
- [x] GFS automated data collection (gfs_logger.py) - uses conda for pygrib
- [x] Hourly METAR collection (metar_logger.py) - multi-station support
- [x] CPC extended outlook data collection (cpc_logger.py) - 8-14 day and monthly
- [x] Forecast accuracy tracking (weather_accuracy.py) - compare predictions to actual
- [ ] Air quality (AirNow API)

---

## Forecast Accuracy Visualization

**Script:** `D:\Scripts\weather_accuracy.py`

Compares NWS forecast temperatures vs actual METAR observations.

### Usage

```powershell
# Daily high/low comparison (1-day ahead forecasts)
D:\Python313\python.exe D:\Scripts\weather_accuracy.py --daily

# Hourly forecast vs actual (last 3 days)
D:\Python313\python.exe D:\Scripts\weather_accuracy.py --hourly

# Accuracy by forecast lead time (days ahead)
D:\Python313\python.exe D:\Scripts\weather_accuracy.py --lead-time

# Save to file instead of display
D:\Python313\python.exe D:\Scripts\weather_accuracy.py --daily --save
```

**Note:** Use Windows Python (`D:\Python313\python.exe`) - matplotlib not installed in WSL.

### Output Files

Saved to `D:\Scripts\weather_data\`:
- `accuracy_daily_YYYYMMDD_HHMMSS.png`
- `accuracy_hourly_YYYYMMDD_HHMMSS.png`
- `accuracy_leadtime_YYYYMMDD_HHMMSS.png`

### Metrics

| Metric | Description |
|--------|-------------|
| MAE | Mean Absolute Error (average error in °F) |
| RMSE | Root Mean Square Error (penalizes large errors) |
| Bias | Systematic over/under prediction (positive = too warm) |

### Initial Findings (Jan 2026, 11 days data)

- High temperature MAE: 7.7°F
- Low temperature MAE: 3.6°F
- Consistent cold bias: NWS forecasts 5-7°F colder than observed
- Error increases with lead time: 6.8°F same-day → 9.5°F at 5 days

---

## Database Backup

**Repository:** https://github.com/manchesterjm/weather-database-backup

### What's Backed Up

| Item | Location in Repo | Description |
|------|------------------|-------------|
| `weather.db` | `/weather.db` | SQLite database (Git LFS) |
| All scripts | `/scripts/` | Python loggers, PowerShell tasks |
| Tests | `/tests/` | Unit tests for scripts |
| This reference | `/Weather_Data_Reference.md` | This documentation file |

### Backup Script

**Script:** `D:\Scripts\Backup-WeatherDatabase.ps1`
**Desktop Shortcut:** "Backup Weather Database" (cloud icon)

```powershell
# Manual run
D:\Scripts\Backup-WeatherDatabase.ps1
```

### How It Works

1. Copies `weather.db` to `D:\Scripts\weather_database_backup\`
2. Updates README with timestamp and size
3. Force pushes to GitHub (no history, just current backup)
4. Logs to `D:\Scripts\weather_backup.log`

### Log Format

```
2026_01_19_07_16_04 | Size: 5.13 MB | weather.db backed up to GitHub
```

### Git LFS Enabled

The repo uses Git LFS for weather.db files. The `.gitattributes` file contains:
```
*.db filter=lfs diff=lfs merge=lfs -text
```

**GitHub LFS limits (free tier):**
- 1 GB storage
- 1 GB bandwidth/month

### Size Projection

| Timeframe | Estimated Size |
|-----------|----------------|
| Current | ~5 MB |
| 6 months | ~60 MB |
| 1 year | ~120 MB |

With LFS enabled and force-push (no history), storage usage stays at current file size only.

---

## WunderMap PWS Screenshot Capture (2026-01-23)

Automated hourly screenshots of Weather Underground's WunderMap showing crowdsourced Personal Weather Station (PWS) data for the Colorado Springs area.

### Purpose

Captures visual PWS data (temperature, wind direction/speed) from ~100+ local stations that isn't available via API without a contributing PWS.

### Files

| File | Purpose |
|------|---------|
| `D:\Scripts\capture_wundermap.sh` | Bash script using shot-scraper |
| `D:\Scripts\Create-WunderMapTask.ps1` | Admin script to create scheduled task |
| `D:\Pictures\Screenshots\WunderMap\` | Screenshot storage |
| `D:\Pictures\Screenshots\WunderMap\capture.log` | Capture log |

### Schedule

Runs every hour on the hour via Windows Task Scheduler ("WunderMap Screenshot Capture" task).

### URL Parameters

```
https://www.wunderground.com/wundermap?lat=38.9&lon=-104.75&zoom=12
```

| Parameter | Value | Purpose |
|-----------|-------|---------|
| `lat` | 38.9 | Latitude (Colorado Springs) |
| `lon` | -104.75 | Longitude (Colorado Springs) |
| `zoom` | 12 | Zoom level for metro area detail |

### shot-scraper Command

```bash
shot-scraper "https://www.wunderground.com/wundermap?lat=38.9&lon=-104.75&zoom=12" \
    -o /mnt/d/Pictures/Screenshots/WunderMap/wundermap_$(date +"%Y-%m-%d_%H%M").png \
    --wait 10000 --width 1920 --height 1080 --timeout 60000
```

### Retention

- Screenshots older than 7 days automatically deleted
- ~168 screenshots per week (~250 MB at 1.5 MB each)

### Manual Run

```bash
# From WSL
/mnt/d/Scripts/capture_wundermap.sh

# From Windows
wsl -e /mnt/d/Scripts/capture_wundermap.sh
```

### Recreate Scheduled Task

Run as Administrator:
```powershell
D:\Scripts\Create-WunderMapTask.ps1
```

### Data Interpretation

Each PWS station shows:
- **Blue circle with number:** Temperature (°F)
- **Wind barb:** Direction wind is coming FROM
  - Barb points toward wind source
  - Short line = ~5 kt, long line = ~10 kt, flag = 50 kt

### Outlier Detection

Screenshots can't be automatically filtered for bad data (e.g., one station reporting 35°F when neighbors show 8°F). When analyzing:
- Compare each station to its neighbors
- Flag readings >10°F different from surrounding stations
- Consider elevation differences (mountains 5-10°F colder)

**For automated outlier filtering:** Would require WU API access (free with contributing PWS).

### Dependencies

- **shot-scraper:** `pip install shot-scraper --break-system-packages`
- **Playwright browser:** `shot-scraper install`

---

## PWS Data Extraction to Database (2026-01-23)

Extracts crowdsourced PWS temperature data from WunderMap and saves to the weather database.

### How It Works

Uses `shot-scraper javascript` to extract temperature values directly from the WunderMap page DOM. The temperatures are embedded in the page content (not visible labels on the map).

### Files

| File | Purpose |
|------|---------|
| `D:\Scripts\capture_pws_data.py` | Main extraction script |
| `D:\Scripts\Create-PWSDataTask.ps1` | Scheduled task setup (run as Admin) |

### Database Table: `pws_observations`

| Column | Type | Description |
|--------|------|-------------|
| `timestamp` | TEXT | Hour-rounded UTC timestamp |
| `station_count` | INTEGER | Number of PWS stations reporting |
| `min_temp` | REAL | Minimum temperature (°F) |
| `max_temp` | REAL | Maximum temperature (°F) |
| `avg_temp` | REAL | Average temperature (°F) |
| `temps_json` | TEXT | JSON array of all temperatures |

### Schedule

Runs every hour at 5 minutes past the hour via Windows Task Scheduler ("PWS Data Capture" task).

**Note:** Script uses full path `/home/josh/.local/bin/shot-scraper` because PATH isn't set when running via `wsl.exe -e` from scheduled task.

### Error Handling

The script includes robust error handling for network/service outages:

| Feature | Setting | Purpose |
|---------|---------|---------|
| File Logging | `/mnt/d/Scripts/pws_capture.log` | Persistent log for troubleshooting |
| Retry Logic | 3 attempts, 30s delay | Handles transient failures |
| Min Stations | 20 required | Rejects partial/broken data loads |
| Timeout | 90 seconds | Prevents hanging on slow responses |

If all retries fail or fewer than 20 stations are detected, the script exits with error code 1 (no data saved).

### Manual Run

```bash
# Test extraction (no database save)
python3 /mnt/d/Scripts/capture_pws_data.py --test

# Extract and save to database
python3 /mnt/d/Scripts/capture_pws_data.py
```

### Query Examples

```sql
-- Recent observations
SELECT timestamp, station_count, min_temp, max_temp, avg_temp
FROM pws_observations
ORDER BY timestamp DESC
LIMIT 24;

-- Temperature distribution for a specific hour
SELECT temps_json FROM pws_observations
WHERE timestamp = '2026-01-23T16:00:00+00:00';
```

### Data Coverage

- ~100 PWS stations in Colorado Springs metro area
- Temperatures filtered to -50°F to 130°F range
- Duplicate hours are replaced (INSERT OR REPLACE)

---

---

## Script Metrics & Error Logging (2026-01-25)

Centralized metrics tracking for all 6 weather collection scripts. Every run logs: success/failure status, error details, retry attempts, and record counts.

### Files

| File | Purpose |
|------|---------|
| `D:\Scripts\script_metrics.py` | Shared metrics module (context manager pattern) |
| `D:\Scripts\check_weather_status.py` | CLI status monitor |

### Database Tables

**script_runs** - One row per script execution
| Column | Description |
|--------|-------------|
| `run_id` | Unique 8-char identifier |
| `script_name` | Name of script |
| `start_time`, `end_time` | Execution timing |
| `status` | success/partial/failed |
| `total_items_expected`, `total_items_succeeded`, `total_items_failed` | Item tracking |
| `total_records_inserted` | Database records added |
| `total_retries` | Retry count |
| `model_run` | GFS/NBM model run identifier |
| `error_message`, `error_traceback` | Error details |

**script_run_items** - Granular item tracking within a run
| Column | Description |
|--------|-------------|
| `run_id` | Links to script_runs |
| `item_name` | Item identifier (e.g., "f024", "KCOS") |
| `item_type` | Category (e.g., "forecast_hour", "metar") |
| `status`, `records_inserted`, `error_message` | Item outcome |

**script_retries** - Detailed retry history
| Column | Description |
|--------|-------------|
| `run_id`, `item_name` | Links to run/item |
| `attempt_number`, `attempt_time` | Retry info |
| `error_message`, `error_type` | Error details |

### Scripts Integrated

| Script | Items Tracked | Notes |
|--------|---------------|-------|
| `cpc_logger.py` | 2 outlook types | 8_14_day, monthly |
| `metar_logger.py` | 6 stations | KCOS, KFLY, KAFF, KFCS, KAPA, KPUB |
| `capture_pws_data.py` | 1 extraction | Includes retry logging |
| `nbm_logger.py` | 10 forecast hours | Includes model_run |
| `gfs_logger.py` | 7 forecast hours | Includes model_run |
| `weather_logger.py` | 8 data types | forecast, hourly, alerts, digital, snowfall, climate, observations, metar |

### Status Monitor Usage

```bash
# Last 24 hours summary
python check_weather_status.py

# Show only failures
python check_weather_status.py --failures

# Filter by script
python check_weather_status.py --script gfs_logger

# Custom time window
python check_weather_status.py --hours 48

# Show run details
python check_weather_status.py --details RUN_ID

# 7-day reliability report
python check_weather_status.py --reliability
```

### Query Examples

```sql
-- What failed in the last 24 hours?
SELECT script_name, status, start_time, error_message
FROM script_runs
WHERE start_time > datetime('now', '-24 hours')
  AND status IN ('failed', 'partial');

-- Script reliability over last 7 days
SELECT script_name, COUNT(*) AS runs,
       ROUND(100.0 * SUM(CASE WHEN status='success' THEN 1 ELSE 0 END) / COUNT(*), 1) AS success_rate
FROM script_runs
WHERE start_time > datetime('now', '-7 days')
GROUP BY script_name;

-- Items that failed for a specific run
SELECT item_name, item_type, error_message
FROM script_run_items
WHERE run_id = 'abc12345' AND status = 'failed';

-- Retry history for a run
SELECT item_name, attempt_number, error_type, error_message
FROM script_retries
WHERE run_id = 'abc12345'
ORDER BY attempt_time;
```

### Design Features

- **Fail-safe:** If metrics logging fails, scripts continue normally
- **Automatic timing:** Start/end times captured automatically
- **Status determination:** success/partial/failed based on item outcomes
- **Retry tracking:** Records each retry attempt with error details
- **Model run tracking:** GFS/NBM runs include model identifier
- **Storage impact:** ~1KB per script run, negligible (<50 MB/year)

---

*Last updated: 2026-01-26 (added centralized db_utils.py module, refactored all scripts to use it)*
