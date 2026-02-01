import sqlite3
conn = sqlite3.connect(r'D:\Scripts\weather_data\weather.db')

# Current METAR
r = conn.execute('SELECT observation_time,temperature_c*9/5+32,dewpoint_c*9/5+32,wind_speed_kt,sky_condition,weather_phenomena,altimeter_inhg FROM metar ORDER BY observation_time DESC LIMIT 1').fetchone()
print(f"=== CURRENT (METAR KCOS) ===")
print(f"{r[0]}: {r[1]:.0f}°F (dp {r[2]:.0f}°F), Wind {r[3]}kt, {r[4]}, {r[5] or 'Clear'}, {r[6]} inHg")

# Your PWS
r = conn.execute('SELECT timestamp,temp_f,feels_like_f,humidity,wind_speed_mph,wind_gust_mph,pressure_rel_in FROM ambient_observations ORDER BY timestamp DESC LIMIT 1').fetchone()
print(f"\n=== YOUR PWS ===")
print(f"{r[0]}: {r[1]}°F (feels {r[2]}°F), {r[3]}% humidity, Wind {r[4]}/{r[5]}mph, {r[6]} inHg")

# NWS 7-day
print(f"\n=== NWS FORECAST ===")
for r in conn.execute('''SELECT forecast_date, forecast_hour, temperature, sky_cover, wind_direction, wind_speed, precip_probability
    FROM digital_forecast WHERE datetime(forecast_date||' '||printf('%02d:00',forecast_hour)) >= datetime('now','localtime')
    GROUP BY forecast_date, forecast_hour ORDER BY forecast_date, forecast_hour LIMIT 24'''):
    pop = f" POP{r[6]}%" if r[6] else ""
    print(f"  {r[0]} {r[1]:02d}:00 - {r[2]}°F, Sky {r[3]}%, {r[4]}@{r[5]}mph{pop}")

# CPC outlooks
print(f"\n=== CPC 8-14 DAY ===")
r = conn.execute("SELECT issued_date, discussion FROM cpc_outlooks WHERE outlook_type='8_14_day' ORDER BY fetch_time DESC LIMIT 1").fetchone()
if r: print(f"Issued {r[0]}: {r[1][:400]}...")

print(f"\n=== CPC 30-DAY ===")
r = conn.execute("SELECT issued_date, discussion FROM cpc_outlooks WHERE outlook_type='monthly' ORDER BY fetch_time DESC LIMIT 1").fetchone()
if r: print(f"Issued {r[0]}: {r[1][:400]}...")

conn.close()
