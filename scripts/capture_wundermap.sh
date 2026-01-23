#!/bin/bash
# Capture WunderMap screenshot for Colorado Springs area
# Runs via scheduled task hourly

SCREENSHOT_DIR="/mnt/d/Pictures/Screenshots/WunderMap"
TIMESTAMP=$(date +"%Y-%m-%d_%H%M")
FILENAME="wundermap_${TIMESTAMP}.png"

# Create directory if it doesn't exist
mkdir -p "$SCREENSHOT_DIR"

# Capture screenshot
shot-scraper "https://www.wunderground.com/wundermap?lat=38.9&lon=-104.75&zoom=12" \
    -o "${SCREENSHOT_DIR}/${FILENAME}" \
    --wait 10000 \
    --width 1920 \
    --height 1080 \
    --timeout 60000

# Log the capture
echo "$(date): Captured ${FILENAME}" >> "${SCREENSHOT_DIR}/capture.log"

# Keep only last 7 days of screenshots (168 hourly images)
find "$SCREENSHOT_DIR" -name "wundermap_*.png" -mtime +7 -delete 2>/dev/null

echo "Screenshot saved: ${SCREENSHOT_DIR}/${FILENAME}"
