#!/bin/bash
# entrypoint.sh – Starts Flask app with Spark available in PATH

set -e

echo "================================================"
echo " GraphRec — Project 36 | BDA Lab RCOEM Nagpur"
echo " Graph-Based Recommendation System"
echo "================================================"

# Generate dataset if not present
if [ ! -f /app/data/reviews.csv ]; then
    echo "[INFO] Generating dataset..."
    python /app/data/generate_data.py
else
    echo "[INFO] Dataset found."
fi

echo "[INFO] Starting Flask UI server on port ${PORT:-5000}..."
exec python3 /app/app.py
