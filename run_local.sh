#!/usr/bin/env bash
set -euo pipefail

# Install deps (once)
python -m pip install --upgrade pip >/dev/null
python -m pip install -r requirements.txt

echo "▶︎ Running Bronze... (generating 1,000,000 txns)"
python etl/bronze_job.py --rows 1000000

echo "▶︎ Running Silver..."
python etl/silver_job.py

echo "▶︎ Running Gold..."
python etl/gold_job.py

echo "▶︎ Running tests..."
pytest