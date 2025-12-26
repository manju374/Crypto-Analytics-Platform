#!/bin/sh
echo "--- 1. Initializing Database ---"
python -m app.init_db

echo "--- 2. Running Data Pipeline ---"
python -m app.ingestion.pipeline

echo "--- 3. Starting Server ---"
exec python -m uvicorn app.main:app --host 0.0.0.0 --port 8000
