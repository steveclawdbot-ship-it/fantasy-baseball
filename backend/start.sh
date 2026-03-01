#!/bin/bash

# Fantasy Baseball API Startup Script

echo "Starting Fantasy Baseball API..."

# Start API server - app is in current directory
echo "Starting Uvicorn server on http://0.0.0.0:8000"
./venv/bin/python -m uvicorn app.main:app --host 0.0.0.0 --port 8000 --reload
