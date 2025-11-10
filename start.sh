#!/bin/bash

# Start script for Iceberg Explorer
# This script starts both the backend and frontend servers

echo "Starting Iceberg Explorer..."

# Check if GOOGLE_APPLICATION_CREDENTIALS is set
if [ -z "$GOOGLE_APPLICATION_CREDENTIALS" ]; then
    echo "Warning: GOOGLE_APPLICATION_CREDENTIALS environment variable is not set."
    echo "Please set it to your GCS service account JSON key file path."
    echo "Example: export GOOGLE_APPLICATION_CREDENTIALS=/path/to/key.json"
    echo ""
fi

# Start backend in background
echo "Starting backend server on port 8000..."
cd backend
# Use virtual environment Python
../.venv/bin/python -m uvicorn main:app --host 0.0.0.0 --port 8000 --reload &
BACKEND_PID=$!
cd ..

# Wait a moment for backend to start
sleep 2

# Start frontend
echo "Starting frontend server on port 3000..."
npm run dev &
FRONTEND_PID=$!

echo ""
echo "✅ Iceberg Explorer is running!"
echo "   Frontend: http://localhost:3000"
echo "   Backend:  http://localhost:8000"
echo ""
echo "Press Ctrl+C to stop both servers"

# Wait for user interrupt
trap "kill $BACKEND_PID $FRONTEND_PID; exit" INT TERM
wait

