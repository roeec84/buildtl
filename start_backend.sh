#!/bin/bash
# Quick start script for backend

echo "🚀 Starting Bob the Bot Backend..."
echo ""

# Navigate to backend directory
cd "$(dirname "$0")/backend" || exit 1

# Activate virtual environment
if [ ! -d "venv" ]; then
    echo "❌ Virtual environment not found!"
    echo "Please run: python -m venv venv"
    exit 1
fi

source venv/bin/activate

# Check if dependencies are installed
if ! python -c "import fastapi" 2>/dev/null; then
    echo "📦 Installing dependencies..."
    pip install -r requirements.txt
fi

# Start the server
echo "✅ Starting server on http://localhost:8000"
echo "📚 API docs at http://localhost:8000/docs"
echo ""
uvicorn app.main:app --reload --host 0.0.0.0 --port 8000
