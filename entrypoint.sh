#!/bin/bash

# Production entrypoint script for SAP B1 Sync Tool

set -e

echo "🚀 Starting SAP B1 Sync Tool (Production Mode)"
echo "============================================================"

# Check required environment variables
required_vars=(
    "SQL_PROXY_URL"
    "MYSQL_HOST"
    "MYSQL_DATABASE"
    "MYSQL_USER"
    "MYSQL_PASSWORD"
    "FLASK_SECRET_KEY"
)

echo "🔍 Checking required environment variables..."
missing_vars=()
for var in "${required_vars[@]}"; do
    if [ -z "${!var}" ]; then
        missing_vars+=("$var")
    fi
done

if [ ${#missing_vars[@]} -ne 0 ]; then
    echo "❌ Missing required environment variables:"
    printf '   - %s\n' "${missing_vars[@]}"
    echo ""
    echo "Please set these variables before running the container."
    exit 1
fi

echo "✅ All required environment variables are set"

# Set default values for optional variables
export WEB_USERNAME="${WEB_USERNAME:-admin}"
export WEB_PASSWORD="${WEB_PASSWORD:-d1sapsync2024}"
export BATCH_SIZE="${BATCH_SIZE:-50}"
export BARCODE_SYNC_ENABLED="${BARCODE_SYNC_ENABLED:-true}"
export BARCODE_SYNC_AUTO_START="${BARCODE_SYNC_AUTO_START:-true}"
export BARCODE_SYNC_INTERVAL="${BARCODE_SYNC_INTERVAL:-300}"
export FLASK_ENV="${FLASK_ENV:-production}"
export FLASK_DEBUG="${FLASK_DEBUG:-false}"

# Create logs directory if it doesn't exist
mkdir -p /app/logs

echo "📍 Server will be available on port 9000"
echo "🔐 Web UI credentials: ${WEB_USERNAME} / [password hidden]"
echo "⚡ Background sync enabled: ${BARCODE_SYNC_ENABLED}"
echo "🔄 Sync interval: ${BARCODE_SYNC_INTERVAL} seconds"
echo "============================================================"

# Start the application with gunicorn
exec gunicorn \
    --bind 0.0.0.0:9000 \
    --workers 2 \
    --threads 4 \
    --timeout 120 \
    --access-logfile - \
    --error-logfile - \
    --log-level info \
    --preload \
    app:app