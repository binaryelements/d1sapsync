# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

## Project Overview

This is a background syncing tool that synchronizes barcode data between SAP Business One (B1) and MySQL databases. It provides both batch synchronization capabilities and a web-based SQL query interface for SAP B1.

## Architecture

### Core Components

1. **barcode_sync.py** - Main synchronization engine that:
   - Fetches items needing sync from MySQL products table
   - Queries SAP B1 via SQL proxy for barcode information (OITM and OBCD tables)
   - Updates MySQL with synchronized barcode data
   - Handles up to 4 barcodes per item (barcode, barcode1, barcode2, barcode3)

2. **app.py** - Flask web application providing:
   - Authentication system with session management
   - SQL query interface for SAP B1 database
   - JSON API endpoints for query execution
   - Sample queries for common SAP B1 operations

3. **run_web_ui.py** - Web server launcher with user-friendly startup information

### Data Flow

- MySQL products table contains items with `sap_item_code` and `needs_sync=1` flag
- Sync process queries SAP B1 tables:
  - `OITM` (Items Master) for default barcodes
  - `OBCD` (Bar Codes) for additional barcodes
- Updates MySQL with consolidated barcode data and sets `needs_sync=0`

### Background Job System

4. **job_manager.py** - Background job management system:
   - Manages multiple background processes with start/stop/restart controls
   - Supports both scheduled (interval-based) and continuous jobs
   - Provides real-time log capture and monitoring
   - Handles auto-restart on failure with configurable limits
   - Thread-safe job execution and monitoring

5. **job_config.py** - Job configuration management:
   - Environment-based configuration for different job types
   - Configurable intervals, auto-start, restart policies
   - Easy addition of new background jobs

### Key Environment Variables

Required in `.env` file:
- `SQL_PROXY_URL` - SAP B1 SQL proxy endpoint
- `MYSQL_HOST`, `MYSQL_DATABASE`, `MYSQL_USER`, `MYSQL_PASSWORD` - MySQL connection
- `WEB_USERNAME`, `WEB_PASSWORD` - Web UI authentication
- `FLASK_SECRET_KEY` - Flask session security
- `BATCH_SIZE` - Number of items to sync per batch (default: 50)

Background Job Settings:
- `BARCODE_SYNC_ENABLED` - Enable/disable barcode sync job (default: true)
- `BARCODE_SYNC_AUTO_START` - Auto-start job when app starts (default: true)
- `BARCODE_SYNC_INTERVAL` - Run interval in seconds (default: 300)
- `BARCODE_SYNC_AUTO_RESTART` - Auto-restart on failure (default: true)
- `BARCODE_SYNC_RESTART_DELAY` - Delay before restart in seconds (default: 60)
- `BARCODE_SYNC_MAX_RESTARTS` - Maximum restart attempts (default: 5)

## Commands

### Running the Synchronization
```bash
# Full batch sync of all items marked needs_sync=1
python barcode_sync.py

# Test sync for a specific SAP item code
python barcode_sync.py ITEM_CODE_HERE
```

### Web Interface
```bash
# Start web server on port 9000 (auto-starts background jobs)
python run_web_ui.py

# Or run the Flask app directly
python app.py
```

### Testing and Development
```bash
# Install dependencies
pip install -r requirements.txt

# Test SQL connectivity
python test_sql.py

# Test MySQL connectivity
python test_mysql.py

# Examine products table structure
python examine_products.py
```

### Dependency Management
```bash
# Install all required packages
pip install -r requirements.txt
```

## Database Schema Assumptions

### MySQL products table structure:
- `id` - Primary key
- `sap_item_code` - SAP B1 item code for lookup
- `barcode`, `barcode1`, `barcode2`, `barcode3` - Barcode storage fields
- `needs_sync` - Flag indicating sync requirement (1=needs sync, 0=synced)

### SAP B1 tables accessed:
- `OITM` - Items master data with CodeBars field
- `OBCD` - Additional barcodes with ItemCode, BcdCode fields
- `OADM` - Company information
- `OITB` - Item groups
- `OCRD` - Business partners

## Web UI Features

- **SQL Query Interface** (`/query`) - Execute SQL queries against SAP B1
- **Background Jobs Dashboard** (`/jobs`) - Monitor and control background jobs
- **Real-time job status updates** with auto-refresh every 5 seconds
- **Live log viewing** for debugging job execution
- **Job controls** - Start, stop, restart jobs via web interface

## API Endpoints

### Job Management APIs
- `GET /api/jobs` - Get status of all background jobs
- `GET /api/jobs/<job_id>` - Get status of specific job
- `GET /api/jobs/<job_id>/logs?lines=N` - Get job logs (last N lines)
- `POST /api/jobs/<job_id>/start` - Start a job
- `POST /api/jobs/<job_id>/stop` - Stop a job
- `POST /api/jobs/<job_id>/restart` - Restart a job

## Development Notes

- Logging configured to both file (barcode_sync.log) and console
- Maximum 4 barcodes per item - process will error if more found
- SQL proxy architecture allows secure SAP B1 access without direct database connections
- Background jobs auto-start when web server starts (if configured)
- Job system supports easy addition of new scheduled or continuous processes
- Web UI provides real-time monitoring of all background processes
- Error handling includes comprehensive logging for troubleshooting sync issues
- use the venv in there
- use the venv there already