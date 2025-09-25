#!/usr/bin/env python3
"""
SAP B1 Query Tool - Web UI Launcher
Run this script to start the web interface
"""

import os
import sys
from app import app
from job_manager import initialize_jobs

def main():
    print("🚀 Starting SAP B1 Query Tool Web Interface...")
    print("=" * 60)
    print()
    print("📍 Server will be available at:")
    print("   🌐 http://localhost:9000")
    print("   🌐 http://127.0.0.1:9000")
    print()
    print("🔐 Default Login Credentials:")
    print("   👤 Username: admin")
    print("   🔑 Password: d1sapsync2024")
    print()
    print("⚡ Features:")
    print("   • Execute SQL queries against SAP B1 database")
    print("   • Background job management and monitoring")
    print("   • Real-time job logs and status updates")
    print("   • Sample queries included")
    print("   • Export results to CSV")
    print("   • Responsive web interface")
    print("   • 🔄 Auto-reload on code changes (development mode)")
    print()
    print("🛑 Press Ctrl+C to stop the server")
    print("=" * 60)

    try:
        # Initialize background jobs
        print("🔄 Initializing background jobs...")
        initialize_jobs()
        print("✅ Background jobs initialized")
        print()

        app.run(debug=True, host='0.0.0.0', port=9000, use_reloader=True)
    except KeyboardInterrupt:
        print("\n\n👋 Server stopped by user")
        print("Thank you for using SAP B1 Query Tool!")
        sys.exit(0)
    except Exception as e:
        print(f"\n❌ Error starting server: {e}")
        sys.exit(1)

if __name__ == '__main__':
    main()