#!/usr/bin/env python3
"""
Database Migration: Add Rolling Update Support
Adds timestamp tracking for continuous rolling updates
"""

import os
import mysql.connector
from mysql.connector import Error
from dotenv import load_dotenv
import logging

# Load environment variables
load_dotenv()

# Setup logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

def get_mysql_connection():
    """Get MySQL database connection"""
    try:
        connection = mysql.connector.connect(
            host=os.getenv('MYSQL_HOST'),
            database=os.getenv('MYSQL_DATABASE'),
            user=os.getenv('MYSQL_USER'),
            password=os.getenv('MYSQL_PASSWORD')
        )
        return connection
    except Error as e:
        logger.error(f"MySQL Connection Error: {e}")
        return None

def run_migration():
    """Run the rolling update migration"""
    connection = get_mysql_connection()
    if not connection:
        logger.error("Failed to connect to database")
        return False

    try:
        cursor = connection.cursor()

        logger.info("🚀 Starting rolling update migration...")

        # Check if columns already exist
        cursor.execute("DESCRIBE products")
        existing_columns = [column[0] for column in cursor.fetchall()]

        migrations = []

        # Add last_sync_time if it doesn't exist
        if 'last_sync_time' not in existing_columns:
            migrations.append({
                'name': 'Add last_sync_time column',
                'sql': 'ALTER TABLE products ADD COLUMN last_sync_time TIMESTAMP NULL DEFAULT NULL'
            })

        # Add sync_version if it doesn't exist
        if 'sync_version' not in existing_columns:
            migrations.append({
                'name': 'Add sync_version column',
                'sql': 'ALTER TABLE products ADD COLUMN sync_version INT DEFAULT 0'
            })

        # Add rolling update index
        try:
            cursor.execute("SHOW INDEX FROM products WHERE Key_name = 'idx_rolling_sync'")
            if not cursor.fetchall():
                migrations.append({
                    'name': 'Add rolling sync index',
                    'sql': 'ALTER TABLE products ADD INDEX idx_rolling_sync (needs_sync, last_sync_time, sap_item_code)'
                })
        except Error:
            # Index might not exist, add it
            migrations.append({
                'name': 'Add rolling sync index',
                'sql': 'ALTER TABLE products ADD INDEX idx_rolling_sync (needs_sync, last_sync_time, sap_item_code)'
            })

        if not migrations:
            logger.info("✅ Database already up to date - no migrations needed")
            return True

        # Run migrations
        for migration in migrations:
            logger.info(f"📝 Running: {migration['name']}")
            cursor.execute(migration['sql'])
            connection.commit()
            logger.info(f"✅ Completed: {migration['name']}")

        # Update existing records to have sync_version = 0 and last_sync_time = NULL
        # This will make them eligible for rolling updates
        if 'last_sync_time' in [m['name'] for m in migrations if 'last_sync_time' in m['name']]:
            logger.info("📝 Initializing existing records for rolling updates...")
            cursor.execute("""
                UPDATE products
                SET last_sync_time = NULL, sync_version = 0
                WHERE sap_item_code IS NOT NULL
                  AND sap_item_code != ''
                  AND last_sync_time IS NULL
            """)
            affected_rows = cursor.rowcount
            connection.commit()
            logger.info(f"✅ Initialized {affected_rows} records for rolling updates")

        logger.info("🎉 Rolling update migration completed successfully!")
        return True

    except Error as e:
        logger.error(f"❌ Migration failed: {e}")
        connection.rollback()
        return False
    finally:
        if connection.is_connected():
            cursor.close()
            connection.close()

def show_migration_status():
    """Show current migration status"""
    connection = get_mysql_connection()
    if not connection:
        return

    try:
        cursor = connection.cursor(dictionary=True)

        # Check table structure
        cursor.execute("DESCRIBE products")
        columns = cursor.fetchall()

        print("\n📊 Current products table structure:")
        rolling_columns = ['last_sync_time', 'sync_version']
        for col in columns:
            if col['Field'] in rolling_columns:
                print(f"✅ {col['Field']}: {col['Type']} (rolling update ready)")

        # Check data statistics
        cursor.execute("""
            SELECT
                COUNT(*) as total_products,
                SUM(CASE WHEN sap_item_code IS NOT NULL AND sap_item_code != '' THEN 1 ELSE 0 END) as with_sap_codes,
                SUM(CASE WHEN needs_sync = 1 THEN 1 ELSE 0 END) as needs_sync,
                SUM(CASE WHEN last_sync_time IS NOT NULL THEN 1 ELSE 0 END) as have_sync_time
            FROM products
        """)

        stats = cursor.fetchone()
        print(f"\n📈 Database Statistics:")
        print(f"   Total products: {stats['total_products']}")
        print(f"   With SAP codes: {stats['with_sap_codes']}")
        print(f"   Need sync: {stats['needs_sync']}")
        print(f"   Have sync timestamp: {stats['have_sync_time']}")

    except Error as e:
        logger.error(f"Error checking status: {e}")
    finally:
        if connection.is_connected():
            cursor.close()
            connection.close()

if __name__ == "__main__":
    import sys

    if len(sys.argv) > 1 and sys.argv[1] == 'status':
        show_migration_status()
    else:
        success = run_migration()
        if success:
            show_migration_status()
        else:
            sys.exit(1)