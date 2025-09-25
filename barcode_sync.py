import json
import requests
import os
import mysql.connector
from mysql.connector import Error
from dotenv import load_dotenv
from datetime import datetime, timezone, timedelta
import logging

# Load environment variables
load_dotenv()

# Setup logging with AEST timezone
AEST = timezone(timedelta(hours=10))

class AESTFormatter(logging.Formatter):
    def formatTime(self, record, datefmt=None):
        ct = datetime.fromtimestamp(record.created, AEST)
        if datefmt:
            s = ct.strftime(datefmt)
        else:
            s = ct.strftime('%Y-%m-%d %H:%M:%S')
        return s

# Setup logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler('barcode_sync.log'),
        logging.StreamHandler()
    ]
)

# Apply AEST formatter to all handlers
for handler in logging.getLogger().handlers:
    handler.setFormatter(AESTFormatter('%(asctime)s - %(levelname)s - %(message)s'))
logger = logging.getLogger(__name__)

def send_sql_query(query):
    """
    Send SQL query to SAP B1 database via proxy
    """
    sql_proxy_url = os.getenv('SQL_PROXY_URL')
    headers = {'Content-Type': 'application/json'}
    payload = json.dumps({"query": query})

    try:
        response = requests.post(sql_proxy_url, headers=headers, data=payload)
        if response.status_code == 200:
            data = response.json()
            if "error" in data:
                logger.error(f"SAP Query Error: {data['error']}")
                return None
            return data["data"]
        else:
            logger.error(f"SAP Request Error: {response.status_code} - {response.text}")
            return None
    except Exception as e:
        logger.error(f"SAP Connection error: {e}")
        return None

def get_mysql_connection():
    """
    Get MySQL database connection
    """
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

def ensure_table_structure():
    """
    Ensure products table has required columns for rolling updates
    Auto-migrates if columns are missing
    """
    connection = get_mysql_connection()
    if not connection:
        logger.error("Cannot validate table structure - no database connection")
        return False

    try:
        cursor = connection.cursor(dictionary=True)

        # Check current table structure
        cursor.execute("DESCRIBE products")
        existing_columns = {row['Field']: row for row in cursor.fetchall()}

        migrations_needed = []

        # Check for last_sync_time column
        if 'last_sync_time' not in existing_columns:
            migrations_needed.append({
                'column': 'last_sync_time',
                'sql': 'ALTER TABLE products ADD COLUMN last_sync_time TIMESTAMP NULL DEFAULT NULL'
            })

        # Check for sync_version column
        if 'sync_version' not in existing_columns:
            migrations_needed.append({
                'column': 'sync_version',
                'sql': 'ALTER TABLE products ADD COLUMN sync_version INT DEFAULT 0'
            })

        # Run migrations if needed
        if migrations_needed:
            logger.info(f"🔧 Table migration needed - adding {len(migrations_needed)} column(s)")

            for migration in migrations_needed:
                logger.info(f"   Adding column: {migration['column']}")
                cursor.execute(migration['sql'])

            # Add/update index for rolling updates
            try:
                cursor.execute("SHOW INDEX FROM products WHERE Key_name = 'idx_rolling_sync'")
                if not cursor.fetchall():
                    logger.info("   Adding rolling sync index")
                    cursor.execute("ALTER TABLE products ADD INDEX idx_rolling_sync (needs_sync, last_sync_time, sap_item_code)")
            except Error as index_error:
                logger.warning(f"Could not add index: {index_error}")

            connection.commit()
            logger.info("✅ Table migration completed successfully")

            # Initialize existing records
            logger.info("🔄 Initializing existing records for rolling updates")
            cursor.execute("""
                UPDATE products
                SET sync_version = 0
                WHERE sap_item_code IS NOT NULL
                  AND sap_item_code != ''
                  AND sync_version IS NULL
            """)
            initialized_count = cursor.rowcount
            connection.commit()

            if initialized_count > 0:
                logger.info(f"   Initialized {initialized_count} existing records")
        else:
            logger.debug("✅ Table structure is up to date")

        return True

    except Error as e:
        logger.error(f"❌ Table structure validation failed: {e}")
        if connection:
            connection.rollback()
        return False
    finally:
        if connection and connection.is_connected():
            cursor.close()
            connection.close()

def get_items_to_sync():
    """
    Get items from MySQL that need barcode sync (with rolling update support)
    """
    connection = get_mysql_connection()
    if not connection:
        return []

    try:
        cursor = connection.cursor(dictionary=True)

        # Get configuration from environment
        batch_size = int(os.getenv('BATCH_SIZE', 50))
        rolling_mode = os.getenv('ROLLING_UPDATE_MODE', 'timestamp')
        sync_interval_hours = int(os.getenv('SYNC_INTERVAL_HOURS', 24))

        if rolling_mode == 'timestamp':
            # Timestamp-based rolling updates
            query = f"""
            SELECT id, sap_item_code, barcode, barcode1, barcode2, barcode3,
                   last_sync_time, sync_version,
                   TIMESTAMPDIFF(HOUR, last_sync_time, NOW()) as hours_since_sync
            FROM products
            WHERE sap_item_code IS NOT NULL
              AND sap_item_code != ''
              AND (needs_sync = 1
                   OR last_sync_time IS NULL
                   OR last_sync_time < DATE_SUB(NOW(), INTERVAL {sync_interval_hours} HOUR))
            ORDER BY needs_sync DESC, COALESCE(last_sync_time, '1970-01-01') ASC
            LIMIT {batch_size}
            """

        elif rolling_mode == 'round_robin':
            # Round-robin mode - cycle through all items
            import time
            current_hour = int(time.time() // 3600)  # Change batch every hour

            # Get total count for offset calculation
            cursor.execute("""
                SELECT COUNT(*) as total
                FROM products
                WHERE sap_item_code IS NOT NULL AND sap_item_code != ''
            """)
            total_items = cursor.fetchone()['total']

            if total_items > 0:
                offset = (current_hour * batch_size) % total_items
            else:
                offset = 0

            query = f"""
            SELECT id, sap_item_code, barcode, barcode1, barcode2, barcode3,
                   last_sync_time, sync_version,
                   TIMESTAMPDIFF(HOUR, last_sync_time, NOW()) as hours_since_sync
            FROM products
            WHERE sap_item_code IS NOT NULL AND sap_item_code != ''
            ORDER BY id
            LIMIT {batch_size} OFFSET {offset}
            """

        else:
            # Fallback to original mode
            query = f"""
            SELECT id, sap_item_code, barcode, barcode1, barcode2, barcode3,
                   last_sync_time, sync_version, NULL as hours_since_sync
            FROM products
            WHERE sap_item_code IS NOT NULL
              AND sap_item_code != ''
              AND needs_sync = 1
            LIMIT {batch_size}
            """

        cursor.execute(query)
        items = cursor.fetchall()

        # Log rolling update info
        if items:
            priority_items = [item for item in items if item.get('needs_sync') == 1]
            rolling_items = len(items) - len(priority_items)

            logger.info(f"Found {len(items)} items to sync (mode: {rolling_mode})")
            if priority_items:
                logger.info(f"  📌 {len(priority_items)} priority items (needs_sync=1)")
            if rolling_items:
                logger.info(f"  🔄 {rolling_items} rolling update items (due for refresh)")
        else:
            logger.info("No items to sync")

        return items

    except Error as e:
        logger.error(f"Error getting items to sync: {e}")
        return []
    finally:
        if connection.is_connected():
            cursor.close()
            connection.close()

def get_sap_barcodes(item_code):
    """
    Get all barcodes from SAP B1 for given item code
    Returns list of barcodes (default + additional)
    """
    all_barcodes = []

    # Get default barcode from OITM
    query = f"SELECT ItemCode, ItemName, CodeBars AS DefaultBarcode FROM OITM WHERE ItemCode = '{item_code}'"
    result = send_sql_query(query)

    if result and len(result) > 0:
        default_barcode = result[0].get('DefaultBarcode')
        if default_barcode and default_barcode.strip():
            all_barcodes.append(default_barcode.strip())
            logger.debug(f"Found default barcode for {item_code}: {default_barcode}")

    # Get additional barcodes from OBCD
    query = f"SELECT ItemCode, BcdCode AS Barcode, BcdName AS BarcodeName, UomEntry FROM OBCD WHERE ItemCode = '{item_code}'"
    result = send_sql_query(query)

    if result and len(result) > 0:
        for barcode_record in result:
            additional_barcode = barcode_record.get('Barcode')
            if additional_barcode and additional_barcode.strip():
                # Avoid duplicates
                barcode_clean = additional_barcode.strip()
                if barcode_clean not in all_barcodes:
                    all_barcodes.append(barcode_clean)
                    logger.debug(f"Found additional barcode for {item_code}: {additional_barcode} ({barcode_record.get('BarcodeName', 'N/A')})")

    logger.info(f"Total barcodes found for {item_code}: {len(all_barcodes)} - {all_barcodes}")
    return all_barcodes

def update_mysql_barcodes(item_id, barcodes):
    """
    Update MySQL product with barcodes from SAP
    """
    connection = get_mysql_connection()
    if not connection:
        return False

    try:
        cursor = connection.cursor()

        # Prepare barcode fields (up to 3 additional barcodes)
        barcode_fields = {
            'barcode': None,
            'barcode1': None,
            'barcode2': None,
            'barcode3': None
        }

        # Check if we have too many barcodes
        if len(barcodes) > 4:
            logger.error(f"🚨 CRITICAL: Item ID {item_id} has {len(barcodes)} barcodes! Maximum is 4. Barcodes: {barcodes}")
            # Send notification (you can implement email/slack notification here)
            return False

        # Assign barcodes to fields (or None/empty string if no barcodes)
        for i, barcode in enumerate(barcodes):
            if i == 0:
                barcode_fields['barcode'] = barcode
            elif i == 1:
                barcode_fields['barcode1'] = barcode
            elif i == 2:
                barcode_fields['barcode2'] = barcode
            elif i == 3:
                barcode_fields['barcode3'] = barcode

        # If no barcodes found, explicitly clear all fields
        if len(barcodes) == 0:
            logger.info(f"Clearing all barcode fields for item ID {item_id}")

        # Update the product with rolling update support
        update_query = """
        UPDATE products
        SET barcode = %s, barcode1 = %s, barcode2 = %s, barcode3 = %s,
            needs_sync = 0, last_sync_time = NOW(), sync_version = sync_version + 1
        WHERE id = %s
        """

        cursor.execute(update_query, (
            barcode_fields['barcode'],
            barcode_fields['barcode1'],
            barcode_fields['barcode2'],
            barcode_fields['barcode3'],
            item_id
        ))

        connection.commit()
        logger.info(f"✅ Updated item ID {item_id} with {len(barcodes)} barcodes")
        return True

    except Error as e:
        logger.error(f"Error updating MySQL barcodes: {e}")
        return False
    finally:
        if connection.is_connected():
            cursor.close()
            connection.close()

def log_sync_analytics(success_count, error_count):
    """
    Log rolling update analytics and statistics
    """
    connection = get_mysql_connection()
    if not connection:
        return

    try:
        cursor = connection.cursor(dictionary=True)

        # Get comprehensive sync statistics
        stats_query = """
        SELECT
            COUNT(*) as total_items,
            SUM(CASE WHEN sap_item_code IS NOT NULL AND sap_item_code != '' THEN 1 ELSE 0 END) as items_with_sap_codes,
            SUM(CASE WHEN needs_sync = 1 THEN 1 ELSE 0 END) as items_pending_sync,
            SUM(CASE WHEN last_sync_time IS NOT NULL THEN 1 ELSE 0 END) as items_with_sync_history,
            SUM(CASE WHEN last_sync_time > DATE_SUB(NOW(), INTERVAL 1 HOUR) THEN 1 ELSE 0 END) as synced_last_hour,
            SUM(CASE WHEN last_sync_time > DATE_SUB(NOW(), INTERVAL 24 HOUR) THEN 1 ELSE 0 END) as synced_last_24h,
            SUM(CASE WHEN last_sync_time > DATE_SUB(NOW(), INTERVAL 7 DAY) THEN 1 ELSE 0 END) as synced_last_7d,
            AVG(CASE WHEN last_sync_time IS NOT NULL
                THEN TIMESTAMPDIFF(HOUR, last_sync_time, NOW())
                ELSE NULL END) as avg_hours_since_sync,
            MAX(sync_version) as max_sync_version,
            AVG(sync_version) as avg_sync_version
        FROM products
        WHERE sap_item_code IS NOT NULL AND sap_item_code != ''
        """

        cursor.execute(stats_query)
        stats = cursor.fetchone()

        if stats:
            total_sap_items = stats['items_with_sap_codes'] or 0
            pending = stats['items_pending_sync'] or 0
            with_history = stats['items_with_sync_history'] or 0

            # Calculate coverage percentages
            coverage_pct = (with_history / total_sap_items * 100) if total_sap_items > 0 else 0
            recent_coverage_24h = (stats['synced_last_24h'] / total_sap_items * 100) if total_sap_items > 0 else 0

            logger.info("📊 Rolling Update Analytics:")
            logger.info(f"   📦 Total SAP items: {total_sap_items}")
            logger.info(f"   ⏳ Pending sync: {pending}")
            logger.info(f"   📈 Coverage: {coverage_pct:.1f}% ({with_history}/{total_sap_items})")
            logger.info(f"   🕐 Last 24h: {recent_coverage_24h:.1f}% ({stats['synced_last_24h']}/{total_sap_items})")

            if stats['avg_hours_since_sync']:
                logger.info(f"   ⏱️  Avg age: {stats['avg_hours_since_sync']:.1f} hours since sync")

            if success_count > 0 or error_count > 0:
                logger.info(f"   📋 This run: {success_count} success, {error_count} errors")

            # Log efficiency metrics
            rolling_mode = os.getenv('ROLLING_UPDATE_MODE', 'timestamp')
            sync_interval_hours = int(os.getenv('SYNC_INTERVAL_HOURS', 24))

            logger.info(f"   ⚙️  Mode: {rolling_mode}, Interval: {sync_interval_hours}h")

            # Estimate time to full coverage (if in timestamp mode and we have data)
            if rolling_mode == 'timestamp' and success_count > 0 and pending > 0:
                batch_size = int(os.getenv('BATCH_SIZE', 50))
                job_interval_minutes = int(os.getenv('BARCODE_SYNC_INTERVAL', 300)) / 60
                estimated_runs = (pending + batch_size - 1) // batch_size  # Ceiling division
                estimated_hours = (estimated_runs * job_interval_minutes) / 60

                if estimated_hours < 24:
                    logger.info(f"   🎯 Est. time to clear backlog: {estimated_hours:.1f} hours")

    except Error as e:
        logger.error(f"Error logging sync analytics: {e}")
    finally:
        if connection and connection.is_connected():
            cursor.close()
            connection.close()

def sync_barcodes():
    """
    Main function to sync barcodes from SAP to MySQL
    """
    logger.info("🚀 Starting barcode sync process...")

    # Ensure table structure is ready for rolling updates
    if not ensure_table_structure():
        logger.error("❌ Table structure validation failed - aborting sync")
        return

    items = get_items_to_sync()
    if not items:
        logger.info("No items to sync")
        return

    success_count = 0
    error_count = 0

    for item in items:
        item_id = item['id']
        sap_item_code = item['sap_item_code']

        logger.info(f"Processing item {sap_item_code} (ID: {item_id})")

        # Get barcodes from SAP
        sap_barcodes = get_sap_barcodes(sap_item_code)

        if not sap_barcodes:
            logger.warning(f"No barcodes found in SAP for item {sap_item_code} - clearing existing barcodes")
            # Clear existing barcodes if no SAP barcodes found
            sap_barcodes = []

        # Update MySQL with SAP barcodes (or clear if empty)
        if update_mysql_barcodes(item_id, sap_barcodes):
            success_count += 1
        else:
            error_count += 1

    logger.info(f"🎯 Sync completed: {success_count} successful, {error_count} errors")

    # Log rolling update analytics
    log_sync_analytics(success_count, error_count)

def sync_single_item(sap_item_code):
    """
    Sync a single item by SAP item code (for testing)
    """
    logger.info(f"🧪 Testing sync for item: {sap_item_code}")

    # Get the item from MySQL
    connection = get_mysql_connection()
    if not connection:
        return False

    try:
        cursor = connection.cursor(dictionary=True)
        query = "SELECT id, sap_item_code, barcode, barcode1, barcode2, barcode3 FROM products WHERE sap_item_code = %s"
        cursor.execute(query, (sap_item_code,))
        item = cursor.fetchone()

        if not item:
            logger.error(f"Item {sap_item_code} not found in MySQL")
            return False

        logger.info(f"Found MySQL item: {item}")

        # Get barcodes from SAP
        sap_barcodes = get_sap_barcodes(sap_item_code)
        logger.info(f"SAP barcodes: {sap_barcodes}")

        if not sap_barcodes:
            logger.warning(f"No barcodes found in SAP for item {sap_item_code} - clearing existing barcodes")
            # Clear existing barcodes if no SAP barcodes found
            sap_barcodes = []

        # Update MySQL with SAP barcodes (or clear if empty)
        return update_mysql_barcodes(item['id'], sap_barcodes)

    except Error as e:
        logger.error(f"Error in single item sync: {e}")
        return False
    finally:
        if connection.is_connected():
            cursor.close()
            connection.close()

if __name__ == "__main__":
    import sys

    if len(sys.argv) > 1:
        # Test mode with specific item
        item_code = sys.argv[1]
        sync_single_item(item_code)
    else:
        # Full sync
        sync_barcodes()