"""
Rolling Update Utilities
Shared utilities for implementing rolling updates across all sync jobs
"""

import os
import logging
import mysql.connector
from mysql.connector import Error
from dotenv import load_dotenv

# Load environment variables
load_dotenv()

logger = logging.getLogger(__name__)

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

def ensure_rolling_update_columns(table_name, primary_key_column='id'):
    """
    Ensure table has required columns for rolling updates
    """
    connection = get_mysql_connection()
    if not connection:
        logger.error(f"Cannot validate table structure for {table_name} - no database connection")
        return False

    try:
        cursor = connection.cursor(dictionary=True)

        # Check current table structure
        cursor.execute(f"DESCRIBE {table_name}")
        existing_columns = {row['Field']: row for row in cursor.fetchall()}

        migrations_needed = []

        # Check for last_sync_time column
        if 'last_sync_time' not in existing_columns:
            migrations_needed.append({
                'column': 'last_sync_time',
                'sql': f'ALTER TABLE {table_name} ADD COLUMN last_sync_time TIMESTAMP NULL DEFAULT NULL'
            })

        # Check for sync_version column
        if 'sync_version' not in existing_columns:
            migrations_needed.append({
                'column': 'sync_version',
                'sql': f'ALTER TABLE {table_name} ADD COLUMN sync_version INT DEFAULT 0'
            })

        # Run migrations if needed
        if migrations_needed:
            logger.info(f"🔧 Rolling update migration for {table_name} - adding {len(migrations_needed)} column(s)")

            for migration in migrations_needed:
                logger.info(f"   Adding column: {migration['column']} to {table_name}")
                cursor.execute(migration['sql'])

            # Add rolling update index if it doesn't exist
            try:
                index_name = f'idx_{table_name}_rolling_sync'
                cursor.execute(f"SHOW INDEX FROM {table_name} WHERE Key_name = '{index_name}'")
                if not cursor.fetchall():
                    logger.info(f"   Adding rolling sync index to {table_name}")
                    # Create index based on common patterns
                    if table_name == 'products':
                        cursor.execute(f"ALTER TABLE {table_name} ADD INDEX {index_name} (needs_sync, last_sync_time, sap_item_code)")
                    elif table_name == 'product_associated_details':
                        cursor.execute(f"ALTER TABLE {table_name} ADD INDEX {index_name} (last_sync_time, product_id)")
                    elif table_name == 'app_users':
                        cursor.execute(f"ALTER TABLE {table_name} ADD INDEX {index_name} (last_sync_time, sap_import_flag)")
                    else:
                        cursor.execute(f"ALTER TABLE {table_name} ADD INDEX {index_name} (last_sync_time, {primary_key_column})")
            except Error as index_error:
                logger.warning(f"Could not add index to {table_name}: {index_error}")

            connection.commit()
            logger.info(f"✅ Rolling update migration completed for {table_name}")

            # Initialize existing records
            logger.info(f"🔄 Initializing existing records in {table_name}")
            cursor.execute(f"""
                UPDATE {table_name}
                SET sync_version = 0
                WHERE sync_version IS NULL
            """)
            initialized_count = cursor.rowcount
            connection.commit()

            if initialized_count > 0:
                logger.info(f"   Initialized {initialized_count} existing records in {table_name}")
        else:
            logger.debug(f"✅ Table {table_name} structure is up to date")

        return True

    except Error as e:
        logger.error(f"❌ Rolling update validation failed for {table_name}: {e}")
        if connection:
            connection.rollback()
        return False
    finally:
        if connection and connection.is_connected():
            cursor.close()
            connection.close()

def get_rolling_update_query(table_name, where_conditions="", additional_columns="", join_clause=""):
    """
    Generate rolling update query based on configuration
    """
    rolling_mode = os.getenv('ROLLING_UPDATE_MODE', 'timestamp')
    sync_interval_hours = int(os.getenv('SYNC_INTERVAL_HOURS', 24))
    batch_size = int(os.getenv('BATCH_SIZE', 50))

    base_columns = f"{table_name}.*, last_sync_time, sync_version, TIMESTAMPDIFF(HOUR, last_sync_time, NOW()) as hours_since_sync"

    if additional_columns:
        columns = f"{base_columns}, {additional_columns}"
    else:
        columns = base_columns

    if rolling_mode == 'timestamp':
        # Timestamp-based rolling updates
        sync_condition = f"""
        (last_sync_time IS NULL
         OR last_sync_time < DATE_SUB(NOW(), INTERVAL {sync_interval_hours} HOUR))
        """

        if where_conditions:
            full_where = f"({where_conditions}) AND ({sync_condition})"
        else:
            full_where = sync_condition

        order_by = "COALESCE(last_sync_time, '1970-01-01') ASC"

    elif rolling_mode == 'round_robin':
        # Round-robin mode
        import time
        current_hour = int(time.time() // 3600)

        # Get offset for this hour
        connection = get_mysql_connection()
        if connection:
            try:
                cursor = connection.cursor()
                count_query = f"SELECT COUNT(*) as total FROM {table_name}"
                if join_clause:
                    count_query += f" {join_clause}"
                if where_conditions:
                    count_query += f" WHERE {where_conditions}"

                cursor.execute(count_query)
                total_items = cursor.fetchone()[0]

                if total_items > 0:
                    offset = (current_hour * batch_size) % total_items
                else:
                    offset = 0

                connection.close()
            except Error:
                offset = 0
                if connection:
                    connection.close()
        else:
            offset = 0

        full_where = where_conditions if where_conditions else "1=1"
        order_by = f"{table_name}.id"
        batch_size = f"{batch_size} OFFSET {offset}"

    else:
        # Legacy mode - no rolling updates
        full_where = where_conditions if where_conditions else "1=1"
        order_by = f"{table_name}.id"

    # Build final query
    query = f"""
    SELECT {columns}
    FROM {table_name}
    """

    if join_clause:
        query += f" {join_clause}"

    if full_where and full_where != "1=1":
        query += f" WHERE {full_where}"

    query += f" ORDER BY {order_by}"

    if rolling_mode != 'round_robin':
        query += f" LIMIT {batch_size}"
    else:
        query += f" LIMIT {batch_size}"

    return query

def update_sync_timestamp(table_name, record_id, primary_key_column='id'):
    """
    Update last_sync_time and increment sync_version for a record
    """
    connection = get_mysql_connection()
    if not connection:
        return False

    try:
        cursor = connection.cursor()
        update_query = f"""
        UPDATE {table_name}
        SET last_sync_time = NOW(), sync_version = sync_version + 1
        WHERE {primary_key_column} = %s
        """
        cursor.execute(update_query, (record_id,))
        connection.commit()
        return True
    except Error as e:
        logger.error(f"Error updating sync timestamp for {table_name} record {record_id}: {e}")
        return False
    finally:
        if connection and connection.is_connected():
            cursor.close()
            connection.close()

def log_rolling_update_analytics(table_name, sync_name, success_count, error_count,
                                where_condition="", job_interval_var=""):
    """
    Log comprehensive analytics for rolling updates
    """
    connection = get_mysql_connection()
    if not connection:
        return

    try:
        cursor = connection.cursor(dictionary=True)

        # Build analytics query
        base_condition = where_condition if where_condition else "1=1"

        stats_query = f"""
        SELECT
            COUNT(*) as total_items,
            SUM(CASE WHEN last_sync_time IS NOT NULL THEN 1 ELSE 0 END) as items_with_sync_history,
            SUM(CASE WHEN last_sync_time > DATE_SUB(NOW(), INTERVAL 1 HOUR) THEN 1 ELSE 0 END) as synced_last_hour,
            SUM(CASE WHEN last_sync_time > DATE_SUB(NOW(), INTERVAL 24 HOUR) THEN 1 ELSE 0 END) as synced_last_24h,
            SUM(CASE WHEN last_sync_time > DATE_SUB(NOW(), INTERVAL 7 DAY) THEN 1 ELSE 0 END) as synced_last_7d,
            AVG(CASE WHEN last_sync_time IS NOT NULL
                THEN TIMESTAMPDIFF(HOUR, last_sync_time, NOW())
                ELSE NULL END) as avg_hours_since_sync,
            MAX(sync_version) as max_sync_version,
            AVG(sync_version) as avg_sync_version
        FROM {table_name}
        WHERE {base_condition}
        """

        cursor.execute(stats_query)
        stats = cursor.fetchone()

        if stats:
            total_items = stats['total_items'] or 0
            with_history = stats['items_with_sync_history'] or 0

            # Calculate coverage percentages
            coverage_pct = (with_history / total_items * 100) if total_items > 0 else 0
            recent_coverage_24h = (stats['synced_last_24h'] / total_items * 100) if total_items > 0 else 0

            logger.info(f"📊 {sync_name} Rolling Update Analytics:")
            logger.info(f"   📦 Total items: {total_items}")
            logger.info(f"   📈 Coverage: {coverage_pct:.1f}% ({with_history}/{total_items})")
            logger.info(f"   🕐 Last 24h: {recent_coverage_24h:.1f}% ({stats['synced_last_24h']}/{total_items})")

            if stats['avg_hours_since_sync']:
                logger.info(f"   ⏱️  Avg age: {stats['avg_hours_since_sync']:.1f} hours since sync")

            if success_count > 0 or error_count > 0:
                logger.info(f"   📋 This run: {success_count} success, {error_count} errors")

            # Log efficiency metrics
            rolling_mode = os.getenv('ROLLING_UPDATE_MODE', 'timestamp')
            sync_interval_hours = int(os.getenv('SYNC_INTERVAL_HOURS', 24))
            logger.info(f"   ⚙️  Mode: {rolling_mode}, Interval: {sync_interval_hours}h")

    except Error as e:
        logger.error(f"Error logging {sync_name} analytics: {e}")
    finally:
        if connection and connection.is_connected():
            cursor.close()
            connection.close()