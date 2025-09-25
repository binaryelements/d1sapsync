import json
import requests
import os
import mysql.connector
from mysql.connector import Error
from dotenv import load_dotenv
from datetime import datetime
import logging

# Load environment variables
load_dotenv()

# Setup logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler('barcode_sync.log'),
        logging.StreamHandler()
    ]
)
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

def get_items_to_sync():
    """
    Get items from MySQL that need barcode sync
    """
    connection = get_mysql_connection()
    if not connection:
        return []

    try:
        cursor = connection.cursor(dictionary=True)

        # Get batch size from environment
        batch_size = int(os.getenv('BATCH_SIZE', 50))

        # Get items with sap_item_code that need sync
        query = f"""
        SELECT id, sap_item_code, barcode, barcode1, barcode2, barcode3
        FROM products
        WHERE sap_item_code IS NOT NULL
        AND sap_item_code != ''
        AND needs_sync = 1
        LIMIT {batch_size}
        """

        cursor.execute(query)
        items = cursor.fetchall()
        logger.info(f"Found {len(items)} items to sync")
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

        # Update the product
        update_query = """
        UPDATE products
        SET barcode = %s, barcode1 = %s, barcode2 = %s, barcode3 = %s, needs_sync = 0
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

def sync_barcodes():
    """
    Main function to sync barcodes from SAP to MySQL
    """
    logger.info("🚀 Starting barcode sync process...")

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