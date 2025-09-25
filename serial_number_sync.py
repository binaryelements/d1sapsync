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
        logging.FileHandler('serial_number_sync.log'),
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

def get_serial_number_items():
    """
    Get items from SAP that require serial numbers
    Returns list of item codes that require serial number tracking
    """
    batch_size = int(os.getenv('SERIAL_SYNC_BATCH_SIZE', 50))

    query = f"""
    SELECT TOP {batch_size} ItemCode
    FROM OITM
    WHERE frozenFor <> 'Y'
    AND SellItem = 'Y'
    AND ManSerNum = 'Y'
    """

    result = send_sql_query(query)

    if result:
        item_codes = [item['ItemCode'] for item in result]
        logger.info(f"Found {len(item_codes)} items requiring serial numbers: {item_codes}")
        return item_codes
    else:
        logger.warning("No serial number items found or query failed")
        return []

def get_product_by_sap_code(sap_item_code):
    """
    Get product from MySQL by SAP item code
    """
    connection = get_mysql_connection()
    if not connection:
        return None

    try:
        cursor = connection.cursor(dictionary=True)
        query = "SELECT id FROM products WHERE sap_item_code = %s"
        cursor.execute(query, (sap_item_code,))
        product = cursor.fetchone()
        return product
    except Error as e:
        logger.error(f"Error getting product by SAP code {sap_item_code}: {e}")
        return None
    finally:
        if connection.is_connected():
            cursor.close()
            connection.close()

def update_product_associated_details(product_id, sap_item_code):
    """
    Update or create product_associated_details for serial number requirement
    """
    connection = get_mysql_connection()
    if not connection:
        return False

    try:
        cursor = connection.cursor()

        # Determine if it's a CRF item (starts with "CRF")
        is_select = 1 if sap_item_code.startswith('CRF') else 0

        # Check if record already exists
        check_query = """
        SELECT id FROM product_associated_details
        WHERE product_id = %s AND fieldName = 'serial_number'
        """
        cursor.execute(check_query, (product_id,))
        existing = cursor.fetchone()

        if existing:
            # Update existing record
            update_query = """
            UPDATE product_associated_details
            SET isRequired = 1, isSelect = %s, toValidate = 1, allowSalesIfValidationFails = 0
            WHERE product_id = %s AND fieldName = 'serial_number'
            """
            cursor.execute(update_query, (is_select, product_id))
            logger.info(f"✅ Updated serial_number requirement for product_id {product_id} (SAP: {sap_item_code})")
        else:
            # Create new record
            insert_query = """
            INSERT INTO product_associated_details
            (product_id, fieldName, isRequired, isSelect, toValidate, allowSalesIfValidationFails)
            VALUES (%s, 'serial_number', 1, %s, 1, 0)
            """
            cursor.execute(insert_query, (product_id, is_select))
            logger.info(f"✅ Created serial_number requirement for product_id {product_id} (SAP: {sap_item_code})")

        connection.commit()
        return True

    except Error as e:
        logger.error(f"Error updating product_associated_details for product_id {product_id}: {e}")
        return False
    finally:
        if connection.is_connected():
            cursor.close()
            connection.close()

def sync_serial_number_requirements():
    """
    Main function to sync serial number requirements from SAP to MySQL
    """
    logger.info("🚀 Starting serial number requirement sync process...")

    # Get items requiring serial numbers from SAP
    serial_items = get_serial_number_items()
    if not serial_items:
        logger.info("No serial number items found to sync")
        return

    success_count = 0
    error_count = 0
    not_found_count = 0

    for sap_item_code in serial_items:
        logger.info(f"Processing SAP item: {sap_item_code}")

        # Find corresponding product in MySQL
        product = get_product_by_sap_code(sap_item_code)

        if not product:
            logger.warning(f"⚠️ Product not found in MySQL for SAP code: {sap_item_code}")
            not_found_count += 1
            continue

        product_id = product['id']

        # Update product_associated_details
        if update_product_associated_details(product_id, sap_item_code):
            success_count += 1
        else:
            error_count += 1

    logger.info(f"🎯 Serial number sync completed: {success_count} successful, {error_count} errors, {not_found_count} not found")

if __name__ == "__main__":
    import sys

    if len(sys.argv) > 1:
        # Test mode with specific item
        item_code = sys.argv[1]
        logger.info(f"🧪 Testing serial number sync for item: {item_code}")

        product = get_product_by_sap_code(item_code)
        if product:
            update_product_associated_details(product['id'], item_code)
        else:
            logger.error(f"Product not found for SAP code: {item_code}")
    else:
        # Full sync
        sync_serial_number_requirements()