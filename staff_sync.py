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
        logging.FileHandler('staff_sync.log'),
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

def get_sap_staff():
    """
    Get active staff from SAP B1 OSLP table
    """
    query = "SELECT SlpCode, SlpName, Active FROM OSLP WHERE Active = 'Y'"
    result = send_sql_query(query)

    if result:
        logger.info(f"Found {len(result)} active staff members in SAP")
        return result
    else:
        logger.warning("No active staff found in SAP")
        return []


def parse_staff_name(full_name):
    """
    Parse full name into first and last name
    """
    if not full_name or not full_name.strip():
        return "", ""

    parts = full_name.strip().split(" ", 1)
    first_name = parts[0] if len(parts) > 0 else ""
    last_name = parts[1] if len(parts) > 1 else ""

    return first_name, last_name

def sync_staff():
    """
    Main function to sync staff from SAP to MySQL app_users table
    Only creates new records, leaves existing records untouched
    """
    logger.info("🚀 Starting staff sync process...")

    # Get active staff from SAP
    sap_staff = get_sap_staff()
    if not sap_staff:
        logger.info("No staff to sync")
        return

    connection = get_mysql_connection()
    if not connection:
        logger.error("Could not establish MySQL connection")
        return

    try:
        cursor = connection.cursor(dictionary=True)
        staff_inserted = []
        success_count = 0
        error_count = 0
        skipped_count = 0

        for staff_member in sap_staff:
            staff_id = staff_member.get('SlpCode')
            staff_name_full = staff_member.get('SlpName', '')
            sap_active = staff_member.get('Active', 'N')

            if not staff_id or staff_id <= 0:
                logger.warning(f"Invalid staff ID: {staff_id}, skipping")
                continue

            # Only process active SAP staff
            if sap_active != 'Y':
                logger.debug(f"⏭️  Skipping inactive SAP staff: {staff_name_full} (ID: {staff_id})")
                continue

            first_name, last_name = parse_staff_name(staff_name_full)

            try:
                # Check if staff already exists in MySQL
                cursor.execute("SELECT id FROM app_users WHERE id = %s", (staff_id,))
                existing_staff = cursor.fetchone()

                if existing_staff:
                    # Skip existing records - do not update
                    logger.debug(f"⏭️  Skipping existing staff: {staff_name_full} (ID: {staff_id})")
                    skipped_count += 1
                else:
                    # Insert new staff only
                    import uuid
                    email = f"{uuid.uuid4()}@{uuid.uuid4()}.com"
                    password = str(uuid.uuid4())

                    insert_query = """
                    INSERT INTO app_users
                    (id, first_name, last_name, email_address, password, active_flag, salesman_flag, sap_import_flag)
                    VALUES (%s, %s, %s, %s, %s, %s, %s, %s)
                    """
                    cursor.execute(insert_query, (
                        staff_id, first_name, last_name, email, password,
                        1, 1, 1  # active_flag=1, salesman_flag=1, sap_import_flag=1
                    ))

                    staff_inserted.append(f"---> Inserted ... {staff_name_full} id: {staff_id}")
                    logger.info(f"➕ Inserted new staff: {staff_name_full} (ID: {staff_id})")
                    success_count += 1

            except Error as e:
                logger.error(f"Error processing staff {staff_name_full} (ID: {staff_id}): {e}")
                error_count += 1
                continue

        connection.commit()

        # Log summary
        logger.info("=" * 60)
        logger.info("Staff Sync Summary:")
        for insert in staff_inserted:
            logger.info(insert)
        logger.info("=" * 60)
        logger.info(f"🎯 Sync completed: {success_count} new records created, {skipped_count} existing records left unchanged, {error_count} errors")

    except Error as e:
        logger.error(f"Error during staff sync: {e}")
        connection.rollback()
    finally:
        if connection.is_connected():
            cursor.close()
            connection.close()

def sync_single_staff(staff_id):
    """
    Sync a single staff member by ID (for testing)
    Only creates new records, leaves existing records untouched
    """
    logger.info(f"🧪 Testing sync for staff ID: {staff_id}")

    # Get the staff from SAP
    query = f"SELECT SlpCode, SlpName, Active FROM OSLP WHERE SlpCode = {staff_id}"
    result = send_sql_query(query)

    if not result or len(result) == 0:
        logger.error(f"Staff ID {staff_id} not found in SAP OSLP table")
        return False

    staff_member = result[0]
    logger.info(f"Found SAP staff: {staff_member}")

    sap_active = staff_member.get('Active', 'N')
    if sap_active != 'Y':
        logger.info(f"⏭️  Staff {staff_id} is inactive in SAP (Active: {sap_active}), skipping")
        return False

    # Process this single staff member
    connection = get_mysql_connection()
    if not connection:
        return False

    try:
        cursor = connection.cursor(dictionary=True)

        sap_staff_id = staff_member.get('SlpCode')
        staff_name_full = staff_member.get('SlpName', '')
        first_name, last_name = parse_staff_name(staff_name_full)

        # Check if staff exists in MySQL
        cursor.execute("SELECT id FROM app_users WHERE id = %s", (sap_staff_id,))
        existing_staff = cursor.fetchone()

        if existing_staff:
            logger.info(f"⏭️  Staff already exists in MySQL, leaving unchanged: {staff_name_full} (ID: {sap_staff_id})")
            return True
        else:
            # Insert new staff only
            import uuid
            email = f"{uuid.uuid4()}@{uuid.uuid4()}.com"
            password = str(uuid.uuid4())

            insert_query = """
            INSERT INTO app_users
            (id, first_name, last_name, email_address, password, active_flag, salesman_flag, sap_import_flag)
            VALUES (%s, %s, %s, %s, %s, %s, %s, %s)
            """
            cursor.execute(insert_query, (
                sap_staff_id, first_name, last_name, email, password,
                1, 1, 1  # active_flag=1, salesman_flag=1, sap_import_flag=1
            ))

            logger.info(f"➕ Inserted new staff: {staff_name_full} (ID: {sap_staff_id})")

        connection.commit()
        return True

    except Error as e:
        logger.error(f"Error in single staff sync: {e}")
        return False
    finally:
        if connection.is_connected():
            cursor.close()
            connection.close()

if __name__ == "__main__":
    import sys

    if len(sys.argv) > 1:
        # Test mode with specific staff ID
        staff_id = sys.argv[1]
        sync_single_staff(staff_id)
    else:
        # Full sync
        sync_staff()