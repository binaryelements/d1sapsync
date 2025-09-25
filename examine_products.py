import os
import mysql.connector
from mysql.connector import Error
from dotenv import load_dotenv

# Load environment variables
load_dotenv()

def examine_products_table():
    """
    Examine the products table structure and sample data
    """
    connection = None
    cursor = None

    try:
        connection = mysql.connector.connect(
            host=os.getenv('MYSQL_HOST'),
            database=os.getenv('MYSQL_DATABASE'),
            user=os.getenv('MYSQL_USER'),
            password=os.getenv('MYSQL_PASSWORD')
        )

        if connection.is_connected():
            cursor = connection.cursor()

            # Check if products table exists
            cursor.execute("SHOW TABLES LIKE 'products';")
            table_exists = cursor.fetchone()

            if not table_exists:
                print("❌ 'products' table not found!")
                print("📋 Available tables:")
                cursor.execute("SHOW TABLES;")
                tables = cursor.fetchall()
                for table in tables:
                    print(f"  - {table[0]}")
                return False

            print("✅ Found 'products' table")

            # Get table structure
            print("\n📊 Table structure:")
            cursor.execute("DESCRIBE products;")
            columns = cursor.fetchall()
            for col in columns:
                print(f"  {col[0]:<20} {col[1]:<15} {col[2]:<5} {col[3]:<5} {col[4] or ''}")

            # Check for required fields
            required_fields = ['sap_item_code', 'barcode', 'barcode1', 'barcode2', 'barcode3']
            column_names = [col[0] for col in columns]

            print(f"\n🔍 Checking for required fields:")
            for field in required_fields:
                if field in column_names:
                    print(f"  ✅ {field}")
                else:
                    print(f"  ❌ {field} - MISSING!")

            # Sample data
            print(f"\n📝 Sample data (first 5 rows):")
            cursor.execute("SELECT sap_item_code, barcode, barcode1, barcode2, barcode3 FROM products LIMIT 5;")
            rows = cursor.fetchall()

            if rows:
                print(f"{'sap_item_code':<15} {'barcode':<15} {'barcode1':<15} {'barcode2':<15} {'barcode3':<15}")
                print("-" * 75)
                for row in rows:
                    print(f"{str(row[0] or ''):<15} {str(row[1] or ''):<15} {str(row[2] or ''):<15} {str(row[3] or ''):<15} {str(row[4] or ''):<15}")
            else:
                print("  No data found in products table")

            # Count total records
            cursor.execute("SELECT COUNT(*) FROM products;")
            total_count = cursor.fetchone()[0]
            print(f"\n📊 Total records: {total_count}")

            # Count records with sap_item_code
            cursor.execute("SELECT COUNT(*) FROM products WHERE sap_item_code IS NOT NULL AND sap_item_code != '';")
            sap_code_count = cursor.fetchone()[0]
            print(f"📊 Records with sap_item_code: {sap_code_count}")

            return True

    except Error as e:
        print(f"❌ Error: {e}")
        return False

    finally:
        if cursor:
            cursor.close()
        if connection and connection.is_connected():
            connection.close()

if __name__ == "__main__":
    print("Examining products table...")
    print("=" * 50)
    examine_products_table()