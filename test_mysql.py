import os
import mysql.connector
from mysql.connector import Error
from dotenv import load_dotenv

# Load environment variables from .env file
load_dotenv()

def test_mysql_connection():
    """
    Test MySQL database connection
    """
    connection = None
    cursor = None

    try:
        # Get connection details from environment variables
        connection = mysql.connector.connect(
            host=os.getenv('MYSQL_HOST'),
            database=os.getenv('MYSQL_DATABASE'),
            user=os.getenv('MYSQL_USER'),
            password=os.getenv('MYSQL_PASSWORD')
        )

        if connection.is_connected():
            db_info = connection.get_server_info()
            print(f"✅ Successfully connected to MySQL Server version {db_info}")

            cursor = connection.cursor()
            cursor.execute("SELECT DATABASE();")
            database_name = cursor.fetchone()
            print(f"📊 Connected to database: {database_name[0]}")

            # Test with a simple query to show tables
            cursor.execute("SHOW TABLES;")
            tables = cursor.fetchmany(5)  # Fetch only first 5 tables

            if tables:
                print(f"📋 Found {len(tables)} tables (showing first 5):")
                for table in tables:
                    print(f"  - {table[0]}")
            else:
                print("📋 No tables found in database")

            # Consume any remaining results
            cursor.fetchall()

            return True

    except Error as e:
        print(f"❌ Error connecting to MySQL: {e}")
        return False

    finally:
        if cursor:
            cursor.close()
        if connection and connection.is_connected():
            connection.close()
            print("🔌 MySQL connection closed")

if __name__ == "__main__":
    print("Testing MySQL database connection...")
    print("=" * 50)
    test_mysql_connection()