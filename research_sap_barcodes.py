import json
import requests
import os
from dotenv import load_dotenv

# Load environment variables
load_dotenv()

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
                print("Error:", data["error"])
                return None
            else:
                return data["data"]
        else:
            print(f"Error: {response.status_code} - {response.text}")
            return None
    except Exception as e:
        print(f"Connection error: {e}")
        return None

def research_sap_barcode_tables():
    """
    Research SAP B1 tables that store barcode information
    """
    print("🔍 Researching SAP B1 barcode storage...")

    # Check common SAP B1 tables for barcode information
    tables_to_check = [
        "OITM",  # Items - Master Data
        "OITB",  # Item Groups
        "UPC1",  # Alternative Item Codes / Barcodes (if exists)
        "OBAR"   # Bar Codes (if exists)
    ]

    for table in tables_to_check:
        print(f"\n📋 Checking table: {table}")

        # First, check if table exists by trying to get its structure
        query = f"SELECT TOP 1 * FROM {table}"
        result = send_sql_query(query)

        if result:
            print(f"  ✅ Table {table} exists")
            if result:
                print(f"  📊 Sample columns: {list(result[0].keys())}")
        else:
            print(f"  ❌ Table {table} not found or access denied")

    # Look specifically at OITM table structure for barcode fields
    print(f"\n🔍 Examining OITM (Items) table for barcode fields...")

    # Get a sample item to see available fields
    query = "SELECT TOP 1 * FROM OITM"
    result = send_sql_query(query)

    if result and len(result) > 0:
        item_fields = list(result[0].keys())
        barcode_fields = [field for field in item_fields if 'bar' in field.lower() or 'upc' in field.lower() or 'ean' in field.lower()]

        print(f"  📊 Total fields in OITM: {len(item_fields)}")
        print(f"  🏷️  Potential barcode fields: {barcode_fields}")

        # Test with a known item code
        print(f"\n🧪 Testing with sample item codes...")
        test_items = ['INSPIRE2', 'P4PRO+', 'P4PRO']

        for item_code in test_items:
            print(f"\n  🔍 Testing item: {item_code}")

            # Try different possible barcode field combinations
            barcode_queries = [
                f"SELECT ItemCode, CodeBars FROM OITM WHERE ItemCode = '{item_code}'",
                f"SELECT ItemCode, BarCode FROM OITM WHERE ItemCode = '{item_code}'",
                f"SELECT ItemCode, UPC FROM OITM WHERE ItemCode = '{item_code}'",
                f"SELECT ItemCode, EAN FROM OITM WHERE ItemCode = '{item_code}'",
            ]

            for query in barcode_queries:
                result = send_sql_query(query)
                if result:
                    print(f"    ✅ Query successful: {query}")
                    print(f"    📄 Result: {result}")
                    break
            else:
                # If specific barcode fields don't work, get all fields for this item
                query = f"SELECT * FROM OITM WHERE ItemCode = '{item_code}'"
                result = send_sql_query(query)
                if result:
                    print(f"    📄 Full item data found - checking for barcode-like values...")
                    item_data = result[0]
                    for key, value in item_data.items():
                        if value and str(value).isdigit() and len(str(value)) >= 8:  # Potential barcode
                            print(f"      🏷️  {key}: {value}")

    # Check for alternative barcode tables
    print(f"\n🔍 Checking for alternative barcode storage...")

    alternative_queries = [
        "SELECT TOP 5 * FROM UPC1",  # Alternative item codes
        "SELECT TOP 5 * FROM OBAR",  # Barcode table
        "SELECT TOP 5 * FROM OBTN",  # Batch numbers (sometimes contains barcodes)
    ]

    for query in alternative_queries:
        print(f"\n  📋 Testing: {query}")
        result = send_sql_query(query)
        if result:
            print(f"    ✅ Table found!")
            print(f"    📊 Columns: {list(result[0].keys())}")
            print(f"    📄 Sample data: {result[0]}")
        else:
            print(f"    ❌ Table not found")

if __name__ == "__main__":
    print("Researching SAP B1 barcode storage...")
    print("=" * 50)
    research_sap_barcode_tables()