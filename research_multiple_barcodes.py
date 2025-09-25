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
            return data["data"]
        else:
            print(f"Error: {response.status_code} - {response.text}")
            return None
    except Exception as e:
        print(f"Connection error: {e}")
        return None

def research_multiple_barcodes():
    """
    Research if SAP B1 supports multiple barcodes per item
    """
    print("🔍 Researching multiple barcodes support in SAP B1...")

    # Check if there are alternative item code tables
    print("\n📋 Checking for alternative barcode tables...")

    # Common SAP B1 tables that might store multiple barcodes
    tables_to_check = [
        "OALC",  # Alternative Item Codes
        "ALC1",  # Alternative Item Codes Lines
        "OUGP",  # Unit of Measurement Groups
        "UGP1",  # Unit of Measurement Groups Lines
        "OBTN",  # Batch Numbers (sometimes contain barcodes)
        "OSRN",  # Serial Numbers (sometimes contain barcodes)
    ]

    found_tables = []

    for table in tables_to_check:
        print(f"\n🔍 Checking table: {table}")
        query = f"SELECT TOP 3 * FROM {table}"
        result = send_sql_query(query)

        if result:
            print(f"  ✅ Table {table} exists")
            print(f"  📊 Columns: {list(result[0].keys())}")
            found_tables.append(table)

            # Look for barcode-like fields
            for item in result:
                for key, value in item.items():
                    if value and isinstance(value, str) and len(value) >= 8 and value.isdigit():
                        print(f"    🏷️  Potential barcode in {key}: {value}")
        else:
            print(f"  ❌ Table {table} not found")

    # If OALC (Alternative Item Codes) exists, check for our test items
    if "OALC" in found_tables:
        print(f"\n🧪 Testing Alternative Item Codes for known items...")

        test_items = ['INSPIRE2', 'P4PRO+', 'P4PRO']

        for item_code in test_items:
            print(f"\n  🔍 Checking alternative codes for: {item_code}")

            # Check if this item has alternative codes
            query = f"SELECT * FROM OALC WHERE ItemCode = '{item_code}'"
            result = send_sql_query(query)

            if result:
                print(f"    ✅ Found {len(result)} alternative code records")
                for alt_record in result:
                    print(f"    📄 Alternative record: {alt_record}")
            else:
                print(f"    ❌ No alternative codes found")

    # Check UoM groups which sometimes have different barcodes per unit
    if "OUGP" in found_tables and "UGP1" in found_tables:
        print(f"\n🧪 Checking Unit of Measurement groups for barcode variations...")

        # Get items that use UoM groups
        query = "SELECT TOP 5 ItemCode, UgpEntry FROM OITM WHERE UgpEntry IS NOT NULL AND UgpEntry > 0"
        result = send_sql_query(query)

        if result:
            for item in result:
                item_code = item['ItemCode']
                ugp_entry = item['UgpEntry']

                print(f"\n  🔍 Item {item_code} uses UoM group {ugp_entry}")

                # Get UoM group details
                query = f"SELECT * FROM UGP1 WHERE UgpEntry = {ugp_entry}"
                ugp_result = send_sql_query(query)

                if ugp_result:
                    for uom in ugp_result:
                        print(f"    📦 UoM: {uom}")

                        # Look for barcode-like values in UoM data
                        for key, value in uom.items():
                            if value and isinstance(value, str) and len(value) >= 8 and value.isdigit():
                                print(f"      🏷️  Potential barcode in {key}: {value}")

    # Check if there are custom user-defined fields that might store barcodes
    print(f"\n🔍 Checking for custom barcode fields in OITM...")

    # Get sample item with all fields
    query = "SELECT TOP 1 * FROM OITM WHERE CodeBars IS NOT NULL"
    result = send_sql_query(query)

    if result:
        item_data = result[0]
        potential_barcode_fields = []

        for key, value in item_data.items():
            # Look for fields that might contain additional barcodes
            if 'code' in key.lower() or 'bar' in key.lower() or 'upc' in key.lower() or 'ean' in key.lower():
                if key != 'ItemCode' and key != 'CodeBars':  # Skip the main fields we know
                    potential_barcode_fields.append(key)

        if potential_barcode_fields:
            print(f"  🏷️  Potential additional barcode fields: {potential_barcode_fields}")

            # Test these fields with our known items
            for field in potential_barcode_fields:
                query = f"SELECT ItemCode, CodeBars, {field} FROM OITM WHERE {field} IS NOT NULL AND {field} != '' LIMIT 5"
                result = send_sql_query(query)
                if result:
                    print(f"    📄 Items with data in {field}:")
                    for item in result:
                        print(f"      {item}")
        else:
            print(f"  ❌ No additional barcode fields found")

    print(f"\n📊 Summary:")
    print(f"Found tables: {found_tables}")
    print(f"Current implementation only uses OITM.CodeBars (single barcode)")

if __name__ == "__main__":
    print("Researching multiple barcodes support...")
    print("=" * 50)
    research_multiple_barcodes()