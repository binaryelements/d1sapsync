from barcode_sync import send_sql_query, get_sap_barcodes

# Test the multiple barcode functionality
def test_multiple_barcodes():
    """
    Test items with multiple barcodes
    """
    # Test items we know have multiple barcodes
    test_items = [
        'ARS-ATD-OSMOA5PRO',  # 3 barcodes
        'ARS-DD-DJIMINI3',    # 2 barcodes
        'ARS-OSMOA5PRO-SC'    # 3 barcodes
    ]

    for item_code in test_items:
        print(f"\n🧪 Testing item: {item_code}")
        print("=" * 50)

        # Check default barcode in OITM
        query = f"SELECT ItemCode, CodeBars FROM OITM WHERE ItemCode = '{item_code}'"
        oitm_result = send_sql_query(query)

        if oitm_result:
            print(f"OITM default barcode: {oitm_result[0].get('CodeBars', 'None')}")
        else:
            print("OITM: Item not found")

        # Check additional barcodes in OBCD
        query = f"SELECT ItemCode, BcdCode, BcdName FROM OBCD WHERE ItemCode = '{item_code}'"
        obcd_result = send_sql_query(query)

        if obcd_result:
            print(f"OBCD additional barcodes ({len(obcd_result)}):")
            for i, barcode_record in enumerate(obcd_result, 1):
                print(f"  {i}. {barcode_record.get('BcdCode')} ({barcode_record.get('BcdName', 'N/A')})")
        else:
            print("OBCD: No additional barcodes found")

        # Test our function
        all_barcodes = get_sap_barcodes(item_code)
        print(f"\n📊 Function result: Found {len(all_barcodes)} total barcodes")
        for i, barcode in enumerate(all_barcodes, 1):
            print(f"  {i}. {barcode}")

        # Test error condition (>4 barcodes)
        if len(all_barcodes) > 4:
            print(f"🚨 ERROR: Item has {len(all_barcodes)} barcodes (maximum is 4)!")
        elif len(all_barcodes) > 0:
            print(f"✅ Item has {len(all_barcodes)} barcodes (within limit)")

if __name__ == "__main__":
    print("Testing Multiple Barcode Functionality")
    print("=" * 60)
    test_multiple_barcodes()