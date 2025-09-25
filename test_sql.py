import json
import requests
import os
from dotenv import load_dotenv

# Load environment variables from .env file
load_dotenv()

def send_sql_query(query):
    """
    Send SQL query to SAP B1 database via proxy
    """
    sql_proxy_url = os.getenv('SQL_PROXY_URL', 'http://dsbo01:8088/run-sql')

    my_config = {
        'sql_proxy_url': sql_proxy_url
    }

    headers = {'Content-Type': 'application/json'}
    payload = json.dumps({"query": query})

    try:
        response = requests.post(my_config['sql_proxy_url'], headers=headers, data=payload)

        if response.status_code == 200:
            data = response.json()
            if "error" in data:
                print("Error:", data["error"])
                return None
            else:
                print("Data:", data["data"])
                return data["data"]
        else:
            print(f"Error: {response.status_code} - {response.text}")
            return None
    except Exception as e:
        print(f"Connection error: {e}")
        return None

def test_simple_query():
    """
    Test with a simple SAP B1 system query
    """
    # Simple test query - gets basic company information
    test_query = "SELECT TOP 5 CompnyName, CompnyAddr FROM OADM"

    print("Testing SQL connection with simple query...")
    print(f"Query: {test_query}")
    print("-" * 50)

    result = send_sql_query(test_query)

    if result:
        print("✅ Test successful!")
        return True
    else:
        print("❌ Test failed!")
        return False

if __name__ == "__main__":
    test_simple_query()