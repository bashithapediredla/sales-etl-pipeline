import requests
from azure.storage.blob import BlobServiceClient
import json
from dotenv import load_dotenv
import os

# -------------------- LOAD ENV --------------------
load_dotenv()
connection_string = os.getenv("AZURE_CONN_STRING")

# -------------------- EXTRACT --------------------
url = "https://api.coingecko.com/api/v3/simple/price?ids=bitcoin&vs_currencies=usd"

try:
    response = requests.get(url, timeout=10)
    response.raise_for_status()
    data = response.json()
    print("API data fetched:", data)

except requests.exceptions.RequestException as e:
    print("API failed:", e)
    data = None

# Stop if API failed
if not data:
    exit()

# -------------------- CONNECT --------------------
blob_service_client = BlobServiceClient.from_connection_string(connection_string)

container_name = "raw-data"
blob_name = "crypto_data.json"

blob_client = blob_service_client.get_blob_client(container=container_name, blob=blob_name)

# -------------------- LOAD --------------------
blob_client.upload_blob(json.dumps(data), overwrite=True)

print("Data uploaded to Azure successfully")