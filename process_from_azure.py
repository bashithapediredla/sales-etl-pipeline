import json
import sqlite3
from azure.storage.blob import BlobServiceClient
from dotenv import load_dotenv
import os
from datetime import datetime

# -------------------- LOAD ENV --------------------
load_dotenv()
connection_string = os.getenv("AZURE_CONN_STRING")

# -------------------- CONNECT --------------------
blob_service_client = BlobServiceClient.from_connection_string(connection_string)

container_name = "raw-data"
blob_name = "crypto_data.json"

blob_client = blob_service_client.get_blob_client(container=container_name, blob=blob_name)

# -------------------- EXTRACT FROM AZURE --------------------
downloaded_data = blob_client.download_blob().readall()
data = json.loads(downloaded_data)

print("Fetched from Azure:", data)

# -------------------- TRANSFORM --------------------
price = data['bitcoin']['usd']
timestamp = datetime.now().strftime("%Y-%m-%d %H:%M:%S")

print("Extracted:", price, timestamp)

# -------------------- LOAD INTO DATABASE --------------------
conn = sqlite3.connect("crypto.db")
cursor = conn.cursor()

cursor.execute("""
CREATE TABLE IF NOT EXISTS crypto_prices (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    price REAL,
    timestamp TEXT
)
""")

cursor.execute(
    "INSERT INTO crypto_prices (price, timestamp) VALUES (?, ?)",
    (price, timestamp)
)

conn.commit()

print("Inserted into database")

# -------------------- QUERY --------------------
cursor.execute("SELECT * FROM crypto_prices")
rows = cursor.fetchall()

print("\nAll records:")
for row in rows:
    print(row)

conn.close()