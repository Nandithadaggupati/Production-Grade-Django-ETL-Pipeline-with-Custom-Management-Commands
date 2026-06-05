import time
import requests
import psycopg2
import threading

# Config inside container
DB_USER = "postgres"
DB_PASSWORD = "postgres_password"
DB_NAME = "cdc_db"
DB_HOST = "postgres"
DB_PORT = 5432
MEILI_HOST = "http://meilisearch:7700"
MEILI_KEY = "meili_master_key"
API_URL = "http://api:8000"

print("--- STARTING CDC PIPELINE VERIFICATION TEST ---")

sse_events = []
stop_sse = False

def listen_sse():
    global sse_events, stop_sse
    try:
        r = requests.get(f"{API_URL}/api/cdc-stream", stream=True, timeout=15)
        for line in r.iter_lines():
            if stop_sse:
                break
            if line:
                line_str = line.decode('utf-8')
                if line_str.startswith("data:"):
                    import json
                    try:
                        data = json.loads(line_str[5:].strip())
                        sse_events.append(data)
                        print(f"[SSE Client] Received Event: {data}")
                    except Exception as e:
                        pass
    except Exception as e:
         pass

# Start SSE listener thread
t = threading.Thread(target=listen_sse, daemon=True)
t.start()
time.sleep(2) # Wait for SSE client connection

# Connect to Postgres
conn = psycopg2.connect(
    user=DB_USER,
    password=DB_PASSWORD,
    dbname=DB_NAME,
    host=DB_HOST,
    port=DB_PORT
)
conn.autocommit = True
cur = conn.cursor()

# 1. Test INSERT
print("\n1. Testing INSERT...")
cur.execute("SELECT category_id FROM categories LIMIT 1;")
cat_id = cur.fetchone()[0]

unique_name = "CDC_TEST_PRODUCT_12345"
cur.execute(
    "INSERT INTO products (name, description, price, category_id) VALUES (%s, %s, %s, %s) RETURNING product_id;",
    (unique_name, "This is a test product for change data capture verification.", 199.99, cat_id)
)
product_id = cur.fetchone()[0]

# Add inventory (making in_stock = True)
cur.execute("INSERT INTO inventory (product_id, quantity) VALUES (%s, %s);", (product_id, 15))

time.sleep(2) # Wait for replication to complete

# Query Meilisearch
res = requests.get(
    f"{MEILI_HOST}/indexes/products/documents/{product_id}",
    headers={"Authorization": f"Bearer {MEILI_KEY}"}
)

if res.status_code == 200:
    doc = res.json()
    assert doc['name'] == unique_name
    assert doc['price'] == 199.99
    assert doc['in_stock'] is True
    assert doc['quantity'] == 15
    print("Meilisearch: INSERT verified successfully!")
else:
    print(f"Meilisearch: INSERT verification FAILED. Status code: {res.status_code}, Response: {res.text}")

# 2. Test UPDATE
print("\n2. Testing UPDATE...")
cur.execute("UPDATE products SET price = 249.99 WHERE product_id = %s;", (product_id,))
cur.execute("UPDATE inventory SET quantity = 0 WHERE product_id = %s;", (product_id,))

time.sleep(2) # Wait for replication to complete

res = requests.get(
    f"{MEILI_HOST}/indexes/products/documents/{product_id}",
    headers={"Authorization": f"Bearer {MEILI_KEY}"}
)

if res.status_code == 200:
    doc = res.json()
    assert doc['price'] == 249.99
    assert doc['in_stock'] is False
    assert doc['quantity'] == 0
    print("Meilisearch: UPDATE verified successfully!")
else:
    print(f"Meilisearch: UPDATE verification FAILED.")

# 3. Test DELETE
print("\n3. Testing DELETE...")
cur.execute("DELETE FROM inventory WHERE product_id = %s;", (product_id,))
cur.execute("DELETE FROM products WHERE product_id = %s;", (product_id,))

time.sleep(2) # Wait for replication to complete

res = requests.get(
    f"{MEILI_HOST}/indexes/products/documents/{product_id}",
    headers={"Authorization": f"Bearer {MEILI_KEY}"}
)

if res.status_code == 404:
    print("Meilisearch: DELETE verified successfully!")
else:
    print(f"Meilisearch: DELETE verification FAILED. Status code: {res.status_code}")

# 4. Check SSE Events
print("\n4. Verifying SSE Events...")
print(f"Total captured SSE events: {len(sse_events)}")
for evt in sse_events:
    print(f"- Table: {evt.get('table')}, Operation: {evt.get('operation')}")

# Stop thread
stop_sse = True
conn.close()

print("\n--- CDC PIPELINE VERIFICATION COMPLETED ---")
