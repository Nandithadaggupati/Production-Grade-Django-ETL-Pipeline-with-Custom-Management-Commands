import os
import sys
import time
import logging
import struct
import datetime
import requests
import psycopg2
import psycopg2.extras
from faker import Faker

# Configure Logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s [%(levelname)s] %(name)s: %(message)s',
    handlers=[logging.StreamHandler(sys.stdout)]
)
logger = logging.getLogger("cdc-consumer")

# Environment Variables
DB_USER = os.getenv("POSTGRES_USER", "postgres")
DB_PASSWORD = os.getenv("POSTGRES_PASSWORD", "postgres_password")
DB_NAME = os.getenv("POSTGRES_DB", "cdc_db")
DB_HOST = os.getenv("POSTGRES_HOST", "postgres")
DB_PORT = int(os.getenv("POSTGRES_PORT", "5432"))

MEILI_HOST = os.getenv("MEILI_HOST", "http://meilisearch:7700")
MEILI_KEY = os.getenv("MEILI_MASTER_KEY", "meili_master_key")

API_URL = os.getenv("API_URL", "http://api:8000")

CHECKPOINT_PATH = "/app/checkpoint/lsn_checkpoint.txt"

# Ensure the checkpoint directory exists
os.makedirs(os.path.dirname(CHECKPOINT_PATH), exist_ok=True)

def lsn_to_int(lsn_str):
    if not lsn_str:
        return 0
    try:
        x, y = lsn_str.split('/')
        return (int(x, 16) << 32) + int(y, 16)
    except Exception:
        return 0

class PgOutputParser:
    def __init__(self, pg_conn):
        self.relations = {}
        self.pg_conn = pg_conn

    def decode_string(self, payload, offset):
        end = payload.find(b'\x00', offset)
        if end == -1:
            raise ValueError("String not null-terminated")
        s = payload[offset:end].decode('utf-8')
        return s, end + 1

    def decode_tuple(self, payload, offset, num_cols):
        values = []
        for _ in range(num_cols):
            kind = chr(payload[offset])
            offset += 1
            if kind == 'n':
                values.append(None)
            elif kind == 'u':
                # Unchanged TOAST value
                values.append(None)
            elif kind == 't':
                length = struct.unpack_from('>I', payload, offset)[0]
                offset += 4
                val_bytes = payload[offset:offset+length]
                offset += length
                values.append(val_bytes.decode('utf-8'))
            else:
                raise ValueError(f"Unknown tuple column value kind: {kind}")
        return values, offset

    def fetch_relation_metadata(self, relation_id):
        try:
            with self.pg_conn.cursor() as cur:
                cur.execute("""
                    SELECT nspname, relname
                    FROM pg_class c
                    JOIN pg_namespace n ON c.relnamespace = n.oid
                    WHERE c.oid = %s
                """, (relation_id,))
                row = cur.fetchone()
                if not row:
                    return None
                schema_name, table_name = row
                
                cur.execute("""
                    SELECT attname
                    FROM pg_attribute
                    WHERE attrelid = %s AND attnum > 0 AND NOT attisdropped
                    ORDER BY attnum
                """, (relation_id,))
                cols = cur.fetchall()
                columns = [{'name': r[0]} for r in cols]
                
                return {
                    'schema': schema_name,
                    'table': table_name,
                    'columns': columns
                }
        except Exception as e:
            logger.error(f"Failed to fetch metadata for relation {relation_id}: {e}")
            return None

    def parse(self, payload):
        if not payload:
            return None
        
        msg_type = chr(payload[0])
        offset = 1
        
        if msg_type == 'B':  # Begin
            final_lsn, timestamp, xid = struct.unpack_from('>QQI', payload, offset)
            return {
                'type': 'BEGIN',
                'final_lsn': final_lsn,
                'timestamp': timestamp,
                'xid': xid
            }
            
        elif msg_type == 'C':  # Commit
            flags = payload[offset]
            offset += 1
            commit_lsn, end_lsn, timestamp = struct.unpack_from('>QQQ', payload, offset)
            return {
                'type': 'COMMIT',
                'commit_lsn': commit_lsn,
                'end_lsn': end_lsn,
                'timestamp': timestamp
            }
            
        elif msg_type == 'R':  # Relation
            relation_id = struct.unpack_from('>I', payload, offset)[0]
            offset += 4
            schema_name, offset = self.decode_string(payload, offset)
            table_name, offset = self.decode_string(payload, offset)
            replica_identity = chr(payload[offset])
            offset += 1
            num_columns = struct.unpack_from('>H', payload, offset)[0]
            offset += 2
            
            columns = []
            for _ in range(num_columns):
                flags = payload[offset]
                offset += 1
                col_name, offset = self.decode_string(payload, offset)
                col_type = struct.unpack_from('>I', payload, offset)[0]
                offset += 4
                col_typmod = struct.unpack_from('>i', payload, offset)[0]
                offset += 4
                columns.append({
                    'flags': flags,
                    'name': col_name,
                    'type_id': col_type,
                    'typmod': col_typmod
                })
            
            self.relations[relation_id] = {
                'schema': schema_name,
                'table': table_name,
                'columns': columns
            }
            logger.debug(f"Cached relation schema for table: {table_name} (ID: {relation_id})")
            return {
                'type': 'RELATION',
                'relation_id': relation_id,
                'schema': schema_name,
                'table': table_name,
                'columns': columns
            }
            
        elif msg_type == 'I':  # Insert
            relation_id = struct.unpack_from('>I', payload, offset)[0]
            offset += 4
            tuple_type = chr(payload[offset])
            offset += 1
            if tuple_type != 'N':
                raise ValueError("Expected 'N' for new tuple in Insert")
            num_columns = struct.unpack_from('>H', payload, offset)[0]
            offset += 2
            values, offset = self.decode_tuple(payload, offset, num_columns)
            
            rel = self.relations.get(relation_id)
            if not rel:
                logger.warning(f"Relation ID {relation_id} not found in cache. Attempting DB lookup...")
                rel = self.fetch_relation_metadata(relation_id)
                if rel:
                    self.relations[relation_id] = rel
                else:
                    raise ValueError(f"Relation {relation_id} schema could not be resolved.")
                
            row_dict = {}
            for col, val in zip(rel['columns'], values):
                row_dict[col['name']] = val
                
            return {
                'type': 'INSERT',
                'table': rel['table'],
                'schema': rel['schema'],
                'new': row_dict
            }
            
        elif msg_type == 'U':  # Update
            relation_id = struct.unpack_from('>I', payload, offset)[0]
            offset += 4
            
            next_byte = chr(payload[offset])
            offset += 1
            
            old_row_dict = None
            rel = self.relations.get(relation_id)
            if not rel:
                logger.warning(f"Relation ID {relation_id} not found in cache. Attempting DB lookup...")
                rel = self.fetch_relation_metadata(relation_id)
                if rel:
                    self.relations[relation_id] = rel
                else:
                    raise ValueError(f"Relation {relation_id} schema could not be resolved.")

            if next_byte in ('O', 'K'):
                num_columns = struct.unpack_from('>H', payload, offset)[0]
                offset += 2
                old_values, offset = self.decode_tuple(payload, offset, num_columns)
                old_row_dict = {}
                for col, val in zip(rel['columns'], old_values):
                    old_row_dict[col['name']] = val
                next_byte = chr(payload[offset])
                offset += 1
                
            if next_byte != 'N':
                raise ValueError(f"Expected 'N' for new tuple in Update, got {next_byte}")
                
            num_columns = struct.unpack_from('>H', payload, offset)[0]
            offset += 2
            new_values, offset = self.decode_tuple(payload, offset, num_columns)
            
            new_row_dict = {}
            for col, val in zip(rel['columns'], new_values):
                new_row_dict[col['name']] = val
                
            return {
                'type': 'UPDATE',
                'table': rel['table'],
                'schema': rel['schema'],
                'old': old_row_dict,
                'new': new_row_dict
            }
            
        elif msg_type == 'D':  # Delete
            relation_id = struct.unpack_from('>I', payload, offset)[0]
            offset += 4
            
            tuple_type = chr(payload[offset])
            offset += 1
            
            if tuple_type not in ('K', 'O'):
                raise ValueError(f"Expected 'K' or 'O' in Delete, got {tuple_type}")
                
            num_columns = struct.unpack_from('>H', payload, offset)[0]
            offset += 2
            old_values, offset = self.decode_tuple(payload, offset, num_columns)
            
            rel = self.relations.get(relation_id)
            if not rel:
                logger.warning(f"Relation ID {relation_id} not found in cache. Attempting DB lookup...")
                rel = self.fetch_relation_metadata(relation_id)
                if rel:
                    self.relations[relation_id] = rel
                else:
                    raise ValueError(f"Relation {relation_id} schema could not be resolved.")
                
            old_row_dict = {}
            for col, val in zip(rel['columns'], old_values):
                old_row_dict[col['name']] = val
                
            return {
                'type': 'DELETE',
                'table': rel['table'],
                'schema': rel['schema'],
                'old': old_row_dict
            }
            
        return None

def wait_for_services():
    # Wait for PostgreSQL
    while True:
        try:
            logger.info("Attempting connection to PostgreSQL...")
            conn = psycopg2.connect(
                user=DB_USER,
                password=DB_PASSWORD,
                dbname=DB_NAME,
                host=DB_HOST,
                port=DB_PORT
            )
            # Check if tables exist
            with conn.cursor() as cur:
                cur.execute("SELECT to_regclass('products')")
                exists = cur.fetchone()[0]
                if exists:
                    conn.close()
                    logger.info("PostgreSQL is ready and initialized.")
                    break
            conn.close()
        except Exception as e:
            logger.warning(f"PostgreSQL not ready yet ({e}). Retrying in 2 seconds...")
        time.sleep(2)

    # Wait for Meilisearch
    while True:
        try:
            logger.info("Checking connection to Meilisearch...")
            res = requests.get(f"{MEILI_HOST}/health", headers={"Authorization": f"Bearer {MEILI_KEY}"}, timeout=2)
            if res.status_code == 200:
                logger.info("Meilisearch is ready.")
                break
        except Exception as e:
            logger.warning(f"Meilisearch not ready yet ({e}). Retrying in 2 seconds...")
        time.sleep(2)

def configure_meilisearch():
    headers = {"Authorization": f"Bearer {MEILI_KEY}"}
    
    # Create index if it does not exist
    try:
        res = requests.post(f"{MEILI_HOST}/indexes", headers=headers, json={"uid": "products", "primaryKey": "id"})
        if res.status_code in (201, 400):  # 400 means already exists
            logger.info("Meilisearch 'products' index initialized.")
    except Exception as e:
        logger.error(f"Failed to create Meilisearch index: {e}")

    # Set Searchable Attributes
    requests.put(
        f"{MEILI_HOST}/indexes/products/settings/searchable-attributes",
        headers=headers,
        json=["name", "description", "category"]
    )
    # Set Filterable Attributes
    requests.put(
        f"{MEILI_HOST}/indexes/products/settings/filterable-attributes",
        headers=headers,
        json=["category", "in_stock"]
    )
    # Set Sortable Attributes
    requests.put(
        f"{MEILI_HOST}/indexes/products/settings/sortable-attributes",
        headers=headers,
        json=["price"]
    )
    logger.info("Meilisearch 'products' settings configured successfully.")

def fetch_denormalized_product(product_id, pg_conn):
    with pg_conn.cursor() as cur:
        cur.execute("""
            SELECT p.product_id, p.name, p.description, p.price, c.name as category, COALESCE(i.quantity, 0) as quantity
            FROM products p
            LEFT JOIN categories c ON p.category_id = c.category_id
            LEFT JOIN inventory i ON p.product_id = i.product_id
            WHERE p.product_id = %s
        """, (product_id,))
        row = cur.fetchone()
        if row:
            return {
                "id": row[0],
                "name": row[1],
                "description": row[2],
                "price": float(row[3]),
                "category": row[4],
                "in_stock": row[5] > 0,
                "quantity": row[5]
            }
        return None

def fetch_product_id_by_inventory(inventory_id, pg_conn):
    with pg_conn.cursor() as cur:
        cur.execute("SELECT product_id FROM inventory WHERE inventory_id = %s", (inventory_id,))
        row = cur.fetchone()
        return row[0] if row else None

def seed_database_if_empty():
    logger.info("Checking if database seeding is required...")
    conn = psycopg2.connect(
        user=DB_USER,
        password=DB_PASSWORD,
        dbname=DB_NAME,
        host=DB_HOST,
        port=DB_PORT
    )
    with conn.cursor() as cur:
        cur.execute("SELECT count(*) FROM products;")
        count = cur.fetchone()[0]
        if count >= 5000:
            logger.info(f"Database already contains {count} products. Seeding skipped.")
            conn.close()
            return
        
    logger.info("Seeding database with 10,000 realistic products...")
    fake = Faker()
    
    categories = ['Electronics', 'Clothing', 'Books', 'Home & Kitchen', 'Beauty', 'Sports', 'Automotive', 'Toys', 'Office Products', 'Grocery']
    
    with conn.cursor() as cur:
        # Insert categories
        category_ids = []
        for cat in categories:
            cur.execute("INSERT INTO categories (name) VALUES (%s) RETURNING category_id;", (cat,))
            category_ids.append(cur.fetchone()[0])
        
        # Insert products in batches
        logger.info("Generating products and inventory data...")
        products_batch = []
        for i in range(10000):
            name = fake.catch_phrase()
            desc = f"{fake.sentence()} {fake.paragraph(nb_sentences=2)}"
            price = round(fake.random.uniform(5.0, 1000.0), 2)
            cat_id = fake.random.choice(category_ids)
            products_batch.append((name, desc, price, cat_id))
            
        # Bulk insert products using execute_values
        from psycopg2.extras import execute_values
        logger.info("Bulk inserting products...")
        execute_values(
            cur,
            "INSERT INTO products (name, description, price, category_id) VALUES %s RETURNING product_id",
            products_batch
        )
        product_ids = [r[0] for r in cur.fetchall()]
        
        # Generate and insert inventory
        logger.info("Bulk inserting inventory...")
        inventory_batch = [(pid, fake.random.randint(0, 100)) for pid in product_ids]
        execute_values(
            cur,
            "INSERT INTO inventory (product_id, quantity) VALUES %s",
            inventory_batch
        )
        
    conn.commit()
    logger.info("PostgreSQL seeding complete.")
    
    # Bulk index to Meilisearch directly so Meilisearch is also initialized and fast
    logger.info("Bulk indexing seeded records to Meilisearch...")
    headers = {"Authorization": f"Bearer {MEILI_KEY}"}
    
    with conn.cursor() as cur:
        cur.execute("""
            SELECT p.product_id, p.name, p.description, p.price, c.name as category, COALESCE(i.quantity, 0) as quantity
            FROM products p
            LEFT JOIN categories c ON p.category_id = c.category_id
            LEFT JOIN inventory i ON p.product_id = i.product_id
        """)
        rows = cur.fetchall()
        
    documents = []
    for r in rows:
        documents.append({
            "id": r[0],
            "name": r[1],
            "description": r[2],
            "price": float(r[3]),
            "category": r[4],
            "in_stock": r[5] > 0,
            "quantity": r[5]
        })
        
    # Chunk Meilisearch bulk index
    chunk_size = 1000
    for idx in range(0, len(documents), chunk_size):
        chunk = documents[idx:idx+chunk_size]
        res = requests.post(f"{MEILI_HOST}/indexes/products/documents", headers=headers, json=chunk)
        if res.status_code != 202:
            logger.error(f"Meilisearch bulk index error: {res.text}")
            
    logger.info("Meilisearch bulk seeding indexing complete.")
    conn.close()

def send_sse_notification(table, operation):
    try:
        payload = {
            "table": table,
            "operation": operation,
            "timestamp": datetime.datetime.utcnow().isoformat() + "Z"
        }
        res = requests.post(f"{API_URL}/api/internal/cdc-event", json=payload, timeout=2)
        if res.status_code != 200:
            logger.error(f"Failed to post CDC event to API: {res.text}")
    except Exception as e:
        logger.error(f"Error notifying API of CDC event: {e}")

def main():
    logger.info("Starting CDC Consumer Service...")
    wait_for_services()
    configure_meilisearch()
    seed_database_if_empty()
    
    # We open a standard connection for querying database details during denormalization
    pg_std_conn = psycopg2.connect(
        user=DB_USER,
        password=DB_PASSWORD,
        dbname=DB_NAME,
        host=DB_HOST,
        port=DB_PORT
    )
    pg_std_conn.autocommit = True
    
    # We open the replication connection
    logger.info("Connecting to replication slot...")
    repl_conn = psycopg2.connect(
        user=DB_USER,
        password=DB_PASSWORD,
        dbname=DB_NAME,
        host=DB_HOST,
        port=DB_PORT,
        connection_factory=psycopg2.extras.LogicalReplicationConnection
    )
    
    repl_cur = repl_conn.cursor()
    
    # Create slot if not exists
    try:
        repl_cur.create_replication_slot("my_replication_slot", output_plugin="pgoutput")
        logger.info("Replication slot 'my_replication_slot' created.")
    except psycopg2.errors.DuplicateObject:
        logger.info("Replication slot 'my_replication_slot' already exists.")
        
    # Read LSN checkpoint if it exists
    start_lsn = 0
    if os.path.exists(CHECKPOINT_PATH):
        try:
            with open(CHECKPOINT_PATH, "r") as f:
                checkpoint = f.read().strip()
                if checkpoint:
                    if '/' in checkpoint:
                        start_lsn = lsn_to_int(checkpoint)
                    else:
                        start_lsn = int(checkpoint)
                    logger.info(f"Resuming streaming from LSN checkpoint: {start_lsn}")
        except Exception as e:
            logger.error(f"Failed to read LSN checkpoint: {e}")
            
    # Start Replication Stream
    logger.info("Starting streaming replication...")
    repl_cur.start_replication(
        slot_name="my_replication_slot",
        start_lsn=start_lsn,
        decode=False,
        status_interval=10,
        options={'proto_version': '1', 'publication_names': 'my_publication'}
    )
    
    parser = PgOutputParser(pg_std_conn)
    headers = {"Authorization": f"Bearer {MEILI_KEY}"}
    
    # We keep buffer of changes within a transaction
    tx_changes = []
    last_commit_lsn = None
    
    def consume_message(msg):
        nonlocal last_commit_lsn, tx_changes
        
        # msg.payload is the raw pgoutput binary data
        payload = msg.payload
        lsn = msg.data_start
        
        try:
            event = parser.parse(payload)
            if not event:
                return
            
            logger.debug(f"Parsed CDC Message: {event['type']} at LSN {lsn}")
            
            if event['type'] == 'BEGIN':
                tx_changes = []
                
            elif event['type'] == 'COMMIT':
                # Process the accumulated changes for this transaction
                logger.info(f"Transaction committed. Processing {len(tx_changes)} CDC events.")
                for change in tx_changes:
                    tbl = change['table']
                    op = change['op']
                    
                    if tbl == 'products':
                        pid = change['id']
                        if op in ('INSERT', 'UPDATE'):
                            doc = fetch_denormalized_product(pid, pg_std_conn)
                            if doc:
                                requests.post(f"{MEILI_HOST}/indexes/products/documents", headers=headers, json=[doc])
                                logger.info(f"Indexed product {pid} into Meilisearch.")
                                send_sse_notification(tbl, op)
                        elif op == 'DELETE':
                            requests.delete(f"{MEILI_HOST}/indexes/products/documents/{pid}", headers=headers)
                            logger.info(f"Deleted product {pid} from Meilisearch.")
                            send_sse_notification(tbl, op)
                            
                    elif tbl == 'inventory':
                        # Inventory changes affect product denormalized state
                        pid = change['product_id']
                        if op in ('INSERT', 'UPDATE'):
                            doc = fetch_denormalized_product(pid, pg_std_conn)
                            if doc:
                                requests.post(f"{MEILI_HOST}/indexes/products/documents", headers=headers, json=[doc])
                                logger.info(f"Updated product {pid} from inventory change in Meilisearch.")
                                send_sse_notification('products', 'UPDATE')
                        elif op == 'DELETE':
                            # Mark product out of stock
                            doc = fetch_denormalized_product(pid, pg_std_conn)
                            if doc:
                                requests.post(f"{MEILI_HOST}/indexes/products/documents", headers=headers, json=[doc])
                            logger.info(f"Deleted inventory for product {pid}. Product updated in Meilisearch.")
                            send_sse_notification('products', 'UPDATE')
                
                # Update checkpoint file
                last_commit_lsn = lsn
                with open(CHECKPOINT_PATH, "w") as f:
                    f.write(str(last_commit_lsn))
                logger.info(f"LSN Checkpoint written: {last_commit_lsn}")
                
                # Send replication feedback/ack to PostgreSQL
                msg.cursor.send_feedback(flush_lsn=last_commit_lsn)
                tx_changes = []
                
            elif event['type'] in ('INSERT', 'UPDATE', 'DELETE'):
                # Append to current transaction buffer
                tbl = event['table']
                op = event['type']
                
                if tbl == 'products':
                    pid = event['new']['product_id'] if op in ('INSERT', 'UPDATE') else event['old']['product_id']
                    tx_changes.append({
                        'table': tbl,
                        'op': op,
                        'id': pid
                    })
                elif tbl == 'inventory':
                    pid = event['new']['product_id'] if op in ('INSERT', 'UPDATE') else event['old']['product_id']
                    tx_changes.append({
                        'table': tbl,
                        'op': op,
                        'product_id': pid
                    })
                    
        except Exception as e:
            logger.error(f"Error processing replication message: {e}", exc_info=True)
            
    # Start loop
    logger.info("Replication consumer loop running...")
    try:
        repl_cur.consume_stream(consume_message)
    except KeyboardInterrupt:
        logger.info("KeyboardInterrupt received. Stopping.")
    finally:
        repl_cur.close()
        repl_conn.close()
        pg_std_conn.close()

if __name__ == '__main__':
    main()
