# PostgreSQL to Meilisearch Change Data Capture (CDC) Pipeline

A real-time Change Data Capture (CDC) pipeline that replicates database modifications (inserts, updates, deletes) in a PostgreSQL database to a Meilisearch index.

Built from scratch using native PostgreSQL binary logical replication protocols (specifically `pgoutput`), this pipeline serves as a low-latency, event-driven search indexing system.

## Key Features
- **Zero Polling Real-Time Replication**: Uses Postgres logical replication to stream WAL logs directly, ensuring less than 1-second latency.
- **Custom `pgoutput` Protocol Parser**: Unpacks logical replication messages (`Begin`, `Relation`, `Insert`, `Update`, `Delete`, `Commit`) using Python's binary `struct` unpacking.
- **LSN Checkpointing**: Confirms transaction progress and persists Log Sequence Numbers (LSNs) to disk to guarantee at-least-once delivery and error recovery on crash restarts.
- **Server-Sent Events (SSE)**: Exposes a reactive event-stream endpoint `/api/cdc-stream` to notify web clients of events as they occur.
- **Premium Frontend Dashboard**: React SPA with live search, filters, a real-time event monitor sidebar, and a flashing status indicator.

---

## System Architecture

```
                                          +---------------------------------+
                                          |                                 |
                                          |        Browser Frontend         |
                                          |                                 |
                                          +----+-----------------------^----+
                                               |                       |
                                        Search |                       | SSE Stream
                                       Queries |                       | (/api/cdc-stream)
                                               v                       |
+-------------------+                     +----+------------------+    |
|                   |                     |                       |    |
|   Meilisearch     |                     |    FastAPI Server     |----+
|                   |                     |                       |
+^------------------+                     +------------------^----+
 |                                                           |
 | Documents                                                 | HTTP POST CDC Event
 |                                                           |
+------------------------------------------------------------+
|                                                            |
|                    CDC Consumer (Python)                   |
|                                                            |
+^-----------------------------------------------------------+
 |
 | PG Logical Replication Stream (pgoutput binary)
 |
+-------------------+
|                   |
|    PostgreSQL     |
|                   |
+-------------------+
```

1. **PostgreSQL** publishes changes for all tables.
2. **CDC Consumer** reads the logical replication slot and parses the binary `pgoutput` packets:
   - Maintains a schema cache of relation IDs.
   - Denormalizes data from `products`, `categories`, and `inventory` by executing standard Postgres queries.
   - Pushes updates to the **Meilisearch** index.
   - Dispatches a post-commit notification containing operation metadata to the API.
   - Checkpoints progress by writing the latest LSN to `lsn_checkpoint.txt`.
3. **API (FastAPI)** broadcasts events to connected clients via **Server-Sent Events (SSE)** and serves static frontend bundle assets.
4. **React SPA** renders a live searchable list and a live Activity Log feed.

---

## Folder Structure

```
├── init-db/
│   └── init.sql            # Table schema, replica identity FULL, publication my_publication
├── cdc-consumer/
│   ├── Dockerfile          # Builds consumer service
│   ├── requirements.txt    # Python requirements
│   └── main.py             # Binary stream consumer, database seeder
├── api/
│   ├── Dockerfile          # Multi-stage build (builds UI with Node, serves with FastAPI)
│   ├── requirements.txt    # Python requirements
│   └── main.py             # FastAPI backend with /api/cdc-stream & /api/config
├── frontend/
│   ├── index.html          # Root template
│   ├── package.json        # Frontend package definition
│   ├── vite.config.js      # Vite configuration
│   └── src/
│       ├── main.jsx        # Entrypoint
│       ├── App.jsx         # Dashboard application logic
│       └── index.css       # Premium cyberpunk styling
├── docker-compose.yml      # Orchestrates all services
├── .env.example            # Environment variables documentation
└── submission.json         # Evaluation settings file
```

---

## Getting Started

### Prerequisites
- Docker and Docker Compose

### Run the Pipeline
Run the following command at the repository root:
```bash
docker-compose up --build
```
This single command will:
1. Boot PostgreSQL and initialize the tables + publication slot.
2. Boot Meilisearch.
3. Build and run the `cdc-consumer` (which seeds Postgres with 10,000 realistic products and indexes them into Meilisearch on first startup).
4. Build the React frontend SPA and run the FastAPI server to serve both API routes and static pages.

Open **`http://localhost:8000`** in your browser to view the premium dashboard.

---

## Verification & Testing

### 1. Database Seeding
Verify that Postgres was correctly seeded with 10,000 products:
```bash
docker exec -it cdc-postgres psql -U postgres -d cdc_db -c "SELECT count(*) FROM products;"
```

### 2. Manual DML Operations
Verify that direct database changes are automatically synchronized to Meilisearch and show up on the UI:

- **Insert a new product**:
  ```sql
  INSERT INTO products (name, description, price, category_id) 
  VALUES ('Ultra Quantum Soundbar', 'Immersive spatial audio system.', 499.99, 1);
  
  -- Insert inventory to make it active
  INSERT INTO inventory (product_id, quantity) 
  VALUES ((SELECT product_id FROM products WHERE name='Ultra Quantum Soundbar'), 45);
  ```
  Check the browser UI or query Meilisearch for "Soundbar". The item will appear within 1 second, and the live activity log will append an `INSERT` event.

- **Update product details**:
  ```sql
  UPDATE products SET price = 399.99 WHERE name = 'Ultra Quantum Soundbar';
  ```
  The price in the UI and search results will update immediately, accompanied by an `UPDATE` event.

- **Delete product**:
  ```sql
  DELETE FROM products WHERE name = 'Ultra Quantum Soundbar';
  ```
  The product will disappear from the search results, accompanied by a `DELETE` event.

### 3. Checkpoint & Recovery Test
To prove that LSN checkpointing works and guarantees at-least-once delivery:
1. Stop the consumer service:
   ```bash
   docker-compose stop cdc-consumer
   ```
2. Insert a product while the consumer is offline:
   ```sql
   INSERT INTO products (name, description, price, category_id) 
   VALUES ('Offline Replicated Product', 'Should be caught up after boot.', 89.99, 1);
   ```
3. Check the UI or search index; the product will **not** be there.
4. Restart the consumer service:
   ```bash
   docker-compose start cdc-consumer
   ```
5. Check the logs: `docker-compose logs cdc-consumer`. You will see it reading the LSN checkpoint from `lsn_checkpoint.txt` and catches up.
6. Verify that "Offline Replicated Product" is now fully searchable in Meilisearch.
