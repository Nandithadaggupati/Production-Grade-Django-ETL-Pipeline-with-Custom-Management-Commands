# Robust Django Data Migration Pipeline

This project implements a highly performant, memory-efficient, idempotent, and resumable ETL (Extract, Transform, Load) data migration pipeline using Django and PostgreSQL. It demonstrates how to migrate **500,000 denormalized legacy order records** into a normalized database structure at scale without exhausting system memory or database network resources.

---

## 1. System Architecture and Normalization

```
LegacyOrder (JSONField)
  ├── customer_email
  ├── total
  └── items [SKU, quantity, unit_price]
         │
         │ (ETL Migration)
         ▼
Order (Table)  ◄───────┐
  ├── customer_email   │ (Foreign Key)
  └── total            │
                       │
OrderLine (Table) ─────┘
  ├── SKU
  ├── quantity
  └── unit_price
```

### Data Models
1. **`LegacyOrder`**: Denormalized legacy storage with raw customer/order JSON data and a `migrated` tracker boolean flag.
2. **`Order`**: Normalized orders containing parent customer email, total value, and primary key index.
3. **`OrderLine`**: Normalized order items linked to their parent `Order` via a foreign key relationship.

---

## 2. Key Optimization Techniques

1. **Server-Side Cursor (`QuerySet.iterator()`)**: Bypasses Django's QuerySet caching by fetching and yielding records in small batches from the database cursor. Keeps memory footprint constant ($O(1)$) and prevents Out-Of-Memory (OOM) failures.
2. **Bulk Insertion (`bulk_create`)**: Consolidates individually written INSERT operations into database-level batch INSERT statements.
3. **Natural Key ID Mapping**: Resolves parent `Order` primary keys from the database using their unique `external_id` natural keys after bulk insertion, allowing correct association of child `OrderLines` before their subsequent bulk insert.
4. **Idempotency & Resumability**: Filters and processes only unprocessed legacy records (`migrated=False`). The `--start-from` argument allows resuming the pipeline starting at a specific alphabetically sorted ID in the event of failure.
5. **Atomic Transactions (`transaction.atomic()`)**: Wraps batch inserts inside an atomic block, ensuring that if any command fails, the batch rolls back completely, preserving database consistency.

---

## 3. Getting Started

### Prerequisites
- Docker
- Docker Compose

### Setup Instructions

1. **Clone/Move into the workspace folder**:
   ```bash
   cd C:/Users/nandi/.gemini/antigravity/scratch/data_migration_pipeline
   ```

2. **Spin up the Docker services**:
   ```bash
   docker-compose up --build -d
   ```
   This command starts:
   - A `db` service (PostgreSQL 15 image with health checks).
   - An `app` service (Django container waiting for the database to become healthy).

3. **Run database migrations**:
   ```bash
   docker-compose exec app python manage.py migrate
   ```

---

## 4. How to Run Commands

### 1. Seeding Legacy Data
Seed the `LegacyOrder` table with 500,000 unique records:
```bash
docker-compose exec app python manage.py seed_legacy_data
```

### 2. Running the Migration
Run the optimized migration pipeline:
```bash
docker-compose exec app python manage.py migrate_orders
```

#### Supported Arguments:
- `--batch-size <INTEGER>`: Customizes the batch size (default: `1000`).
- `--dry-run`: Performs the transformation and reports log counts without writing to PostgreSQL.
- `--start-from <STRING>`: Specifies an `external_id` from which to resume processing.
- `--naive`: Runs the naive database write benchmarking logic (uses single inserts).
- `--limit <INTEGER>`: Restricts processing to a maximum number of records.

---

## 5. Verification Queries

Validate the state of the database by connecting to PostgreSQL:
```bash
docker-compose exec db psql -U postgres -d migration_db
```

Run the following checks:
```sql
-- Ensure legacy orders exist
SELECT COUNT(*) FROM migration_app_legacyorder;

-- Ensure all orders are marked as migrated
SELECT COUNT(*) FROM migration_app_legacyorder WHERE migrated = true;

-- Validate counts in the normalized tables
SELECT COUNT(*) FROM migration_app_order;
SELECT COUNT(*) FROM migration_app_orderline;
```
