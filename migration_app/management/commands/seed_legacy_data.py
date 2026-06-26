from django.core.management.base import BaseCommand
from migration_app.models import LegacyOrder
import time

class Command(BaseCommand):
    help = 'Seeds the database with 500,000 legacy order records.'

    def handle(self, *args, **options):
        total_records = 500000
        batch_size = 10000
        self.stdout.write(f"Clearing existing legacy orders...")
        LegacyOrder.objects.all().delete()

        self.stdout.write(f"Seeding {total_records} legacy order records in batches of {batch_size}...")
        start_time = time.perf_counter()

        created_count = 0
        batch = []
        for i in range(1, total_records + 1):
            external_id = f"legacy-{i}"
            if i % 2 == 0:
                raw_data = {
                    "customer_email": f"user{i}@example.com",
                    "total": "199.98",
                    "items": [
                        {"sku": "SKU-A1", "quantity": 2, "unit_price": "49.99"},
                        {"sku": "SKU-B2", "quantity": 1, "unit_price": "99.99"}
                    ]
                }
            else:
                raw_data = {
                    "customer_email": f"customer{i}@example.com",
                    "total": "49.99",
                    "items": [
                        {"sku": "SKU-C3", "quantity": 1, "unit_price": "49.99"}
                    ]
                }

            batch.append(LegacyOrder(
                external_id=external_id,
                raw_data=raw_data,
                migrated=False
            ))

            if len(batch) >= batch_size:
                LegacyOrder.objects.bulk_create(batch)
                created_count += len(batch)
                self.stdout.write(f"Seeded {created_count}/{total_records} records...")
                batch = []

        if batch:
            LegacyOrder.objects.bulk_create(batch)
            created_count += len(batch)
            self.stdout.write(f"Seeded {created_count}/{total_records} records...")

        end_time = time.perf_counter()
        self.stdout.write(self.style.SUCCESS(
            f"Successfully seeded {created_count} legacy records in {end_time - start_time:.2f} seconds."
        ))
