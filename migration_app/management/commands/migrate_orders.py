from django.core.management.base import BaseCommand
from django.db import connection, transaction
from django.conf import settings
from migration_app.models import LegacyOrder, Order, OrderLine
from decimal import Decimal
import time
import tracemalloc

class Command(BaseCommand):
    help = 'Migrates data from LegacyOrder to normalized Order and OrderLine tables.'

    def add_arguments(self, parser):
        parser.add_argument(
            '--batch-size',
            type=int,
            default=1000,
            help='Specifies the number of records to process in a single batch.'
        )
        parser.add_argument(
            '--dry-run',
            action='store_true',
            help='Prevents any database writes.'
        )
        parser.add_argument(
            '--start-from',
            type=str,
            default=None,
            help='Specifies an external_id from which to begin the migration.'
        )
        parser.add_argument(
            '--naive',
            action='store_true',
            help='Run the naive one-by-one migration approach for benchmarking.'
        )
        parser.add_argument(
            '--limit',
            type=int,
            default=None,
            help='Limit the number of records processed (useful for testing/naive benchmarking).'
        )

    def handle(self, *args, **options):
        # 1. NAIVE IMPLEMENTATION (Benchmark only)
        if options['naive']:
            self.stdout.write("Running naive migration approach...")
            tracemalloc.start()
            start_time = time.perf_counter()

            queryset = LegacyOrder.objects.filter(migrated=False).order_by('external_id')
            if options['start_from']:
                queryset = queryset.filter(external_id__gte=options['start_from'])
            if options['limit']:
                queryset = queryset[:options['limit']]

            # Naive loads entire QuerySet into memory
            legacy_orders = list(queryset)
            total_records = len(legacy_orders)

            initial_queries = len(connection.queries) if settings.DEBUG else 0
            processed_count = 0

            for legacy_order in legacy_orders:
                if options['dry_run']:
                    self.stdout.write(f"[Dry Run] Would process {legacy_order.external_id}")
                    processed_count += 1
                    continue

                try:
                    with transaction.atomic():
                        raw_data = legacy_order.raw_data
                        # Create Order
                        order = Order.objects.create(
                            external_id=legacy_order.external_id,
                            customer_email=raw_data['customer_email'],
                            total=Decimal(raw_data['total'])
                        )
                        # Create OrderLines
                        for item in raw_data.get('items', []):
                            OrderLine.objects.create(
                                order=order,
                                sku=item['sku'],
                                quantity=item['quantity'],
                                unit_price=Decimal(item['unit_price'])
                            )
                        # Mark legacy order as migrated
                        legacy_order.migrated = True
                        legacy_order.save()
                        processed_count += 1

                except Exception as e:
                    self.stdout.write(self.style.ERROR(f"Error migrating {legacy_order.external_id}: {e}"))
                    break

            end_time = time.perf_counter()
            final_queries = len(connection.queries) if settings.DEBUG else 0

            snapshot = tracemalloc.take_snapshot()
            tracemalloc.stop()

            duration = end_time - start_time
            throughput = processed_count / duration if duration > 0 else 0

            self.stdout.write(self.style.SUCCESS("Naive migration finished."))
            self.stdout.write(f"Processed: {processed_count} records")
            self.stdout.write(f"Total time: {duration:.4f} seconds")
            self.stdout.write(f"Throughput: {throughput:.2f} records per second")
            if settings.DEBUG:
                self.stdout.write(f"Database Queries: {final_queries - initial_queries}")

            top_stats = snapshot.statistics('lineno')
            self.stdout.write("Top 5 Memory Allocations:")
            for stat in top_stats[:5]:
                self.stdout.write(str(stat))
            return

        # 2. OPTIMIZED IMPLEMENTATION
        self.stdout.write("Running optimized migration approach...")
        tracemalloc.start()
        start_time = time.perf_counter()

        batch_size = options['batch_size']
        dry_run = options['dry_run']

        queryset = LegacyOrder.objects.filter(migrated=False).order_by('external_id')
        if options['start_from']:
            queryset = queryset.filter(external_id__gte=options['start_from'])
        if options['limit']:
            queryset = queryset[:options['limit']]

        initial_queries = len(connection.queries) if settings.DEBUG else 0

        orders_to_create = []
        lines_to_create = []
        processed_ids = []
        total_migrated = 0

        def process_batch(orders, lines, legacy_ids):
            if dry_run:
                self.stdout.write(f"[Dry Run] Would process {len(orders)} records.")
                return

            try:
                with transaction.atomic():
                    # Step 1: Create Orders
                    Order.objects.bulk_create(orders)

                    # Step 2: Re-fetch created orders by unique external_id
                    created_orders = Order.objects.filter(
                        external_id__in=[o.external_id for o in orders]
                    ).in_bulk(field_name='external_id')

                    # Step 3: Associate OrderLines with the saved Orders
                    for line in lines:
                        line.order = created_orders[line.order.external_id]

                    # Step 4: Create OrderLines
                    OrderLine.objects.bulk_create(lines)

                    # Step 5: Mark legacy orders as migrated
                    LegacyOrder.objects.filter(id__in=legacy_ids).update(migrated=True)

                self.stdout.write(self.style.SUCCESS(f"Successfully processed batch of {len(orders)} records."))
            except Exception as e:
                self.stdout.write(self.style.ERROR(f"An error occurred in batch: {e}"))
                raise e

        try:
            # Using server-side cursor iterator
            for legacy_order in queryset.iterator(chunk_size=batch_size):
                raw_data = legacy_order.raw_data

                # Create Order object in memory (unsaved)
                new_order = Order(
                    external_id=legacy_order.external_id,
                    customer_email=raw_data['customer_email'],
                    total=Decimal(raw_data['total'])
                )
                orders_to_create.append(new_order)

                # Create OrderLine objects in memory (unsaved)
                for item in raw_data.get('items', []):
                    new_line = OrderLine(
                        order=new_order, # temporary unsaved Order reference
                        sku=item['sku'],
                        quantity=item['quantity'],
                        unit_price=Decimal(item['unit_price'])
                    )
                    lines_to_create.append(new_line)

                processed_ids.append(legacy_order.id)

                if len(orders_to_create) >= batch_size:
                    process_batch(orders_to_create, lines_to_create, processed_ids)
                    total_migrated += len(orders_to_create)
                    orders_to_create = []
                    lines_to_create = []
                    processed_ids = []

            # Process remaining items in last partial batch
            if orders_to_create:
                process_batch(orders_to_create, lines_to_create, processed_ids)
                total_migrated += len(orders_to_create)

        except Exception as e:
            self.stdout.write(self.style.ERROR(f"An error occurred during migration: {e}"))
            raise e

        end_time = time.perf_counter()
        final_queries = len(connection.queries) if settings.DEBUG else 0

        snapshot = tracemalloc.take_snapshot()
        tracemalloc.stop()

        duration = end_time - start_time
        throughput = total_migrated / duration if duration > 0 else 0

        self.stdout.write(self.style.SUCCESS("Optimized migration finished."))
        self.stdout.write(f"Processed: {total_migrated} records")
        self.stdout.write(f"Total time: {duration:.4f} seconds")
        self.stdout.write(f"Throughput: {throughput:.2f} records per second")
        if settings.DEBUG:
            self.stdout.write(f"Database Queries: {final_queries - initial_queries}")

        top_stats = snapshot.statistics('lineno')
        self.stdout.write("Top 5 Memory Allocations:")
        for stat in top_stats[:5]:
            self.stdout.write(str(stat))
