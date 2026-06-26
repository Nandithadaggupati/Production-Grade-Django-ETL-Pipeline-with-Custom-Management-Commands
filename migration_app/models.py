from django.db import models

class LegacyOrder(models.Model):
    external_id = models.CharField(max_length=255, unique=True, db_index=True)
    raw_data = models.JSONField()
    migrated = models.BooleanField(default=False, db_index=True)

    def __str__(self):
        return f"LegacyOrder {self.external_id} (Migrated: {self.migrated})"

class Order(models.Model):
    external_id = models.CharField(max_length=255, unique=True, db_index=True)
    customer_email = models.EmailField()
    total = models.DecimalField(max_digits=12, decimal_places=2)
    created_at = models.DateTimeField(auto_now_add=True)

    def __str__(self):
        return f"Order {self.external_id} - {self.customer_email}"

class OrderLine(models.Model):
    order = models.ForeignKey(Order, on_delete=models.CASCADE, related_name='lines')
    sku = models.CharField(max_length=100)
    quantity = models.PositiveIntegerField()
    unit_price = models.DecimalField(max_digits=12, decimal_places=2)

    def __str__(self):
        return f"OrderLine {self.sku} (Qty: {self.quantity})"
