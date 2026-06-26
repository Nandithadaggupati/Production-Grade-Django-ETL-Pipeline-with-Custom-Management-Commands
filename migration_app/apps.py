from django.apps import AppConfig

class MigrationAppConfig(AppConfig):
    default_auto_field = 'django.db.models.BigAutoField'
    name = 'migration_app'
