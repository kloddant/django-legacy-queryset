# django-legacy-queryset
Provides a LegacyQuerySet class that can be used to support database versions that Django refuses to support anymore even though the underlying sql queries still work.

## Usage

  1. Override the model's default manager with LegacyManager.
  2. Override the model's base manager by adding base_manager_name = "objects" to its Meta class.  Otherwise foreign relations won't work, and the models will throw errors in the admin.

## Notes

* Extending the default QuerySet proved difficult.  It was easier to create a wrapper class instead.
* Overriding the model admin's get_queryset method does not appear to be necessary if the base_manager_name is overwritten.
* This will break django-debug-toolbar.
