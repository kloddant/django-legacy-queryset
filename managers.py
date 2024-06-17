from django.db import models

from .query import LegacyQuerySet

class LegacyManager(models.Manager):
	
	def get_queryset(self):
		return LegacyQuerySet(super().get_queryset())
