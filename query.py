import MySQLdb, copy

from django.conf import settings
from django.db.models.fields.related import RelatedField, ForeignKey, ManyToManyField, OneToOneField
from django.db.models.query import QuerySet, EmptyQuerySet, ModelIterable, ValuesIterable, ValuesListIterable
from django.core.exceptions import EmptyResultSet

connections = {
	key: MySQLdb.connect(
		host = value['HOST'],
		port = int(value.get('PORT', 3306)),
		database = value['NAME'],
		user = value['USER'], 
		password = value['PASSWORD'],
	) for key, value in settings.DATABASES.items()
}

class LegacyQuerySet:
	
	queryset = QuerySet
	cursor = None
	columns = []
	field_names = []
	model = None
	field_indices = []
	sql = ""
	params = ()
	executed = False
	rows = []
	pointer = 0
			
	def __init__(self, queryset):
		assert(isinstance(queryset, (QuerySet, type(self))))
		while isinstance(queryset, type(self)) and hasattr(queryset, 'queryset'):
			queryset = queryset.queryset
		self.queryset = queryset
		self.query = self.queryset.query
		self.cursor = None
		if isinstance(self.queryset, EmptyQuerySet):
			return
		compiler = self.queryset.query.get_compiler(using=self.queryset.db)
		self.columns = [x[0] for x in compiler.get_select()[0]]
		self.field_names = self.queryset._fields
		self.model = compiler.get_select()[1]['model']
		self.field_indices = compiler.get_select()[1]['select_fields']
		try:
			self.sql, self.params = compiler.as_sql()
		except EmptyResultSet:
			return
		self.db = connections[self.queryset.db]
		self.cursor = self.db.cursor()		
		
	def __iter__(self):
		return self
		
	def execute(self):
		if not self.sql:
			return
		if self.cursor._executed or self.executed:
			return
		self.cursor.execute(self.sql, self.params)
		self.cursor._executed = True
		self.executed = True
		
	def reset_cursor(self):
		self.cursor = self.db.cursor()
		self.executed = False
	
	def row_to_object(self, row):
		keys_and_values = {
			**{self.columns[i].target.name: row[i] for i in self.field_indices if not isinstance(self.columns[i].target, RelatedField)},
			**{self.field_names[i]: row[i] for i in self.field_indices if self.field_names},
		}
		if self.queryset._iterable_class == ModelIterable:
			obj = self.model(**keys_and_values)
			for i in self.field_indices:
				if isinstance(self.columns[i].target, (ForeignKey, OneToOneField)):
					setattr(obj, self.columns[i].target.name+"_id", row[i])
		elif self.queryset._iterable_class == ValuesIterable:
			obj = keys_and_values
		elif issubclass(self.queryset._iterable_class, ValuesListIterable):
			obj = tuple(keys_and_values.values())
		elif settings.DEBUG:
			raise Exception(self.queryset._iterable_class)
		return obj
		
	def fetch_all(self):
		if not self.cursor:
			return
		if self.rows:
			return
		self.execute()
		self.rows = self.cursor.fetchall()
		self.pointer = 0
		
	def __next__(self, default=None):
		if not self.cursor:
			raise StopIteration
		self.execute()
		if self.rows:
			if self.pointer >= len(self.rows):
				raise StopIteration
			row = self.rows[self.pointer]
			self.pointer += 1
		else:
			row = self.cursor.fetchone()
		if not row:
			raise StopIteration
		obj = self.row_to_object(row)
		if not obj:
			raise StopIteration
		return obj
		
	def __getitem__(self, key):
		if not self.cursor:
			return None
		limit = None
		offset = 0
		key = int(key) if isinstance(key, str) and key.isdigit() else key
		if isinstance(key, int):
			limit = 1
			offset = key
		elif isinstance(key, slice):
			limit = key.stop - key.start if key.start and key.stop else key.stop
			offset = key.start
		elif settings.DEBUG:
			raise Exception(type(key))
		original_sql = copy.copy(self.sql)
		if limit:
			self.sql += " LIMIT {limit} ".format(limit=limit)
		if offset:
			self.sql += " OFFSET {offset} ".format(offset=offset)
		result = None
		if isinstance(key, int):
			for obj in self:
				result = obj
				break
			if not result:
				raise IndexError
		else:
			self.fetch_all()
			result = list(self)
		self.sql = copy.copy(original_sql)
		return result
		
	def first(self):
		for obj in self:
			return obj
			break
		return None
		
	def last(self):
		last = None
		for obj in self:
			last = obj
		return last
	
	def values(self, *fields, **expressions):
		return type(self)(queryset=self.queryset.values(*fields, **expressions))
		
	def values_list(self, *fields, flat=False, named=False):
		if len(fields) == 1 and flat:
			values = self.values()
			values_list = [row.get(fields[0]) for row in values]
		else:
			values_list = type(self)(queryset=self.queryset.values_list(*fields, flat=False, named=False))
		return values_list
		
	def count(self):
		if not self.cursor:
			return 0
		if self.rows:
			result = len(self.rows)
		else:
			self.execute()
			result = self.cursor.rowcount
			self.reset_cursor()
		return result
		
	def __len__(self):
		return self.count()
		
	def filter(self, *args, **kwargs):
		queryset = self.queryset.filter(*args, **kwargs)
		return type(self)(queryset)
	
	def exclude(self, *args, **kwargs):
		queryset = self.queryset.exclude(*args, **kwargs)
		return type(self)(queryset)
		
	def distinct(self):
		queryset = self.queryset.distinct()
		return type(self)(queryset)
		
	def order_by(self, *args, **kwargs):
		queryset = self.queryset.order_by(*args, **kwargs)
		return type(self)(queryset)
		
	def exists(self):
		if not self.cursor:
			return False
		return bool(self.count())
		
	def none(self):
		return self.queryset.none()
		
	def all(self):
		self.fetch_all()
		return self
		
	def get(self, *args, **kwargs):
		queryset = self.queryset.filter(*args, **kwargs)
		return type(self)(queryset).first()
		
	def select_related(self, *args, **kwargs):
		queryset = self.queryset.select_related(*args, **kwargs)
		return type(self)(queryset)
		
	def prefetch_related(self, *args, **kwargs):
		queryset = self.queryset.prefetch_related(*args, **kwargs)
		return type(self)(queryset)
		
	def _add_hints(self, **hints):
		pass
		
	def _next_is_sticky(self):
		return self
		
	def using(self, alias):
		return self
		
	@property
	def ordered(self):
		return self.queryset.ordered
