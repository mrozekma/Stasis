from Safe import Safe
from Singleton import get as store
from StasisError import StasisError

from inspect import getargspec, getmembers

class ActiveRecord(object):
	def __init__(self):
		self.safe = Safe(self)

	@classmethod
	def table(cls):
		return cls.__name__.lower() + 's'

	@classmethod
	def fields(cls):
		return getargspec(cls.__init__).args[1:]

	@classmethod
	def loadDataFilter(cls, data, **attrs):
		return data

	@classmethod
	def saveDataFilter(cls, data):
		return data

	@classmethod
	def load(cls, id = None, **attrs):
		if id is None:
			data = store()[cls.table()].values()
		else:
			if id not in store()[cls.table()]:
				return None
			data = [store()[cls.table()][id]]
		data = filter(None, (cls.loadDataFilter(row, **attrs) for row in data))
		if attrs:
			data = filter(lambda row: all(row[k] == v for k, v in attrs.iteritems()), data)
		if len(data) == 0:
			return None
		elif len(data) == 1:
			return cls(**data[0])
		else:
			raise StasisError("Too many load results (cls %s, id %s, attrs %s)" % (cls, id, attrs))

	@classmethod
	def loadAll(cls, orderby = None, **attrs):
		data = store()[cls.table()].values()
		data = filter(None, (cls.loadDataFilter(row, **attrs) for row in data))
		if not data:
			return []
		if attrs:
			data = filter(lambda row: all(row[k] == v for k, v in attrs.iteritems()), data)
		if orderby is not None:
			reverse = False
			if orderby.startswith('-'):
				reverse = True
				orderby = orderby[1:]
			data.sort(key = lambda row: row[orderby], reverse = reverse)
		return [cls(**row) for row in data]

	@classmethod
	def cacheAll(cls):
		store().loadCache(cls.table())

	def save(self):
		cls = self.__class__
		data = {field: getattr(self, field) for field in cls.fields()}
		if not self.id:
			self.id = data['id'] = store()[cls.table()].nextID()
		data = cls.saveDataFilter(data)
		store()[cls.table()][self.id] = data

	def delete(self):
		cls = self.__class__
		if self.id:
			del store()[cls.table()][self.id]

	def __eq__(self, other):
		return self and other and self.id == other.id

	def __ne__(self, other):
		return not (self == other)

	def __hash__(self):
		return self.id or object.__hash__(self)

# getter
def idToObj(cls, field):
	def fn(self):
		if not self:
			return None
		val = getattr(self, field)
		if val is None:
			return None
		if isinstance(val, list):
			return map(cls.load, val)
		elif isinstance(val, (set, frozenset)):
			return frozenset(map(cls.load, val))
		else:
			return cls.load(val)
	return fn

# setter
def objToID(field):
	def fn(self, obj):
		if isinstance(obj, list):
			if not all(o.id for o in obj):
				raise ValueError("Attempted to pull ID from unsaved object in list")
			setattr(self, field, [o.id for o in obj])
		elif isinstance(obj, (frozenset, set)):
			if not all(o.id for o in obj):
				raise ValueError("Attempted to pull ID from unsaved object in set")
			setattr(self, field, set(o.id for o in obj))
		elif obj:
			if not obj.id:
				raise ValueError("Attempted to pull ID from unsaved object")
			setattr(self, field, obj.id)
		else:
			setattr(self, field, 0)
	return fn

def link(cls, field):
	return property(idToObj(cls, field), objToID(field), None)
