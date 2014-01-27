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
	def load(cls, id):
		data = store()[cls.table()][id]
		return cls(**data)

	@classmethod
	def loadAll(cls, orderby = None, **attrs):
		data = store()[cls.table()].all().items()
		if attrs:
			data = filter(lambda (k, v): all(v[filtK] == filtV for filtK, filtV in attrs.iteritems()), data)
		if orderby is not None:
			reverse = False
			if orderby.startswith('-'):
				reverse = True
				orderby = orderby[1:]
			data.sort(key = lambda row: row[orderby], reverse = reverse)
		return [cls(**row) for row in data]

	def save(self):
		cls = self.__class__
		if not self.id:
			self.id = store()[cls.table()].nextID()
		data = {field: getattr(self, field) for field in cls.fields()}
		store()[cls.table()][self.id] = data

	def delete(self):
		cls = self.__class__
		if self.id:
			del store()[cls.table()][self.id]

# getter
def idToObj(cls, field):
	def fn(self):
		if not self:
			return None
		val = getattr(self, field)
		if isinstance(val, list):
			return map(cls.load, val)
		elif isinstance(val, set):
			return set(map(cls.load, val))
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
		elif isinstance(obj, set):
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
