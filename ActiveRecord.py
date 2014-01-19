from Safe import Safe
from StasisError import StasisError

from inspect import getargspec, getmembers

_db = None
def db(set = None):
	if set is not None:
		global _db
		_db = set
	if _db is None:
		raise StasisError("No backing store set")
	return _db

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
		data = db()[cls.table()][id]
		return cls(**data)

	@classmethod
	def loadAll(cls):
		data = db()[cls.table()].all()
		return {k: cls(**v) for k, v in data.iteritems()}

	def save(self):
		cls = self.__class__
		if not self.id:
			self.id = db()[cls.table()].nextID()
		data = {field: getattr(self, field) for field in cls.fields()}
		db()[cls.table()][self.id] = data
