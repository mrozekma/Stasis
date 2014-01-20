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
	def loadAll(cls):
		data = store()[cls.table()].all()
		return {k: cls(**v) for k, v in data.iteritems()}

	def save(self):
		cls = self.__class__
		if not self.id:
			self.id = store()[cls.table()].nextID()
		data = {field: getattr(self, field) for field in cls.fields()}
		store()[cls.table()][self.id] = data
