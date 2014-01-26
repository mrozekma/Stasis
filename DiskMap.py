import jsonpickle
from os import listdir, mkdir, remove
from os.path import exists, isfile, isdir, join as pathjoin

from StasisError import StasisError

def to_int(n, allowAny = True):
	try:
		return int(n)
	except ValueError:
		return n if allowAny else None

class DiskMap:
	def __init__(self, dir, create = False, cache = False):
		self.dir = dir
		self.cache = None

		if not isdir(self.dir):
			if not create:
				raise StasisError("No database at: %s" % self.dir)
			mkdir(self.dir)

		if cache:
			self.cache = {}
			# Preload all existing tables
			for table in listdir(self.dir):
				self[table]

	def __getitem__(self, key):
		if self.cache is not None:
			if key not in self.cache:
				self.cache[key] = CachedTableMap(self, pathjoin(self.dir, key))
			return self.cache[key]
		return TableMap(pathjoin(self.dir, key))

	def __len__(self):
		return len(listdir(self.dir))

	def __contains__(self, key):
		return exists(pathjoin(self.dir, str(key)))

	def __iter__(self):
		for key in listdir(self.dir):
			yield key

class TableMap:
	def __init__(self, path):
		self.dir = path

	def __getitem__(self, key):
		if not isdir(self.dir):
			raise StasisError("Path not found: %s" % self.dir)
		with open(pathjoin(self.dir, str(key)), 'r') as f:
			return jsonpickle.decode(f.read())

	def __setitem__(self, key, value):
		if not isdir(self.dir):
			mkdir(self.dir)
		with open(pathjoin(self.dir, str(key)), 'w') as f:
			f.write(jsonpickle.encode(value))
		if isinstance(key, int) and key >= self.nextID():
			self['__nextid'] = key + 1

	def __delitem__(self, key):
		path = pathjoin(self.dir, str(key))
		if isfile(path):
			remove(path)

	def __len__(self):
		return len(self.keys())

	def __contains__(self, key):
		return exists(pathjoin(self.dir, str(key)))

	def __iter__(self):
		for key in self.keys():
			yield key

	def keys(self):
		return filter(lambda key: not (isinstance(key, str) and key.startswith('__')), listdir(self.dir))

	def change(self, key):
		return PendingChange(self, key)

	def all(self):
		if not isdir(self.dir):
			return {}
		ids = map(to_int, self.keys())
		return {id: self[id] for id in ids}

	def nextID(self):
		return self['__nextid'] if '__nextid' in self else 1

class CachedTableMap:
	def __init__(self, diskMap, path):
		self.diskMap = diskMap
		self.dir = path
		self.parent = TableMap(self.dir)
		self.cache = self.parent.all()

	def __getitem__(self, key):
		return self.cache[key]

	def __setitem__(self, key, value):
		self.cache[key] = value
		self.parent[key] = value

	def __delitem__(self, key):
		del self.cache[key]
		del self.parent[key]

	def __len__(self):
		return len(self.keys())

	def __contains__(self, key):
		return key in self.cache

	def __iter__(self):
		for key in self.keys():
			yield key

	def keys(self):
		return filter(lambda key: not (isinstance(key, str) and key.startswith('__')), self.cache)

	def change(self, key):
		return PendingChange(self, key)

	def all(self):
		return {key: self[key] for key in self.keys()}

	def nextID(self):
		return self.parent.nextID()

class PendingChange:
	def __init__(self, diskMap, key):
		self.diskMap = diskMap
		self.key = key

	def __enter__(self):
		self.value = self.diskMap[self.key]
		return self.value

	def __exit__(self, type, value, tb):
		self.diskMap[self.key] = self.value
