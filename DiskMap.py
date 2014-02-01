import jsonpickle
from os import close, listdir, mkdir, remove, rename, write
from os.path import dirname, exists, isfile, isdir, join as pathjoin
from shutil import rmtree
from tempfile import mkstemp

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
			for table in self.tables():
				self[table]

	def tables(self):
		return filter(lambda name: not name.startswith('__'), listdir(self.dir))

	def __getitem__(self, key):
		if key.startswith('__'):
			raise KeyError("Bad table name: %s" % key)
		if self.cache is not None:
			if key not in self.cache:
				self.cache[key] = CachedTableMap(self, pathjoin(self.dir, key))
			return self.cache[key]
		return TableMap(pathjoin(self.dir, key))

	def __delitem__(self, key):
		if key not in self.tables():
			raise KeyError("Bad table name: %s" % key)
		if self.cache is not None and key in self.cache:
			del self.cache[key]
		dir = pathjoin(self.dir, key)
		if isdir(dir):
			rmtree(dir)

	def __len__(self):
		return len(self.tables())

	def __contains__(self, key):
		return not key.startswith('__') and exists(pathjoin(self.dir, str(key)))

	def __iter__(self):
		for key in self.tables():
			yield key

class TableMap:
	def __init__(self, path):
		self.dir = path

	def __getitem__(self, key):
		if not isdir(self.dir):
			raise StasisError("Path not found: %s" % self.dir)
		with open(pathjoin(self.dir, str(key)), 'r') as f:
			return jsonpickle.decode(f.read(), keys = True)

	def __setitem__(self, key, value):
		if not isdir(self.dir):
			mkdir(self.dir)
		fd, tempfile = mkstemp(prefix = '__tmp', dir = dirname(self.dir))
		write(fd, jsonpickle.encode(value, keys = True))
		close(fd)
		rename(tempfile, pathjoin(self.dir, str(key)))
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

	def values(self):
		return [self[k] for k in self.keys()]

	def iteritems(self):
		return self.all().iteritems()

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

	def values(self):
		return [v for k, v in self.cache.iteritems() if not (isinstance(k, str) and k.startswith('__'))]

	def iteritems(self):
		return self.all().iteritems()

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
