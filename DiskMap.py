import jsonpickle
from os import close, listdir, mkdir, remove, rename, write
from os.path import dirname, exists, isfile, isdir, join as pathjoin
from shutil import rmtree
import tarfile
from tempfile import mkstemp

from Lock import Lock, synchronized
from StasisError import StasisError

def to_int(n, allowAny = True):
	try:
		return int(n)
	except ValueError:
		return n if allowAny else None

class DiskMap:
	def __init__(self, dir, create = False, cache = False, nocache = [], cacheNotifyFn = None):
		self.dir = dir
		self.cache = None
		self.lock = Lock()

		if not isdir(self.dir):
			if not create:
				raise StasisError("No database at: %s" % self.dir)
			mkdir(self.dir)

		if cache:
			self.cache = {}
			# Preload all existing tables
			for table in self.tables():
				if cacheNotifyFn:
					cacheNotifyFn(table)
				self.__getitem__(table, cached = table not in nocache)

	@synchronized()
	def tables(self):
		return filter(lambda name: not name.startswith('__'), listdir(self.dir))

	@synchronized()
	def __getitem__(self, key, cached = True): # cached only matters if the DiskMap itself uses caching
		if key.startswith('__'):
			raise KeyError("Bad table name: %s" % key)
		if self.cache is not None:
			if key not in self.cache:
				if cached:
					self.cache[key] = CachedTableMap(self, pathjoin(self.dir, key), self.lock)
				else:
					self.cache[key] = TableMap(pathjoin(self.dir, key), self.lock)
			return self.cache[key]
		return TableMap(pathjoin(self.dir, key), self.lock)

	@synchronized(exclusive = True)
	def __delitem__(self, key):
		if key not in self.tables():
			raise KeyError("Bad table name: %s" % key)
		if self.cache is not None and key in self.cache:
			del self.cache[key]
		dir = pathjoin(self.dir, key)
		if isdir(dir):
			rmtree(dir)

	@synchronized()
	def __len__(self):
		return len(self.tables())

	@synchronized()
	def __contains__(self, key):
		return not key.startswith('__') and exists(pathjoin(self.dir, str(key)))

	@synchronized()
	def __iter__(self):
		for key in self.tables():
			yield key

	@synchronized()
	def loadCache(self, key):
		if not self.cache:
			return
		if key in self.cache and isinstance(self.cache[key], CachedTableMap):
			return
		self.cache[key] = CachedTableMap(self, pathjoin(self.dir, key), self.lock)

	@synchronized()
	def archive(self, filename):
		f = tarfile.open(filename, 'w:gz')
		f.add(self.dir)
		f.close()
		if not exists(filename):
			raise StasisError('Stasis archiving failed')

class TableMap:
	def __init__(self, path, lock):
		self.dir = path
		self.lock = lock

	@synchronized()
	def __getitem__(self, key):
		if not isdir(self.dir):
			raise StasisError("Path not found: %s" % self.dir)
		with open(pathjoin(self.dir, str(key)), 'r') as f:
			return jsonpickle.decode(f.read(), keys = True)

	@synchronized(exclusive = True)
	def __setitem__(self, key, value):
		if not isdir(self.dir):
			mkdir(self.dir)
		fd, tempfile = mkstemp(prefix = '__tmp', dir = dirname(self.dir))
		write(fd, jsonpickle.encode(value, keys = True))
		close(fd)
		rename(tempfile, pathjoin(self.dir, str(key)))
		if isinstance(key, int) and key >= self.nextID():
			self['__nextid'] = key + 1

	@synchronized(exclusive = True)
	def __delitem__(self, key):
		path = pathjoin(self.dir, str(key))
		if isfile(path):
			remove(path)

	@synchronized()
	def __len__(self):
		return len(self.keys())

	@synchronized()
	def __contains__(self, key):
		return exists(pathjoin(self.dir, str(key)))

	@synchronized()
	def __iter__(self):
		for key in self.keys():
			yield key

	@synchronized()
	def keys(self):
		if not exists(self.dir):
			return []
		return filter(lambda key: not (isinstance(key, str) and key.startswith('__')), listdir(self.dir))

	@synchronized()
	def values(self):
		return [self[k] for k in self.keys()]

	@synchronized()
	def iteritems(self):
		return self.all().iteritems()

	@synchronized()
	def change(self, key):
		return PendingChange(self, key)

	@synchronized()
	def all(self):
		if not isdir(self.dir):
			return {}
		ids = map(to_int, self.keys())
		return {id: self[id] for id in ids}

	@synchronized()
	def nextID(self):
		return self['__nextid'] if '__nextid' in self else 1

	@synchronized()
	def merge(self, tm):
		for k, v in tm.iteritems():
			self[k] = v

	@synchronized()
	def truncate(self, resetID = True):
		keep = {k: self[k] for k in listdir(self.dir) if k.startswith('__')}
		if resetID and '__nextid' in keep:
			del keep['__nextid']
		rmtree(self.dir)
		for k, v in keep.iteritems():
			self[k] = v

class CachedTableMap:
	def __init__(self, diskMap, path, lock):
		self.diskMap = diskMap
		self.dir = path
		self.lock = lock
		self.parent = TableMap(self.dir, lock)
		self.cache = self.parent.all()

	@synchronized()
	def __getitem__(self, key):
		return self.cache[key]

	@synchronized(exclusive = True)
	def __setitem__(self, key, value):
		self.cache[key] = value
		self.parent[key] = value

	@synchronized(exclusive = True)
	def __delitem__(self, key):
		del self.cache[key]
		del self.parent[key]

	@synchronized()
	def __len__(self):
		return len(self.keys())

	@synchronized()
	def __contains__(self, key):
		return key in self.cache

	@synchronized()
	def __iter__(self):
		for key in self.keys():
			yield key

	@synchronized()
	def keys(self):
		return filter(lambda key: not (isinstance(key, str) and key.startswith('__')), self.cache)

	@synchronized()
	def values(self):
		return [v for k, v in self.cache.iteritems() if not (isinstance(k, str) and k.startswith('__'))]

	@synchronized()
	def iteritems(self):
		return self.all().iteritems()

	@synchronized()
	def change(self, key):
		return PendingChange(self, key)

	@synchronized()
	def all(self):
		return {key: self[key] for key in self.keys()}

	@synchronized()
	def nextID(self):
		return self.parent.nextID()

	@synchronized()
	def merge(self, tm):
		self.parent.merge(tm)
		self.cache = self.parent.all()

	@synchronized()
	def truncate(self, resetID = True):
		self.parent.truncate(resetID = resetID)
		self.cache = {}

class PendingChange:
	def __init__(self, diskMap, key):
		self.diskMap = diskMap
		self.key = key

	def __enter__(self):
		self.value = self.diskMap[self.key]
		return self.value

	def __exit__(self, type, value, tb):
		self.diskMap[self.key] = self.value
