import json
from os import listdir, mkdir, remove
from os.path import isfile, isdir, join as pathjoin

from StasisError import StasisError

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
			return json.load(f)

	def __setitem__(self, key, value):
		if not isdir(self.dir):
			mkdir(self.dir)
		with open(pathjoin(self.dir, str(key)), 'w') as f:
			json.dump(value, f)

	def __delitem__(self, key):
		path = pathjoin(self.dir, str(key))
		if isfile(path):
			remove(path)

	def __len__(self):
		return len(listdir(self.dir))

	def __iter__(self):
		for key in listdir(self.dir):
			yield key

	def all(self):
		if not isdir(self.dir):
			return {}
		ids = sorted(map(int, listdir(self.dir)))
		return {id: self[id] for id in ids}

	def nextID(self):
		if not isdir(self.dir):
			return 1
		ids = map(int, listdir(self.dir))
		if ids == []:
			return 1
		return max(ids) + 1

class CachedTableMap:
	def __init__(self, diskMap, path):
		self.diskMap = diskMap
		self.dir = path
		self.cache = TableMap(self.dir).all()

	def __getitem__(self, key):
		return self.cache[key]

	def __setitem__(self, key, value):
		self.cache[key] = value
		TableMap(self.dir)[key] = value

	def __delitem__(self, key):
		del self.cache[key]
		del TableMap(self.dir)[key]

	def __len__(self):
		return len(self.cache)

	def __iter__(self):
		return self.cache.__iter__()

	def all(self):
		return self.cache

	def nextID(self):
		if self.cache == {}:
			return 1
		return max(self.cache) + 1
