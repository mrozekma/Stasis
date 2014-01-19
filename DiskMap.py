import json
from os import listdir, mkdir
from os.path import isdir, join as pathjoin

from StasisError import StasisError

class DiskMap:
	def __init__(self, dir, create = False):
		self.dir = dir
		if not isdir(self.dir):
			if not create:
				raise StasisError("No database at: %s" % self.dir)
			mkdir(self.dir)

	def __getitem__(self, key):
		return TableMap(pathjoin(self.dir, key))

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
