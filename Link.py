# getter
def idToObj(cls, field):
	def fn(self):
		if not self:
			return None
		val = getattr(self, field)
		if isinstance(val, list):
			return map(cls.load, val)
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
		elif obj:
			if not obj.id:
				raise ValueError("Attempted to pull ID from unsaved object")
			setattr(self, field, obj.id)
		else:
			setattr(self, field, 0)
	return fn

def link(cls, field):
	return property(idToObj(cls, field), objToID(field), None)
