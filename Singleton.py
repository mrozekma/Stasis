from StasisError import StasisError

singleton = None

def get():
	if singleton is None:
		raise StasisError("No backing store set")
	return singleton

def set(store):
	global singleton
	singleton = store
