class Safe(object):
	def __init__(self, ar):
		self.ar = ar

	def __getattribute__(self, var):
		entities = {
			'&': '&amp;',
			'"': '&quot;',
			"'": '&apos;',
			'<': '&lt;',
			'>': '&gt;'
		}

		if var == 'ar': return object.__getattribute__(self, var)
		# return stripTags(self.ar.__getattribute__(var))
		return ''.join(entities.get(c, c) for c in self.ar.__getattribute__(var))
