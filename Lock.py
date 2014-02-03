import functools
import sys
from thread import get_ident
from threading import Lock as ParentLock
from time import sleep

from StasisError import StasisError

class Lock():
	def __init__(self):
		self.lock = mutexProvider()
		self.count = 0
		self.exclusiveHold = None

	def share(self):
		# sys.__stdout__.write("acquiring shared lock: %s\n" % get_ident())
		while True:
			self.lock.acquire()
			if self.exclusiveHold and self.exclusiveHold != get_ident():
				self.lock.release()
				sleep(0)
			else:
				self.count += 1
				# sys.__stdout__.write("got shared lock (%d total)\n" % self.count)
				self.lock.release()
				break

	def exclusive(self):
		# sys.__stdout__.write("acquiring exclusive lock: %s\n" % get_ident())
		# import traceback
		# traceback.print_stack(file = sys.__stdout__)
		while True:
			self.lock.acquire()
			# sys.__stdout__.write("xl lock try\n")
			if self.count > 0 and self.exclusiveHold != get_ident():
				# sys.__stdout__.write("** %d %s %s\n" % self.count, self.exclusiveHold, get_ident())
				self.lock.release()
				# sys.__stdout__.write("xl lock inner end\n")
				sleep(0)
			else:
				# sys.__stdout__.write("got exclusive lock\n")
				self.count += 1
				self.exclusiveHold = get_ident()
				self.lock.release()
				# sys.__stdout__.write("xl lock inner end\n")
				break

	def release(self):
		# sys.__stdout__.write("releasing lock: %d\n" % get_ident())
		# import traceback
		# traceback.print_stack(file = sys.__stdout__)
		self.lock.acquire()
		# sys.__stdout__.write("step\n")
		if self.count == 0:
			self.lock.release()
			raise StasisError("Attempted to release an unheld lock")
		self.count -= 1
		if self.exclusiveHold:
			if get_ident() != self.exclusiveHold:
				self.lock.release()
				raise StasisError("Exclusive lock released by wrong thread")
			if self.count == 0:
				self.exclusiveHold = None
		self.lock.release()
		# sys.__stdout__.write("lock released (%d remain)\n" % self.count)

# Lock itself requires a low-level mutex
# By default it uses threading.Lock, but users can provide another class
# (it must have acquire() and release() methods)
mutexProvider = ParentLock
def setMutexProvider(lock):
	global mutexProvider
	mutexProvider = lock

def synchronized(exclusive = False):
	def wrap(f):
		@functools.wraps(f)
		def wrap2(self, *args, **kw):
			if exclusive:
				self.lock.exclusive()
			else:
				self.lock.share()
			try:
				return f(self, *args, **kw)
			finally:
				self.lock.release()
		return wrap2
	return wrap
