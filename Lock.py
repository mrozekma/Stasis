import functools
import sys
from thread import get_ident
from threading import Lock as ParentLock
from time import sleep

from StasisError import StasisError

class Lock():
	def __init__(self):
		self.lock = mutexProvider()
		self.shareHolds = []
		self.exclusiveHolds = []

	def share(self):
		# sys.__stdout__.write("acquiring shared lock: %s\n" % get_ident())
		while True:
			self.lock.acquire()
			if self.exclusiveHolds != [] and set(self.exclusiveHolds) != {get_ident(),}:
				self.lock.release()
				sleep(0)
			else:
				self.shareHolds.append(get_ident())
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
			holds = self.shareHolds + self.exclusiveHolds
			if holds != [] and set(holds) != {get_ident(),}:
				# sys.__stdout__.write("** %s %s %s\n" % (self.shareHolds, self.exclusiveHolds, get_ident()))
				self.lock.release()
				# sys.__stdout__.write("xl lock inner end\n")
				sleep(0)
			else:
				# sys.__stdout__.write("got exclusive lock\n")
				self.exclusiveHolds.append(get_ident())
				self.lock.release()
				# sys.__stdout__.write("xl lock inner end\n")
				break

	# 'exclusive' is only used when a thread has both a shared and exclusive hold, to decide which to release
	def release(self, exclusive = False):
		# sys.__stdout__.write("releasing lock: %d\n" % get_ident())
		# import traceback
		# traceback.print_stack(file = sys.__stdout__)
		self.lock.acquire()
		# sys.__stdout__.write("step\n")
		ident = get_ident()
		if ident in self.shareHolds and ident in self.exclusiveHolds:
			if exclusive:
				self.exclusiveHolds.remove(ident)
			else:
				self.shareHolds.remove(ident)
		elif ident in self.shareHolds:
			self.shareHolds.remove(ident)
		elif ident in self.exclusiveHolds:
			self.exclusiveHolds.remove(ident)
		else:
			self.lock.release()
			raise StasisError("Attempted to release an unheld lock")
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
				self.lock.release(exclusive = exclusive)
		return wrap2
	return wrap
