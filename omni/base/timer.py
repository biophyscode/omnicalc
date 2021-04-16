#!/usr/bin/python

"""
Track the elapsed time across submodules.
"""

from base.tools import status
import time
import signal
from contextlib import contextmanager

script_start_time = time.time()

def checktime(): 
	"""Report the time anywhere in the calculation workflow."""
	status('%.2f'%(1./60*(time.time()-script_start_time))+' minutes',tag='time')

class TimeoutException(Exception): pass

@contextmanager
def time_limit(seconds):
	"""Add a time limit to a particular line."""
	def signal_handler(signum,frame): raise TimeoutException("Timed out!")
	signal.signal(signal.SIGALRM, signal_handler)
	signal.alarm(seconds)
	try: yield
	finally: signal.alarm(0)
