#!/usr/bin/env python

import time
from joblib import Parallel,delayed
from ortho import status

def framelooper(total,start=None,text='frame'):
	"""
	When performing parallel calculations with joblib we pass a generator to count the number of 
	tasks and report the time.
	"""
	for fr in range(total):
		status(text,i=fr,looplen=total,tag='parallel',start=start)
		yield fr

def basic_compute_loop(compute_function,looper,run_parallel=True,debug=False):
	"""
	Canonical form of the basic compute loop.
	"""
	start = time.time()
	if run_parallel:
		incoming = Parallel(n_jobs=8,verbose=10 if debug else 0.,require='sharedmem')(
			delayed(compute_function)(**looper[ll]) 
			for ll in framelooper(len(looper),start=start))
	else: 
		incoming = []
		for ll in framelooper(len(looper)):
			incoming.append(compute_function(**looper[ll]))
	return incoming
