#!/usr/bin/env python

from __future__ import print_function
from ortho import read_config,write_config
from ortho import dictsub_strict,treeview
import numpy as np

def get_automacs(spot='amx'):
	"""
	Clone and manage a copy of automacs.
	"""
	from ortho import bash,modules,importer
	conf = read_config()
	if 'automacs_spot' not in conf:
		# clone amx once using sync and then use setup to get extension modules
		modules.sync(modules={spot:{
			'address':'https://github.com/biophyscode/automacs',
			'branch':'ortho'}})
		# run make setup all the first time we clone amx
		bash('make setup all',cwd=spot)
		conf['automacs_spot'] = spot
		write_config(conf)
	else: print('status','found automacs at: %s'%conf['automacs_spot'])
	mod = importer('amx/amx')
	return mod

def uniquify(array):
    """Get unique rows in an array."""
    #! note that this may eventually be deprecated by numpy unique over rows
    # contiguous array trick
    alt = np.ascontiguousarray(array).view(
        np.dtype((np.void,array.dtype.itemsize*array.shape[1])))
    unique,idx,counts = np.unique(alt,return_index=True,return_counts=True)
    # sort by count, descending
    idx_sorted = np.argsort(counts)[::-1]
    return idx[idx_sorted],counts[idx_sorted]

def subdivide_trajectory(segnum,n_segments,nframes):
	"""Evenly subdivide a trajectory."""
	return np.where(segnum==np.floor(
		np.arange(nframes)/(nframes/float(n_segments))).astype(int))[0]

class PostAccumulator(object):
	def __init__(self):
		self.meta = []
		self.data = []
	def add(self,meta,data):
		self.meta.append(meta)
		self.data.append(data)
	def _get(self,**meta):
		"""Find unique matches."""
		# first check exact equality
		candidates = [ii for ii,i in enumerate(self.meta) if i==meta]
		if len(candidates)==1: return candidates[0]
		# next check subset
		candidates = [ii for ii,i in enumerate(self.meta) if dictsub_strict(meta,i)]
		if len(candidates)==1: return candidates[0]
		# next check superset
		candidates = [ii for ii,i in enumerate(self.meta) if dictsub_strict(i,meta)]
		if len(candidates)==1: return candidates[0]
		return None
	def get(self,**meta):
		this = self._get(**meta)
		if this==None: 
			treeview(dict(meta=self.meta))
			raise Exception('cannot find data with meta (see above for meta): %s'%meta)
		return self.data[this]
	def done(self,**meta):
		return self._get(**meta)!=None
