#!/usr/bin/env python

from __future__ import print_function
from ortho import read_config,write_config
from ortho import dictsub_strict,treeview
import numpy as np
import os

def make_decorator_local_run(function,spot,cwd):
	"""A decorator which runs a function from a particular spot."""
	def this_func(*args,**kwargs): 
		"""Decorator for remote automacs calls."""
		os.chdir(spot)
		answer = function(*args,**kwargs)
		os.chdir(cwd)
		return answer
	this_func.__name__ = function.__name__
	return this_func

def get_automacs(spot='automacs'):
	"""
	Clone and manage a copy of automacs.
	"""
	#! note that if spot is "amx" then the ortho.importer fails
	from ortho import bash,modules,importer
	conf = read_config()
	if 'automacs_spot' not in conf:
		config = read_config()
		# clone amx once using sync and then use setup to get extension modules
		amx_repo = config.get('automacs',{
			'address':'https://github.com/biophyscode/automacs',
			'branch':'ortho_clean'})
		#! remove this when you merge ortho_clean into master
		if amx_repo['branch'] == 'master':
			raise Exception('get_automacs requires the ortho_clean branch '
				'(not master). remove this when it is merged')
		modules.sync(modules={spot:amx_repo})
		# run make setup all the first time we clone amx
		bash('make setup all',cwd=spot)
		conf['automacs_spot'] = spot
		write_config(conf)
	else: print('status','found automacs at: %s'%conf['automacs_spot'])
	# importing automacs remotely will use chdir, so we change back afterwards
	cwd = os.getcwd()
	mod = importer(os.path.join(spot,'amx'))
	os.chdir(cwd)
	for key,val in [(i,j) for i,j in mod.items() if callable(j)]:
		# running automacs functions might require a local cwd so we 
		#   decorate functions to move and then move back
		mod[key] = make_decorator_local_run(val,spot,cwd)
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
		#! the fuzzy logic here is not working correctly. it needs to be more specific!
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
	get_index = _get
	def get_meta(self,**meta):
		index = self.get_index(**meta)
		if index!=None: return self.meta[index]
		else: return None
	def get(self,**meta):
		this = self._get(**meta)
		#! try stripping None here. this is a recent change and may break things
		if not this:
			this = self._get(**dict([(i,j) for i,j in meta.items() if j!=None]))
		if this==None: 
			treeview(dict(meta=self.meta,request=meta))
			raise Exception('cannot find data with meta (see above for meta): %s'%meta)
		return self.data[this]
	def done(self,**meta):
		return self._get(**meta)!=None
