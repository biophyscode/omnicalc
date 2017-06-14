#!/usr/bin/python

import time
from numpy import *
from joblib import Parallel,delayed
from joblib.pool import has_shareable_memory

from codes.mesh import *
from base.timer import checktime
from base.tools import status,framelooper

def undulations(**kwargs):

	"""
	Compute bilayer midplane structures for studying undulations.
	"""

	#---parameters
	sn = kwargs['sn']
	work = kwargs['workspace']
	calc = kwargs['calc']
	grid_spacing = calc['specs']['grid_spacing']
	dat = kwargs['upstream']['lipid_abstractor']
	nframes = dat['nframes']

	#---choose grid dimensions
	grid = array([round(i) for i in mean(dat['vecs'],axis=0)/grid_spacing])[:2]
	monolayer_indices = dat['monolayer_indices']

	#---parallel
	start = time.time()
	mesh = [[],[]]
	for mn in range(2):
		mesh[mn] = Parallel(n_jobs=work.nprocs,verbose=0)(
			delayed(makemesh_regular)(
				dat['points'][fr][where(monolayer_indices==mn)],dat['vecs'][fr],grid)
			for fr in framelooper(nframes,start=start,text='monolayer %d, frame'%mn))
	checktime()

	#---pack
	attrs,result = {},{}
	result['mesh'] = array(mesh)
	result['grid'] = array(grid)
	result['nframes'] = array(nframes)
	result['vecs'] = dat['vecs']
	result['timeseries'] = work.slice(sn)[kwargs['slice_name']][
		'all' if not kwargs['group'] else kwargs['group']]['timeseries']
	attrs['grid_spacing'] = grid_spacing
	return result,attrs	

