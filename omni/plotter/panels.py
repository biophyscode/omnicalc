#!/usr/bin/env python

import matplotlib as mpl 
import matplotlib.pyplot as plt
import matplotlib.gridspec as gridspec
import itertools
import numpy as np

"""
EXAMPLE LAYOUT 

lay = {
	'out':{'grid':[2,1],'hratios':[1,1.2]},
	'ins':[
		{'grid':[1,3],'wspace':0.5,'hspace':0.2},
		{'grid':[3,4],'wspace':0.75,'hspace':0.75},
		],}

Layout must be a dictionary with 'out' and 'ins' keys.
The 'out' object should be a dictionary with a 'grid' item which is a tuple specifying columns and rows.
The 'ins' object should be a list of dictionaries that each have their own 'grid' item (rows by columns).
Use wspace and hspace to set the spacing between axes within a subplot.
Use hratios and wratios to specify the ratio of the outside object.
All grids of axes are placed in order by column, then row, so you don't need to index the exact position.
The returned axes object only requires two indices: the outside index and the inside one.
If there is only one outer object (e.g. 'out':{'grid':[2,1]}) then you do not need the outside index number.
"""

def panelplot(layout=None,figsize=(8,8),explicit_indexing=False):
	"""
	Nested panel plots.
	"""
	#---default is a single plot
	lay = layout if layout != None else {'out':{'grid':[1,1]},'ins':[{'grid':[1,1]}]}
	axes,axpos = [],[]
	fig = plt.figure(figsize=figsize)
	onrows,oncols = lay['out']['grid']
	outer_hspace = 0.45 if 'hspace' not in lay['out'] else lay['out']['hspace']
	outer_wspace = 0.45 if 'wspace' not in lay['out'] else lay['out']['wspace']
	outer_grid = gridspec.GridSpec(onrows,oncols,wspace=outer_wspace,hspace=outer_hspace)
	if 'hratios' in lay['out']: outer_grid.set_height_ratios(lay['out']['hratios'])
	if 'wratios' in lay['out']: outer_grid.set_width_ratios(lay['out']['wratios'])
	if type(lay['ins'])==dict: lay_ins = [lay['ins'] for i in range(np.product(lay['out']['grid']))]
	else: lay_ins = lay['ins']
	for ii,spot in enumerate(list(itertools.product(*[np.arange(i) for i in lay['out']['grid']]))):
		if ii>len(lay_ins)-1: raise Exception('looks like you have too few ins')
		hspace = lay_ins[ii]['hspace'] if 'hspace' in lay_ins[ii] else None
		wspace = lay_ins[ii]['wspace'] if 'wspace' in lay_ins[ii] else None
		inner_grid = gridspec.GridSpecFromSubplotSpec(*lay_ins[ii]['grid'],
			wspace=wspace,hspace=hspace,subplot_spec=outer_grid[ii])
		if 'hratios' in lay_ins[ii]: inner_grid.set_height_ratios(lay_ins[ii]['hratios'])
		if 'wratios' in lay_ins[ii]: inner_grid.set_width_ratios(lay_ins[ii]['wratios'])
		inaxs = [fig.add_subplot(j) for j in inner_grid]
		axpos.append(list(itertools.product(*[np.arange(i) for i in lay_ins[ii]['grid']])))
		axes.append(inaxs)
	return (axes[0] if len(axes)==1 and not explicit_indexing else axes,fig)

def square_tiles(ntiles,figsize,favor_rows=False,wspace=None,hspace=None):
	"""
	Create a grid of tiles with sequential order.
	"""
	nrows = ncols = int(np.ceil(np.sqrt(ntiles)))
	nrows -= int(1*(ntiles<=(nrows-1)*ncols))
	if not favor_rows: nrows,ncols = ncols,nrows
	layout = {'out':{'grid':[1,1]},'ins':{'grid':[nrows,ncols]}}
	if wspace: layout['ins']['wspace'] = wspace
	if hspace: layout['ins']['hspace'] = hspace
	#---send a single number and we will make the plot proportional (however this might 
	#---...not be perfect if tiles are not square)
	if type(figsize) not in [tuple,list]:
		figsize = tuple([figsize*ncols/max([nrows,ncols]),figsize*nrows/max([nrows,ncols])])
	axes,fig = 	panelplot(figsize=figsize,layout=layout)
	for i in range(nrows*ncols-ntiles): fig.delaxes(axes[-1*(i+1)])
	return axes[:ntiles],fig
