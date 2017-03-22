#!/usr/bin/env python

"""
Canonical color definitions.
"""

import brewer2mpl
colors = dict([(key,brewer2mpl.get_map('Set1','qualitative',9).mpl_colors[val])
	for key,val in {
		'red':0,
		'blue':1,
		'green':2,
		'purple':3,
		'orange':4,
		'yellow':5,
		'brown':6,
		'pink':7,
		'grey':8,
		}.items()])

