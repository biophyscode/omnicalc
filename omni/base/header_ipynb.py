#!/usr/bin/env python 

"""
Header file for an IPython notebook served by the factory.

We use the `is_live` flag to let plotting functions know if they are in IPython or not. Note that the other
header for standard plotting also includes this flag.
"""

#---allow plot functions to determine if we are live or not
is_live = True

import os,sys,re

#---this script is called from root and expects omni to be present
if not os.path.isdir('../omni'): raise Exception('cannot find `omni` folder')
sys.path.insert(0,'../calcs')
sys.path.insert(0,'../omni')
#---calcs has no __init__.py but codes requires one. the following import means you can use e.g.
#---..."from codes import undulate" or "import codes.undulate" and both work. this means that 
#---...external imports from codes by the plotter functions make sense and appear to be local
import codes

from omnicalc import WorkSpace
work = WorkSpace(plot=plotname,cwd='../')

import base.store
#---distribute the workspace to the store module
#---...we have to distribute this way, or internalize these function
base.store.work = work
from base.store import plotload,picturesave
from base.tools import status
from plotter import mpl,plt
from plotter.panels import panelplot
from makeface import tracebacker
import numpy as np

for fn in ['figures','colors']:
	if os.path.isfile(os.path.join('calcs','specs',fn+'.py')):
		with open(os.path.join('calcs','specs',fn+'.py')) as fp: exec(fp.read())

picturesave_omni = picturesave
def picturesave(*args,**kwargs):
	plt.show()
	picturesave_omni(*args,**kwargs)
	plt.close()

