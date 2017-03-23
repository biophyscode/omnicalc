#!/usr/bin/env python

"""
PLOTTING HEADER
Header file which runs automatically before any plotting.
This handles backwards compatibility with old plots --- just remove any header junk.
"""

import os,sys,re

#---we set the file name so tracebacks make sense
__file__,script,plotname,run_type,meta = sys.argv
meta = None if meta=='null' else meta

#---this script is called from root and expects omni to be present
if not os.path.isdir('omni'): raise Exception('cannot find `omni` folder')
sys.path.insert(0,'calcs')
sys.path.insert(0,'omni')
#---calcs has no __init__.py but codes requires one. the following import means you can use e.g.
#---..."from codes import undulate" or "import codes.undulate" and both work. this means that 
#---...external imports from codes by the plotter functions make sense and appear to be local
import codes

from omnicalc import WorkSpace
if run_type=='plot': work = WorkSpace(plot=plotname,meta=meta)
elif run_type=='pipeline': work = WorkSpace(pipeline=plotname,meta=meta)
else: raise Exception('invalid run_type for this header: %s'%run_type)

from base.store import plotload,picturesave
from base.tools import status
from plotter import mpl,plt
from plotter.panels import panelplot
from makeface import tracebacker
import numpy as np

def replot():
	"""
	This function re-executes the script.
	Confirmed that it remembers variables you add.
	"""
	with open(script) as fp: code = fp.read()
	try: exec(compile(code,script,'exec'),globals())
	except Exception as e: tracebacker(e)

print('[PLOTTER] running plots via __file__="%s". execute again with `replot()`.'%__file__)
#---execute once and user can repeat 
replot()
