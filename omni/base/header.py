#!/bin/bash
"exec" "python" "-iB" "$0" "$@"

__doc__ = """
PLOTTING HEADER
Header file which runs automatically before any plotting.
This handles backwards compatibility with old plots --- just remove any header junk.
"""

exec(open('omni/base/pythonrc.py').read())

import os,sys,re

#---we set the file name so tracebacks make sense
__file__,script,plotname,run_type = sys.argv[:4]
meta = None if sys.argv[4]=='null' else sys.argv[4:]

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

import base.store
#---distribute the workspace to the store module
#---...we have to distribute this way, or internalize these function
base.store.work = work
from base.store import plotload,picturesave
from base.tools import status
from plotter import mpl,plt
from plotter.panels import panelplot,square_tiles
from makeface import tracebacker
from hypothesis import hypothesis,sweeper
import numpy as np

#---plot scripts with special names
#---! deprecated!
if False:
	if run_type=='plot':
		for fn in ['figures','colors']:
			if os.path.isfile(os.path.join('calcs','specs',fn+'.py')):
				with open(os.path.join('calcs','specs',fn+'.py')) as fp: exec(fp.read())

#---flag for IPython notebook use
is_live = False

#---custom art director
from plotter.art_director_importer import import_art_director,protected_art_words
art_director = work.vars.get('art_director',None)
if art_director: 
	#---reload the art settings if they are already loaded
	mod_name = re.match('^(.+)\.py$',os.path.basename(art_director)).group(1)
	#---! switced from reload to a python3-compatible. would prefer to avoid pyc files.
	import importlib
	if mod_name in sys.modules: importlib.reload(sys.modules[mod_name])
	art_vars = import_art_director(art_director,cwd='calcs')
	#---unpack these into global
	for key,val in art_vars.items(): globals()[key] = val
#---if not art director then we set all protected variables to null
else: 
	for key in protected_art_words: globals()[key] = None
for key in ['mod_name','art_vars','art_director']:
	if key in globals(): del globals()[key]

def replot():
	"""
	This function re-executes the script.
	Confirmed that it remembers variables you add.
	"""
	with open(script) as fp: code = fp.read()
	try: exec(compile(code,script,'exec'),globals())
	except Exception as e: tracebacker(e)

print('[PLOTTER] running plots via __file__="%s"; you can execute again with `replot()`'%__file__)
#---execute once and user can repeat 
replot()
