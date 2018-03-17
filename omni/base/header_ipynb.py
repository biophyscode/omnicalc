#!/bin/bash
"exec" "python" "-iB" "$0" "$@"

__doc__ = """
PLOTTING HEADER
Header file which runs automatically before any plotting.
Note that this duplicates features in omnicalc.WorkSpace
This was duplicated almost verbatim from the updated header.py.
"""

#---modified path from header.py points to omnicalc instance
this_path = '../'

import os,sys,re
#---simplified from header.py
for i in ['omni','calcs']: sys.path.insert(0,os.path.join(this_path,i))

from omnicalc import WorkSpace

#---generate a workspace
work = WorkSpace(plot=True,plot_args=(plotname,),is_live=True,
	plot_kwargs=dict(header_caller=True),cwd=this_path)
work.plot_prepare()
#---prepare variables for export into the global namepsace of the script
from base.autoplotters import inject_supervised_plot_tools
out = dict(work=work,plotname=plotname)
inject_supervised_plot_tools(out,mode='interactive',silent=True)
#---dump the injected functions into the global namespace and builtims
import builtins
for key,val in out.items(): builtins.__dict__[key] = val
builtins._plotrun_specials = out.keys()
globals().update(**out)

#---ignore annoying future warnings
import warnings
warnings.simplefilter(action='ignore',category=FutureWarning)

#---decorate picturesave so plots are visible in the notebook
picturesave_omni = picturesave
def picturesave(*args,**kwargs):
	"""Custom procedure for showing *and* saving the figure."""
	fig = plt.gcf()
	plt.show()
	kwargs['figure_held'] = fig
	picturesave_omni(*args,**kwargs)
	plt.close()

#---clean up
for key in ['this_path','key']:
	if key in globals(): del globals()[key]

#---downstream scripts need to know if we are in a notebook
is_live = True

#! currently no way to detect autoplot without user input
if work.do_autoplot():
	print('[NOTE] this plot is an *autoplot* which you must run with `replot()` in a new cell '
		'which will run the decorated load and plot functions. After you do this once, you can '
		'update the plot functions, run the plots by calling those functions, and visualize the result.')

def replot():
	"""..."""
	if work.do_autoplot():
		work.plot_prepare()
		if plotrun.loader_ran==False:
			plotrun.loader()
			plotrun.loader_ran = True
			globals().update(**plotrun.residue)
		for plot_name,func in plotrun.plot_functions.items():
			print('[PLOT] interactive plotter is running `%s`'%plot_name)
			func()
	else: raise Exception('The `replot` function does not apply for scripts which are not "autoplot".')
