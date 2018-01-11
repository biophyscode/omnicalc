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
