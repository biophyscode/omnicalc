#!/bin/bash
"exec" "python" "-iB" "$0" "$@"

__doc__ = """
PLOTTING HEADER
Header file which runs automatically before any plotting.
Note that this duplicates features in omnicalc.WorkSpace
"""

import os,sys,re
for i in ['omni','calcs']:
	if i not in sys.path: sys.path.insert(0,i)
from omnicalc import WorkSpace

#---flag for IPython notebook use
is_live = False
#---tab completion
exec(open('omni/base/pythonrc.py').read())
#---collect incoming names
__file__,script,plotname = sys.argv[:3]
meta = None if sys.argv[3]=='null' else sys.argv[3:]
#---generate a workspace
work = WorkSpace(plot=plotname,meta=meta)
#---prepare variables for export into the global namepsace of the script
from base.autoplotters import inject_supervised_plot_tools
out = dict(work=work,plotname=plotname)
inject_supervised_plot_tools(out,mode='interactive')
#---dump the injected functions into the global namespace
globals().update(**out)

#---define the replotter
def replot():
	"""
	This function re-executes the script.
	Confirmed that it remembers variables you add.
	"""
	import os
	with open(script) as fp: code = fp.read()
	try:
		#---we execute the script once with a weird name in case there is main code there
		#---...note that decorators for load and plot functions will run here
		status('reimporting functions from "%s"'%os.path.basename(script),tag='status')
		#---this is a problem because it reregisters the scripts
		local_env = {'__name__':'__looking__'}
		exec(compile(code,script,'exec'),globals(),local_env)
		#---we have to load local_env into globals here otherwise stray functions in the plot
		#---...will not be found by other functions which might be decorated
		globals().update(**local_env)
		#---run the loader function which should conditionally referesh data (i.e. only as needed)
		status('running the loader function "%s" from "%s"'%(
			plot_super.loader_name,os.path.basename(script)),tag='load')
		#---if the loader is a function we run it otherwise it defaults to None
		if plot_super.loader!=None: plot_super.loader()
		#---run any plots in the routine
		plot_super.autoplot()
		#---in case the user has prototyped code in an if-main section we run the script once more
		#---...noting of course that it is very unlikely to have changed since the compile above
		exec(compile(code,script,'exec'),globals())
	except Exception as e: tracebacker(e)
print('[PLOTTER] running plots via __file__="%s"; you can execute again with `replot()`'%__file__)
#---execute once and user can repeat 
replot()
