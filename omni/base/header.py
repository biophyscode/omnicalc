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
#---dump the injected functions into the global namespace and builtims
import builtins
for key,val in out.items(): builtins.__dict__[key] = val
builtins._plotrun_specials = out.keys()
globals().update(**out)

#---old-school replotter
def replot_old_school():
	"""
	This function re-executes the script.
	Confirmed that it remembers variables you add.
	"""
	with open(script) as fp: code = fp.read()
	try: exec(compile(code,script,'exec'),globals())
	except Exception as e: tracebacker(e)

#---define the replotter
def replot():
	"""
	This function re-executes the script.
	Confirmed that it remembers variables you add.
	"""
	#import ipdb;ipdb.set_trace()
	#---redirect to standard plotting
	if not work.plots.get(plotname,{}).get('autoplot',False): 
		replot_old_school()
		return
	import os
	with open(script) as fp: code = fp.read()
	try:
		#---we execute the script once with a weird name in case there is main code there
		#---...note that decorators for load and plot functions will run here
		status('reimporting functions from "%s"'%os.path.basename(script),tag='status')
		#---this is a problem because it reregisters the scripts
		local_env = {'__name__':'__looking__'}
		try: exec(compile(code,script,'exec'),globals(),local_env)
		except Exception as e:
			#---note that old-school scripts might have problems with globals when you send out the local_env
			#---...to replace __name__ above. in that case we just revert to the standard method. note that 
			#---...this ensuress the global namespace is executed in the usual way, instead of 
			#---...the more heavy-handed approach in the new-style plot scripts
			status('falling back to old-school automatic plotting',tag='warning')
			#---! added this exception reporter to investigate annoying replot-with-code-in-globals issue
			status('exception was: %s'%e,tag='exception')
			exec(compile(code,script,'exec'),globals())
		local_env['__name__'] = '__replotting__'
		#---we have to load local_env into globals here otherwise stray functions in the plot
		#---...will not be found by other functions which might be decorated
		globals().update(**local_env)
		#---run the loader function which should conditionally referesh data (i.e. only as needed)
		status('running the loader function "%s" from "%s"'%(
			plotrun.loader_name,os.path.basename(script)),tag='load')
		import ipdb;ipdb.set_trace()
		#---if the loader is a function we run it otherwise it defaults to None
		if plotrun.loader!=None: plotrun.loader()
		#---run any plots in the routine
		plotrun.autoplot()
		#---in case the user has prototyped code in an if-main section we run the script once more
		#---...noting of course that it is very unlikely to have changed since the compile above
		exec(compile(code,script,'exec'),globals())
	except Exception as e: tracebacker(e)

#---clean up
for key in ['key']:
	if key in globals(): del globals()[key]

print('[PLOTTER] running plots via __file__="%s"; you can execute again with `replot()`'%__file__)
#---execute once and user can repeat 
replot()
