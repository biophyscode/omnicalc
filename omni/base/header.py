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
from base.tools import status

#---flag for IPython notebook use
is_live = False
#---tab completion
exec(open('omni/base/pythonrc.py').read())
#---collect incoming names
__file__,script,plotname = sys.argv[:3]
# decide if this is an autoplot or legacy plot
use_autoplot = True
if len(sys.argv)>3:
	#! we allow three arguments for autoplot and an extra flag for legacy plotting
	if len(sys.argv)>4: 
		raise Exception('this header can accept only three arguments (header.py, plot script, plot name '
			'with an optional NO_AUTOPLOT flag. we recieved %s'%sys.argv)
	elif len(sys.argv)==4:
		if sys.argv[3]=='NO_AUTOPLOT': use_autoplot = False
		else: raise Exception('invalid arguments %s'%sys.argv)
# updated call to the workspace signals that we do not need to run the plot because we are already here
status('preparing workspace inside plot header',tag='status')
work = WorkSpace(plot=True,plot_args=(plotname,),plot_kwargs=dict(header_caller=True))
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
def replot(loading=False):
	"""
	This function re-executes the script.
	Confirmed that it remembers variables you add.
	"""
	#---legacy workspace members
	work.plot_prepare()
	# previously we checked autoplot flags from the plots metadata here
	# ... however now the autoplotting is controlled in several places in the workspace and passed as a flag
	if not use_autoplot:
		replot_old_school()
		return

	import os
	with open(script) as fp: code = fp.read()
	try:
		#---we execute the script once with a weird name in case there is main code there
		#---...note that decorators for load and plot functions will run here
		status('reimporting functions from `%s`'%os.path.basename(script),tag='status')
		#---this is a problem because it reregisters the scripts
		local_env = {'__name__':'__looking__'}
		if not re.search('plotrun',code):
			raise Exception('cannot find the text "plotrun" anywhere in your plot script at %s. '%script+
				'this means the script is a legacy plot. you should set "autoplot: False" in the plot specs')
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
		#! rename the following to main
		local_env['__name__'] = plotrun.script_name = '__main__'
		#---we have to load local_env into globals here otherwise stray functions in the plot
		#---...will not be found by other functions which might be decorated
		globals().update(**local_env)
		#---if the loader is a function we run it otherwise it defaults to None
		if plotrun.loader!=None and (not plotrun.loader_ran or loading): 
		#---run the loader function which should conditionally referesh data (i.e. only as needed)
			status('running the loader function `%s` from `%s`'%(
				plotrun.loader_name,os.path.basename(script)),tag='load')
			# the loader is decorated with a function that catches locals
			plotrun.loader()
			# expose locals from the loader to globals automatically
			globals().update(**plotrun.residue)
			plotrun.loader_ran = True
		#---run any plots in the routine unless we are reloading
		if not loading:
			plotrun.autoplot()
			#---in case the user has prototyped code in an if-main section we run the script once more
			#---...noting of course that it is very unlikely to have changed since the compile above
			status(('replot is re-executing with __name__=="%s" in case you are '+
				'developing')%local_env['__name__'])
			exec(compile(code,script,'exec'),globals())
	except Exception as e: tracebacker(e)

def reload(): 
	"""
	The `replot` function parses the code again and runs any plots.
	To run the loader function again and then redo the plots, use this function.
	"""
	replot(loading=True)
	plotrun.loader_ran = False
	plotrun.reload()
	replot()

#---clean up
for key in ['key']:
	if key in globals(): del globals()[key]

print('[PLOTTER] running plots via __file__="%s"; you can execute again with `replot()`'%__file__)
#---execute once and user can repeat 
replot()
