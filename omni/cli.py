#!/usr/bin/env python

"""
Omnicalc command-line interface.
"""

from __future__ import print_function

# universal listing of all required packages
required_python_packages = (
	'yaml','h5py','joblib','matplotlib>2.0.0')

# functions exposed to the command line
if 1:
	__all__ = [
		'setup','clone_calcs',
		'blank_meta','audit','go',
		'compute','plot','look','clear_stale',
		#! temporary
		'default_spot']

import os,sys,re
from ortho import read_config,write_config,bash,requires_python,treeview,status
#from omni.omnicalc import Workspace,load

def setup():
	"""
	Check for required settings.
	"""
	config = read_config()
	required_settings = ['post_data_spot','post_plot_spot']
	needs_keys = [i for i in required_settings if i not in config]
	if any(needs_keys): 
		print('note','setting is incomplete until you add: %s. use `make set key="val"`.'%needs_keys)

def clone_calcs(source):
	"""
	Clone a calculations repository.
	"""
	#! replace with sync from ortho
	config = read_config()
	if 'calculations_repo' in config and not os.path.isdir('calcs/.git'):
		raise Exception('config has a calculations repo registered but we cannot find calcs/.git')
	elif not 'calculations_repo' in config and os.path.isdir('calcs/.git'):
		raise Exception('found calcs/.git but no calculations_repo in the config')
	elif 'calculations_repo' in config and os.path.isdir('calcs/.git'):
		raise Exception('you already have a calculations repo at calcs')
	# clone and register
	bash('git clone %s calcs'%source)
	config['calculations_repo'] = source
	write_config(config)

#! deprecated
if False:
	#! should be deprecated
	calcs_template = """
	variables: {}
	collections: {}
	plots: {}
	""".strip()
	def blank_meta(make_template=True):
		"""
		Set up an empty specs container. You can opt to only make the specs folder in the event that
		you do not want a blank meta file (the factory does this because the GUI tells the user how to make one).
		"""
		if not os.path.isdir('calcs/specs'): os.mkdir('calcs/specs')
		if make_template:
			if not os.path.isfile('calcs/specs/meta.yaml'):
				with open('calcs/specs/meta.yaml','w') as fp: fp.write(calcs_template)
	def audit(debug=False,source='calcs/auditor.py'):
		"""
		Command-line interface to an auditor for tracking the status of different calculations.
		"""
		if not os.path.isfile(source): raise Exception('requires source code at %s'%source)
		else: 
			from makeface import import_remote
			auditor = import_remote(source)
			auditor['CalcsAuditor'](debug=debug)

#!!! temporary. needs better coordination with factory!
def default_spot(spot):
	#! temporary until a better method
	config = read_config()
	spots = {'sims': {'route_to_data': '/Users/rpb/worker/factory/data/demo', 'regexes': {'step': '([stuv])([0-9]+)-([^\\/]+)', 'part': {'edr': 'md\\.part([0-9]{4})\\.edr', 'trr': 'md\\.part([0-9]{4})\\.trr', 'xtc': 'md\\.part([0-9]{4})\\.xtc', 'tpr': 'md\\.part([0-9]{4})\\.tpr', 'structure': '(system|system-input|structure)\\.(gro|pdb)'}, 'top': '(.+)'}, 'namer': 'lambda name,spot=None: name', 'spot_directory': 'sims'}}
	if not os.path.isdir(spot): raise Exception
	spots['sims']['route_to_data'] = os.path.dirname(spot)
	spots['sims']['spot_directory'] = os.path.basename(spot)
	config['spots'] = spots
	write_config(config)

"""
required defaults
	right now this needs to be connected to the factory somehow
	...!!!
		post_data_spot
		spots
		calculations_repo
		post_plot_spot
"""

###
### INTERFACE FUNCTIONS
### note that these are imported by omni/cli.py and exposed to makeface

@requires_python(*required_python_packages)
def plot(*args,**kwargs):
	status('generating workspace for plot',tag='status')
	# since cli.py is inside omnicalc, this import must be just in time here
	from omni import WorkSpace
	work = WorkSpace(plot=True,plot_args=args,plot_kwargs=kwargs)

@requires_python(*required_python_packages)
def analysis(script):
	"""Standard analysis environment called with `make go script=calcs/script_name.py`."""
	from ortho import interact
	print('status','preparing analysis environment')
	from omni.base.analysis_hooks import omni_analysis_hook
	# prepare the hooks to reexec for replot
	from ortho.reexec import ReExec
	class ReExecOmni(ReExec):
		def reload(self):
			out = self.namespace
			out['__name__'] = 'ortho.reexec'
			self.get_text()
			exec(self.text,out,out)
			exec('control.reload()',out,out)
		def main(self):
			out = self.namespace
			out['__name__'] = '__main__'
			self.get_text()
			exec(self.text,out,out)
		def replot(self):
			out = self.namespace
			out['__name__'] = 'ortho.reexec'
			self.get_text()
			exec(self.text,out,out)	
			exec('control.autoplot()',out,out)
			out['__name__'] = '__main__'
	# names in the commands list must be in the subclass above and cannot
	#   include the standard do or redo names
	kwargs = {'reexec_class':ReExecOmni,'commands':['main','replot','reload']}
	# the coda is passed along for the first execution of the script
	# otherwise the commands in ReExecOmni will rerun things from interact
	interact(hooks=(omni_analysis_hook,),script=script,msg='',
		coda='control.reload()\ncontrol.autoplot()',**kwargs)

def go(*args,**kwargs): 
	"""Alias for plot. Calls the standard analysis pipeline."""
	#! need a python 2/3 compatible dict_keys comparison method or use this
	if not args and set(kwargs.keys())=={'script'}: analysis(**kwargs)
	else: plot(*args,**kwargs)

def look(*args,**kwargs):
	# since cli.py is inside omnicalc, this import must be just in time here
	from omni import WorkSpace
	work = WorkSpace(look=dict(args=args,kwargs=kwargs))

def clear_stale(meta=None):
	"""Check for stale jobs."""
	# since cli.py is inside omnicalc, this import must be just in time here
	from omni import WorkSpace,load
	work = WorkSpace(compute=True,meta_cursor=meta,debug='stale')
	fn_sizes = dict([((v.files['dat'],v.files['spec']),os.path.getsize(v.files['dat'])) 
		for k,v in work.post.posts().items()])
	targets = [(dat_fn,spec_fn) for (dat_fn,spec_fn),size in fn_sizes.items() if size<=10**4]
	stales = []
	for dat_fn,spec_fn in targets:
		data = load(os.path.basename(dat_fn),cwd=os.path.dirname(dat_fn))
		if data.get('error',False)=='error': 
			# one of very few places where we delete files because we are sure they are gabage
			# ... the other place being the dat file deletion before writing the final file
			status('removing stale dat file %s'%dat_fn)
			try: os.remove(dat_fn)
			except: status('failed to delete %s'%dat_fn,tag='warning')
			status('removing stale spec file %s'%spec_fn)
			try: os.remove(spec_fn)
			except: status('failed to delete %s'%spec_fn,tag='warning')
		stales.append(dat_fn)
	if stales:
		treeview({'cleaned files':sorted(stales)})
		status('you can continue with `make compute` now. '
			'we cleaned up stale dat files and corresponding spec files listed above')

@requires_python(*required_python_packages)
def compute(debug=False,debug_slices=False,meta=None,back=False):
	"""
	Main compute loop runs from here.
	"""
	# automatically clear stale
	clear_stale()
	if back: 
		from base.tools import backrun
		backrun(command='make compute',log='log-compute')
	else:
		status('generating workspace for compute',tag='status')
		# since cli.py is inside omnicalc, this import must be just in time here
		from omni import WorkSpace
		work = WorkSpace(compute=True,meta_cursor=meta,debug=debug)
