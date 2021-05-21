#!/usr/bin/env python

import os,sys,re,collections
from base.tools import status,Observer

"""
AUTOPLOTTERS
Functions exported to supervised plot scripts.
"""

class PlotSupervisor:

	def __init__(self,mode='supervised'):
		"""
		The supervisor keeps track of loading and plotting functions and is sent to the decorator which
		names these functions in the plot script.
		"""
		self.mode = mode
		#---supervised mode runs once and exists while interactive uses header.py
		if mode not in ['supervised','interactive']:
			raise Exception('invalid mode %s'%mode)
		self.routine = None
		self.plot_names = []
		self.plot_functions = {}
		self.loader = None
		self.loader_name = None
		self.loader_ran = False
		self.script_name = None
		self.residue = {}

	def register_loader(self,name,function):
		"""
		Register the load function and hold its code for comparison. 
		We only reexecute code if it changes.
		"""
		self.loader = function
		self.loader_name = name

	def reload(self): 
		"""The loader only ever runs once. Users can reload with this alias."""
		self.loader()

	def register(self,name,function):
		"""Maintain a list of plot functions."""
		self.plot_functions[name] = function
		if name not in self.plot_names: self.plot_names.append(name)

	def autoplot(self,out=None):
		"""Execute the replot sequence."""
		#---plot everything unless routine
		targets = (self.plot_names if self.routine==None else self.routine)
		#---for supervised execution we get locals from the exec on the script and pass them
		#---...out to globals here because the function call at the end of this function may need to 
		#---...see them. this is unorthodox however these functions only run once
		if self.mode=='supervised' and any(targets) and out!=None: globals().update(**out)
		for plot_name in targets:
			#! plotname is wrong here. sometimes it is "plot"
			status('executing plot function `%s`'%plot_name,tag='autoplot')
			if plot_name not in self.plot_functions:
				raise Exception('this script does not have a plot function named %s'%plot_name)

			self.plot_functions.update(**self.residue)
			self.plot_functions[plot_name]()

def autoload(plotrun):
	"""
	Decorate a loader function to be run every time the script is run.
	The loader functions should only reload data on a particular condition i.e. a key variable is not found
	in the global namespace.
	"""
	def autoload_decorator(function):
		#---the autoload decorator nested here so we get the supervisor as a parameter
		#---add the function to the supervisor
		name = function.__name__
		# only announce the wrap when looking otherwise confusing
		if plotrun.script_name!='__main__':
			status('wrapping the loader function named `%s`'%name)
		#! plotrun.register_loader(name,function)
		def wrapper(*args,**kwargs):
			#---you cannot call status here. have the function announce itself
			#---...actually this comes through in the jupyter notebook. removed for clarity
			status('running autoload args=%s, kwargs=%s'%(args,kwargs),tag='load')
			# we are using the Observer to get persistent locals from the function
			# ... note that we are calling Observer manually here because it is a decorator
			obs = Observer(function)
			obs.__call__(*args,**kwargs)
			# save locals for later loading into globals in replot
			plotrun.residue = obs._locals
		plotrun.register_loader(name,wrapper)
		return wrapper
	return autoload_decorator

def autoplot(plotrun):
	"""
	Register a plot function with the supervisor.
	"""
	def autoplot_decorator(function):
		#---the autoplot decorator nested here so we get the supervisor as a parameter
		#---add the function to the supervisor
		name = function.__name__
		# only announce the wrap when looking otherwise confusing
		if plotrun.script_name!='__main__':
			status('wrapping the plot function named `%s`'%name)
		plotrun.register(name,function)
		def wrapper(*args,**kwargs):
			status('executing plot function `%s`'%name)
			return function(*args,**kwargs)
		return wrapper
	return autoplot_decorator

def inject_supervised_plot_tools(out,mode='supervised',silent=False):
	"""
	Add important tools to a dictionary which is later exported to the namespace for plotting.
	This function was centralized here so that both the interactive header and non-interactive execution
	modes can use it.
	"""
	import os,sys,re
	work = out['work']
	#---save keys before the additions
	keys_incoming = set(out.keys())
	#---import sequence from original header.py
	from . import store
	#---distribute the workspace to the store module
	#---...we have to distribute this way, or internalize these function
	store.work = work
	from .store import plotload,picturesave,picturesave_redacted
	from .tools import status
	if work.metadata.director.get('redacted',False):
		picturesave = picturesave_redacted
	#---handle latex and matplotlibrc here
	from config import read_config
	try: 
		config = read_config()
		cwd = '.'
	#---if execution does not happen in the omnicalc root we are in interactive mode running from calcs
	except: 
		cwd = '../'
		config = read_config(cwd=cwd)
	matplotlibrc_path = os.path.join(cwd,config.get('matplotlibrc','omni/plotter/matplotlibrc'))
	#---without an explicit matplotlibrc file, we check the config and then check for the latex binary
	if (os.path.basename(matplotlibrc_path)!='matplotlibrc' or
		not os.path.isfile(matplotlibrc_path)):
		raise Exception('cannot find a file called "matplotlibrc" here: %s'%matplotlibrc_path)
	os.environ['MATPLOTLIBRC'] = os.path.abspath(os.path.join(os.getcwd(),
		os.path.dirname(matplotlibrc_path)))
	#---matplotlib is first loaded here
	import matplotlib as mpl 
	if work.mpl_agg: mpl.use('Agg')
	import matplotlib.pyplot as plt
	#---we default to latex if it is available otherwise we consult config
	use_latex = config.get('use_latex',None)
	if use_latex==None:
		from distutils.spawn import find_executable
		use_latex = find_executable('latex')
	out.update(mpl=mpl,plt=plt)
	#---the plotter __init__.py sets the imports (importantly, including mpl,plt with modifications)
	from plotter.panels import panelplot,square_tiles
	from makeface import tracebacker
	from .hypothesis import hypothesis,sweeper
	from copy import deepcopy
	#---we would prefer not to import numpy here but it is important for backwards compatibility
	import numpy as np
	out.update(np=np,os=os,sys=sys,re=re)
	#---load custom functions
	out.update(plotload=plotload,picturesave=picturesave,status=status,panelplot=panelplot,
		square_tiles=square_tiles,tracebacker=tracebacker,hypothesis=hypothesis,sweeper=sweeper,
		deepcopy=deepcopy,np=np)
	#---add a plot supervisor instance and the autoplotter decorators
	from .autoplotters import PlotSupervisor,autoload,autoplot
	out.update(plotrun=PlotSupervisor(mode=mode))
	#---we use str_types frequently for python 2,3 cross-compatibility
	str_types = [str,unicode] if sys.version_info<(3,0) else [str]
	out.update(autoload=autoload,autoplot=autoplot,str_types=str_types)

	#---custom "art director" can be useful for coordinating aesthetics for different projects
	from plotter.art_director_importer import import_art_director,protected_art_words
	#---you can set the art director in the variables or ideally in the director
	art_director = work.metadata.director.get('art_director',work.metadata.variables.get('art_director',None))
	#---always set protected variables to null
	for key in protected_art_words: out[key] = None
	if art_director: 
		#---reload the art settings if they are already loaded
		mod_name = re.match('^(.+)\.py$',os.path.basename(art_director)).group(1)
		#---! switched from reload to a python3-compatible. would prefer to avoid pyc files.
		#---! currently disabled on the development branch (getting error module has no attribute reload)
		try:
			import importlib
			if mod_name in sys.modules: importlib.reload(sys.modules[mod_name])
			art_vars = import_art_director(art_director,cwd=os.path.join(cwd,'calcs'))
			#---unpack these into outgoing variables
			for key,val in art_vars.items(): out[key] = val
		except: status('cannot reload the art director',tag='warning')
	out['_plot_environment_keys'] = list(set(out.keys())-keys_incoming)
	#---tell the user which variables are automagically loaded
	if silent: return
	status('the following variables are loaded into your plot script environment',tag='note')
	from datapack import asciitree
	def key_types(obj):
		"""Organize injected variables for the user."""
		if hasattr(obj,'__name__') and obj.__name__ in ['numpy']: return 'external'
		elif callable(obj): return 'function'
		elif hasattr(obj,'__class__') and obj.__class__.__name__ in ['WorkSpace','PlotSupervisor']:
			return 'instance'
		else: return 'variable'
	key_catalog = dict([(key,key_types(out[key])) for key in out])
	report = collections.OrderedDict()
	for name in ['variable','function','instance']:
		report[name] = collections.OrderedDict()
		items = [(key,out[key]) for key in sorted(out) if key_catalog[key]==name]
		for key,val in items: 
			if type(val).__name__=='classobj':
				report[name][key] = '<class \'%s\'>'%val.__name__
			elif val.__class__.__name__=='module': 
				report[name][key] = '<module \'%s\'>'%val.__name__
			elif val.__class__.__name__=='function': 
				report[name][key] = '<function \'%s\'>'%val.__name__
			else:
				if val.__class__.__module__ in ['omnicalc','base.autoplotters']: 
					report[name][key] = str(val)
				elif key in ['str_types']: report[name][key] = str(val)
				else: report[name][key] = val
	asciitree(dict({'plot_environment':report}))
