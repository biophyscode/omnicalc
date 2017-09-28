#!/usr/bin/env python

import os,sys
from base.tools import status

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

	def register_loader(self,name,function):
		"""Only register the loader once."""
		if self.loader==None: 
			self.loader = function
			self.loader_name = name
		#---only allow the loader to be registered once
		elif self.mode!='interactive': 
			raise Exception('we already have a function decorated with autoload: "%s"'%self.loader_name)

	def register(self,name,function):
		"""Maintain a list of plot functions."""
		if name not in self.plot_names: 
			status('registering plot function `%s`'%name,tag='plot')
			self.plot_names.append(name)
			self.plot_functions[name] = function
		else: pass

	def autoplot(self,out=None):
		"""Execute the replot sequence."""
		#---plot everything unless routine
		targets = (self.plot_names if self.routine==None else self.routine)
		#---for supervised execution we get locals from the exec on the script and pass them
		#---...out to globals here because the function call at the end of this function may need to 
		#---...see them. this is unorthodox however these functions only run once
		if self.mode=='supervised' and any(targets) and out!=None: globals().update(**out)
		for plot_name in targets:
			status('executing plot %s'%plot_name,tag='plot')
			if plot_name not in self.plot_functions:
				raise Exception('this script does not have a plot function named %s'%plot_name)
			self.plot_functions[plot_name]()

def autoload(plot_super):
	"""
	Decorate a loader function to be run every time the script is run.
	The loader functions should only reload data on a particular condition i.e. a key variable is not found
	in the global namespace.
	"""
	def autoload_decorator(function):
		#---the autoload decorator nested here so we get the supervisor as a parameter
		#---add the function to the supervisor
		name = function.__name__
		plot_super.register_loader(name,function)
		def wrapper(*args,**kwargs):
			#---you cannot call status here. have the function announce itself
			status('!!!!!!!!!!!!!!!!',tag='load')
			function(*args,**kwargs)
		return wrapper
	return autoload_decorator

def autoplot(plot_super):
	"""
	Register a plot function with the supervisor.
	"""
	def autoplot_decorator(function):
		#---the autoplot decorator nested here so we get the supervisor as a parameter
		#---add the function to the supervisor
		name = function.__name__
		plot_super.register(name,function)
		def wrapper(*args,**kwargs):
			function(*args,**kwargs)
		return wrapper
	return autoplot_decorator

def inject_supervised_plot_tools(out,mode='supervised'):
	"""
	Add important tools to a dictionary which is later exported to the namespace for plotting.
	This function was centralized here so that both the interactive header and non-interactive execution
	modes can use it.
	"""
	work = out['work']
	#---import sequence from original header.py
	import store
	#---distribute the workspace to the store module
	#---...we have to distribute this way, or internalize these function
	store.work = work
	from store import plotload,picturesave
	from tools import status
	#---the plotter __init__.py sets the imports (importantly, including mpl,plt with modifications)
	from plotter.panels import panelplot,square_tiles
	from makeface import tracebacker
	from hypothesis import hypothesis,sweeper
	from copy import deepcopy
	#---we would prefer not to import numpy here but it is important for backwards compatibility
	import numpy as np
	out.update(plotload=plotload,picturesave=picturesave,status=status,panelplot=panelplot,
		square_tiles=square_tiles,tracebacker=tracebacker,hypothesis=hypothesis,sweeper=sweeper,
		deepcopy=deepcopy,np=np)
	#---add a plot supervisor instance and the autoplotter decorators
	from autoplotters import PlotSupervisor,autoload,autoplot
	out.update(plot_super=PlotSupervisor(mode=mode))
	#---we use str_types frequently for python 2,3 cross-compatibility
	str_types = [str,unicode] if sys.version_info<(3,0) else [str]
	out.update(autoload=autoload,autoplot=autoplot,str_types=str_types)

	#---matplotlib is first loaded here
	import matplotlib as mpl 
	if work.mpl_agg: mpl.use('Agg')
	import matplotlib.pyplot as plt
	out.update(mpl=mpl,plt=plt)

	#---custom "art director" can be useful for coordinating aesthetics for different projects
	from plotter.art_director_importer import import_art_director,protected_art_words
	art_director = work.vars.get('art_director',None)
	#---always set protected variables to null
	for key in protected_art_words: out[key] = None
	if art_director: 
		#---reload the art settings if they are already loaded
		mod_name = re.match('^(.+)\.py$',os.path.basename(art_director)).group(1)
		#---! switced from reload to a python3-compatible. would prefer to avoid pyc files.
		import importlib
		if mod_name in sys.modules: importlib.reload(sys.modules[mod_name])
		art_vars = import_art_director(art_director,cwd='calcs')
		#---unpack these into outgoing variables
		for key,val in art_vars.items(): out[key] = val

	#---tell the user which variables are automagically loaded
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
	asciitree(dict(plot_environment=dict([
		(name,dict([(key,str(out[key])) for key in sorted(out) if key_catalog[key]==name]))
		for name in ['variable','function','instance']])))
