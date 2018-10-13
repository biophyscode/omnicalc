#!/usr/bin/env python

from __future__ import print_function
from omni import WorkSpace
import os,sys,re,collections
from ortho import status,Observer

class PlotSupervisor:

	def __init__(self,mode='supervised'):
		"""
		The supervisor keeps track of loading and plotting functions and is sent to the decorator which
		names these functions in the plot script.
		"""
		self.mode = mode
		# supervised mode runs once and exists while interactive uses header.py
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
		if self.loader: self.loader()

	def register(self,name,function):
		"""Maintain a list of plot functions."""
		self.plot_functions[name] = function
		if name not in self.plot_names: self.plot_names.append(name)

	def autoplot(self,out=None):
		"""Execute the replot sequence."""
		# plot everything unless routine
		targets = (self.plot_names if self.routine==None else self.routine)
		# for supervised execution we get locals from the exec on the script and pass them
		#   out to globals here because the function call at the end of this function may need to 
		#   see them. this is unorthodox however these functions only run once
		if self.mode=='supervised' and any(targets) and out!=None: globals().update(**out)
		for plot_name in targets:
			#! plotname is wrong here. sometimes it is "plot"
			print('status','executing plot function "%s"'%plot_name)
			if plot_name not in self.plot_functions:
				raise Exception('this script does not have a plot function named %s'%plot_name)
			self.plot_functions.update(**self.residue)
			self.plot_functions[plot_name]()

	def refresh(self):
		if self.loader_ran==False: 
			print('status','running the load function')
			self.reload()
			self.loader_ran = True
		#! better message for reexecution
		else: print('status',
			'loader function already ran. '
			'use "do" to load again.')

def omnicalc_analysis_environment(namespace,**kwargs):

	"""
	A function which is introspected by Observer to generate a namespace for 
	omnicalc analysis. Everything defined here is exported to the global 
	namespace of the analysis/plot script.
	"""
	
	# note that defining the plotrun here and defining functions locally with access to it
	#   means that have nested wrappers instead of double-nested wrappers and we also decorate 
	#   with just the decorator and not plotrun as the argument
	control = plotrun = PlotSupervisor(mode='supervised')

	def autoload(function):
		# the autoload decorator nested here so we get the supervisor as a parameter
		# add the function to the supervisor
		name = function.__name__
		# only announce the wrap when looking otherwise confusing
		if plotrun.script_name!='__main__' and not plotrun.loader_ran:
			status('refreshing the load function named "%s"'%name)
		def wrapper(*args,**kwargs):
			# you cannot call status here. have the function announce itself
			#   actually this comes through in the jupyter notebook. removed for clarity
			print('status','running load function args=%s, kwargs=%s'%(args,kwargs))
			# we are using the Observer to get persistent locals from the function
			#    note that we are calling Observer manually here because it is a decorator
			obs = Observer(function)
			obs.__call__(*args,**kwargs)
			# save locals for later loading into globals in replot
			plotrun.residue = obs._locals
			"""
			this is fairly lit. the interact function sends its globals to the hook function omni_analysis_hook as a namespace. note that kwargs follow the namespace but do not do anything yet. omni_analysis_hook uses the Observer to take all of the locals in omnicalc_analysis_environment and deposit them in the namespace, which later becomes the globals for the interactive script. the autoload function can decorate a single load function that runs only once for doing a lot of time-consuming loading. further execution of the script during development with "redo" or "replot" will skip loading it. the autoload decorator also uses the observer to dump everything in the load script into globals. this magic is possible with the following line, where the namespace is updated with locals. it's turtles all the way down.
			"""
			namespace.update(**obs._locals)
		plotrun.register_loader(name,wrapper)
		return wrapper

	def autoplot(function):
		"""
		"""
		# previously the autoplot decorator was nested here again 
		#   so we get the supervisor as a parameter
		# add the function to the supervisor
		name = function.__name__
		# only announce the wrap when looking otherwise confusing
		if plotrun.script_name!='__main__':
			print('status','refreshing the function named "%s"'%name)
		plotrun.register(name,function)
		def wrapper(*args,**kwargs):
			print('status','executing plot function "%s"'%name)
			return function(*args,**kwargs)
		return wrapper

	# extra aliases
	function = autoplot
	loader = autoload

def omni_analysis_hook(namespace,**kwargs):
	"""Prepare the omnicalc analysis environment."""
	# use the Observer to get locals from a function which become globals
	#   in the namespace of the omnicalc analysis environment
	obs = Observer(omnicalc_analysis_environment)
	obs.__call__(namespace,**kwargs)
	# save locals for later loading into globals in replot
	namespace.update(**obs._locals)
	return # namespace is edited in-place
