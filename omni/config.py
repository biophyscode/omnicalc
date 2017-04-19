#!/usr/bin/env python

import os,sys
from makeface import abspath
from datapack import delveset
import subprocess

__all__ = ['set_config','unset']

#---hardcoded config
config_fn = 'config.py'

"""
Manipulate the config.py in root.
"""

def read_config(cwd=None,source=None):
	"""
	Read the configuration from a single dictionary literal in config.py (or the config_fn).
	"""
	source = config_fn if not source else source
	if cwd: source = os.path.join(cwd,source)
	if not os.path.isfile(abspath(source)): raise Exception('cannot find file "%s"'%source)
	try: return eval(open(abspath(source)).read())
	except: raise Exception('[ERROR] failed to read master config from "%s"'%source)

def write_config(config):
	"""
	Write the configuration.
	"""
	import pprint
	#---write the config
	with open(config_fn,'w') as fp: 
		fp.write('#!/usr/bin/env python -B\n'+str(pprint.pformat(config,width=110)))

def rewrite_config(source='config.py'):
	"""
	Reformat the config.py file in case you change it and want it to look normal again.
	"""
	if not os.path.isfile(os.path.abspath(source)): raise Exception('cannot find file "%s"'%source)
	try: config = eval(open(os.path.abspath(source)).read())
	except: raise Exception('[ERROR] failed to read master config from "%s"'%source)
	import pprint
	#---write the config
	with open(source,'w') as fp: 
		fp.write('#!/usr/bin/env python -B\n'+str(pprint.pformat(config,width=110)))

def is_terminal_command(name):
	"""
	"""
	check_which = subprocess.Popen('which %s'%name,shell=True,executable='/bin/bash',
		stdout=subprocess.PIPE,stderr=subprocess.PIPE)
	check_which.communicate()
	return check_which.returncode

def bash(command,log=None,cwd=None,inpipe=None):
	"""
	Run a bash command
	"""
	if not cwd: cwd = './'
	if log == None: 
		if inpipe: raise Exception('under development')
		kwargs = dict(cwd=cwd,shell=True,executable='/bin/bash')
		proc = subprocess.Popen(command,**kwargs)
		stdout,stderr = proc.communicate()
	else:
		#---if the log is not in cwd we see if it is accessible from the calling directory
		if not os.path.isdir(os.path.dirname(os.path.join(cwd,log))): 
			output = open(os.path.join(os.getcwd(),log),'w')
		else: output = open(os.path.join(cwd,log),'w')
		kwargs = dict(cwd=cwd,shell=True,executable='/bin/bash',
			stdout=output,stderr=output)
		if inpipe: kwargs['stdin'] = subprocess.PIPE
		proc = subprocess.Popen(command,**kwargs)
		if not inpipe: stdout,stderr = proc.communicate()
		else: stdout,stderr = proc.communicate(input=inpipe)
	if stderr: raise Exception('[ERROR] bash returned error state: %s'%stderr)
	if proc.returncode: 
		if log: raise Exception('[ERROR] bash error, see %s'%log)
		else: 
			extra = '\n'.join([i for i in [stdout,stderr] if i])
			raise Exception('[ERROR] bash error'+(': '+extra if extra else ''))

def set_config(*args,**kwargs):
	"""
	Command-line interface to update configuration in config_fn (typically config.py). 
	This function routes ``make set ...` requests to functions here in the acme.py module, which manages all 
	experiments. Since ``set`` is a python type, we make use of the config.py alias scheme to map this 
	function to ``make set ...``.
	This was adapted from the automacs.runner.acme version to be more generic.
	"""
	config_toc = {'post_plot_spot':'single','post_data_spot':'single','calculations_repo':'single',
		'meta_filter':'many','activate_env':'single'}
	if len(args)>=2: what,args = args[0],args[1:]
	elif len(args)==1: raise Exception('cannot accept a single argument')
	else: what = None
	if what and what not in config_toc: 
		raise Exception('the argument to `make set` must be in %s'%config_toc.keys())
	elif what:
		#---! check the "what" ...
		if config_toc[what] == 'single':
			if len(args)<1: raise Exception('must have an argument to set config %s'%what)
			elif len(args)>1: raise Exception('too many arguments for singleton setting %s: %s'%(what,args))
			add_config(what,value=args[0],many=False)
		else: raise Exception('DEV')
	if kwargs:
		invalids = [i for i in kwargs if i not in config_toc]
		if invalids: raise Exception('invalid keys: %s'%invalids)
		for key,val in kwargs.items():
			if config_toc[key]=='single': add_config(key,value=val,many=False)
			elif config_toc[key]=='many': add_config(key,value=val,many=True)
			else: raise Exception('DEV')
	return

def unset(*args):
	"""
	Remove items from config.
	"""
	config = read_config()
	for arg in args: 
		if arg in config: del config[arg]
		else: print('[WARNING] cannot unset %s because it is absent'%arg)
	write_config(config)

def add_config(*args,**kwargs):
	"""
	Add something to the configuration dictionary located in config_fn (typically config.py).
	The path through the nested config dictionary is specified by *args.
	The many flag ensures that we add items to a non-redundant list.
	The hashed flag makes sure that we add items to a dictionary (this allows for overwriting).
	"""
	value = kwargs.pop('value',None)
	many = kwargs.pop('many',False)
	hashed = kwargs.pop('hashed',False)
	if many and hashed: raise Exception('can only use `many` or `hashed` options to add_config separately')
	if kwargs: raise Exception('unprocessed kwargs %s'%str(kwargs))
	config = read_config()
	if not hashed and not value: raise Exception('specify configuration value')
	setting_change = False
	if not hashed:
		try: exists = delve(config,*args)
		except: exists,setting_change = None,True
	if not many and not hashed:
		if exists == value: return False
		else: setting_change = True
	elif many:
		if not exists: exists = []
		elif type(exists)!=list:
			raise Exception('requested many but the return value is not a list: "%s"'%str(exists))
		#---disallow any redundancy even in a preexisting list
		if len(list(set(exists)))!=len(exists): 
			raise Exception('redundancy in settings list %s'%str(exists))
		if value not in set(exists): 
			exists.append(value)
			setting_change = True
			value = exists
		else: return False
	elif hashed:
		hash_name = args[0]
		if hash_name not in config: config[hash_name] = {}
		for arg in args[1:]:
			#---manually process equals sign since makeface cannot do 
			if '=' not in arg: raise Exception(
				'received argument to add_config with hashed=True but no equals sign: "%s"'%arg)
			key,val = arg.split('=')
			config[hash_name][key] = val
	#---set via delve as long as we are not setting a hash
	if not hashed: delveset(config,*args,value=value)
	write_config(config)
	return setting_change