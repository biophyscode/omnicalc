#!/usr/bin/env python

import re,glob

__all__ = ['repl','pipeline','test_clean','docker_clean','get_jupyter_token']
#! the all above does not filter Handler and Site out of the makefile interface

import ortho
from ortho import str_types
from .formula import get_jupyter_token
from .formula import *

#! no: from ortho import hook_merge
#!   no: from ortho.hooks import hook_merge
#!   instead use import ortho then later ortho.hook_merge
#! turning this off for now because of import issues. needs revisited
#if False:

# +++ allow user to hook in other handlers here
#import ortho
#ortho.hooks.hook_merge(hook='replicator',namespace=globals())

### READERS

@requires_python('yaml')
def replicator_read_yaml(sources,name=None,args=None,kwargs=None):
	"""
	Read a replicator instruction and send it to the Guide for execution.
	"""
	if args or kwargs: raise Exception((
		'not sure what to do with these: args=%s, kwargs=%s'%(
			str(args),kwargs)))
	import yaml
	incoming,special = {},{}
	for source in sources:
		with open(source) as fp: 
			this = yaml.load(fp.read())
			this_reduced = ReplicatorSpecial(**this)
			if this_reduced.specials:
				special[source] = this_reduced.specials
			# replicator special passes regular items via kwargs
			for key,val in this_reduced.kwargs.items():
				if key in incoming: 
					raise Exception(
						'duplicate keys in replicator YAML files (%s): %s'%(
							source,key))
				incoming[key] = val
	# we load into a MultiDict to forbid spaces (replaced with 
	#   underscores) in top-level dictionary.
	instruct = MultiDict(base=incoming,
		underscores=True,strict=True)
	"""
	special handling for dockerfiles
	note that the ReplicatorSpecial picks off the dockerfiles key in the recipes
	these are merged here with name collision protection
	however this is basically a hardcoded merge
	development goal is to put this into another generalized handler
	"""
	dockerfiles_merged = {}
	for key,val in special.items():
		for layer_name,chunk in val.get('dockerfiles',{}).items():
			if layer_name in dockerfiles_merged:
				raise Exception('dockerfile name collision: %s'%layer_name)
			dockerfiles_merged[layer_name] = chunk
	# save the resulting dockerfiles
	reference = {'dockerfiles':dockerfiles_merged}
	# end of special dockerfile handling
	# leftovers from special handling must be tests
	if not name and len(instruct)>1: 
		raise Exception(
			('found multiple keys in source %s. you must choose '
				'one with the name argument: %s')%(source,instruct.keys()))
	elif not name and len(instruct)==1: 
		test_name = instruct.keys()[0]
		print('status','found one instruction in source %s: %s'%(
			source,test_name))
	elif name: test_name = name
	else: raise Exception('source %s is empty'%source)
	if test_name not in instruct: 
		import ipdb;ipdb.set_trace()
		raise Exception('cannot find replicate %s'%test_name)
	test_detail = instruct[test_name]
	reference['complete'] = instruct
	return dict(name=test_name,detail=test_detail,meta=reference)

### INTERFACE

def test_clean():
	#! needs more care?
	os.system(' rm -rf repl_*')

def docker_clean():
	"""Remove unused docker images and completed processes."""
	os.system('docker rm $(docker ps -a -q)')
	os.system('docker rmi $(docker images -f "dangling=true" -q)')

def many_files(spec):
	"""
	Specify one file, a list, or a glob or a list of globs.
	"""
	if isinstance(spec,list):
		missing = [i for i in spec 
		if not os.path.isfile(i) and not glob.glob(i)]
		if any(missing): 
			raise Exception('missing files from %s: %s'%(spec,missing))
		return list(set([i for j in 
			[[k] if os.path.isfile(k) else glob.glob(k) for k in spec] 
			for i in j]))
	elif isinstance(spec,str_types) and os.path.isfile(spec):
		return [spec]
	elif isinstance(spec,str_types):
		fns = glob.glob(spec)
		if any(fns): return fns
		else: raise Exception('cannot find files: %s'%spec)
	else: raise Exception('cannot intepret file selection: %s'%spec)

def repl(*args,**kwargs):
	"""
	Run a test.
	Disambiguates incoming test format and sends it to the right reader.
	Requires explicit kwargs from the command line.
	"""
	"""
	# instructions for extending ReplicatorGuide
	# see hooks/replicator_alt.py
	# EXTEND the ReplicatorGuide class
	# make set_hook replicator="\"{'s':'hooks/replicator_alt.py',
	#   'f':'update_replicator_guide'}\""
	# import the class, subclass it, and export that inside a function
	from ortho.replicator.formula import ReplicatorGuide
	class ReplicatorGuide(ReplicatorGuide):
		def new_handler(self,param):
			# do things and return to ReplicatorGuide(param=1).solve
			return dict(result=123)
	def update_replicator_guide(): 
		# this hook function adds the updated guide
		return dict(ReplicatorGuide=ReplicatorGuide)
	"""
	# +++ allow user to hook in other handlers here
	import ortho
	ortho.hooks.hook_merge(hook='replicator',namespace=globals())
	# some special flags from the terminal
	rebuild = False
	if 'rebuild' in args:
		print('status received the `rebuild` flag from terminal')
		args = tuple([i for i in args if i!='rebuild'])
		rebuild = True
	cname = None
	if 'cname' in kwargs: 
		#! this feature does not work with docker exec for some reason
		cname = kwargs.pop('cname')
	# allow args to be handled by the interface key for easier CLI
	if args:
		if kwargs: raise Exception('cannot use kwargs with args here')
		if 'replicator_recipes' not in ortho.conf:
			raise Exception('calling repl with args requires the '
				'"replicator_recipes" variable be set in the config')
		sources = many_files(ortho.conf['replicator_recipes'])
		this_test = replicator_read_yaml(
			name=args[0],args=args[1:],kwargs=kwargs,sources=sources)
	# specific format uses only kwargs
	elif (set(kwargs.keys())<={'source','name'} 
		and 'source' in kwargs 
		and re.match(r'^(.*)\.(yaml|yml)$',kwargs['source'])):
		kwargs['sources'] = [kwargs.pop('source')]
		this_test = replicator_read_yaml(**kwargs)
	else: raise Exception('unclear request')
	# handle special arguments
	if rebuild and 'rebuild' not in this_test['detail']:
		raise Exception(('cannot pass rebuild to ReplicatorGuide '
			'because this experiment lacks it: %s'%this_test['name']))
	elif rebuild: this_test['detail']['rebuild'] = True
	# container name is passed on and will cause an error in the
	#   ReplicatorGuide if it cannot be handled, however the docker_compose 
	#   recipe dynamically adds it to the compose file for this container
	if cname: this_test['detail']['cname'] = cname
	# run the replicator
	rg = ReplicatorGuide(name=this_test['name'],
		meta=this_test['meta'],**this_test['detail'])

# alias for the replicator
pipeline = repl
