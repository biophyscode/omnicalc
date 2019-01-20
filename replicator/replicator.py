#!/usr/bin/env python

import re

__all__ = ['repl','pipeline','test_clean','docker_clean']

import ortho
from .formula import *

#! no: from ortho import hook_merge
#!   no: from ortho.hooks import hook_merge
#!   instead use import ortho then later ortho.hook_merge
#! turning this off for now because of import issues. needs revisited
if False:
	# +++ allow user to hook in other handlers here
	import ortho
	ortho.hook_merge(hook='replicator',namespace=globals())

### READERS

@requires_python('yaml')
def replicator_read_yaml(source,name=None,args=None,kwargs=None):
	"""
	Read a replicator instruction and send it to the Guide for execution.
	"""
	import yaml
	with open(source) as fp: 
		# we load into a MultiDict to forbid spaces (replaced with 
		#   underscores) in top-level dictionary.
		instruct = MultiDict(base=yaml.load(fp.read()),
			underscores=True,strict=True)
	# special handling
	reference = {}
	# previously used custom taxonomy but here we infer it via inspect
	taxonomy_rs = ReplicatorSpecial(inspect=True).taxonomy
	for key in taxonomy_rs:
		if key in instruct: 
			reference[key] = ReplicatorSpecial(name=key,
				**{key:instruct.pop(key)})
	# leftovers from special handling must be tests
	if not name and len(instruct)>1: 
		raise Exception(
			('found multiple keys in source %s. you must choose '
				'one with the name argument: %s')%(source,instruct.keys()))
	elif not name and len(instruct)==1: 
		test_name = instruct.keys()[0]
		print('status','found one instruction in source %s: %s'%(
			source,test_name))
	elif name:test_name = name
	else: raise Exception('source %s is empty'%source)
	if test_name not in instruct: 
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

def repl(*args,**kwargs):
	"""
	Run a test.
	Disambiguates incoming test format and sends it to the right reader.
	Requires explicit kwargs from the command line.
	"""
	# allow args to be handled by the interface key for easier CLI
	if args:
		if kwargs: raise Exception('cannot use kwargs with args here')
		if 'replicator_recipes' not in ortho.conf:
			raise Exception('calling repl with args requires the '
				'"replicator_recipes" variable be set in the config')
		source = ortho.conf['replicator_recipes']
		if not os.path.isfile(source):
			raise Exception('cannot find %s'%source)
		this_test = replicator_read_yaml(
			name=args[0],args=args[1:],kwargs=kwargs,source=source)
	# specific format uses only kwargs
	elif (set(kwargs.keys())<={'source','name'} 
		and 'source' in kwargs 
		and re.match(r'^(.*)\.(yaml|yml)$',kwargs['source'])):
		this_test = replicator_read_yaml(**kwargs)
	else: raise Exception('unclear request')
	# run the replicator
	rg = ReplicatorGuide(name=this_test['name'],
		meta=this_test['meta'],**this_test['detail'])

# alias for the replicator
pipeline = repl
