#!/usr/bin/env python

#! once we added replicator/__init__.py to expose pipeline in 
#!   ortho/__init__.py we had to change e.g. from ortho import read_config
#!   hence internal ortho imports need to have the path to the submodule
from ortho.requires import requires_python,requires_python_check
from ortho.dictionary import MultiDict
from ortho.bash import bash_basic,bash
from ortho.handler import Handler
from ortho.config import read_config

import re,tempfile,os,copy
import datetime as dt
import uuid

class SpotLocal:
	"""Make a local directory."""
	#! needs cleanup option
	def __init__(self,site=None,persist=False):
		"""Make a local folder."""
		abspath = lambda x: os.path.abspath(os.path.expanduser(x))
		if persist and site==None: 
			raise Exception('the persist flag is meaningless without site')
		if not site:
			ts = dt.datetime.now().strftime('%Y%m%d%H%M') 
			code = uuid.uuid4().hex[:2].upper()
			self.path = 'repl_%s.%s'%(ts,code)
			#! alternate location for making one-off sites?
			os.mkdir('./%s'%self.path)
			self.abspath = abspath(site)
		else: 
			self.path = site
			self.abspath = abspath(site)
			if persist and os.path.isdir(self.abspath): 
				print('status','found persistent spot: %s'%self.abspath)
			else: os.mkdir(self.abspath)

class Runner:
	"""Execute a file with Bash."""
	def __init__(self,**kwargs):
		self.script = kwargs.pop('script')
		self.cwd = kwargs.pop('cwd')
		self.log = kwargs.pop('log','log')
		self.fn = kwargs.pop('fn')
		self.path_full = self.script_fn = os.path.join(self.cwd,self.fn)
		self.subs = dict(path=self.script_fn,fn=self.fn)
		self.cmd = kwargs.pop('cmd','bash %(fn)s')%self.subs
		if kwargs: raise Exception('unprocessed kwargs: %s'%kwargs)
		self.run()
	def run(self):
		if self.script:
			with open(self.path_full,'w') as fp: fp.write(self.script)
		print('status running command %s'%self.cmd)
		bash_basic(self.cmd,cwd=self.cwd,log=self.log)

class ReplicatorSpecial(Handler):

	def dockerfiles(self,dockerfiles):
		# unorthodox: this function overwrites itself with its key
		#   note that this is a neat way to hook something: we expect the 
		#   ReplicatorSpecial to get a portion of a YAML file and just add
		#   it to the class right here, however the way this is called from
		#   replicator_read_yaml means we can easily process it here
		self.dockerfiles = dockerfiles

	#! UNDER CONSTRUCTION def interface(self,interface):
	#! import ipdb;ipdb.set_trace()

class DockerFileChunk(Handler):

	def substitutes(self,text,subs):
		self.text = text%subs

class DockerFileMaker(Handler):

	def sequence(self,sequence,addendum=None):
		"""Assemble a sequence of dockerfiles."""
		index = MultiDict(base=self.meta['dockerfiles'].dockerfiles,
			underscores=True)
		self.dockerfile = '\n'.join([self.refine(index[i]) for i in sequence])
		if addendum: 
			for i in addendum: self.dockerfile += "\n%s"%i

	def refine(self,this):
		"""Refine the Dockerfiles."""
		if isinstance(this,dict): 
			return DockerFileChunk(**this).text
		else: return this

### SUPERVISOR

class ReplicatorGuide(Handler):

	"""
	def __init__(self,*args,**kwargs):
		print('status subclassed in the ReplicatorGuide 1')
		super().__init__(*args,**kwargs)
	"""

	def bash(self,call):
		"""Run a bash call."""
		bash(call)

	def simple(self,script,site=None,persist=False):
		"""
		Execute a script.
		"""
		spot = SpotLocal(site=site,persist=persist)
		run = Runner(script=script,fn='script.sh',cwd=spot.path)

	def simple_docker(self,script,dockerfile,tag,site=None,persist=False):
		"""
		Run a script in a docker container.
		"""
		dfm = DockerFileMaker(meta=self.meta,**dockerfile)
		spot = SpotLocal(site=site,persist=persist)
		with open(os.path.join(spot.path,'Dockerfile'),'w') as fp: 
			fp.write(dfm.dockerfile)
		script_build = '\n'.join([
			'docker build -t %s .'%tag,])
		# write the script before building the docker
		with open(os.path.join(spot.path,'script.sh'),'w') as fp: 
			fp.write(script)
		run = Runner(script=script_build,fn='script_build.sh',
			log='log-build',cwd=spot.path,local_bash=False)
		run = Runner(script=None,
			#! note that this name needs to match the COPY command in Docker
			cwd=spot.path,fn='script.sh',log='log-run',
			cmd='docker run %s'%tag)#+' %(path)s')

	def docker_compose(self,compose,dockerfile,site,
		command,script=None,persist=True,rebuild=True,prelim=None):
		"""
		Prepare a docker-compose folder and run a command in the docker.
		"""
		#! note that the Handler class uses introspection to detect args,kwargs
		#!   to classify incoming calls and this is broken by the decorator
		requires_python_check('yaml')
		import yaml
		if prelim:
			#! might not need to remove at sign
			result = read_config(hook=prelim)[re.sub('@','',prelim)]
			#! do something with result? right now this is just a do hook
		dfm = DockerFileMaker(meta=self.meta,**dockerfile)
		spot = SpotLocal(site=site,persist=persist)
		with open(os.path.join(spot.path,'Dockerfile'),'w') as fp: 
			fp.write(dfm.dockerfile)
		with open(os.path.join(spot.path,'docker-compose.yml'),'w') as fp:
			fp.write(yaml.dump(compose))
		# script is optional. it only runs if you run a docker command below
		#   which also depends on it via an entrypoint
		if script:
			with open(os.path.join(spot.path,'script.sh'),'w') as fp: 
				fp.write(script)
		if rebuild: bash_basic('docker-compose build',cwd=spot.path)
		# no need to log this since it manipulates a presumably 
		#   persistent set of files
		print('status running command %s'%command)
		#! note that we could use docker_compose just for building if we 
		#!   made the rebuild True when no script or command. this might be
		#!   somewhat more elegant? this could be done with another method
		#!   for clarity
		bash_basic(command,cwd=spot.path)

	def via(self,via,overrides=None):
		"""
		Run a replicate with a modification. Extremely useful for DRY.
		"""
		if not overrides: overrides = {}
		if via not in self.meta['complete']: 
			raise Exception('reference to replicate %s is missing'%via)
		fname = self.classify(*self.meta['complete'][via].keys())
		outgoing = copy.deepcopy(self.meta['complete'][via])
		outgoing.update(**overrides)
		getattr(self,fname)(**outgoing)

	def singularity_via_vagrant(self,vagrant_site):
		"""
		Run something in Singularity in Vagrant (on macos).
		"""
		spot = SpotLocal(site=vagrant_site,persist=True)
		print(spot.abspath)
		raise Exception('yay')