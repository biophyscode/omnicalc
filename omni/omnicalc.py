#!/usr/bin/env python

"""
OMNICALC WORKSPACE
See structs.py for data structures.
We consolidate all omnicalc conditionals here so they can access the global namer.
Otherwise, parts of the workspace are passed to down to member instances.
"""

import os,sys,re,glob,copy,json,time,tempfile

from config import read_config,bash
from datapack import json_type_fixer
from base.tools import catalog,delve,str_or_list,str_types,status
from base.hypothesis import hypothesis
from datapack import asciitree,delveset,dictsub,dictsub_sparse
from structs import NameManager,Calculation,TrajectoryStructure,NoisyOmnicalcObject
from base.autoplotters import inject_supervised_plot_tools
from base.store import load,store
from makeface import tracebacker

global namer
# the namer is used throughout
namer = None

class WorkSpaceState:
	def __init__(self,kwargs):
		self.compute = kwargs.pop('compute',False)
		self.plot = kwargs.pop('plot',False)
		self.look = kwargs.pop('look',False)
		self.checkup = kwargs.pop('checkup',False)
		if kwargs: raise Exception('unprocessed kwargs %s'%kwargs)
		# the main compute loop is decided here
		if self.compute and not self.plot: self.execution_name = 'compute'
		elif self.plot and not self.compute: self.execution_name = 'plot'
		elif self.look and not self.compute: self.execution_name = 'look'
		elif self.checkup and not self.compute and not self.look: self.execution_name = 'checkup'
		else: 
			msg = ('The WorkSpaceState cannot determine the correct state. '
				'Something has gone horribly wrong or development is incomplete.')
			raise Exception(msg)

class Specifications:
	"""
	Manage a folder of possibly many different specs files (which we call "metadata").
	"""
	def __init__(self,**kwargs):
		"""Catalog available specs files."""
		self.parent_cwd = kwargs.pop('parent_cwd',None)
		if not self.parent_cwd: raise Exception('need a parent_cwd')
		# the cursor comes in from the user
		self.meta_cursor = kwargs.pop('meta_cursor',None)
		# the meta_filter comes in from the config.py and serves as a default
		self.meta_filter = kwargs.pop('meta_filter',None)
		# specs path holds the relative path and a glob to the specs files
		self.specs_path = kwargs.pop('specs_path',())
		# compute the path to the specs folder
		self.cwd = os.path.dirname(os.path.join(self.parent_cwd,*self.specs_path))
		# merge method tells us how to combine specs files
		self.merge_method = kwargs.pop('merge_method','careful')
		if kwargs: raise Exception('unprocessed kwargs %s'%kwargs)
		# catalog available files
		self.avail = glob.glob(os.path.join(self.parent_cwd,*self.specs_path))

	def identify_specs_files(self):
		"""Locate the specs files."""
		if not self.meta_cursor: 
			# the meta_filter from the config is the default
			if self.meta_filter: 
				# treat each item in the meta_filter as a possible glob for files in the specs folder
				# paths for the globs are relative to the specs folder
				self.specs_files = list(set([j for k in [
					glob.glob(os.path.join(self.cwd,os.path.basename(i))) for i in self.meta_filter]
					for j in k]))
			# otherwise use all files
			if not self.meta_filter: self.specs_files = list(self.avail)
		else:
			# the cursor can point to a single file if it comes in from the interface function
			# note that we use a path relative to omnicalc for this option because it allows tab completion
			if os.path.isfile(os.path.join(self.parent_cwd,self.meta_cursor)):
				self.specs_files = [os.path.join(self.parent_cwd,self.meta_cursor)]
			else:
				raise Exception('under development. need to process glob in meta="calcs/specs/*name.yaml"')	

	def interpret(self):
		"""Main loop for interpreting specs."""
		# refresh the list of specs files
		self.identify_specs_files()
		self.specs = MetaData(specs_files=self.specs_files,merge_method=self.merge_method)
		# return the specs object to the workspace
		return self.specs

class MetaData:
	"""
	Supervise the metadata.
	"""
	# define the key categories
	_cats = ['slices','variables', 'meta','collections','calculations','plots','director']
	def __init__(self,**kwargs):
		"""Create metadata from a list of specs files."""
		self.merge_method = kwargs.pop('merge_method','careful')
		self.specs_files = kwargs.pop('specs_files',[])
		if kwargs: raise Exception('unprocessed kwargs %s'%kwargs)
		# empty dictionaries by default
		for key in self._cats: self.__dict__[key] = {}
		# load the metadata right into this class object
		self.__dict__.update(**self.specs_to_metadata(self.specs_files))

	def variables_unpacker(self,specs,variables):
		"""Internal variable substitutions using the "+" syntax."""
		# apply "+"-delimited internal references in the yaml file
		for path,sub in [(i,j[-1]) for i,j in catalog(specs) if type(j)==list 
			and type(j)==str and re.match('^\+',j[-1])]:
			source = delve(variables,*sub.strip('+').split('/'))
			point = delve(specs,*path[:-1])
			point[path[-1]][point[path[-1]].index(sub)] = source
		for path,sub in [(i,j) for i,j in catalog(specs) if type(j)==str and re.match('^\+',j)]:
			path_parsed = sub.strip('+').split('/')
			try: source = delve(variables,*path_parsed)
			except: raise Exception('failed to locate internal reference with path: %s'%path_parsed)
			point = delve(specs,*path[:-1])
			point[path[-1]] = source
		#! need to implement internal references (in variables) here?
		return specs

	def specs_to_metadata(self,specs_files):
		"""Parse the files in the specs_files list and generate the metadata."""
		import yaml
		allspecs = []
		# load all YAML files
		for fn in specs_files:
			with open(fn) as fp: 
				if (self.merge_method != 'override_factory' or 
					not re.match('^meta\.factory\.',os.path.basename(fn))):
					try: allspecs.append(yaml.load(fp.read()))
					except Exception as e:
						raise Exception('failed to parse YAML (are you sure you have no tabs?): %s'%e)
		# previously raised an exception if allspecs was empty but execution should continue
		# merge the YAML dictionaries according to one of several methods
		if self.merge_method=='strict':
			specs = allspecs.pop(0)
			for spec in allspecs:
				for key,val in spec.items():
					if key not in specs: specs[key] = copy.deepcopy(val)
					else: raise Exception('redundant key %s in more than one meta file'%key)
		elif self.merge_method=='careful':
			#! recurse only ONE level down in case e.g. calculations is defined in two places but there
			#! ... are no overlaps, then this will merge the dictionaries at the top level
			specs = allspecs.pop(0)
			for spec in allspecs:
				for topkey,topval in spec.items():
					if topkey not in specs: specs[topkey] = copy.deepcopy(topval)
					else: 
						for key,val in topval.items():
							if key not in specs[topkey]: specs[topkey][key] = val
							else: 
								raise Exception(('performing careful merge in the top-level specs '+
									'dictionary "%s" but there is already a child key "%s". this error '+
									'usually occurs because you have many meta files and you only want '+
									'to use one. try the "meta" keyword argument to specify the path '+
									'to the meta file you want.')%(topkey,key))
		elif self.merge_method=='sequential':
			# load yaml files in the order they are specified in the config.py file with overwrites
			specs = allspecs.pop(0)
			for spec in allspecs:
				specs.update(**spec)
		else: raise Exception('\n[ERROR] unclear meta specs merge method %s'%self.merge_method)
		return self.variables_unpacker(specs=specs,variables=specs.get('variables',{}))

	def get_simulations_in_collection(self,*names):
		"""
		Read a collections list.
		"""
		if any([name not in self.collections for name in names]): 
			raise Exception('cannot find collection %s'%name)
		sns = []
		for name in names: sns.extend(self.collections.get(name,[]))
		return sorted(list(set(sns)))

class ComputeJob:
	def __repr__(self): return str(self.__dict__)
	"""Supervise a single computation."""
	def __init__(self,**kwargs):
		self.calc = kwargs.pop('calc')
		self.slice = kwargs.pop('slice')
		#! calc has top-level information about slice and so does the slice. this should be checked!
		if kwargs: raise Exception('unprocessed kwargs %s'%kwargs)

class Calculations:
	"""
	Supervise the calculations objects.
	"""
	def __init__(self,**kwargs):
		"""..."""
		# receive the specs from the parent
		self.specs = kwargs.pop('specs',None)
		if not self.specs: raise Exception('instance of Calculations resquires a MetaData')
		if kwargs: raise Exception('unprocessed kwargs %s'%kwargs)
		# determine the calculation ordering
		self.calc_order = self.infer_calculation_order(calcs_meta=self.specs.calculations)
		# interpret calculations (previously this was the CalcMeta class)
		self.interpret_calculations(calcs_meta=self.specs.calculations)

	def infer_calculation_order(self,calcs_meta):
		"""
		Catalog the upstream calculation dependencies for all of the calculations and generate a sequence 
		which ensures that each calculation follows its dependencies. Note that we have a 10s timer in place 
		to warn the user that they might have a loop (which would cause infinite recursion). 
		"""
		#---infer the correct order for the calculation keys from their upstream dependencies
		if not calcs_meta: return []
		upstream_catalog = [i for i,j in catalog(calcs_meta) if 'upstream' in i]
		#---if there are no specs required to get the upstream data object the user can either 
		#---...use none/None as a placeholder or use the name as the key as in "upstream: name"
		for uu,uc in enumerate(upstream_catalog):
			if uc[-1]=='upstream': upstream_catalog[uu] = upstream_catalog[uu]+[delve(calcs_meta,*uc)]
		depends = {}
		#---formulate a list of dependencies while accounting for multiple upstream dependencies
		for t in upstream_catalog:
			if t[0] not in depends: depends[t[0]] = []
			depends[t[0]].extend([t[ii+1] for ii,i in enumerate(t) if ii<len(t)-1 and t[ii]=='upstream'])
		calckeys = [i for i in calcs_meta if i not in depends]
		#---if the calculation uses an upstream list instead of dictionary we flatten it
		depends = dict([(k,(v if not all([type(i)==list for i in v]) else 
			[j for k in v for j in k])) for k,v in depends.items()])
		#---check that the calckeys has enough elements 
		list(set(calckeys+[i for j in depends.values() for i in j]))
		#---paranoid security check for infinite loop
		start_time = time.time()
		while any(depends):
			ii,i = depends.popitem()
			if all([j in calckeys for j in i]) and i!=[]: calckeys.append(ii)
			else: depends[ii] = i
			if time.time()>(start_time+10): 
				raise Exception('this is taking too long. '
					'you might have a loop in your graph of dependencies')
		return calckeys

	def unroll_loops(self,details,return_stubs=False):
		"""The jobs list may contain loops. We "unroll" them here."""
		#---this loop interpreter allows for a loop key at any point over specs in list or dict
		#---trim a copy of the specs so all loop keys are terminal
		details_trim = copy.deepcopy(details)
		#---get all paths to a loop
		nonterm_paths = list([tuple(j) for j in set([tuple(i[:i.index('loop')+1]) 
			for i,j in catalog(details_trim) if 'loop' in i[:-1]])])
		#---some loops end in a list instead of a sub-dictionary
		nonterm_paths_list = list([tuple(j) for j in set([tuple(i[:i.index('loop')+1]) 
			for i,j in catalog(details_trim) if i[-1]=='loop'])])
		#---for each non-terminal path we save everything below and replace it with a key
		nonterms = []
		for path in nonterm_paths:
			base = copy.deepcopy(delve(details_trim,*path[:-1]))
			nonterms.append(base['loop'])
			pivot = delve(details_trim,*path[:-1])
			pivot['loop'] = base['loop'].keys()
		#---hypothesize over the reduced specifications dictionary
		sweeps = [{'route':i[:-1],'values':j} for i,j in catalog(details_trim) if 'loop' in i]
		#---! note that you cannot have loops within loops (yet?) but this would be the right place for it
		if sweeps==[]: new_calcs = [copy.deepcopy(details)]
		else: new_calcs = hypothesis(sweeps,default=details_trim)
		new_calcs_stubs = copy.deepcopy(new_calcs)
		#---replace non-terminal loop paths with their downstream dictionaries
		for ii,i in enumerate(nonterms):
			for nc in new_calcs:
				downkey = delve(nc,*nonterm_paths[ii][:-1])
				upkey = nonterm_paths[ii][-2]
				point = delve(nc,*nonterm_paths[ii][:-2])
				point[upkey] = nonterms[ii][downkey]
		#---loops over lists (instead of dictionaries) carry along the entire loop which most be removed
		for ii,i in enumerate(nonterm_paths_list):
			for nc in new_calcs: 
				#---! this section is supposed to excise the redundant "loop" list if it still exists
				#---! however the PPI project had calculation metadata that didn't require it so we just try
				try:
					pivot = delve(nc,*i[:-2]) if len(i)>2 else nc
					val = delve(nc,*i[:-1])[i[-2]]
					pivot[i[-2]] = val
				except: pass
		return new_calcs if not return_stubs else (new_calcs,new_calcs_stubs)

	def infer_group(self,calc,loud=False):
		"""Figure out groups for a downstream calculation."""
		# note that this code was rewritten from a legacy version that used "specs_linked" to interpret calcs
		groups = []
		calc_names = [calc]
		#! the following is under development and will need to be updated after fully-linking calculation 
		#! ... loops but for now we protect against hanging
		from base.timer import time_limit
		try:
			with time_limit(30): 

				#! protection against infinite looping? also consider adding a fully-linked calc graph?
				while calc_names:
					name = calc_names.pop()
					this_calc = self.specs.calculations[name]
					group = this_calc.get('group',None)
					if group!=None: groups.append(group)
					else: 
						ups = this_calc.get('specs',{}).get('upstream')
						if type(ups)==dict: 
							#! NOTE THIS WILL FAIL IF NONREDUNDANT MATCHES
							calc_names.extend(ups.keys())
							#raise Exception('dev. need to get upstream calculations')
						elif type(ups) in str_types: calc_names.append(ups)
						elif type(ups)==list: calc_names.extend(ups)
						else: raise Exception('cannot parse upstream spec %s'%ups)
				groups_u = list(set(groups))
				if len(groups_u)>1: raise Exception('multiple possible groups %s'%groups_u)
				elif len(groups_u)==0: raise Exception('failed to get upstream group for %s'%calc)
				else: return groups_u[0]

		except: raise Exception('taking too long to infer groups')

	def interpret_calculations(self,calcs_meta):
		"""Expand calculations and apply loops."""
		self.toc = {}
		for calcname,calc in calcs_meta.items():
			if calc.get('ignore',False): continue
			# unroll each calculation and store the stubs because they map from the keyword used in the 
			# ... parameter sweeps triggered by "loop" and the full specs
			expanded_calcs,expanded_stubs = self.unroll_loops(calc,return_stubs=True)
			added_calcs = []
			for spec,stub in zip(expanded_calcs,expanded_stubs):
				# we have to be careful with a None in a list from YAML so we fix types here 
				# ... otherwise omnicalc will try to recompute because 'None' fails to match None
				json_type_fixer(spec)
				calc_this = Calculation(name=calcname,specs=spec,stubs=stub)
				added_calcs.append(calc_this)
			self.toc[calcname] = added_calcs

	def prepare_jobs(self,**kwargs):
		"""
		Match calculations with simulations.
		This function prepares all pending calculations unless you ask for a specific one.
		"""
		sns = kwargs.pop('sns',[])
		if kwargs: raise Exception('unprocessed kwargs %s'%kwargs)
		sns_overrides = None if not sns else list(str_or_list(sns))
		# jobs are nameless in a list
		jobs = []
		# loop over calculations
		#! override the calculation order? or should the user only use meta files for this?
		for calckey in self.calc_order:
			# opportunity to ignore calculations without awkward block commenting or restructuring
			# ... in the yaml file
			if calckey in self.specs.calculations and self.specs.calculations[calckey].get('ignore',False):
				status('you have marked "ignore: True" in calculation %s so we are skipping'%calckey,
					tag='note')
				continue
			# loop over calculation jobs in the toc which were expanded by the "loop" keyword
			for calc in self.toc.get(calckey,[]):
				# get slice name
				slice_name = calc.raw['slice_name']
				# loop over simulations
				if not sns_overrides: 
					sns = self.specs.get_simulations_in_collection(
						*str_or_list(calc.raw['collections']))
				# custom simulation names request will whittle the sns list here
				else: sns = list(sns_overrides)
				group_name = calc.raw.get('group',self.infer_group(calc=calckey))
				# loop over simulations
				for sn in sns:
					request_slice = dict(sn=sn,slice_name=slice_name,group=group_name)
					# +++ BUILD slice object
					sliced = Slice(data=request_slice)
					# join the slice and calculation in a job
					jobs.append(ComputeJob(slice=sliced,calc=calc))
		# the caller should save the result
		return jobs

class Slice(TrajectoryStructure):
	"""A class which holds trajectory data of many kinds."""
	pass

class Group(TrajectoryStructure):
	"""A class which holds a group specification for a slice."""
	pass

class PostData(NoisyOmnicalcObject):

	"""
	Represent a calculation result.
	"""

	def __init__(self,**kwargs):
		"""..."""
		self.style = kwargs.pop('style')
		# specs reflect the raw data in a spec file
		self.specs = kwargs.pop('specs',{})
		self.fn,self.dn = kwargs.pop('fn',None),kwargs.pop('dn',None)
		if kwargs: raise Exception('unprocessed kwargs %s'%kwargs)
		self.valid = True
		#! check validity later?
		global namer
		self.namer = namer
		if self.style=='read': 
			if self.specs: raise Exception('cannot parse a spec file if you already sent specs')
			try: self.parse(fn=self.fn,dn=self.dn)
			#! hiding parse errors here because the code is tested on legacy data and namer lookup failures
			#! ... occur if the user does not account for old data in the collections metadata
			#! ... hence we ignore parsing errors. DEVELOPMENT NOTE: turn this off to test/fix the parse
			except: self.valid = False
		elif self.style=='new': self.construct()
		else: raise Exception('invalid style %s'%self.style)

	def construct(self):
		"""
		Build a result object from new data.
		"""
		# build a slice
		slice_raw = self.specs.get('slice',{})
		sn = self.specs.get('meta',{}).get('sn','MISSING SN')
		# +++ BUILD slice object
		self.slice = Slice(data=dict(slice_raw,sn=sn))
		# get the specification version to mimic the parsed spec files
		self.spec_version = self.specs.get('meta',{}).get('spec_version',3)
		# build the calculation
		#! awkward construction below
		self.calc = Calculation(name=self.specs.get('calc',{}).get('name'),
			specs={'specs':self.specs.get('calc',{}).get('specs')})
		dat_fn = re.sub('\.spec$','.dat',self.fn)
		self.files = dict(dat=os.path.join(self.dn,dat_fn),
			spec=os.path.join(self.dn,self.fn))
		for fn in self.files.values():
			if os.path.isfile(fn): raise Exception('cannot preallocate filename %s because it exists'%fn)
		# write an empty result and spec file before any computation to preempt file errors
		try:
			# the compute function will change the style from new to read after rewriting the dat file
			store(obj={'error':'error'},name=os.path.basename(self.files['dat']),
				path=self.dn,attrs={},verbose=False)
		except: raise Exception('failed to prewrite file %s with PostData: %s'%(
			self.files['dat'],self.__dict__))
		try:
			# write a dummy spec file
			with open(self.files['spec'],'w') as fp: 
				fp.write(json.dumps(self.specs))
		except: raise Exception('failed to prewrite file %s with PostData: %s'%(
			self.files['spec'],self.__dict__))
		# hold the basename for entry into the PostDataLibrary
		self.basename = re.sub('\.spec$','',self.fn)

	def parse(self,**kwargs):
		"""
		Read a spec file into a result object.
		This function handles the classification of specs and their transformation into Slice and Calculation
		"""
		fn,dn = kwargs.pop('fn'),kwargs.pop('dn'),
		if kwargs: raise Exception('unprocessed kwargs %s'%kwargs)
		self.files = dict([(k,os.path.join(dn,fn+'.%s'%k)) for k in ['spec','dat']])
		if not os.path.isfile(self.files['spec']):
			raise Exception('cannot find this spec file %s'%self.files['spec'])
		self.specs = json.load(open(self.files['spec']))
		json_type_fixer(self.specs)
		self.namedat = self.namer.interpret_name(fn+'.spec')
		self.spec_version = None
		# +++ COMPARE keys on incoming specs to see which kind of transformations will be needed
		if set(self.specs.keys())=={'calc','meta','slice'}: self.spec_version = 3
		# first we determine the version
		elif all(['slice' in self.specs,'specs' in self.specs,
			'calc' in self.specs and 'calc_name' in self.specs['calc']]): self.spec_version = 2
		# version one spec files just have calculation specs in the top level. this was updated because 
		# ... it is much more robust to save slice information in the spec file in case of naming issues, 
		# ... particularly when using incoming data not generated with omnicalc. however since the calculation
		# ... specs in a version 1 spec file could be anything, we have to check all other versions first
		else: self.spec_version = 1
		# +++ BUILD a slice and calculation object for the incoming post
		if self.spec_version==2:
			# unpack this specification
			calcname = self.specs['calc']['calc_name']
			# only load calculation specs since slices will be compared independently
			calc_specs = {'specs':self.specs['specs']}
			# +++ BUILD a calculation
			self.calc = Calculation(name=calcname,specs=calc_specs)
			# build a slice from the version 2 specification
			slice_raw = self.specs['slice']
			json_type_fixer(slice_raw)
			if slice_raw.get('dat_type',None)=='gmx' and slice_raw.get('slice_type',None)=='standard':
				# get the long simulation name from the table
				sn = self.namer.names_long[slice_raw['short_name']]
				# +++ BUILD slice object
				self.slice = Slice(data=dict(slice_raw,sn=sn))
				#! we could check the postprocessing name here to see if it matches its own slice data
			else: raise Exception('cannot classify this version 2 spec file: %s'%slice_raw)
		elif self.spec_version==1:
			# for version 1 spec files we have to get important information from the filename
			calcname = self.namedat['body']['calc_name']
			calc_specs = {'specs':self.specs}
			# +++ BUILD a calculation
			self.calc = Calculation(name=calcname,specs=calc_specs)
			# constructing slice_raw to mimic the result from a version 2 spec
			slice_raw = dict([(k,self.namedat['body'][k]) for k in 
				['short_name','start','end','skip']])
			group = self.namedat['body'].get('group',None)
			if group!=None: slice_raw['group'] = group
			pbc = self.namedat['body'].get('pbc',None)
			if pbc!=None: slice_raw['pbc'] = pbc
			slice_raw.update(name_style='standard_gmx')
			json_type_fixer(slice_raw)
			# get the long simulation name from the table
			sn = self.namer.names_long[slice_raw['short_name']]
			# we currently match legacy_spec_v2 but we could add a key to match a separate one
			# +++ BUILD slice object
			self.slice = Slice(data=dict(slice_raw,sn=sn))
		elif self.spec_version==3:
			sn = self.specs['meta']['sn']
			slice_raw = self.specs['slice']
			# +++ BUILD slice object
			self.slice = Slice(data=dict(slice_raw,sn=sn))
			# +++ BUILD a calculation
			self.calc = Calculation(name=self.specs['calc']['name'],
				#! only sending specs
				specs={'specs':self.specs['calc']['specs']})
		else: raise Exception('invalid spec version %s'%self.spec_version)
		return

class PostDataLibrary:
	"""
	A library of post-processed data.
	This class mirrors the data in the post_spot (aka post_data_spot). It includes both post-processing 
	dat/spec file pairs, as well as sliced trajectories in gro/xtc or psf/dcd formats.
	"""
	def __init__(self,**kwargs):
		"""Parse a post-processed data directory."""
		global namer
		self.namer = namer
		self.where = kwargs.pop('where')
		strict_sns = kwargs.pop('strict_sns',False)
		# we have a copy of the director in case there are special instructions there
		self.director = kwargs.pop('director',{})
		# handle previous additions to the library which we want to save
		self.previous = kwargs.pop('previous',None)
		if kwargs: raise Exception('unprocessed kwargs %s'%kwargs)
		# generate a "stable" or "corral" of data objects
		self.stable = [os.path.basename(i) for i in glob.glob(os.path.join(self.where,'*'))]
		# populate the toc with post slices not on disk from previous make_slices i.e. the readymade
		if self.previous:
			self.toc = dict([(k,v) for k,v in self.previous.toc.items() 
				if v and v.__dict__.get('style',None)=='readymade'])
		else: self.toc = {}
		nfiles = len(self.stable)
		# master classification loop
		while self.stable: 
			name = self.stable.pop()
			status(name,tag='import',i=nfiles-len(self.stable)-1,looplen=nfiles,bar_width=10,width=65)
			# interpret the name
			namedat = self.namer.interpret_name(name)
			# this puts the slice in limbo. we ignore stray files in post spot
			if not namedat: self.toc[name] = {}
			else:
				# if this is a datspec file we find its pair and read the spec file
				# +++ COMPARE namedat to two possible name_style values from the NameManager
				if namedat['name_style'] in ['standard_datspec','standard_datspec_pbc_group']:
					basename = self.get_twin(name,('dat','spec'))
					#try: 
					this_datspec = PostData(fn=basename,dn=self.where,style='read')
					#! note that there are many ways that PostData can fail but a common one is a failure
					#! ... to do reverse name lookups. basically any failure to read the spec dumps it into
					#! ... limbo and the user is expected to go find it and debug things. this exception
					#! ... was necessary to prevent errors parsing old simulation results that are not in
					#! ... the collections metadata and hence is essnetial 
					#except: self.toc[name] = {}
					if this_datspec.valid: 
						import ipdb;ipdb.set_trace()
						self.toc[basename] = this_datspec
					#! handle invalid datspecs?
					else: self.toc[basename] = {}
				# register datspec files from readymade simulations here
				elif namedat['name_style']=='readymade_datspec':
					basename = self.get_twin(name,('dat','spec'))
					this_datspec = PostData(fn=basename,dn=self.where,style='read')
					if this_datspec.valid: self.toc[basename] = this_datspec
					else: self.toc[basename] = {}
				# if this is a standard gromacs file we check twins and register it as a slice
				elif namedat['name_style']=='standard_gmx':
					# decided to pair gro/xtc because they are always made/used together
					#! should we make this systematic? check for other trajectory types?
					basename = self.get_twin(name,('xtc','gro'))
					# +++ BUILD slice object
					json_type_fixer(namedat)
					#! +++ DEV name the namedat more uniform?
					short_name = namedat['body']['short_name']
					try: sn = self.namer.names_long[short_name]
					except: 
						if strict_sns:
							raise Exception(
							'failed to do the reverse name lookup for a simulation with alias "%s". '%
							short_name+'you can add the full name to any collection and retry.')
						# allow shortnames
						else: sn = short_name
					self.toc[basename] = Slice(data=dict(namedat,sn=sn,
						basename=basename,suffixes=['xtc','gro']))
				#! alternate slice types (e.g. gro/trr) would go here
				else: raise Exception('PostDataLibrary cannot parse post data %s'%namedat)

	def limbo(self): return dict([(key,val) for key,val in self.toc.items() if val=={}])
	def slices(self): return dict([(key,val) for key,val in self.toc.items() 
		if val.__class__.__name__=='Slice'])
	def posts(self): return dict([(key,val) for key,val in self.toc.items() 
		if val.__class__.__name__=='PostData'])

	def search_results(self,job,debug=True):
		"""Search the posts for a particular result."""
		candidates = [key for key,val in self.posts().items() 
			# we find a match by matching the slice and calc, both of which have custom equivalence operators
			if val.slice==job.slice and val.calc==job.calc]
		if len(candidates)!=1:
			# second attempt searches for a unique job with calc specs that are a superset of 
			# ... the calc specs in the request. this maintains backwards compatibility with old data
			# ... which appended extra stuff to the specs before writing. the current method
			# ... eliminates this and relegates extra so-called specs to the dat file itself to 
			# ... avoid interfering with job construction. this dictsub test and better slice matching
			# ... eliminated several (at least five) additional comparisons
			candidates = [key for key,val in self.posts().items() 
				if val.slice==job.slice and val.calc.name==job.calc.name 
				and dictsub(job.calc.specs,val.calc.specs)]
			if len(candidates)==1: return candidates[0]
			# this is the obvious place to debug if you think that omnicalc is trying to rerun completed jobs
			else: return False
		else: return self.toc[candidates[0]]

	def get_twin(self,name,pair):
		"""
		Many slices files have natural twins e.g. dat/spec and gro/xtc.
		This function finds them.
		"""
		this_suffix = re.match('^.+\.(%s)$'%'|'.join(pair),name).group(1)
		basename = re.sub('\.(%s)$'%'|'.join(pair),'',name)
		twin = basename+'.%s'%dict([pair,pair[::-1]])[this_suffix]
		# omnicalc *never* deletes files so we ask the user to clean up on errors
		if twin not in self.stable: raise Exception('cannot find the twin %s of %s ... '%(pair,name)+
			'this is typically due to a past failure to write these files together. '+
			'we recommend deleting the existing file (and fix the upstream error) to continue.')
		else: self.stable.remove(twin)
		return basename

class SliceMeta(TrajectoryStructure):
	"""
	Catalog the slice requests.
	"""
	def __init__(self,raw,**kwargs):
		self.raw = copy.deepcopy(raw)
		self.slice_structures = kwargs.pop('slice_structures',{})
		if self.slice_structures: raise Exception('dev. handle custom structures?')
		if kwargs: raise Exception('unprocessed kwargs %s'%kwargs)
		self.toc = []
		# interpreting slices using the request structures
		self.kind = 'request'
		# loop over simulations and then slices
		for sn,slices_spec in self.raw.items():
			# detect the style of a particular slice
			style = self.classify(slices_spec)
			# once slice specification can yield many slices (cross works for multiple types)
			if style in ['slices_request','readymade_request']:
				slices_raw = self.cross(style=style,data=slices_spec)
			else: raise Exception('unclear how to turn style %s into a SliceMeta'%style)
			# make a formal slice element out of the raw data
			for key,val in slices_raw.items(): 
				# register different types with the toc
				if style=='slices_request' and key[0]=='slices':
					# +++ TRRANFORM a cross output into a slice
					slice_transformed = dict(val,sn=sn,slice_name=key[1])
					# +++ BUILD slice object
					self.toc.append(Slice(slice_transformed))
				elif style=='slices_request' and key[0]=='groups':
					# +++ TRANSFORM a cross output into a group
					group_transformed = dict(selection=val,sn=sn,group_name=key[1])
					self.toc.append(Group(group_transformed))
				elif style=='readymade_request':
					# +++ BUILD slice object
					self.toc.append(Slice(data=dict(slice_name=key[0],sn=sn,**val)))
				else: raise Exception('expecting a group or slice but instead received %s'%{key:val})

	def search(self,candidate):
		"""Search the requested slices."""
		matches = [sl for sl in self.toc if sl==candidate]
		if len(matches)>1: raise Exception('redundant matches for %s'%candidate)
		elif len(matches)==0: 
			import ipdb;ipdb.set_trace()
			return None
		else: return matches[0]

class PlotSpec:
	"""Manage inferences about what to plot."""
	def __init__(self,metadata,plotname,calcs,**kwargs):
		if 'workspace' in kwargs: self.work = kwargs.pop('workspace')
		if kwargs: raise Exception('unprocessed kwargs %s'%kwargs)
		#! no arguments protection here because we are subclassing a dict
		# point to the calculations
		self.calcs = calcs
		# point to the metadata
		self.metadata = metadata
		# plotname cursor. the user can change this manually
		self.plotname = plotname
		# defaults
		self.specs = {}
		self.script = 'plot-%s.py'%self.plotname
		# modifications and inference
		self._get_cursor()
	def get(self,*args): return self.specs.get(*args)
	def _get_cursor(self):
		"""Populate the PlotSpec with the correct objects and infer upstream calculations."""
		# search the plot dictionary for the calculation we need
		if self.plotname in self.metadata.plots:
			self.specs = self.metadata.plots[self.plotname]
			self.request_calc = self.specs.get('calculation',self.specs.get('calculations',None))
			if not self.request_calc: self.request_calc = str(self.plotname)
			self.collections = self.specs.get('collections',[])
			if not self.collections: 
				raise Exception('cannot assemble collections for plot %s. specs are: %s'%(
					self.plotname,self.specs))
			# note the script name
			self.script = self.specs.pop('script',self.script)
			#! this is where we would process any plot-specific specs sometimes written to the plot section
			#! ... for example the distance_ranges_by_metric key in ptdins.yaml
			#! should we remove items from specs as they are processed or otherwise enforce a data structure?
			# use the workspace to fill in calculations that are not specified by dictionaries
			if type(self.request_calc) in str_types+[list]: 
				upstream = self.work.collect_upstream_calculations(self.request_calc)
				# reformulate calculations for the plotter
				self.request_calc = dict([(k,v.specs) for k,v in upstream.items()])
		# if the plotname is not in the plot metadata we check calculations
		elif self.plotname in self.metadata.calculations:
			# note when falling back to calculations we can only have one upstream calculation
			# ... which we put in a dictionary to mimic the calculations key in a plot metadata
			# ... note that this will fail if you refer to an upstream calculation with a loop 
			# ... since you have only supplied a name. to be more specific, you have to add a plots entry
			self.request_calc = {self.plotname:self.metadata.calculations[self.plotname].get('specs',{})}
			self.collections = self.metadata.calculations[self.plotname].get('collections',[])
			if not self.collections: 
				raise Exception('cannot assemble collections for plot %s after falling back to calcs'%(
					self.plotname))
		else: raise Exception('cannot find plotname %s in plots or calculations metadata'%self.plotname)
	def sns(self):
		return list(set(self.metadata.get_simulations_in_collection(
			*str_or_list(self.collections))))

class PlotLoaded(dict):
	def __init__(self,calcnames,sns): 
		#! development code for a new method of loading data for plots and analysis
		self.calcnames,self.sns = calcnames,sns

class WorkSpace:
	"""
	The workspace is the parent class for omnicalc.
	"""
	# hard-coded paths for specs files
	specs_path = 'calcs','specs','*.yaml'
	# version numbering for spec files (previously 1,2 and now leap to 10)
	versioning = {'spec_file':10}
	# number of processors to try
	nprocs = 4
	# note the member (child) classes
	_children = ['specs']

	def __init__(self,**kwargs):
		"""
		Adorn the workspace with various data.
		"""
		# settings and defaults
		self.cwd = kwargs.pop('cwd',os.getcwd())
		self.meta_cursor = kwargs.pop('meta_cursor',None)
		# remove arguments for plotting
		self.plot_args = kwargs.pop('plot_args',())
		self.plot_kwargs = kwargs.pop('plot_kwargs',{})
		self.is_live = kwargs.pop('is_live',False)
		debug_flags = [False,'slices','compute','stale']
		self.debug = kwargs.pop('debug',False)
		if self.debug not in debug_flags: raise Exception('debug argument must be in %s'%debug_flags)
		# determine the state (kwargs is passed by reference so we clear it)
		self.state = WorkSpaceState(kwargs)
		# checkup for interactions with factory
		if kwargs: raise Exception('unprocessed kwargs %s'%kwargs)
		# process the config
		self.read_config()
		# settings which depend on the config
		self.mpl_agg = self.config.get('mpl_agg',False)
		# the specifications folder is read only once per workspace, but specs can be refreshed
		self.specs_folder = Specifications(specs_path=self.specs_path,
			meta_cursor=self.meta_cursor,parent_cwd=self.cwd,
			meta_filter=self.config.get('meta_filter',None))
		# placeholders for children
		for child in self._children: self.__dict__[child] = None
		# the main compute loop for the workspace determines the execution function
		getattr(self,self.state.execution_name)()

	def simulation_names(self):
		"""Return all simulation names from the metadata."""
		return list(set([i for j in self.metadata.collections.values() for i in j]))

	def prepare_namer(self):
		"""Parse metadata and config to check for the short_namer."""
		# users can set a "master" short_namer in the meta dictionary if they have a very complex
		# ... naming scheme i.e. multiple spots with spotnames in the post names
		self.short_namer = self.metadata.meta.get('short_namer',self.metadata.director.get('renamer',None))
		if self.short_namer==None:
			nspots = self.config.get('spots',{})
			# if no "master" short_namer in the meta and multiple spots we force the user to make one
			if len(nspots)>1: raise Exception('create a namer which is compatible with all of your spots '+
				'(%s) and save it to "short_namer" in meta dictionary in the YAML file. '%nspots.keys()+
				'this is an uncommon use-case which lets you use multiple spots without naming collisions.')
			elif len(nspots)==0: self.short_namer = None
			# if you have one spot we infer the namer from the omnicalc config.py
			else: self.short_namer = self.config.get('spots',{}).values()[0]['namer']	
		global namer
		# prepare the namer, used in several places in omnicalc.py
		namer = self.namer = NameManager(short_namer=self.short_namer,spots=self.config.get('spots',{}))
		#! is this necessary?
		# prepare lookup tables for other functions to map short names back to full names
		self.namer.names_short,self.namer.names_long = {},{}
		for sn in self.simulation_names():
			# get the spot name
			spotname = self.namer.get_spotname(sn)
			short_name = self.namer.short_namer(sn,spotname)
			if short_name in self.namer.names_long: raise Exception('short name collision: %s'%short_name)
			# the short names are handy aliases for dealing with unwielding sns in the metadata
			self.namer.names_short[sn] = short_name
			self.namer.names_long[short_name] = sn

	def read_config(self):
		"""Read the config and set paths."""
		self.config = read_config(cwd=self.cwd)
		#! is this deprecated? use a better data structure?
		self.paths = dict([(key,self.config[key]) for key in ['post_plot_spot','post_data_spot']])
		self.paths['spots'] = self.config.get('spots',{})
		# hard-coded paths
		self.postdir = self.paths['post_data_spot']
		self.plotdir = self.paths['post_plot_spot']

	def find_script(self,name,root='calcs'):
		"""Find a generic script somewhere in the calculations folder."""
		#! legacy code needs reviewed
		# find the script with the funtion
		fns = []
		for (dirpath, dirnames, filenames) in os.walk(os.path.join(self.cwd,root)): 
			fns.extend([dirpath+'/'+fn for fn in filenames])
		search = [fn for fn in fns if re.match('^%s(?:\.py)?$'%name,os.path.basename(fn))]
		if len(search)==0: 
			raise Exception('cannot find a script for %s'%name)
		elif len(search)>1: raise Exception('redundant matches: %s'%str(search))
		# manually import the function
		return search[0]

	def sns(self):
		"""Get the list of simulations for a plot."""
		# by the time you use sns you should already have a plotspec generated in plotload
		return self.plotspec.sns()

	def collect_upstream_calculations(self,requests):
		"""
		Given an "upstream" request we assemble a set of calculations for either plotload or compute.
		"""
		# if requests is a single string then we search for a single calculation with that name
		if type(requests) in str_types: targets = [Calculation(name=requests)]
		# if requests is a list then we find a single unique calculation under that name
		elif type(requests)==list: targets = [Calculation(name=name) for name in requests]
		elif type(requests)==dict:
			targets = [Calculation(name=name,specs={'specs':specs}) for name,specs in requests.items()]
		else: raise Exception('cannot convert calculation requests: %s'%requests)
		packaged = {}
		# link each target with an upstream target
		for target in targets:
			# search upstream calculations
			candidates = self.calcs.toc.get(target.name,[])
			"""
			in the following comparison we allow users to name an upstream calculation in two ways. if there 
			is a loop in the upstream calculation you can use the key below the "loop" keyword which only 
			acts as a shortcut. this is made possible by saving the calculation stubs. alternately, you can 
			specify any routes which are a subset of the full calculation, as long as you uniquely identify 
			the upstream calculation. this is why we use dictsub_sparse instead of dictsub, since the former 
			uses catalog to unroll everything. this mechanism should handle any upstream naming task except 
			for the one where you want to run a downstream calculation on all upstream calculations in a 
			loop, which will require special handling. no explication is necessary if there is only one
			upstream calculation
			"""
			# +++ COMPARE caclulation requests to full calculation specs to identify a unique match
			culled = [cd for cd in candidates if target.name==cd.name and 
				all([(key,val) in cd.specs.items() 
				or dictsub_sparse({key:val},cd.stubs.get('specs',{})) 
				for key,val in target.specs.items()])]
			#! no protection against repeated calculation names
			if len(culled)==1: packaged[target.name] = culled[0]
			else: 
				raise Exception('failed to uniquely identify a requested calculation in the upstream '+
					'calculations: %s'%target.__dict__)
		return packaged

	def connect_upstream_calculation(self,sn,request):
		"""
		Search calculation request against jobs.
		"""
		# +++ COMPARE a barebones calculation with the jobs list
		candidates = [jc for jc in self.jobs 
			if jc.calc==request and jc.slice.data['sn']==sn]
		if len(candidates)==1: return candidates[0]
		else: 
			raise Exception(('cannot find an upstream job which computes '
				'"%s" for simulation "%s" with specs: %s')%(
				request.name,request.slice.data['sn'],request.__dict__))

	def plotload(self,plotname,**kwargs):
		"""
		Export completed calculations to a plot environment.
		"""
		whittle_calc = kwargs.pop('whittle_calc',None)
		if kwargs: raise Exception('unprocessed kwargs %s'%kwargs)
		# plotspec is first instantiated by Workspace.plot and it is important to replace it after
		# ... running plotload so that items like Workspace.sns() still return the correct result
		plotname_cursor = str(self.plotname)
		# make sure you have the right plotname
		if plotname!=plotname_cursor:
			#! ensure changes to the plotname do not change the simulations we require
			#! ... note that this might not be necessary since we might want to change simulations
			#! ... for example when plotting membrane heights with and without protein hulls from
			# the plotname is needed by other functions namely self.sns
			self.plotname = plotname
			# update the plotspec if the plotname changes. this also updates the upstream pointers
			self.plotspec = PlotSpec(metadata=self.metadata,plotname=self.plotname,
				calcs=self.calcs,workspace=self)
		if whittle_calc: raise Exception('dev')
		# we always run the compute loop to make sure calculations are complete but also to get the jobs
		# note that compute runs may be redundant if we call plotload more than once but it avoids repeats
		#! skip can be dangerous
		self.compute(automatic=False)
		sns = self.plotspec.sns()
		if not sns: raise Exception('cannot get simulations for plot %s'%self.plotname)
		# many plot functions use a deprecated work.plots[plotname]['specs'] call to get details about 
		# ... the plots from the metadata. to support this feature we add the cursor from the plotspec
		# ... object to the workspace. these calls must happen after plotload is called, but we prefer
		# ... to wait until the plotspec is made. if this is a problem the plotspec should be made during
		# ... plot_supervised. note  that we only supply specs via plotname but it will fall back to calcs
		if not hasattr(self,'plots'): self.plots = {}
		# save plot details at the top level in the workspace because some scripts expect it
		if self.plotname not in self.plots: 
			# carefully reformulate the original plots as the scripts would expect to see them
			#! make a more flexible data structure for the future?
			self.plots.update(**dict([(key,{'specs':val}) 
				for key,val in copy.deepcopy(self.plotspec.request_calc).items()]))
			#!!! DEVELOPMENT NOTE. how should we populate work.plots
			#! currrently set to override with specs in the plot section
			if self.plotname in self.metadata.plots and 'specs' in self.metadata.plots[self.plotname]:
				#! conservative
				if self.plotname not in self.plots: self.plots[self.plotname] = {}
				#! conservative
				if 'specs' not in self.plots[self.plotname]: self.plots[self.plotname]['specs'] = {}
				self.plots[self.plotname]['specs'].update(**self.metadata.plots[self.plotname]['specs'])
		# see if the requested calculation can be unrolled
		unrolled = self.calcs.unroll_loops(self.plotspec.request_calc)
		if len(unrolled)>1:
			upstream_requests = {}
			# extra index if we are inside a loop. users will have to get the one they want in the plot code
			for ii,item in enumerate(unrolled):
				ups = self.collect_upstream_calculations(item)
				for key,val in ups.items(): upstream_requests[(key,ii)] = val
		# convert upstream calculation requests into proper calculations
		else: upstream_requests = self.collect_upstream_calculations(self.plotspec.request_calc)
		# convert upstream calculation requests into proper calculations
		calcnames = upstream_requests.keys()
		# package the data for export to the plot environment in a custom dictionary
		#! this will be useful if we add a different plotload return format later
		bundle = dict([(k,PlotLoaded(calcnames=calcnames,sns=sns)) for k in ['data','calc']])
		for cnum,(calcname,request) in enumerate(upstream_requests.items()):
			for snum,sn in enumerate(sns):
				status('caching upstream data from calculation %s, simulation %s'%(calcname,sn),
					i=snum+len(sns)*cnum,looplen=len(sns)*len(upstream_requests),tag='load')
				if calcname not in bundle['data']: bundle['data'][calcname] = {}
				job = self.connect_upstream_calculation(request=request,sn=sn)
				fn = job.result.files['dat']
				data = load(os.path.basename(fn),cwd=os.path.dirname(fn))
				if data.get('error',False)=='error':
					raise Exception('calculation failed. clear the dat/spec files corresponding '
						'to %s and recompute'%fn)
				bundle['data'][calcname][sn] = {'data':data}
				#! adding trajectory data here. assumes that the slice is the same for all calculations
				try: 
					# find the trajectory slice 
					keys = [key for key,val in self.post.slices().items() if val==job.slice]	
					if len(keys)>1 or len(keys)==0: raise Exception
					else: slice_upstream = self.post.toc[keys[0]]	
					if 'extras' not in bundle['calc']: bundle['calc']['extras'] = {}
					bundle['calc']['extras'][sn] = {
						# assumes a single postdir and passes along the basename and suffixes
						'slice_path':slice_upstream.data['basename'],
						'suffixes':slice_upstream.data.get('suffixes',[])}
					# send along times for frame information
					bundle['calc']['extras'][sn].update(**dict([(k,slice_upstream.data['body'][k]) 
						for k in ['start','end','skip']]))
				except: pass
			bundle['calc'][calcname] = {'calcs':{'specs':job.calc.specs}}
		# data are returned according to a versioning system
		plotload_version = self.plotspec.get('plotload_version',
			self.metadata.director.get('plotload_output_style',1))
		# original plot codes expect a data,calc pair from this function
		if plotload_version==1: 
			# remove calculation name from the nested dictionary if only one
			if len(calcnames)==1 and len(bundle['data'])==1:
				bundle['data'] = bundle['data'].values()[0]
			if len(bundle['calc'])==1: bundle['calc'] = bundle['calc'].values()[0]
			# note that calc may also include extras to specify the trajectory (see plot-actinlink_videos.py)
			outgoing = bundle['data'],bundle['calc']
		#! alternate plotload returns can be managed here with a global plotload_version from the director
		#! ... or a plot-specific plotload_version set in the plot metadata
		else: raise Exception('invalid plotload_version: %s'%plotload_version)
		# since we may run plotload several times we always return to the original plot specificaiton
		self.plotspec = PlotSpec(metadata=self.metadata,plotname=plotname_cursor,
			calcs=self.calcs,workspace=self)
		return outgoing

	def plot_legacy(self,plotname,meta=None,autoplot=False,look=False):
		"""Legacy plotting mode."""
		# when looking from the factory we need a plotspec
		if look: 
			self.plotspec = PlotSpec(metadata=self.metadata,plotname=plotname,
				calcs=self.calcs,workspace=self)
		plots = self.metadata.plots
		#---we hard-code the plot script naming convention here
		plot_script_fn = self.plotspec.script
		script_name = self.find_script(plot_script_fn)
		if not os.path.isfile(script_name):
			raise Exception('cannot find script %s'%script_name)
		if plotname in plots: plotspec = plots[plotname]
		else: 
			#---previously required a plots entry however the following code makes a default plot
			#---...object for this plotname, assuming it is the same as the calculation
			try:
				plotspec = {'calculation':plotname,
					'collections':self.metadata.calculations[plotname]['collections'],
					'slices':self.metadata.calculations[plotname]['slice_name']}
				print('[NOTE] there is no %s entry in plots so we are using calculations'%plotname)
			except Exception as e: 
				raise Exception('you should add %s to plots '%plotname+'since we could not '
					'formulate a default plot for that calculation.')			
		# the look method is used by the factory to get details for this plot
		if look: return {'script_name':script_name}
		header_script = 'omni/base/header.py'
		meta_out = ' '.join(meta) if type(meta)==list else ('null' if not meta else meta)
		# call the header script with a flag for legacy execution
		bash('./%s %s %s%s'%(header_script,script_name,plotname,'' if autoplot else ' NO_AUTOPLOT'))

	def plot_prepare(self):
		"""Rename internal variables for brevity in plot scripts."""
		# note that plotload also attaches work.plot
		# we use the existence of vars to check if we already prepared
		if not hasattr(self,'vars'):
			self.meta = self.metadata.meta
			self.vars = self.metadata.variables

	def plot_supervised(self,plotname,**kwargs):
		"""
		Supervised plot execution.
		This largely mimics omni/base/header.py.
		"""	
		#---plotspecs include args/kwargs coming in from the command line so users can make choices there
		plotspecs = kwargs.pop('plotspecs',{})
		if kwargs: raise Exception('unprocessed kwargs %s'%kwargs)
		#---prepare the environment for the plot
		out = dict(work=self,plotname=plotname)
		outgoing_locals = dict()
		#---per header.py we have to add to the path
		for i in ['omni','calcs']:
			if i not in sys.path: sys.path.insert(0,i)
		#---inject supervised plot functions into the outgoing environment
		inject_supervised_plot_tools(out)
		#---execute the plot script
		plot_script_fn = self.plotspec.script
		script = self.find_script(plot_script_fn)
		with open(script) as fp: code = fp.read()
		#---handle builtins before executing. we pass all keys in out through builtins for other modules
		import builtins
		builtins._plotrun_specials = out.keys()
		for key in out: builtins.__dict__[key] = out[key]
		#---tell ths script if we are live. you can run supervised or legacy from the notebook
		out.update(is_live=self.is_live)
		#---supervised execution is noninteractive and runs plots based on plotspecs
		#---...hence we run the script as per the replot() function in omni/base/header.py
		#---...see the header function for more detail
		#---run the script once with name not main (hence no global execution allowed)
		local_env = {'__name__':'__main__'}
		exec(compile(code,script,'exec'),out,local_env)
		#---we need to get plot script globals back into globals in the autoplot so we 
		#---...pass locals into out which goes to autoplot then to globals if the mode is supervised
		out.update(**local_env)
		plotrun = out['plotrun']
		#---the loader is required for this method
		plotrun.loader()
		#---intervene to interpret command-line arguments
		kwargs_plot = plotspecs.pop('kwargs',{})
		#---command line arguments that follow the plot script name must name the functions
		plotrun.routine = plotspecs.pop('args',None)
		#---change empty tuple to None because that means "everything" in plotrun
		if plotrun.routine==(): plotrun.routine = None
		if plotspecs: raise Exception('unprocessed plotspecs %s'%plotspecs)
		if kwargs_plot: raise Exception('unprocessed plotting kwargs %s'%kwargs_plot)
		self.plot_prepare()
		plotrun.autoplot(out=out)

	def parse_sources(self): 
		"""Parse the source data in preparation for making slices."""
		from base.parser import ParsedRawData
		self.source = ParsedRawData(spots=self.config.get('spots',{}))

	def make_slices(self,jobs_require_slices):
		"""
		Make slices
		"""
		#! make sure all slices are handled somewhere in these lists?
		if any([job.slice.style!='readymade' for job in jobs_require_slices]):
			self.make_slices_automacs([job for job in jobs_require_slices if job.slice.style=='slice_request_named'])
		# this happens everytime we run with readymade because the slices need to be found on disk
		else: self.make_slices_readymade(
			[job for job in make_slices_automacs if job.slice.style=='readymade'])

	def make_slices_readymade(self,jobs):
		"""
		Identify readymade slices on disk. Note that this task is placed here as a mimic for 
		make_slices_automacs because it would be wasteful to catalog the parts of a readymade slice via
		PostDataLibrary. Note that this function could also be replaced with an alternate checker so that
		other data could be imported with a custom function later.
		"""
		registered_slices = []
		for job in jobs:
			# check slices on disk
			for key in ['structure','trajectory']:
				for fn in str_or_list(job.slice.data[key]):
					fn_abs = os.path.join(self.postdir,fn)
					if not os.path.isfile(fn_abs):
						raise Exception('readymade file not found %s'%fn_abs)
			# add to the postdata with a unique name for this slice
			if job.slice.__dict__ not in registered_slices:
				registered_slices.append(copy.deepcopy(job.slice.__dict__))
				# readymade slices must use the slice name in the post processing file name so choose wisely
				name = '%s.readymade.%s'%(
					self.namer.short_namer(job.slice.data['sn']),job.slice.data['slice_name'])
				if name in self.post.toc: raise Exception('already found %s'%name)
				self.post.toc[name] = job.slice
			# we already added this slice to post
			else: pass

	def make_slices_automacs(self,jobs):
		"""
		Make GROMACS trajectory slices from source data.
		"""
		if not jobs: return
		# only parse simulation data if we need to make slices
		from base.parser import ParsedRawData
		self.parse_sources()
		from base.slicer import make_slice_gromacs,edrcheck
		# cache important parts of the slice
		slices_new,slices_new_base = [],[]
		# prepare simple slices (this prevents redundancy)
		for jnum,job in enumerate(jobs):
			# prepare slice information for the slicer
			#! note that the make_slice_gromacs function is legacy and hence requires careful inputs
			#! ... otherwise naming errors
			slice_spec = {'spec':dict([(k,job.slice.data[k]) 
				for k in ['sn','start','end','skip','group','pbc']])}
			sn = slice_spec['spec']['sn']
			slice_spec['sn'] = sn
			slice_spec['group_name'] = job.slice.data['group']
			if slice_spec not in slices_new_base: slices_new_base.append(slice_spec)
		# loop over basic slices and populate with other requirements
		for slice_spec in slices_new_base:
			sn = slice_spec['spec']['sn']
			slice_spec['sequence'] = self.source.get_timeseries(sn)
			slice_spec['sn_prefixed'] = self.namer.alias(sn)
			spotname = self.namer.get_spotname(sn)
			#! hard-coded trajectory format below
			slice_spec['tpr_keyfinder'] = self.source.keyfinder((spotname,'tpr'))
			slice_spec['traj_keyfinder'] = self.source.keyfinder((spotname,'xtc'))
			slice_spec['gro_keyfinder'] = self.source.keyfinder((spotname,'structure'))
			# +++COMPARE find the right group to get the selection for the slicer
			#! note that we do not need to actually use the NDX file on disk, but this might be a good idea
			candidates = [val for val in self.slices.toc 
				if val.style=='gromacs_group' and val.data['sn']==sn 
				and val.data['group_name']==slice_spec['group_name']]
			if len(candidates)!=1: 
				raise Exception('failed to find group %s for simulation %s in the SliceMeta')
			else: slice_spec['group_selection'] = candidates[0].data['selection']
			try: slice_spec['last_structure'] = self.source.get_last(sn,'structure')
			except: raise Exception(('failed to get a starting structure for %s. this is a common '
				'problem when you restart a simulation and do not have any structure files matching the '
				'structure regex. we recommend converting a stray CPT file to match the structure regex for '
				'this spot in order to continue.')%sn)
			slices_new.append(slice_spec)
		if self.debug=='slices':
			status('welcome to the debugger. check out self.queue_computes and jobs_require_slices '
				'to see pending calculations. see slices_new for pending slices. exit and rerun to continue.',
				tag='debug')
			import ipdb
			ipdb.set_trace()
			sys.exit(1)
		# make the slices
		for snum,slice_spec in enumerate(slices_new):
			asciitree(dict(slice=slice_spec))
			status('making slice %d/%d'%(snum+1,len(slices_new)),tag='slice')
			make_slice_gromacs(postdir=self.postdir,**slice_spec)

	def prepare_compute(self,jobs):
		"""
		Prepare and simulate the calculations to prevent name collisions.
		"""
		#!? check for missing twins?
		# assemble a list of result file names in order to generate new ones
		post_fns = [os.path.basename(v.files['spec']) for k,v in self.post.posts().items()]
		# track spec files for each basename
		spec_toc = {}
		for fn in post_fns:
			basename = re.match('^(.+)\.n\d+\.spec$',fn).group(1)
			if basename not in spec_toc: spec_toc[basename] = []
			spec_toc[basename].append(fn)
		for k,v in spec_toc.items(): spec_toc[k] = sorted(v)
		# acquire slices
		jobs_require_slices = []
		for job in jobs:
			# find the trajectory slice
			keys = [key for key,val in self.post.slices().items() if val==job.slice]
			if len(keys)!=1: jobs_require_slices.append(job)
			else: job.slice_upstream = self.post.toc[keys[0]]
		# if we need to make slices we will return
		if jobs_require_slices: 
			self.make_slices(jobs_require_slices)
			# after loading slices we have to re-parse the postdata
			#! this will overwrite things!
			self.post = PostDataLibrary(where=self.postdir,director=self.metadata.director,
				previous=self.post)
			# match the upstream slices here
			for job in jobs:
				# find the trajectory slice
				keys = [key for key,val in self.post.slices().items() if val==job.slice]
				if len(keys)!=1: raise Exception('failed to make and identify slices. development error')
				else: job.slice_upstream = self.post.toc[keys[0]]
		# acquire upstream data without loading it yet
		for job in jobs:
			# convert the upstream calculation requests into proper calculations
			upstream_requests = self.collect_upstream_calculations(job.calc.specs.get('upstream',{}))
			# connect requests to jobs
			upstream = dict([(name,self.connect_upstream_calculation(
				request=request,sn=job.slice.data['sn'])) for name,request in upstream_requests.items()])
			# tack the upstream jobs on for later
			job.upstream = upstream
		# loop over jobs and register filenames
		self.pending = []
		for job in jobs:
			#! style will be updated from standard/datspec later on
			#! intervene here to name i.e. "undulations" with the group and pbc since that is unnecessary
			# by default we do not pass PBC or group name to the datspec anymore; this was standard in 
			# ... version 1,2 spec files. set name_style: standard_datspec_pbc_group in the calculation
			# ... if you want to force the full name. otherwise all post-data omits this. all downstream
			# ... calculations also omit the PBC and group by default anyway.
			name_style = job.calc.name_style
			# if readymade we set the name_style here
			if not name_style and job.slice.style=='readymade': name_style_this = 'readymade_datspec'
			elif not name_style: name_style_this = 'standard_datspec'
			else: name_style_this = name_style
			basename = self.namer.basename(job=job,name_style=name_style_this)
			# prepare suffixes for new dat files
			keys = [int(re.match('^%s\.n(\d+)\.spec$'%basename,key).group(1)) 
				for key in spec_toc.get(basename,[]) if re.match('^%s'%basename,key)]
			if keys and not sorted([int(i) for i in keys])==list(range(len(keys))):
				raise Exception('non sequential keys found for data objects prefixed with %s'%basename)
			# make the new filename
			fn = '%s.n%d.spec'%(basename,len(keys))
			if basename not in spec_toc: spec_toc[basename] = []
			# save the filename so new files give unique spec file names
			spec_toc[basename].append(fn)
			#! +++ BUILD slice object this slice went nowhere: new_slice = Slice(data=job.slice.data)
			# designing the new version three (v3) spec format here
			sn = job.slice.data['sn']
			spec_new = dict(
				meta={'spec_version':3,'sn':job.slice.data['sn']},
				slice=job.slice.data,calc={'name':job.calc.name,'specs':job.calc.specs})
			# create the new result file
			status('preparing data file for new calculation %s'%fn,tag='status')
			job.result = PostData(fn=fn,dn=self.postdir,style='new',specs=spec_new)
			if job.result.basename in self.post.toc:
				raise Exception('created a new PostData object but %s exists'%job.result.basename)
			# register the result with the postdat library so we can simulate the compute loop
			else: self.post.toc[job.result.basename] = job.result
			self.pending.append(job)
		# after make new postdata objects we want to check for new computations
		self.check_compute(debug=True)

	def prelim(self):
		"""Preliminary materials for compute and plot."""
		# get the specs from the specs_folder object
		self.metadata = self.specs_folder.interpret()
		# prepare the namer
		self.prepare_namer()
		# prepare a calculations object
		self.calcs = Calculations(specs=self.metadata)

	def check_compute(self,debug=False):
		"""
		See if we need to run any calculations.
		"""
		# save completed jobs as results
		self.results,self.queue_computes = [],[]
		# join jobs with results
		for job in self.jobs:
			# jobs have slices in alternate/calculation_request form and they must be fleshed out
			# +++ COMPARE job slice to slices in the metadata
			slice_match = self.slices.search(job.slice)
			if not slice_match: 
				raise Exception('failed to find the requested slice in the metadata: %s'%job.slice.__dict__)
			# replace the job slice with the metadata slice if we found a match
			else: job.slice = slice_match
			# search for a result
			job.result = self.post.search_results(job=job)
			if not job.result: 
				# the debug mode throws an exception to indicate that the preemptive compute failed
				if debug: raise Exception('failed to simulate compute loop for job %s'%job)
				self.queue_computes.append(job)
			else: self.results.append(job)

	def attach_standard_tools(self,mod):
		"""
		Send standard tools to the calculation functions.
		"""
		#---! under development
		#---MASTER LISTING OF STANDARD TOOLS
		#---MDAnalysis
		import MDAnalysis
		mod.MDAnalysis = MDAnalysis
		#---looping tools
		from base.tools import status,framelooper
		from base.store import alternate_module,uniquify
		mod.alternate_module = alternate_module
		mod.uniquify = uniquify
		mod.status = status
		mod.framelooper = framelooper
		#---parallel processing
		from joblib import Parallel,delayed
		from joblib.pool import has_shareable_memory
		mod.Parallel = Parallel
		mod.delayed = delayed
		mod.has_shareable_memory = has_shareable_memory

	def get_calculation_function(self,calcname):
		"""
		Search the calcs subdirectory for a calculation function.
		Note that this lookup function enforces the naming rule which is hard-coded: namely, that all 
		calculations must be in a function in a script which each use the calculation name.
		"""
		script_name = self.find_script(calcname)
		#---! needs python3
		sys.path.insert(0,os.path.dirname(script_name))
		mod = __import__(re.sub('\.py$','',os.path.basename(script_name)),locals(),globals())
		#---attach standard tools
		self.attach_standard_tools(mod)
		if not hasattr(mod,calcname): raise Exception(('performing calculation "%s" and we found '+
			'%s but it does not contain a function named %s')%(calcname,script_name,calcname))
		return getattr(mod,calcname)

	def run_compute(self):
		"""
		Run jobs and save to preemptive dat files.
		"""
		for jnum,job in enumerate(self.pending):
			job.result.style = 'computing'
			#! carefully print the result otherwise it double prints slice, calc
			asciitree(dict(calculation={
				'result':dict([(k,job.result.__dict__[k]) for k in ['files','spec_version','specs']]),
				'slice_request':job.slice.__dict__,'calc':job.calc.__dict__,
				'slice':job.slice_upstream.__dict__,}))
			status('running calculation %d/%d'%(jnum+1,len(self.pending)),tag='compute')
			function = self.get_calculation_function(job.calc.name)
			# prepare the arguments
			# +++ BUILD arguments structure as the compute function would expect
			#! it would be nice to formalize this or make it less gromacs-specific? perhaps by using a mode?
			outgoing = dict(workspace=self,sn=job.slice_upstream.data['sn'],calc=dict(specs=job.calc.specs))
			# unpack files depending on the type of slice
			#! note that this is where you would run a custom importer for non-MD data
			if job.slice.style=='readymade':
				struct_file = os.path.join(self.postdir,job.slice_upstream.data['structure'])
				traj_file = [os.path.join(self.postdir,i) for i in str_or_list(
					job.slice_upstream.data['trajectory'])]
			elif job.slice.style=='slice_request_named':
				#! post directory is hard-coded here
				struct_file = os.path.join(self.postdir,'%s.%s'%(job.slice_upstream.data['basename'],'gro'))
				traj_file = os.path.join(self.postdir,'%s.%s'%(job.slice_upstream.data['basename'],'xtc'))
			else: raise Exception('dev')
			# load upstream data files at the last moment
			upstream = {}
			for unum,(key,val) in enumerate(job.upstream.items()):
				status('caching upstream data from calculation %s'%key,
					i=unum,looplen=len(job.upstream),tag='load')
				fn = val.result.files['dat']
				data = load(os.path.basename(fn),cwd=os.path.dirname(fn))
				upstream[key] = data
			outgoing.update(upstream=upstream)
			# we run plot_prepare because some calculation scripts require it
			self.plot_prepare()
			# redundant keywords are structure/grofile and trajectory/trajfile
			outgoing = dict(grofile=struct_file,trajfile=traj_file,
				structure=struct_file,trajectory=traj_file,**outgoing)
			result,attrs = function(**outgoing)
			# we remove the blank dat file before continuing. one of very few delete commands
			if job.result.style!='computing': raise Exception('attmpting to compute a stale job')
			os.remove(job.result.files['dat'])
			store(obj=result,name=os.path.basename(job.result.files['dat']),
				path=os.path.dirname(job.result.files['dat']),attrs=attrs,verbose=True)
			# register the result as equivalent to one that had been read from disk
			job.result.style = 'read'
			del upstream

	def fail_report(self):
		"""Tell the user which files were incomplete."""
		#! this function is deprecated in favor of standard error reporting with warnings in blank dat files
		asciitree(dict(incomplete_jobs=dict([('job %d: %s'%(jj+1,j.style),j.files) 
			for jj,j in enumerate([i.result for i in self.pending 
				if i.result.__dict__['style'] in ['computing','new']])])))
		status('compute loop failure means there many be preallocated files listed above. '
			'omnicalc never deletes files so you should delete them to continue',tag='error')

	###
	### EXECUTION MODES
	###

	def checkup(self,**kwargs):
		"""Prepare the workspace for calls from the factory i.e. a "checkup"."""
		self.prelim()

	def look(self,**kwargs):
		"""Inspect something."""
		if kwargs: raise Exception('unprocessed kwargs %s'%kwargs)
		look_specs = self.state.look
		args = look_specs.pop('args')
		kwargs = look_specs.pop('kwargs')
		if look_specs: raise Exception('unprocessed kwargs %s'%look_specs)
		if set(kwargs.keys())<={'write_json'} and args==('times',): 
			import collections
			self.parse_sources()
			view_od = [(k,v) for spotname in [k for k in self.source.spots 
				if k[1]=='edr'] for k,v in self.source.toc[spotname].items()]
			# for survey purposes we tryto access all simulations
			sns = []
			for spotname in [k for k in self.source.spots if k[1]=='edr']:
				for key in self.source.toc[spotname].keys(): sns.append(key)
			# populate the toc with timeseries
			for ss,sn in enumerate(sns): 
				status('reading EDR to collect times: %s'%sn,
					i=ss,looplen=len(sns),tag='read',width=65)
				self.source.get_timeseries(sn)
			# print for the user
			if not kwargs.get('write_json',False): 
				view = dict([(name,[
					(('%s%s-%s'%k+' part%s:'%i).ljust(25,'.')+'%s%s'%(
					str(round(j['start'],2)  if j['start'] else '???').rjust(12,'.'),
					str(round(j['stop'],2) if j['stop'] else '???').rjust(12,'.'))) 
					for k,v in obj.items() for i,j in v.items()]) for name,obj in view_od])
				asciitree(dict(simulations=collections.OrderedDict([(k,view[k]) 
					for k in sorted(view.keys())])))
			# systematic view
			else: 
				view = [(sn,[('%s%s-%s'%stepname,[(i,j) for i,j in step.items()]) 
					for stepname,step in details.items()]) for sn,details in view_od]
				print('time_table = %s'%json.dumps(view))
		elif not kwargs.keys() and args==():
			#! ipdb is good enough for now, but it was nice to get dropped into interactive mode
			status('welcome to the workspace. take a look around!',tag='debug')
			self.prelim()
			work = self
			import ipdb;ipdb.set_trace()
		else: raise Exception('invalid call to look with args %s and kwargs %s'%(args,kwargs))

	def compute(self,**kwargs):
		"""
		Run a calculation. This is the main loop, and precedes the plot loop.
		"""
		automatic = kwargs.pop('automatic',True)
		skip = kwargs.pop('skip',False)
		if kwargs: raise Exception('unprocessed kwargs %s'%kwargs)
		self.prelim()
		# prepare jobs from these calculations
		self.jobs = self.calcs.prepare_jobs()
		# parse the post-processing data only once (o/w multiple imports on plotload which calls compute)
		if not hasattr(self,'post'):
			self.post = PostDataLibrary(where=self.postdir,director=self.metadata.director)
		# formalize the slice requests
		# +++ BUILD slicemeta object (only uses the OmnicalcDataStructure for cross)
		self.slices = SliceMeta(raw=self.metadata.slices,
			slice_structures=self.metadata.director.get('slice_structures',{}))
		self.check_compute()
		# if we have incomplete jobs then run them
		if self.queue_computes and not automatic:
			raise Exception('there are pending compute jobs. try `make compute` before plotting')
		elif self.queue_computes and skip:
			status('skipping pending computatations in case you are plotting swiftly',tag='warning')
			return
		elif self.queue_computes and automatic: 
			asciitree(dict(pending_jobs=dict([('pending job %d'%(jj+1),
				dict(calculation=j.calc.__dict__,slice=j.slice.__dict__)) 
				for jj,j in enumerate(self.queue_computes)])))
			status('there are %d incomplete jobs (see above)'%len(
				self.queue_computes),tag='status')
			asciitree(dict(pending_calculations=list(set([i.calc.name for i in self.queue_computes]))))
			# halt the process and drop into the debugger in order to check out the jobs
			if self.debug=='compute':
				status('welcome to the debugger. check out self.queue_computes to see pending calculations. '
					'exit and rerun to continue.',
					tag='debug')
				import ipdb
				ipdb.set_trace()
				sys.exit(1)
			elif self.debug=='stale':
				raise Exception('cannot check for stale jobs when there are pending calculations')
			else: 
				# removed interrupt and error handling in favor of warnings in blank dat files caught by plot
				self.prepare_compute(self.queue_computes)
				self.run_compute()
		# no compute jobs
		else: pass

	def plot(self):
		"""
		Analyze calculations or make plots. This is meant to follow the compute loop.
		"""
		#! alias or alternate naming for when "plot-" becomes tiresome?
		if len(self.plot_args)==0: raise Exception('you must send the plotname as an argument')
		elif len(self.plot_args)==1: plotname,args = self.plot_args[0],()
		else: plotname,args = self.plot_args[0],self.plot_args[1:]
		self.prelim()
		# the plotname is needed by other functions namely self.sns
		self.plotname = plotname
		# once we have a plotname we can generate a plotspec
		self.plotspec = PlotSpec(metadata=self.metadata,plotname=self.plotname,
			calcs=self.calcs,workspace=self)
		# the following code actually runs the plot via legacy or auto plot
		# ... however it is only necessary to prepare the workspace if we are already in the header
		if not self.plot_kwargs.get('header_caller',False):
			do_autoplot = self.plotspec.get('autoplot',self.metadata.director.get('autoplot',False))
			# check plotspec for the autoplot flag otherwise get the default from director
			# trailing arguments to plot indicate functions we want to run and trigger a non-interactive mode
			if do_autoplot and len(args)>0:
				self.plot_supervised(plotname=plotname,plotspecs=dict(args=args,kwargs=self.plot_kwargs))
			# call a separate function for plotting "legacy" plot scripts and interactive autoplot
			# note that this conditional allows you to run autoplot with the header into interactive mode
			else: self.plot_legacy(self.plotname,autoplot=do_autoplot)

###
### INTERFACE FUNCTIONS
### note that these are imported by omni/cli.py and exposed to makeface

def compute(debug=False,debug_slices=False,meta=None,back=False):
	if back: 
		from base.tools import backrun
		backrun(command='make compute',log='log-compute')
	else:
		status('generating workspace for compute',tag='status')
		work = WorkSpace(compute=True,meta_cursor=meta,debug=debug)
def plot(*args,**kwargs):
	status('generating workspace for plot',tag='status')
	work = WorkSpace(plot=True,plot_args=args,plot_kwargs=kwargs)
def go(*args,**kwargs): plot(*args,**kwargs)
def look(*args,**kwargs):
	work = WorkSpace(look=dict(args=args,kwargs=kwargs))
def clear_stale(meta=None):
	"""Check for stale jobs."""
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
		asciitree({'cleaned files':sorted(stales)})
		status('you can continue with `make compute` now. '
			'we cleaned up stale dat files and corresponding spec files listed above')
