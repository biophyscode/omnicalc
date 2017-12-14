#!/usr/bin/env python

"""
OMNICALC WORKSPACE
"""

import os,sys,re,glob,copy,json,time,tempfile
from config import read_config
from base.tools import catalog,delve,str_or_list,str_types,status
from base.hypothesis import hypothesis
from datapack import asciitree,delveset
from structs import NamingConvention,TrajectoryStructure

import yaml

# hold the workspace in globals
global work,namer
work,namer = None,None

###
### Utilities
###

def json_type_fixer(series):
	"""Cast integer strings as integers, recursively. We also fix 'None'."""
	for k,v in series.items():
		if type(v) == dict: json_type_fixer(v)
		elif type(v)in str_types and v.isdigit(): series[k] = int(v)
		elif type(v)in str_types and v=='None': series[k] = None

###
### OMNICALC CLASSES
### all classes are children of WorkSpace

class WorkSpaceState:
	def __init__(self,kwargs):
		self.compute = kwargs.pop('compute',False)
		self.plot = kwargs.pop('plot',False)
		if kwargs: raise Exception('unprocessed kwargs %s'%kwargs)
		# the main compute loop is decided here
		if self.compute and not self.plot: self.execution_name = 'compute'
		elif self.plot and not self.compute: self.execution_name = 'plot'
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
		allspecs = []
		# load all YAML files
		for fn in specs_files:
			with open(fn) as fp: 
				if (self.merge_method != 'override_factory' or 
					not re.match('^meta\.factory\.',os.path.basename(fn))):
					try: allspecs.append(yaml.load(fp.read()))
					except Exception as e:
						raise Exception('failed to parse YAML (are you sure you have no tabs?): %s'%e)
		if not allspecs: raise Exception('dev')
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

class Calculation:
	"""
	A calculation, including settings.
	"""
	def __init__(self,**kwargs):
		"""Construct a calculation object."""
		self.name = kwargs.pop('name')
		# the calculation specs includes slice/group information
		# the settings or specs which uniquely describe the calculation are in a subdictionary
		self.calc_specs = kwargs.pop('calc_specs')
		self.specs = self.calc_specs.pop('specs',{})
		#! save for later?
		self.specs_raw = copy.deepcopy(self.specs)
		#! remove simulation name and/or group from specs
		# we save the stubs because they provide an alternate name for elements in a loop
		self.stubs = kwargs.pop('stubs',None)
		if kwargs: raise Exception('unprocessed kwargs %s'%kwargs)
		# check for completeness
		if self.calc_specs.keys()>={'collections','slice_name'}:
			raise Exception('this calculation (%s) is missing some keys: %s'%(self.name,self.calc_specs))

class ComputeJob:
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
		import ipdb;ipdb.set_trace()
		return 'DEV'
		#! dev. needs checked for relevance
		import ipdb;ipdb.set_trace()
		if loud: status('inferring group for %s'%calc,tag='bookkeeping')
		if type(calc)==dict:
			#! this method is non-recursive
			groups,pending_groupsearch = [],list(calc['specs']['upstream'].keys())
			while pending_groupsearch:
				key = pending_groupsearch.pop()
				if key not in self.calcs: 
					raise Exception(
						'cannot find calculation %s in the metadata hence we cannot infer the group'%key)
				if 'group' in self.calcs[key]: groups.append(self.calcs[key]['group'])
				elif 'upstream' in self.calcs[key]['specs']:
					pending_groupsearch.extend(self.calcs[key]['specs']['upstream'].keys())
				else: raise Exception('no group and no upstream')
			groups_consensus = list(set(groups))
			if len(groups_consensus)!=1: 
				raise Exception('cannot achieve upstream group consensus: %s'%groups_consensus)
			group = groups_consensus[0]
			return group
		# use the fully-linked calculations to figure out the group.
		else:
			groups_consensus = []
			check_calcs = [v for k,v in calc.specs_linked['specs'].items() 
				if type(k)==tuple and k[0]=='up']
			while check_calcs:
				this_calc = check_calcs.pop()
				ups = [v for k,v in this_calc.specs_linked['specs'].items() 
					if type(k)==tuple and k[0]=='up']
				if 'group' in this_calc.specs_linked: groups_consensus.append(this_calc.specs['group'])
				#! the following uses the fully-linked calculation naming scheme which is clumsy
				check_calcs.extend([v for k,v in 
					this_calc.specs_linked['specs'].items() if type(k)==tuple and k[0]=='up'])
			groups_consensus = list(set(groups_consensus))
			if len(groups_consensus)!=1: 
				raise Exception('cannot achieve upstream group consensus: %s'%groups_consensus)
			return groups_consensus[0]

	def interpret_calculations(self,calcs_meta):
		"""Expand calculations and apply loops."""
		self.toc = {}
		for calcname,calc in calcs_meta.items():
			# unroll each calculation and store the stubs because they map from the keyword used in the 
			# ... parameter sweeps triggered by "loop" and the full specs
			expanded_calcs,expanded_stubs = self.unroll_loops(calc,return_stubs=True)
			self.toc[calcname] = [Calculation(name=calcname,calc_specs=spec,stubs=stub)
				for spec,stub in zip(expanded_calcs,expanded_stubs)]

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
				slice_name = calc.calc_specs['slice_name']
				# loop over simulations
				if not sns_overrides: 
					sns = self.specs.get_simulations_in_collection(
						*str_or_list(calc.calc_specs['collections']))
				# custom simulation names request will whittle the sns list here
				else: sns = list(sns_overrides)
				# group is not required and will be filled in later if missing
				group_name = calc.calc_specs.get('group',None)
				# loop over simulations
				for sn in sns:
					request_slice = dict(sn=sn,slice_name=slice_name,group=group_name)
					# join the slice and calculation in a job
					jobs.append(ComputeJob(slice=request_slice,calc=calc))
		# the caller should save the result
		return jobs

if False:
	###
	### Slice and Data classes
	### ...???

	class Slice:

		"""
		Parent class which holds several different representations of what we call a "slice".
		"""

		def __init__(self,**kwargs):
			"""
			This class basically just holds the data and returns it in a "flat" form for some use cases.
			"""
			self.__dict__.update(**kwargs)
			json_type_fixer(self.__dict__)

		def flat(self):
			"""
			Reduce a slice into a more natural form.
			"""
			#---we include dat_type with slice_type
			for key in ['slice_type','dat_type']:
				this_type = self.namedat.get(key,None) if hasattr(self,'namedat') else None
				if not this_type: this_type = self.__dict__.get(key)
				if not this_type: raise Exception('indeterminate data type')
				#---! clumsy
				if key=='slice_type': slice_type = str(this_type)
				elif key=='dat_type': dat_type = str(this_type)
			me = dict(slice_type=slice_type,dat_type=dat_type,**self.namedat.get('body',{}))
			me.update(**self.__dict__.get('spec',{}))
			if 'short_name' not in me and 'short_name' in self.__dict__:
				me['short_name'] = self.short_name
			#---trick to fix integers
			json_type_fixer(me)
			return me

class Slice(TrajectoryStructure):
	"""A class which holds trajectory data of many kinds."""
	def __init__(self,**kwargs):
		"""..."""
		import ipdb;ipdb.set_trace()

class PostData():
	"""
	Parent class for identifying a piece of post-processed data.
	??? DatSpec instances can be picked up from a file on disk but also generated before running a job, which 
	??? ... will inevitably write the post-processed data anyway.
	"""
	def __init__(self,fn=None,dn=None,job=None):
		"""
		We construct the DatSpec to mirror a completed result on disk OR a job we would like to run.
		"""
		global namer
		self.namer = namer
		self.valid = True
		# check the data here
		if fn and job: raise Exception('cannot send specs if you send a file')
		elif fn and not dn: raise Exception('send the post directory')
		elif fn and dn: self.from_file(fn,dn)
		else: raise Exception('dev')

	def from_file(self,fn,dn):
		"""
		DatSpec objects can be imported from file (including from previous versions of omnicalc)
		or they can be constructed in anticipation of finishing a calculation job and *making* the file 
		(see from_job below). This function handles most backwards compatibility.
		"""
		self.files = dict([(k,os.path.join(dn,fn+'.%s'%k)) for k in 'spec','dat'])
		if not os.path.isfile(self.files['spec']):
			raise Exception('cannot find this spec file %s'%self.files['spec'])
		self.specs = json.load(open(self.files['spec']))
		json_type_fixer(self.specs)
		self.namedat = self.namer.interpret_name(fn+'.spec')
		self.spec_version = None
		# first we determine the version
		if all(['slice' in self.specs,'specs' in self.specs,
			'calc' in self.specs and 'calc_name' in self.specs['calc']]): self.spec_version = 2
		if self.spec_version!=2: 
			raise Exception('dev. need backwards compatbility to version 1 specs')
		# construct a calculation from a version 2 specification
		if self.spec_version==2:
			# unpack this specification
			calcname = self.specs['calc']['calc_name']
			# only load calculation specs since slices will be compared independently
			calc_specs = {'specs':self.specs['specs']}
			# build a calculation
			self.calc = Calculation(name=calcname,calc_specs=calc_specs)
			# build a slice
			#! using standard datspec here. will this change later?
			basename = self.namer.parser[('standard','datspec')]['d2n']%self.namedat['body']
			self.slice = 'MAKE SLICE'#$Slice(name=basename,namedat=self.namedat)
		else: raise Exception('invalid spec version %s'%self.spec_version)
		return

		#########################

		#---the namer is permissive so we catch errors here
		if not self.namedat: raise Exception('name interpreter failure')
		#---intervene here to handle backwards compatibility for VERSION 1 jobs
		spec_version_2 = all(['slice' in self.specs,'specs' in self.specs,
			'calc' in self.specs and 'calc_name' in self.specs['calc']])
		#---! note that version 2 also has short_name in the top level but this might be removed?
		#---any old spec files that do not satisfy VERSION 2 must be version 1. we fix them here.
		if not spec_version_2:
			#---version 1 spec files had calculation specs directly at the top level
			self.specs = {'specs':copy.deepcopy(self.specs)}
			#---the slice dictionary contains the information in the name, minus nnum, and calc_name
			self.specs['slice'] = dict([(key,val) for key,val in self.namedat['body'].items()
				if key not in ['nnum','calc_name']])
			#---all version 1 spec files came from what we call standard/gmx slices
			self.specs['slice'].update(slice_type='standard',dat_type='gmx')
			#---version 2 has a simple calc dictionary
			self.specs['calc'] = {'calc_name':self.namedat['body']['calc_name']}
		#---we fill in groups and pbc for certain slice types
		if (self.specs['slice']['slice_type']=='standard' and 
			self.specs['slice']['dat_type']=='gmx' and 
			any([i not in self.specs['slice'] for i in ['group','pbc']])):
			if 'group' not in self.specs['slice']: 
				self.specs['slice']['group'] = self.work.infer_group(self.specs)
			if 'pbc' not in self.specs['slice']:
				self.specs['slice']['pbc'] = self.work.infer_pbc(self.specs)
		#---! good opportunity to match a calc on disk with a calc in calc_meta
		try:
			self.specs['calc'] = self.work.calc_meta.find_calculation(
				name=self.specs['calc']['calc_name'],specs=self.specs['specs'],
				slice=self.specs['slice'],loud=False)
		#---sometimes we hide calculations that are already complete because we are adding data
		except Exception as e: 
			#---create a dummy calcspec if we cannot find the calculation in the meta
			#---! note that we may wish to populate this more. this error was found when trying to find a
			#---! ...match later, and the find_match function was trying to look in the CalcSpec for a 
			#---! ...calculation which had been removed from the meta
			#---we supply a name because find_match will be looking for one
			#---note that after adding multiplexing for the large-memory ENTH calculations here, we added
			#---...the lax flag to prevent this because it is very difficult to debut
			#---! the next user who wants to remove items from the meta and still run the compute with those
			#---! ...objects on disk can connect the lax keyword to the metadata to hide an old calculation if desired
			if lax: self.specs['calc'] = Calculation(name=None,specs={},stub=[])
			else: raise

	def from_job(self,job):
		"""
		Create datspec object from a job in preparation for running the calculation.
		"""
		self.namedat = {}
		#---retain the pointer to the job
		self.job = job
		#---construct specs for a new job
		#---the following defines the VERSION 2 output which adds more, standardized data to the spec file
		#---...in hopes that this will one day allow you to use arbitrary filenames for datspec objects. 
		#---...note that we discard everything from the calculation except the calc_name which is the sole
		#---...entry in the calc subdict and the specs, which are at the top level. we discard e.g. the 
		#---...calculation collection and uptype because it's either obvious or has no bearing
		self.specs = {'specs':self.job.calc.specs['specs'],
			'calc':{'calc_name':self.job.calc.name},'slice':job.slice.flat()}

	def basename(self):
		"""
		Name this slice.
		"""
		#---! hard-coded VERSION 2 here because this is only called for new spec files
		slice_type = self.job.slice.flat()['slice_type']
		#---standard slice type gets the standard naming
		parser_key = {'standard':('standard','datspec'),
			'readymade_namd':('raw','datspec'),
			'readymade_gmx':('raw','datspec'),
			'readymade_meso_v1':('raw','datspec')}.get(slice_type,None)
		if not parser_key: raise Exception('unclear parser key')
		basename = self.parser[parser_key]['d2n']%dict(
			calc_name=self.job.calc.name,**self.job.slice.flat())
		#---note that the basename does not have the nN number yet (nnum)
		return basename

class PostDataLibrary:
	"""
	A library of post-processed data.
	This class mirrors the data in the post_spot (aka post_data_spot). It includes both post-processing 
	dat/spec file pairs, as well as sliced trajectories in gro/xtc or psf/dcd formats.
	"""
	def __init__(self,**kwargs):
		"""Parse a post-processed data directory."""
		global namer
		self.where = kwargs.pop('where')
		if kwargs: raise Exception('unprocessed kwargs %s'%kwargs)
		# generate a "stable" or "corral" of data objects
		self.stable = [os.path.basename(i) for i in glob.glob(os.path.join(self.where,'*'))]
		self.toc = {}
		nfiles = len(self.stable)
		# master classification loop
		while self.stable: 
			name = self.stable.pop()
			status(name,tag='import',i=nfiles-len(self.stable)-1,looplen=nfiles,bar_width=10,width=65)
			# interpret the name
			namedat = namer.interpret_name(name)
			# this puts the slice in limbo. we ignore stray files in post spot
			if not namedat: self.toc[name] = {}
			else:
				# if this is a datspec file we find its pair and read the spec file
				if namedat['dat_type']=='datspec':
					basename = self.get_twin(name,('dat','spec'))
					this_datspec = PostData(fn=basename,dn=self.where)
					if this_datspec.valid: self.toc[basename] = this_datspec
					#! handle invalid datspecs?
					else: self.toc[basename] = {}
				# everything else must be a slice
				#! alternate slice types (e.g. gro/trr) would go here
				else: 
					# decided to pair gro/xtc because they are always made/used together
					basename = self.get_twin(name,('xtc','gro'))
					self.toc[basename] = 'MAKE SLICE'#Slice(name=basename,namedat=namedat)

	def limbo(self): return dict([(key,val) for key,val in self.toc.items() if val=={}])
	def slices(self): return dict([(key,val) for key,val in self.toc.items() 
		if val.__class__.__name__=='Slice'])
	def posts(self): return dict([(key,val) for key,val in self.toc.items() 
		if val.__class__.__name__=='DatSpec'])

	def search_results(self,job):
		"""Search the posts for a particular result."""
		candidates = self.posts()
		key,val = candidates.items()[0]
		import ipdb;ipdb.set_trace()

	#!!!!
	def search_slices(self,**kwargs):
		"""Find a specific slice."""
		slices,results = self.slices(),[]
		#---flatten kwargs
		target = copy.deepcopy(kwargs)
		target.update(**target.pop('spec',{}))
		flats = dict([(slice_key,val.flat()) for slice_key,val in slices.items()])
		results = [key for key in flats if flats[key]==target]
		#---return none for slices that still need to be made
		if not results: return []
		return results

	#!!!!
	def get_twin(self,name,pair):
		"""
		Many slices files have natural twins e.g. dat/spec and gro/xtc.
		This function finds them.
		"""
		this_suffix = re.match('^.+\.(%s)$'%'|'.join(pair),name).group(1)
		basename = re.sub('\.(%s)$'%'|'.join(pair),'',name)
		twin = basename+'.%s'%dict([pair,pair[::-1]])[this_suffix]
		#---omnicalc *never* deletes files so we ask the user to clean up on errors
		if twin not in self.stable: raise Exception('cannot find the twin %s of %s ... '%(pair,name)+
			'this is typically due to a past failure to write these files together. '+
			'we recommend deleting the existing file (and fix the upstream error) to continue.')
		else: self.stable.remove(twin)
		return basename

class GMXGroup:
	def __init__(self,sn,name,spec):
		self.sn,self.name,self.spec = sn,name,spec

if False:

	class GMXSlice():
		def __init__(self,sn,name,spec):
			self.sn,self.slice_name,self.spec = sn,name,spec
			#! how to handle the spotname
			self.spotname = None
		def build_name(self):
			"""Generate a name for this slice. Strict naming is used for GROMACS slices."""
			#! crucial connection to the naming convention
			global namer
			details = dict(self.spec,short_name=namer.short_namer(self.sn,self.spotname),suffix='gro')
			name = namer.parser[('standard','gmx')]['d2n']%details
			#! remove suffix here
			basename = re.match('^(.+)\.gro$',name).group(1)
			self.name = basename
			return self.name

class Slice(TrajectoryStructure):
	def __init__(self,**kwargs):
		self.raw = kwargs

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
		# tell TrajectoryStructure what kind of slice we are
		self.kind = 'request'
		# loop over simulations and then slices
		for sn,slices_spec in self.raw.items():
			kind = self.classify(slices_spec)
			# once slice specification can yield many slices
			slices_raw = self.cross(slices_spec)
			# make a formal slice out of the raw data
			for key,val in slices_raw.items(): self.toc.append(Slice(key=key,val=val))

	def search(self,**kwargs):
		import ipdb;ipdb.set_trace()
		if kwargs: raise Exception('unprocessed kwargs %s'%kwargs)

if False:
	"""
	####### Interpret slices from the metadata.
	Note that all slices have a double loop over simulations, then top-level keys defined by the type.
	Note that type structures are given below as class variables.
	"""
	_structs = {
		# structure definition for a standard slice
		'standard_gromacs':{'slices','groups'},}

	def __init__(self,raw,**kwargs):
		"""Prepare the metadata structure and interpret the slices."""
		self.slice_structures = kwargs.pop('slice_structures',{})
		if kwargs: raise Exception('unprocessed kwargs %s'%kwargs)
		# incoming raw should be the dictionary representation of the slices
		self.raw = raw
		self.structures = copy.deepcopy(self._structs)
		# slice_structures can provide alternate slice definitions or override them
		for key,val in self.slice_structures.items(): self.structures[key] = copy.deepcopy(val)
		# convert structures to stubs for fast comparison
		self.structures_stubs = dict([(name,list([(tuple(i),j) for i,j in catalog(struct)])) 
			for name,struct in self.structures.items()])
		#! slice are organized in a list here ??
		self.gmx_groups = []
		self.gmx_slices = {}
		# loop over simulations and then slices
		for sn,slices_spec in self.raw.items():
			# identify the slice type
			slice_type = self.classify_slice(**slices_spec)
			# route the simulation name and slice type to its processing function
			#! note that we wish this to be extensible so users can supply their own functions for
			#! ... custom data types
			if hasattr(self,'_proc_%s'%slice_type): 
				getattr(self,'_proc_%s'%slice_type)(sn=sn,**slices_spec)
			else: raise Exception('dev')

	def classify_slice(self,**kwargs):
		"""Classify a slice based on its structure."""
		# slice structure now consists only of a list of keys enforced by the first conditional below
		#! to expand the slice structures, try to identify kind via lists of keys and then try a more
		#! ... elaborate comparison
		#! previously prototyped some use of catalog/stubs to match the structures but this was too hard
		kinds = [name for name,keylist in self.structures.items() 
			if type(keylist) in [set,list] and set(kwargs.keys())<=set(keylist)]
		if len(kinds)!=1: raise Exception('failed to uniquely classify the slice: %s'%kwargs)	
		else: return kinds[0]

	def _proc_standard_gromacs(self,sn,**kwargs):
		"""Process a gromacs slice."""
		# process groups
		for group_name,group_spec in kwargs.pop('groups',{}).items(): 
			self.gmx_groups.append(GMXGroup(sn=sn,name=group_name,spec=group_spec))
		# process slices
		for slice_name,slice_spec in kwargs.pop('slices',{}).items(): 
			# cross with groups
			for group_name in slice_spec.pop('groups',[]):
				# slices are saved by the final name
				this_slice = GMXSlice(sn=sn,name=slice_name,spec=dict(slice_spec,group=group_name))
				self.gmx_slices[this_slice.build_name()] = this_slice
		if kwargs: raise Exception('failed to clear the slice definitions: %s'%kwargs)

	def search(self,**kwargs):
		"""Search for a slice."""
		matches = []
		for key,spec in self.gmx_slices.items():
			view = dict(group=spec.spec['group'],sn=spec.sn,slice_name=spec.slice_name)
			if all([item in view.items() for item in kwargs.items()]):
				matches.append((key,spec))
		if len(matches)==0: raise Exception('failed to find slice %s'%kwargs)
		elif len(matches)>1: 
			raise Exception('failed to find slice %s because multiple matches: %s'%(kwargs,matches))
		else: return matches[0]

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
		# determine the state (kwargs is passed by reference so we clear it)
		self.state = WorkSpaceState(kwargs)
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

	def prepare_namer(self):
		"""Parse metadata and config to check for the short_namer."""
		# users can set a "master" short_namer in the meta dictionary if they have a very complex
		# ... naming scheme i.e. multiple spots with spotnames in the post names
		self.short_namer = self.specs.meta.get('short_namer',None)
		if self.short_namer==None:
			nspots = self.config.get('spots',{})
			#---if no "master" short_namer in the meta and multiple spots we force the user to make one
			if len(nspots)>1: raise Exception('create a namer which is compatible with all of your spots '+
				'(%s) and save it to "short_namer" in meta dictionary in the YAML file. '%nspots.keys()+
				'this is an uncommon use-case which lets you use multiple spots without naming collisions.')
			elif len(nspots)==0: self.short_namer = None
			#---if you have one spot we infer the namer from the omnicalc config.py
			else: self.short_namer = self.config.get('spots',{}).values()[0]['namer']	
		global namer
		namer = self.namer = NamingConvention(short_namer=self.short_namer)

	def read_config(self):
		"""Read the config and set paths."""
		self.config = read_config(cwd=self.cwd)
		#! is this deprecated? use a better data structure?
		self.paths = dict([(key,self.config[key]) for key in ['post_plot_spot','post_data_spot']])
		self.paths['spots'] = self.config.get('spots',{})
		# hard-coded paths
		self.postdir = self.paths['post_data_spot']
		self.plotdir = self.paths['post_plot_spot']

	def compute(self):
		"""
		Run a calculation. This is the main loop, and precedes the plot loop.
		"""
		# get the specs from the specs_folder object
		self.specs = self.specs_folder.interpret()
		self.prepare_namer()
		# prepare a calculations object
		self.calcs = Calculations(specs=self.specs)
		# prepare jobs from these calculations
		self.jobs = self.calcs.prepare_jobs()
		# parse the post-processing data
		self.post = PostDataLibrary(where=self.postdir)
		# formalize the slice requests
		self.slices = SliceMeta(raw=self.specs.slices,
			slice_structures=self.specs.director.get('slice_structures',{}))
		# join jobs and slices
		for job in self.jobs:
			# search for the target slice
			import ipdb;ipdb.set_trace()
			job.target_slice = self.slices.search(**job.slice)
			# all slices must be in the metadata
			if not job.target_slice: raise Exception('slice is missing from metadata: %s'%job.slice)
			# search for the result
			#!!!!!!!!!!!!!!!

			#! compare here then move the search elsewhere
			candidates = []
			for key,post in self.post.posts().items():
				# compare the calculation specs
				if post.calc.specs==job.calc.specs:
					candidates.append(key)
				# each post has a slice associated with it
				#! whence these slices
				# each job has a target slice
			#! search slices
			for key,sliced in self.post.slices().items():
				if sliced.namedat==job.slice.namedat:
					print(12431231)
					break
				#! compare the slices?
				"""
				slices
					self.post.slices().values()[0].__dict__ omnicalc.Slice
					post.slice.__dict__ is omnicalc.Slice
					job.slice is just a dictionary
					job.target_slice[1] is omnicalc.GMXSlice
				search post.slices?
				each post has a slice associated with it
				whence these slices?
				each job has a target slice
				the job slice is minimal and the job.target_slice is a specific slice
				the post slice has information from the name in it if gmx but otherwise just a name
					because the post slice information comes from the datspec
				possible plan
					turn all target slices into simple slices
					then compare post slice info from datspec to simple slices
				"""
			import ipdb;ipdb.set_trace()
			self.post.search_results(job=job)

		#! one job
		job = self.jobs[0]
		#! jobs are ugly
		asciitree(dict(calc=job.calc.__dict__))

		#? identify incomplete jobs?
		import ipdb;ipdb.set_trace()

		"""
		what happens next?
			slices are named and checked against postdat to see which ones to make
			groups are ignored until we have to make slices
			once you have a slice and a calculation we have to carefully check the spec files
			if none are found we name it there then start computing
			later once we make slices we check group naming
		"""

	def plot(self):
		"""
		Analyze calculations or make plots. This is meant to follow the compute loop.
		"""
		raise Exception('dev')

###
### INTERFACE FUNCTIONS
### note that these are imported by omni/cli.py and exposed to makeface

def compute(meta=None):
	global work
	if not work: work = WorkSpace(compute=True,meta_cursor=meta)

def plot():
	#! alias or alternate naming for when "plot-" becomes tiresome?
	global work
	if not work: work = WorkSpace(plot=True)
