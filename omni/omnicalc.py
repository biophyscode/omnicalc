#!/usr/bin/env python

"""
OMNICALC WORKSPACE
"""

import os,sys,re,glob,copy,json,time,tempfile
import yaml

from config import read_config
from base.tools import catalog,delve,str_or_list,str_types,status
from base.hypothesis import hypothesis
from datapack import asciitree,delveset
from structs import NamingConvention,TrajectoryStructure
from base.autoplotters import inject_supervised_plot_tools

# hold the workspace in globals
global work,namer
work,namer = None,None

def json_type_fixer(series):
	"""Cast integer strings as integers, recursively. We also fix 'None'."""
	#! move this somewhere else
	for k,v in series.items():
		if type(v) == dict: json_type_fixer(v)
		elif type(v)in str_types and v.isdigit(): series[k] = int(v)
		elif type(v)in str_types and v=='None': series[k] = None

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

	def __eq__(self,other):
		"""See if calculations are equivalent."""
		#! note that calculations are considered identical if they have the same specs
		#! ... we disregard the calc_specs because they include collections, slice names (which might change)
		#! ... and the group name. we expect simulation and group and other details to be handled on 
		#! ... slice comparison
		return self.specs==other.specs

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
					sliced = Slice(kind='alternate',data=request_slice)
					# join the slice and calculation in a job
					jobs.append(ComputeJob(slice=sliced,calc=calc))
		# the caller should save the result
		return jobs

class Slice(TrajectoryStructure):
	"""A class which holds trajectory data of many kinds."""
	def __init__(self,kind,data):
		"""..."""
		self.kind = kind
		self.data = data
		self.style = self.classify(self.data)
	def __eq__(self,other):
		"""Match slices."""
		return self.test_equality(self,other)

class PostData:
	def __init__(self,**kwargs):
		"""..."""
		self.fn,self.dn = kwargs.pop('fn'),kwargs.pop('dn'),
		if kwargs: raise Exception('unprocessed kwargs %s'%kwargs)
		self.valid = True
		#! check validity later?
		global namer
		self.namer = namer
		self.parse(fn=self.fn,dn=self.dn)

	def parse(self,**kwargs):
		"""..."""
		fn,dn = kwargs.pop('fn'),kwargs.pop('dn'),
		if kwargs: raise Exception('unprocessed kwargs %s'%kwargs)
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
			# build a slice from the version 2 specification
			slice_raw = self.specs['slice']
			if slice_raw.get('dat_type',None)=='gmx' and slice_raw.get('slice_type',None)=='standard':
				#! hacking the problem of getting simulation name from shortname
				try: sn = dict([(j,i) for i,j in self.namer.sns_toc.items()])[slice_raw['short_name']]
				except: sn = 'missing simulation'
				self.slice = Slice(kind='alternate',data=dict(slice_raw,sn=sn))
				#! we could check the postprocessing name to see if it matches its own slice data
			else: raise Exception('dev')
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
		if val.__class__.__name__=='PostData'])

	def search_results(self,job):
		"""Search the posts for a particular result."""
		# search posts for the correct calculations
		candidates = [key for key,val in self.posts().items() 
			if val.calc==job.calc and val.slice==job.slice]
		if len(candidates)>1: raise Exception('multiple matches for job %s'%job)
		elif len(candidates)==0: return None
		else: return self.toc[candidates[0]]

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
			# once slice specification can yield many slices
			slices_raw = self.cross(style=style,data=slices_spec)
			# make a formal slice element out of the raw data
			for key,val in slices_raw.items(): 
				self.toc.append(Slice(kind='element',data=dict(key=key,val=val,sn=sn)))

	def search(self,candidate):
		"""Search the requested slices."""
		matches = [sl for sl in self.toc if sl==candidate]
		if len(matches)>1: raise Exception('redundant matches for %s'%candidate)
		elif len(matches)==0: return None
		else: return matches[0]

	# deprecated because we make the slice immediately
	if False:
		def calculation_to_request(self,**kwargs):
			#! minor hack to match a calculation slice request to a slice request in SliceMeta
			#! ... this will be replaced later
			import ipdb;ipdb.set_trace()
			index, = [ii for ii,i in enumerate(self.toc) if 
				kwargs['slice_name']==i.raw['key'][1] and 'slices'==i.raw['key'][0] and
				kwargs['sn']==i.raw['sn'] and kwargs['group']==i.raw['val']['group']]
			return self.toc[index]

class PlotLoaded(dict):
	def __init__(self,calcnames,sns): 
		self.calcnames,self.sns = calcnames,sns
	#def __getitem__(self,name):
		#if name not in self.__dict__:
		#	import ipdb;ipdb.set_trace()

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

	def simulation_names(self):
		"""Return all simulation names from the metadata."""
		return list(set([i for j in self.meta.collections.values() for i in j]))

	def prepare_namer(self):
		"""Parse metadata and config to check for the short_namer."""
		# users can set a "master" short_namer in the meta dictionary if they have a very complex
		# ... naming scheme i.e. multiple spots with spotnames in the post names
		self.short_namer = self.meta.meta.get('short_namer',None)
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
		# populate a table of all simulation names for emergency use
		#! spotname is none for now
		spotname = None
		self.namer.sns_toc = dict([(sn,self.namer.short_namer(sn,None)) 
			for sn in self.simulation_names()])

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
		search = [fn for fn in fns if re.match('^%s\.py$'%name,os.path.basename(fn))]
		if len(search)==0: 
			raise Exception('\n[ERROR] cannot find %s.py'%name)
		elif len(search)>1: raise Exception('\n[ERROR] redundant matches: %s'%str(search))
		# manually import the function
		return search[0]

	def plotload(self,plotname,**kwargs):
		"""..."""
		whittle_calc = kwargs.pop('whittle_calc',None)
		if kwargs: raise Exception('unprocessed kwargs %s'%kwargs)
		if whittle_calc: raise Exception('dev')
		# we always run the compute loop to make sure calculations are complete but also to get the jobs
		self.compute()
		plotspec = self.meta.plots.get(plotname,{})
		plotload_version = plotspec.get('plotload_version',self.meta.director.get('plotload_output_style',1))
		if not plotspec: raise Exception('dev')
		sns = self.meta.get_simulations_in_collection(plotspec.get('collections',[]))
		if not sns: raise Exception('no collections in plotspec. dev')
		calcs = plotspec.get('calculation',{})
		if type(calcs) in str_types: calcnames = [calcs]
		elif type(calcs)==list: calcnames = calcs
		elif type(calcs)==dict: raise Exception('dev')
		else: raise Exception('dev')
		bundle = dict([(k,PlotLoaded(calcnames=calcnames,sns=sns)) for k in ['data','calc']])
		# search for results
		for sn in sns:
			#! handle if they are not strings?
			for calcname in calcnames:
				#! will all slices have a sn?
				results = [r for r in self.results 
					if r.slice.data['sn']==sn and r.calc.name==calcname]
				if len(results)==0: 
					raise Exception('dev. cannot find calculation %s for simulation %s'%(calcname,sn))
				elif len(results)>1: raise Exception('too many matches')
				else: result = results[0]
				# add the data to the bundle
				if calcname not in bundle['data']: bundle['data'][calcname] = {}
				#! need to actually load it here
				bundle['data'][calcname][sn] = {'data':result}
				# add the calculation specs to the bundle
				#! check the right format?
				if calcname not in bundle['calc']: bundle['calc'][calcname] = {}
				bundle['calc'][calcname][sn] = 'MISSING'
		# return in a particular format
		if plotload_version==1: 
			# remove calculation name from the nested dictionary of only one
			if len(calcnames)==1 and len(bundle['data'])==1 and len(bundle['calc'])==1:
				bundle['data'] = bundle['data'].values()[0]
				bundle['calc'] = bundle['calc'].values()[0]
			return bundle['data'],bundle['calc']
		else: raise Exception('dev')

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
		script = self.find_script('plot-%s'%plotname)
		with open(script) as fp: code = fp.read()
		#---handle builtins before executing. we pass all keys in out through builtins for other modules
		import builtins
		builtins._plotrun_specials = out.keys()
		for key in out: builtins.__dict__[key] = out[key]
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
		plotrun.routine = plotspecs.pop('args',{})
		if plotspecs: raise Exception('unprocessed plotspecs %s'%plotspecs)
		if kwargs_plot: raise Exception('unprocessed plotting kwargs %s'%kwargs_plot)
		plotrun.autoplot(out=out)
		import ipdb;ipdb.set_trace()

	def prelim(self):
		"""Preliminary materials for compute and plot."""
		# get the specs from the specs_folder object
		self.meta = self.specs_folder.interpret()

	def compute(self,**kwargs):
		"""
		Run a calculation. This is the main loop, and precedes the plot loop.
		"""
		if kwargs: raise Exception('unprocessed kwargs %s'%kwargs)
		self.prelim()
		self.prepare_namer()
		# prepare a calculations object
		self.calcs = Calculations(specs=self.meta)
		# prepare jobs from these calculations
		self.jobs = self.calcs.prepare_jobs()
		# parse the post-processing data
		self.post = PostDataLibrary(where=self.postdir)
		# formalize the slice requests
		self.slices = SliceMeta(raw=self.meta.slices,
			slice_structures=self.meta.director.get('slice_structures',{}))
		# save completed jobs as results
		self.results,queue_computes = [],[]
		# join jobs with results
		for job in self.jobs:
			# jobs have slices in alternate/calculation_request form and they must be fleshed out
			slice_match = self.slices.search(job.slice)
			if not slice_match: 
				raise Exception('failed to find requested slice in the metadata: %s'%job.slice)
			# replace the job slice with the metadata slice if we found a match
			else: job.slice = slice_match
			# search for a result
			job.result = self.post.search_results(job=job)
			if not job.result: queue_computes.append(job)
			else: self.results.append(job)
		if queue_computes: raise Exception('dev')

	def plot(self):
		"""
		Analyze calculations or make plots. This is meant to follow the compute loop.
		"""
		if len(self.plot_args)==0: raise Exception('you must send the plotname as an argument')
		elif len(self.plot_args)==1: plotname,args = self.plot_args[0],()
		else: plotname,args = self.plot_args[0],self.plot_args[1:]
		self.prelim()
		#! plot from a default calculation
		if plotname in self.meta.plots and self.meta.plots[plotname].get('autoplot',True):
			self.plot_supervised(plotname=plotname,plotspecs=dict(args=args,kwargs=self.plot_kwargs))
		else: raise Exception('dev')

###
### INTERFACE FUNCTIONS
### note that these are imported by omni/cli.py and exposed to makeface

def compute(meta=None):
	global work
	if not work: work = WorkSpace(compute=True,meta_cursor=meta)

def plot(*args,**kwargs):
	#! alias or alternate naming for when "plot-" becomes tiresome?
	global work
	if not work: work = WorkSpace(plot=True,plot_args=args,plot_kwargs=kwargs)
