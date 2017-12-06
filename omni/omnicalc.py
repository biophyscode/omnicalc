#!/usr/bin/env python

import os,sys,re,glob,copy,json,time,tempfile
from config import read_config,bash

from base.tools import catalog,delve,str_or_list,status
from base.hypothesis import hypothesis
from maps import NamingConvention,PostDat,ComputeJob,Calculation
from maps import Slice,SliceMeta,DatSpec,CalcMeta,ParsedRawData
from datapack import asciitree,delve,delveset
from makeface import fab
from base.autoplotters import inject_supervised_plot_tools

#---typical first encounter with super-python reqs so we warn the user if they have no good env yet
#---note that omnicalc now has automatic loading via activate_env
msg_needs_env = ('\n[WARNING] failed to load a key requirement (yaml) '
	'which means you probably need to source the environment. '
	'go to the factory root and run e.g. `source env/bin/activate py2`')
try: 
	import yaml
	import numpy as np
except: print(msg_needs_env)

str_types = [str,unicode] if sys.version_info<(3,0) else [str]

class WorkSpace:

	"""
	User-facing calculation management.
	Style note: the workspace instance is passed around to many classes in maps.py. The author is aware
	that this is highly unusual, but it takes the place of a larger, more hierarchical class.
	"""

	#---hard-coded paths for specs files
	specs_path = 'calcs','specs','*.yaml'
	#---versioning manages fairly generic data structures written to disk
	versioning = {'spec_file':2}
	#---! currently hard-coded
	nprocs = 4

	def __init__(self,plot=None,plot_call=False,pipeline=None,meta=None,plotspecs=None,
		confirm_compute=False,cwd=None,do_slices=True,checkup=False):
		"""
		Prepare the workspace.
		"""
		if not cwd: self.cwd = os.getcwd()
		else: self.cwd = os.path.join(cwd,'')
		if not os.path.isdir(self.cwd): raise Exception('invalid cwd for this WorkSpace: %s'%self.cwd)
		if plot and pipeline: raise Exception('only plot or pipeline')
		#---read the omnicalc config and specs files
		self.config = read_config(cwd=self.cwd)
		self.mpl_agg = self.config.get('mpl_agg',False)
		#---unpack the paths right into the workspace for calculation functions
		#---add paths here for backwards compatibility at the plotting stage
		self.paths = dict([(key,self.config[key]) for key in ['post_plot_spot','post_data_spot']])
		self.paths['spots'] = self.config.get('spots',{})
		meta_incoming = meta
		#---check the config.py for this omnicalc to find restrictions on metafiles
		#---...note that this allows us to avoid using git branches and the meta flag in the CLI for 
		#---...managing multiplce meta files.
		if not meta_incoming and self.config.get('meta_filter',None):
			#---set_config forces meta_filter to be a list. each can be a glob. the path is relative to 
			#---...the calcs/specs folder since that is the only acceptable location for meta files
			#---...we detect meta files here and send them as a list, otherwise read_specs gets a string
			#---...from the CLI which is a glob with the full path to calcs/specs
			meta_incoming = [i for j in [glob.glob(os.path.join(self.cwd,'calcs','specs',g)) 
				for g in self.config['meta_filter']] for i in j]
		#---read the specs according to incoming meta flags
		self.specs = self.read_specs(meta=meta_incoming,merge_method=self.config.get('merge_method',None))
		#---users can set a "master" short_namer in the meta dictionary if they have a very complex
		#---... naming scheme i.e. multiple spots with spotnames in the post names
		short_namer = self.meta.get('short_namer',None)
		if short_namer==None:
			nspots = self.config.get('spots',{})
			#---if no "master" short_namer in the meta and multiple spots we force the user to make one
			if len(nspots)>1: raise Exception('create a namer which is compatible with all of your spots '+
				'(%s) and save it to "short_namer" in meta dictionary in the YAML file. '%nspots.keys()+
				'this is an uncommon use-case which lets you use multiple spots without naming collisions.')
			elif len(nspots)==0: short_namer = None
			#---if you have one spot we infer the namer from the omnicalc config.py
			else: short_namer = self.config.get('spots',{}).values()[0]['namer']	
		#---if there is one spot, we set the short namer
		#---prepare a namer from the global omni_namer
		self.namer = NamingConvention(work=self,
			short_namer=short_namer,
			short_names=self.meta.get('short_names',None))
		#---CALCULATION LOOP
		self.calcs = self.specs.get('calculations',None)
		self.slices = self.specs.get('slices',None)
		if not self.calcs: return
		if not self.calcs and self.slices: 
			raise Exception('cannot continue to calculations without slices')
		#---! note that pipeline/plot cause imports to happen twice which is somewhat inefficient
		#---! ...however it is necessary since we need to make sure jobs are complete, and then import
		#---! ...again when calling the plot function, which requires a separate python call
		#---catalog calculation requests from the metadata
		self.calc_meta = CalcMeta(self.calcs,work=self)
		#---catalog post-processing data
		self.postdat = PostDat(where=self.config.get('post_data_spot',None),namer=self.namer,work=self)
		#---catalog slice requests from the metadata
		self.slice_meta = SliceMeta(self.slices,work=self,do_slices=do_slices)
		#---get the right calculation order
		self.calc_order = self.infer_calculation_order()
		if not plot and not pipeline: 
			asciitree(dict(compute_sequence=[i+(' %s '%fab(' IGNORED! ','cyan_black') 
				if self.calcs.get(i,{}).get('ignore',False) else '') for i in self.calc_order]))
		#---plot and pipeline skip calculations and call the target script
		self.plot_status,self.pipeline_status = plot,pipeline
		if not plot and not pipeline and not checkup:
			#---match calculation codes with target slices
			self.jobs = self.prepare_calculations()
			self.compute(confirm=confirm_compute)
		elif plot: self.plot(plotname=plot,plot_call=plot_call,meta=meta_incoming,plotspecs=plotspecs)
		elif pipeline: self.pipeline(name=pipeline,plot_call=plot_call,meta=meta_incoming)
		#---checkup is for the factory to probe the workspace
		elif checkup: 
			try: self.jobs = self.prepare_calculations()
			except: self.jobs = {'error':'error in prepare_calculations'}
			try: self.compute(checkup=True)
			except: self.tasks = {'error':'error in compute'}

	def get_importer(self,silent=False):
		"""
		Development purposes.
		"""
		#---! note that this is self-referential
		self.raw = ParsedRawData(work=self)
		#---loop over edr spots
		sns = []
		for spotname in [k for k in self.raw.spots if k[1]=='edr']:
			for key in self.raw.toc[spotname].keys(): sns.append(key)
		#---add timeseries to the toc
		for ss,sn in enumerate(sns): 
			if not silent:
				status('reading EDR to collect times for %s'%sn,i=ss,looplen=len(sns),tag='read',width=65)
			self.raw.get_timeseries(sn)

	def times(self,write_json=False):
		"""
		Useful via `make look times`. Shows all of the edr times.
		"""
		self.get_importer(silent=write_json)
		view_od = [(k,v) 
			for spotname in [k for k in self.raw.spots if k[1]=='edr']
			for k,v in self.raw.toc[spotname].items()]
		from datapack import asciitree
		if not write_json: 
			view = dict([(name,[(
				'%s%s-%s'%k+' part%s: %s%s'%(
				i,str(round(j['start'],2)  if j['start'] else '???').rjust(12,'.'),
				str(round(j['stop'],2) if j['stop'] else '???').rjust(12,'.'))) 
				for k,v in obj.items() for i,j in v.items()]) for name,obj in view_od])
			asciitree(view)
		#---systematic view
		else: 
			#import ipdb;ipdb.set_trace()
			#view = dict([(s,[('%s%s-%s'%k,tuple(v)) for k,v in detail.items()]) for s,detail in view_od])
			view = [(sn,[('%s%s-%s'%stepname,[(i,j) for i,j in step.items()]) 
				for stepname,step in details.items()]) for sn,details in view_od]
			print('time_table = %s'%json.dumps(view))
		return view

	def times_json(self):
		"""
		Expose `make look times` to the factory.
		!Need a long-term solution for calling things like this.
		"""
		self.times(write_json=True)

	def variable_unpacker(self,specs):
		"""
		Internal variable substitutions using the "+" syntax.
		"""
		#---apply "+"-delimited internal references in the yaml file
		for path,sub in [(i,j[-1]) for i,j in catalog(specs) if type(j)==list 
			and type(j)==str and re.match('^\+',j[-1])]:
			source = delve(self.vars,*sub.strip('+').split('/'))
			point = delve(specs,*path[:-1])
			point[path[-1]][point[path[-1]].index(sub)] = source
		for path,sub in [(i,j) for i,j in catalog(specs) if type(j)==str and re.match('^\+',j)]:
			path_parsed = sub.strip('+').split('/')
			try: source = delve(self.vars,*path_parsed)
			except: raise Exception('failed to locate internal reference with path: %s'%path_parsed)
			point = delve(specs,*path[:-1])
			point[path[-1]] = source
		#---refresh variables in case they have internal references
		self.vars = copy.deepcopy(specs['variables']) if 'variables' in specs else {}
		self.specs_raw = specs
		return specs

	def read_specs(self,meta=None,merge_method=None):
		"""
		Read and interpret calculation specs.
		Lifted directly from old workspace.load_specs.
		"""
		if not merge_method: merge_method = 'careful'
		#---note that we handle cwd when defining the specs_files, not when checking them
		if not meta: specs_files = glob.glob(os.path.join(self.cwd,*self.specs_path))
		else: 
			#---if meta is a string we assume it is a glob and check for files
			#---note that using the CLI to set meta requires all paths relative to the omnicalc root
			#---...hence they must point to calcs/specs to find valid files
			#---...however globs saved to meta_filter in the config.py via `make set` do not 
			#---...need to be prepended with calcs/specs since this location is assumed
			if type(meta)==str: specs_files = glob.glob(os.path.join(self.cwd,meta))
			#---if meta is a list then it must have come from meta_filter and hence includes valid files
			else:
				if not all([os.path.isfile(i) for i in meta]): 
					missing = [i for i in meta if not os.path.isfile(i)]
					raise Exception('received invalid meta files in a list (cwd="%s"): %s'%(self.cwd,missing))
				specs_files = meta
		#if not specs_files: 
		#	raise Exception('cannot find meta files')
		#---save the specs files
		self.specs_files = specs_files
		allspecs = []
		for fn in specs_files:
			with open(fn) as fp: 
				if (merge_method != 'override_factory' or 
					not re.match('^meta\.factory\.',os.path.basename(fn))):
					try: allspecs.append(yaml.load(fp.read()))
					except Exception as e:
						raise Exception('failed to parse YAML (are you sure you have no tabs?): %s'%e)
		if not allspecs: 
			self.meta,self.plots = {},{}
			return {}
		if merge_method=='strict':
			specs = allspecs.pop(0)
			for spec in allspecs:
				for key,val in spec.items():
					if key not in specs: specs[key] = copy.deepcopy(val)
					else: raise Exception('\n[ERROR] redundant key %s in more than one meta file'%key)
		elif merge_method=='careful':
			#---! recurse only ONE level down in case e.g. calculations is defined in two places but there
			#...! ...are no overlaps, then this will merge the dictionaries at the top level
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
									'to the meta file you want. note meta is "%s"')%(topkey,key,meta))
		elif merge_method=='sequential':
			#---load yaml files in the order they are specified in the config.py file with overwrites
			specs = allspecs.pop(0)
			for spec in allspecs:
				specs.update(**spec)
		else: raise Exception('\n[ERROR] unclear meta specs merge method %s'%merge_method)
		#---allow empty meta files
		if not specs: 
			specs = {}
			print('[WARNING] no metadata found. '+
				'the meta_filter in config.py specifies the following meta files: %s'%specs_files)
		self.vars = specs.get('variables',{})
		specs_unpacked = self.variable_unpacker(specs)
		self.meta = specs.get('meta',{})
		#---for backwards compatibility we put collections in vars
		if 'collections' in self.vars: raise Exception('collection already in vars')
		self.vars['collections'] = specs.get('collections',{})
		#---expose the plots
		self.plots = specs.get('plots',{})
		#---plots can be aliased to themselves
		for key,val in self.plots.items():
			if type(val) in str_types: 
				if key not in self.plots:
					raise Exception('plot alias from %s to %s is invalid'%(key,val))
				else: self.plots[key] = copy.deepcopy(self.plots[val])
		self.plotdir = self.paths['post_plot_spot']
		self.postdir = self.paths['post_data_spot']
		return specs_unpacked

	def infer_calculation_order(self):
		"""
		Catalog the upstream calculation dependencies for all of the calculations and generate a sequence 
		which ensures that each calculation follows its dependencies. Note that we have a 10s timer in place 
		to warn the user that they might have a loop (which would cause infinite recursion). 
		"""
		#---infer the correct order for the calculation keys from their upstream dependencies
		upstream_catalog = [i for i,j in catalog(self.calcs) if 'upstream' in i]
		#---if there are no specs required to get the upstream data object the user can either 
		#---...use none/None as a placeholder or use the name as the key as in "upstream: name"
		for uu,uc in enumerate(upstream_catalog):
			if uc[-1]=='upstream': upstream_catalog[uu] = upstream_catalog[uu]+[delve(self.calcs,*uc)]
		depends = {}
		#---formulate a list of dependencies while accounting for multiple upstream dependencies
		for t in upstream_catalog:
			if t[0] not in depends: depends[t[0]] = []
			depends[t[0]].extend([t[ii+1] for ii,i in enumerate(t) if ii<len(t)-1 and t[ii]=='upstream'])
		calckeys = [i for i in self.calcs if i not in depends]
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

	def get_simulations_in_collection(self,*names):
		"""
		Read a collections list.
		"""
		collections = self.specs.get('collections',{})
		if any([name not in collections for name in names]): 
			raise Exception('cannot find collection %s'%name)
		sns = []
		for name in names: sns.extend(collections.get(name,[]))
		return sorted(list(set(sns)))

	def find_script(self,name,root='calcs'):
		"""
		Find a generic script somewhere in the calculations folder.
		"""
		#---find the script with the funtion
		fns = []
		for (dirpath, dirnames, filenames) in os.walk(os.path.join(self.cwd,root)): 
			fns.extend([dirpath+'/'+fn for fn in filenames])
		search = [fn for fn in fns if re.match('^%s\.py$'%name,os.path.basename(fn))]
		if len(search)==0: 
			raise Exception('\n[ERROR] cannot find %s.py'%name)
		elif len(search)>1: raise Exception('\n[ERROR] redundant matches: %s'%str(search))
		#---manually import the function
		return search[0]

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

	def get_new_dat_name(self,base_name):
		"""
		Get a new filename for the post-processing data.
		Assumes we already checked the data so we aren't doing a redundant calculation.
		"""
		#---we assume that nobody has nefariously added files to the postdat since the run started
		#---! is this assumption reasonable?
		fns = [re.match('^.+\.n(\d+)',key).group(1) 
			for key in self.postdat.toc if re.match(base_name,key)]
		if fns and not sorted([int(i) for i in fns])==list(range(len(fns))): 
			raise Exception('error in data numbering')
		index = len(fns)
		tag = '.n%d'%index
		#---! check if these files don't exist
		return [base_name+tag+i for i in ['.dat','.spec','']]

	def infer_group(self,calc,loud=False):
		"""
		Figure out groups for a downstream calculation.
		"""
		if loud: status('inferring group for %s'%calc,tag='bookkeeping')
		if type(calc)==dict:
			#---failed recursion method
			if False:
				def get_upstream_groups(*args):
					"""Recurse the dependency list for upstream groups."""
					for arg in args:
						if 'group' in self.calcs[arg]: yield self.calcs[arg]['group']
						else: up_again.append(arg)
			#---! non-recursive method
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
			#---! end non-recursive method
			#groups = get_upstream_groups(*calc['specs']['upstream'].keys())
			groups_consensus = list(set(groups))
			if len(groups_consensus)!=1: 
				raise Exception('cannot achieve upstream group consensus: %s'%groups_consensus)
			group = groups_consensus[0]
			return group
		#---use the fully-linked calculations to figure out the group.
		else:
			groups_consensus = []
			check_calcs = [v for k,v in calc.specs_linked['specs'].items() 
				if type(k)==tuple and k[0]=='up']
			while check_calcs:
				this_calc = check_calcs.pop()
				ups = [v for k,v in this_calc.specs_linked['specs'].items() 
					if type(k)==tuple and k[0]=='up']
				if 'group' in this_calc.specs_linked: groups_consensus.append(this_calc.specs['group'])
				#---! the following uses the fully-linked calculation naming scheme which is clumsy
				check_calcs.extend([v for k,v in 
					this_calc.specs_linked['specs'].items() if type(k)==tuple and k[0]=='up'])
			groups_consensus = list(set(groups_consensus))
			if len(groups_consensus)!=1: 
				raise Exception('cannot achieve upstream group consensus: %s'%groups_consensus)
			return groups_consensus[0]

	def infer_pbc(self,calc):
		"""
		Figure out PBC condition for a downstream calculation.

		Important note: calculations with uptype `post` will drop the group and pbc flags from their 
		filenames. To identify a postprocessed data file, we need to infer the original simulation name from 
		the short (or prefixed) name at the beginning of the data file's name. We do this by making a map 
		between simulation names in the metadata slices dictionary and their shortened names. This lets us 
		perform a reverse lookup and figure out which simulations in the slices folder (and elsewhere in 
		the metadata) are the parents of a postprocessed data file we found on the disk. For that reason, 
		the back_namer below sweeps over all possible spot names and all possible slices to figure out the 
		pbc flag for the upstream data. This allows us to drop the pbc flags on downstream calculations with 
		uptype post.
		"""
		back_namer = dict([(self.namer.short_namer(key,spot=spot_candidate),key) 
			for key in self.slices.keys() 
			for spot_candidate in [None]+self.paths['spots'].keys()])
		if calc['slice']['short_name'] not in back_namer:
			raise Exception('check that you have the right short_namer in the meta dictionary. '+
				'back_namer lacks %s: %s'%(calc['slice']['short_name'],back_namer))
		sn = back_namer[calc['slice']['short_name']]
		calcname = calc['calc']['calc_name']
		pbc = self.slices[sn]['slices'][self.calcs[calcname]['slice_name']]['pbc']
		return pbc

	def chase_upstream(self,specs,warn=False):
		"""
		Fill in upstream information. Works in-place on specs.
		"""
		specs_cursors = [copy.deepcopy(specs)]
		while specs_cursors:
			sc = specs_cursors.pop()
			if 'upstream' in sc:
				for calcname in sc['upstream']:
					if sc['upstream'][calcname]:
						sc['upstream'][calcname].items()
						for key,val in sc['upstream'][calcname].items():
							if type(val) in str_types:
								#---! this is pretty crazy. wrote it real fast pattern-matching
								expanded = self.calcs[calcname]['specs'][key]['loop'][
									sc['upstream'][calcname][key]]
								#---replace with the expansion
								specs[key] = copy.deepcopy(expanded)
								del specs['upstream'][calcname][key]
							#---! assert empty?
						del specs['upstream'][calcname]
					else: del specs['upstream'][calcname]
		if 'upstream' in specs:
			if specs['upstream']: raise Exception('failed to clear upstream')
			else: del specs['upstream']

	def store(self,obj,name,path,attrs=None,print_types=False,verbose=True):
		"""Wrap store which must be importable."""
		store(obj,name,path,attrs=attrs,print_types=print_types,verbose=verbose)

	###---COMPUTE LOOP

	def prepare_calculations(self,calcnames=None,sns=None):
		"""
		Match calculations with simulations.
		This function prepares all pending calculations unless you ask for a specific one.
		"""
		sns_overrides = None if not sns else list(str_or_list(sns))
		#---jobs are nameless in a list
		jobs = []
		#---loop over calculations
		for calckey in (self.calc_order if not calcnames else str_or_list(calcnames)):
			#---opportunity to ignore calculations without awkward block commenting or restructuring
			#---...in the yaml file
			if calckey in self.calcs and self.calcs[calckey].get('ignore',False):
				status('you have marked "ignore: True" in calculation %s so we are skipping'%calckey,
					tag='note')
				continue
			#---loop over calculation jobs expanded by the "loop" keyword by the CalcMeta class
			calcset = self.calc_meta.calcjobs(calckey)
			for calc in self.calc_meta.calcjobs(calckey):

				#---get slice name
				slice_name = calc.specs['slice_name']
				#---loop over simulations
				if not sns_overrides: 
					sns = self.get_simulations_in_collection(*str_or_list(calc.specs['collections']))
				#---custom simulation name request will whittle the sns list here
				else: sns = list(sns_overrides)
				#---get the group
				group = calc.specs.get('group',None)
				#---group is absent if this is a downstream calculation
				if not group:
					group = self.infer_group(calc)
				#---loop over simulations
				for sn in sns:
					request_slice = self.slice_meta.get_slice(sn=sn,slice_name=slice_name,group=group)
					#---join the slice and calculation in a job
					jobs.append(ComputeJob(sl=request_slice,calc=calc,work=self))
		#---this function populates workspace.jobs but also has other uses
		#---! which other uses?
		return jobs

	def job_print(self,job):
		"""
		Describe the job.
		"""
		asciitree({job.sn:dict(**{job.calc.name:dict(
			specs=job.calc.specs['specs'],slice=job.slice.flat())})})

	def compute(self,confirm=False,checkup=False,cleanup=None):
		"""
		Run through computations.
		"""
		completed = [job for job in self.jobs if job.result]
		pending = [job for job in self.jobs if not job.result]
		#---flesh out the pending jobs
		tasks = []
		for job in self.jobs: 
			if not job.result:
				post = DatSpec(job=job)
				tasks.append((post.basename(),{'post':post,'job':job}))
		if confirm and len(tasks)>0:
			print('[NOTE] there are %d pending jobs'%len(pending))
			print('[QUESTION] okay to continue? (you are debuggign so hit \'c\'')
			import pdb;pdb.set_trace()
		#----the collect options is meant to pass tasks back to the factory
		if checkup: self.tasks = tasks
		else:
			#---iterate over compute tasks
			for jnum,(jkey,incoming) in enumerate(tasks):
				print('[JOB] running calculation job %d/%d'%(jnum+1,len(pending)))
				self.job_print(incoming['job'])
				self.compute_single(incoming)

	def compute_single(self,incoming):
		"""
		SINGLE COMPUTATION.
		Note that this replaces (most of) computer from the original omnicalc.
		"""
		job,post = incoming['job'],incoming['post']
		#---retrieve the function
		function = self.get_calculation_function(job.calc.name)
		#---prepare data for shipping to the function
		#---note that we send the specs subdictionary of the calc specs because that is what the 
		#---...calculation function expects to find
		outgoing = {'calc':{'specs':job.calc.specs['specs']},'workspace':self,'sn':job.sn}

		#---regardless of uptype we decorate the outgoing kwargs with upstream data objects
		upstreams = [(key,item) for key,item in job.calc.specs_linked['specs'].items() 
			if type(key)==tuple and key[0]=='up']
		if upstreams: outgoing['upstream'] = {}
		for unum,((upmark,calcname),calc) in enumerate(upstreams):
			status('caching upstream: %s'%calcname,tag='status',looplen=len(upstreams),i=unum)
			result = ComputeJob(sl=job.slice,calc=calc,work=self).result
			if not result:
				raise Exception('cannot find result for calculation %s with specs %s'%(
					calcname,calc.__dict__))
			outgoing['upstream'][calcname] = self.load(
				name=self.postdat.toc[result].files['dat'],cwd=self.paths['post_data_spot'])
		#---for backwards compatibility we decorate the kwargs with the slice name and group
		outgoing.update(slice_name=job.slice.slice_name,group=job.slice.group)

		#---THE MOST IMPORTANT LINES IN THE WHOLE CODE (here we call the calculation function)
		if job.calc.specs.get('uptype','simulation')=='simulation':
			if job.slice.flat()['slice_type']=='standard':
				self.postdat.toc[job.slice.name].__dict__['namedat']['dat_type']
				if not self.postdat.toc[job.slice.name].__dict__['namedat']['dat_type']=='gmx':
					raise Exception('dat_type is not gmx')
				struct_file,traj_file = [self.postdat.parser[('standard','gmx')]['d2n']%dict(
					suffix=suffix,**job.slice.flat()) for suffix in ['gro','xtc']]
				struct_file,traj_file = [os.path.join(self.paths['post_data_spot'],i) 
					for i in [struct_file,traj_file]]
				#---! use explicit kwargs to the function however it would be useful to 
				#---! ...introspect on the arguments e.g. grofile vs struct
				incoming_data = function(grofile=struct_file,trajfile=traj_file,**outgoing)
				if type(incoming_data)==type(None) or len(incoming_data)!=2:
					raise Exception('function %s must return a tuple '%function.__name__+
						'with two objects: a result dictionary for HDF5 storage and an unstructured '+
						'attributes dictionary typically')
				result,attrs = incoming_data
			elif job.slice.flat()['slice_type']=='readymade_gmx':
				#---no dat_type for readymade_namd unlike standard/gmx
				struct_file = os.path.join(self.paths['post_data_spot'],job.slice.flat()['gro'])
				traj_file = [os.path.join(self.paths['post_data_spot'],i) for i in 
					str_or_list(job.slice.flat()['xtcs'])]
				result,attrs = function(grofile=struct_file,trajfile=traj_file,**outgoing)
			elif job.slice.flat()['slice_type']=='readymade_namd':
				#---no dat_type for readymade_namd unlike standard/gmx
				struct_file = os.path.join(self.paths['post_data_spot'],job.slice.flat()['psf'])
				traj_file = [os.path.join(self.paths['post_data_spot'],i) for i in 
					str_or_list(job.slice.flat()['dcds'])]
				result,attrs = function(grofile=struct_file,trajfile=traj_file,**outgoing)
			#---placeholders for incoming mesoscale data
			elif job.slice.flat()['slice_type']=='readymade_meso_v1':
				struct_file = 'mesoscale_no_structure'
				traj_file = 'mesoscale_no_trajectory'
				result,attrs = function(grofile=struct_file,trajfile=traj_file,**outgoing)
			else: raise Exception('unclear trajectory mode')
		elif job.calc.specs['uptype']=='post':
			#---! new upstream method above
			if False:
				#---acquire upstream data
				#---! multiple upstreams. double upstreams. loops. specs. etc. THIS IS REALLY COMPLICATED.
				upstreams = str_or_list(job.calc.specs['upstream'])
				outgoing['upstream'] = {}
				for upcalc in upstreams:
					#---get a jobs list for this single simulation since post data is one-simulation only
					upstream_jobs = self.prepare_calculations(calcnames=upstreams,sns=[job.sn])
					missing_ups = [j for j in upstream_jobs if not j.result]
					if any(missing_ups):
						raise Exception('missing upstream data from: %s'%missing_ups)
					for upstream_job in upstream_jobs:
						outgoing['upstream'][upcalc] = self.load(
							name=self.postdat.toc[upstream_job.result].files['dat'],
							cwd=self.paths['post_data_spot'])
			result,attrs = function(**outgoing)
		else: raise Exception('invalid uptype: %s'%job.calc.uptype)

		#---! currently post.specs['specs'] has the real specs in a subdictionary 
		#---! ...alongsize simulation and collection names !!!

		#---check the attributes against the specs so we don't underspecify the data in the spec file
		#---...if any calculation specifications are not in attributes we warn the user here
		if 'specs' in post.specs['specs']:
			unaccounted = [i for i in post.specs['specs'] if i not in attrs]
		else: unaccounted = []
		if 'upstream' in unaccounted and 'upstream' not in attrs: 
			status('automatically appending upstream data',tag='status')
			unaccounted.remove('upstream')
			#---! this sets upstream information so that it mirrors the meta file
			#---! ...however if the meta file changes, it will be out of date
			#---! ...this can be solved with a checker on the dat files and some stern documentation
			#---! ...or we can fill in the actual upstream specs somehow, but then they might need
			#---! ...to get read on the matching steps
			attrs['upstream'] = post.specs['specs']['upstream']
		if any(unaccounted):
			import textwrap
			from maps import computer_error_attrs_passthrough
			print('\n'.join(['[ERROR] %s'%i for i in 
				textwrap.wrap(computer_error_attrs_passthrough,width=80)]))
			raise Exception('some calculation specs were not saved: %s'%unaccounted)

		#---the following storage routine was previously known as "version 2" and is now the default
		dat_fn,spec_fn,base_name_indexed = self.get_new_dat_name(post.basename())
		spec_fn_full,dat_fn_full = [os.path.join(self.paths['post_data_spot'],f) for f in [spec_fn,dat_fn]]
		for fn in [dat_fn,spec_fn]:
			if os.path.isfile(dat_fn): raise Exception('file %s exists'%fn)
		#---save the results
		self.store(obj=result,name=dat_fn,path=self.paths['post_data_spot'],attrs=attrs,verbose=True)
		#---write a lightweight "spec" file, always paired with dat file
		with open(spec_fn_full,'w') as fp: 
			fp.write(json.dumps(post.specs))
		if not os.path.isfile(spec_fn_full) and os.path.isfile(dat_fn_full):
			raise Exception('wrote %s without writing %s so you should delete the former'%(
				dat_fn_full,spec_fn_full))
		#---attach the result to the postdat listing to close the loop
		post.files = {'dat':base_name_indexed+'.dat','spec':base_name_indexed+'.spec'}
		#---note that incoming DatSpec objects have their calculation subdictionaries replaced with proper
		#---...calculation objects. We do the replacement here to stay consistent.
		post.specs['calc'] = job.calc
		self.postdat.toc[base_name_indexed] = post

	###---INTERFACES

	def show_specs(self):
		"""
		Print specs.
		"""
		asciitree(dict([(key,val.specs['specs']) for key,val in self.postdat.posts().items()]))

	def plot(self,plotname,plot_call,meta=None,plotspecs=None):
		"""
		Plot something. The plot_call flag sets the version, incoming from the user-facing plot function.
		Note we have two modes. The version 1 mode was designed to call a header script which supervised the 
		execution of the plot. For a number of reasons (this required a second invocation of the workspace,
		and left a lot of control up the header script), the version 2 mode executes everything here, in a
		hopefully more transparent way.
		"""
		if plot_call==1: self.plot_legacy(plotname,meta=meta)
		elif plot_call==2: self.plot_supervised(plotname,meta=meta,plotspecs=plotspecs)
		#---when the workspace is instantiated in header.py we do not need to run the plot
		elif plot_call==False: pass
		else: raise Exception('invalid plot mode %s'%plot_call)
		
	def plot_legacy(self,plotname,meta=None):
		"""Legacy plotting mode."""
		plots = self.specs.get('plots',{})
		#---we hard-code the plot script naming convention here
		script_name = self.find_script('plot-%s'%plotname)
		if not os.path.isfile(script_name):
			raise Exception('cannot find script %s'%script_name)
		if plotname in plots: plotspec = plots[plotname]
		else: 
			#---previously required a plots entry however the following code makes a default plot
			#---...object for this plotname, assuming it is the same as the calculation
			try:
				plotspec = {'calculation':plotname,
					'collections':self.calcs[plotname]['collections'],
					'slices':self.calcs[plotname]['slice_name']}
				print('[NOTE] there is no %s entry in plots so we are using calculations'%plotname)
			except Exception as e: 
				raise Exception('you should add %s to plots '%plotname+'since we could not '
					'formulate a default plot for that calculation.')			
		header_script = 'omni/base/header.py'
		meta_out = ' '.join(meta) if type(meta)==list else ('null' if not meta else meta)
		#---custom arguments passed to the header so it knows how to execute the plot script
		#bash('./%s %s %s %s %s'%(header_script,script_name,plotname,'plot',meta_out))
		bash('./%s %s %s %s'%(header_script,script_name,plotname,meta_out))

	def plot_supervised(self,plotname,meta=None,**kwargs):
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

	def pipeline(self,name,plot_call=False,meta=None):
		"""
		Plot something.
		! Get this out of the workspace.
		"""
		#---we hard-code the pipeline script naming convention here
		script_name = self.find_script('pipeline-%s'%name)
		header_script = 'omni/base/header.py'
		#---custom arguments passed to the header so it knows how to execute the plot script
		meta_out = ' '.join(meta) if type(meta)==list else ('null' if not meta else meta)
		if plot_call: bash('./%s %s %s %s'%(header_script,script_name,name,meta_out))

	def load(self,name,cwd=None,verbose=False,exclude_slice_source=False,filename=False):
		"""Wrap load which must be used by other modules."""
		return load(name,cwd=cwd,verbose=verbose,exclude_slice_source=exclude_slice_source,filename=filename)

	def plotload(self,plotname,status_override=False,sns=None,whittle_calc=None):
		"""
		Get data for plotting programs.
		"""
		#---usually plotload is called from plots or pipelines but we allow an override here
		if status_override==True: self.plot_status = plotname
		#---get the calculations from the plot dictionary in the meta files
		plot_spec = self.plots.get(plotname,None)
		if not plot_spec:
			print('[NOTE] cannot find plot %s in the metadata. using the entry from calculations'%plotname)
			if plotname not in self.calcs:
				raise Exception(
					'plot "%s" is missing from both plots and calculations in the metadata'%plotname)
			#---if plotname is absent from plots we assemble a default plot based on the calculation
			plot_spec = {'calculation':plotname,
				'collections':self.calcs[plotname]['collections'],
				'slices':self.calcs[plotname]['slice_name']}
		#---special pass-through for the calculation specs used by the collect_upstream function
		if whittle_calc is not None: calcs = whittle_calc
		#---if the plot spec is a dictionary, it needs no changes
		elif 'calculation' in plot_spec and type(plot_spec['calculation'])==dict: 
			calcs = plot_spec['calculation']
		#---strings and lists of calculations require further explication from other elements of meta
		else:
			if 'calculation' not in plot_spec: plot_spec_list = [plotname]
			elif type(plot_spec['calculation']) in str_types: plot_spec_list = [plot_spec['calculation']]
			elif type(plot_spec['calculation'])==list: plot_spec_list = plot_spec['calculation']
			else: raise Exception('dev')
			#---fill in each upstream calculation
			calcs = dict([(c,self.calcs[c]) for c in plot_spec_list])
			#---! note to ryan. in the case of the ocean project with standard naming, the length here matters
		#---in rare cases the user can override the simulation names
		sns_this = self.sns() if not sns else sns
		#---previous codes expect specs to hold the specs in the calcs from plotload
		#---we store the specs for the outgoing calcs variable and then later accumulate some extra
		#---...information for each simulation
		calcs_reform = dict(calcs=dict([(c,{'specs':v}) for c,v in calcs.items()]),extras={})
		#---cache the upstream jobs for all calculations
		upstream_jobs = self.prepare_calculations(calcnames=calcs.keys())
		#---data indexed by calculation name then simulation name
		data = dict([(calc_name,{}) for calc_name in calcs.keys()])
		#---loop over calculations and dig up the right ones
		for calc_name,specs in calcs.items():
			calc = self.calc_meta.find_calculation(calc_name,specs)
			#---! correct to loop over this? is this set by the plotname?
			for sn in sns_this:
				job_filter = [j for j in upstream_jobs if j.calc==calc and j.sn==sn]
				if len(job_filter)>1:
					raise Exception('found too many matching jobs: %s'%job_filter)
				elif len(job_filter)==0:
					raise Exception('you may be asking for calculations that have not yet been run? '
						'we cannot find matching jobs for calculation '
						'%s with specs %s and simulation %s'%
						(calc_name,specs,sn))
				else: 
					job = job_filter[0]
					if job.result not in self.postdat.toc:
						asciitree({'missing calculation: %s'%job.calc.name:job.calc.__dict__['stub']})
						raise Exception(
							'cannot find calculation result (see above) in the requested '+
							'post-processing data. are you sure that all of your calculations are complete?')
					status('fetching %s'%self.postdat.toc[job.result].files['dat'],tag='load')
					data[calc_name][job.sn] = {'data':self.load(
						name=self.postdat.toc[job.result].files['dat'],
						cwd=self.paths['post_data_spot'])}
					#---save important filenames
					#---! note that this is currently only useful for gro/xtc files
					#---pass along the slice name in case the plot or post-processing functions need it
					calcs_reform['extras'][sn] = dict(
						slice_path=job.slice.name)
					#---! added slice name here for curvature coupling
					try: calcs_reform['calcs'][calc_name]['specs']['slice_name'] = job.slice.slice_name
					except: pass
					#---if we only have one calculation we elevate everything for convenience
		if len(data.keys())==1: 
			only_calc_name = data.keys()[0]
			return (data[only_calc_name],
				dict(calcs=calcs_reform.pop('calcs')[only_calc_name],**calcs_reform))
		else: return data,calcs_reform

	def sns(self):
		"""
		For backwards compatibility with plot programs, we serve the list of simulations for a 
		particular plot using this function with no arguments.
		"""
		if not self.plot_status and not self.pipeline_status:
			raise Exception('you can only call WorkSpace.sns if you are plot')
		elif self.plot_status: this_status = self.plot_status
		elif self.pipeline_status: this_status = self.pipeline_status
		#---consult the calculation if the plot does no specify collections
		if this_status not in self.plots:
			if this_status not in self.calcs: 
				raise Exception('missing %s from both plots and calcs. try adding it to plots'%this_status)
			collections = str_or_list(self.calcs[this_status]['collections'])
		else:
			if 'collections' not in self.plots[this_status]:
				calc_specifier = self.plots[this_status]['calculation']
				if type(calc_specifier)==dict:
					collection_names = [tuple(sorted(str_or_list(self.calcs[c]['collections']))) 
						for c in calc_specifier]
					collection_sets = list(set([tuple(sorted(str_or_list(self.calcs[c]['collections']))) 
						for c in calc_specifier]))
					if len(collection_sets)>1: 
						raise Exception('conflicting collections for calculations %s'%calc_specifier.keys())
					else: collections = list(collection_sets[0])
				else: 
					if type(calc_specifier)==list and len(calc_specifier)==1:
						collections = str_or_list(self.calcs[calc_specifier[0]]['collections'])
					elif type(calc_specifier)==list: 
						collections_several = [str_or_list(self.calcs[c]['collections']) 
							for c in calc_specifier]
						if any([set(i)!=set(collections_several[0]) for i in collections_several]):
							raise Exception('upstream collections are not equal: %s'%collections_several+
								'we recommend setting `collections` explicitly in the plot metadata')
						else: collections = str_or_list(self.calcs[calc_specifier[0]]['collections'])
					else: collections = str_or_list(self.calcs[calc_specifier]['collections'])
			else: collections = str_or_list(self.plots[this_status]['collections'])
		try: sns = sorted(list(set([i for j in [self.vars['collections'][k] 
			for k in collections] for i in j])))
		except Exception as e: 
			raise Exception(
			'error compiling the list of simulations from collections: %s'%collections)
		return sns

	def get_gmx_sources(self,calc,sn):
		"""Repeatable procedure for extracting source files from the calculation spect."""
		#---get gro and xtc file
		gro,xtc = [os.path.join(self.postdir,'%s.%s'%(calc['extras'][sn]['slice_path'],suf))
			for suf in ['gro','xtc']]
		#---get the tpr from the raw data
		tpr = self.raw.get_last(sn,subtype='tpr')
		return dict(gro=gro,tpr=tpr,xtc=xtc)

	def collect_upstream_calculations_over_loop(self,plotname,calcname=None):
		"""
		Some plotting and analysis benefits from checking all calculations in an upstream loop (which is 
		contrary to the original design of )
		"""
		plotspecs = self.plots.get(plotname,self.calcs.get(plotname,{})).get('specs',{})
		if not calcname: calcname = plotspecs.get('calcname',plotname)
		#---load the canonical upstream data that would be the focus of a plot in standard omnicalc
		#---! load the upstream data according to the plot. note that this may fail in a loop hence needs DEV!
		try: data,calc = self.plotload(plotname)
		except:
			data,calc = None,None
			status('failed to load a single upstream calculation however this plot script has requested '
				'all of them so we will continue with a warning. if you have downstream problems consider '
				'adding a specific entry to plots to specify which item in an upstream loop you want',
				tag='warning')
		#---in case there is no plot entry in the metadata we copy it
		if plotname not in self.plots: self.plots[plotname] = copy.deepcopy(self.calcs[calcname])
		#---load other upstream data
		#---get all upstream curvature sweeps
		upstreams,upstreams_stubs = self.calc_meta.unroll_loops(self.calcs[calcname],return_stubs=True)
		datas,calcs = {},{}
		#---loop over upstream calculations and load each specifically, using plotload with whittle_calc
		for unum,upstream in enumerate(upstreams_stubs):
			#---use the whittle option to select a particular calculation
			dat,cal = self.plotload(calcname,whittle_calc={calcname:upstream['specs']})
			#---! this is specific to curvature coupling
			try: tag = upstreams_stubs[unum]['specs']['design']
			except: tag = str(unum)
			if type(tag)==dict: tag = 'v%d'%unum
			datas[tag] = dict([(sn,dat[sn]['data']) for sn in self.sns()])
			calcs[tag] = dict([(sn,cal) for sn in self.sns()])
			#---! also specific to curvature coupling
			#---! ... this design question must be resolved
			try:
				for sn in self.sns(): 
					calcs[tag][sn]['calcs']['specs']['design'] = upstreams[unum]['specs']['design']
			except: pass
		#---singluar means the typical "focus" of the upstream calculation, plural is everything else
		return dict(datas=datas,calcs=calcs,data=data,calc=calc)

###---INTERFACE

def compute(meta=None,confirm=False,kill_switch=None):
	"""
	Expose the workspace to the command line.
	Note that the kill_switch is a dummy which is caught by makeface.py.
	"""
	work = WorkSpace(meta=meta,confirm_compute=confirm)

def plot(name,*args,**kwargs):
	"""
	Plot something
	"""
	#---pop off protected keywords and everything else is passed through
	meta = kwargs.pop('meta',None)
	#---currently we have two execution modes: the standard interactive one and a non-interactive version
	#---...where the user specifies exactly which plot to create
	#---we infer the mode from the presence of args
	plot_call = 2 if args or kwargs else 1
	#---direct the workspace that this is a plot and hence avoid other calculations
	work = WorkSpace(plot=name,plot_call=plot_call,meta=meta,plotspecs={'kwargs':kwargs,'args':args})

def pipeline(name,meta=None):
	"""
	Plot something
	"""
	#---! avoid unnecessary calculations for the plot we want
	work = WorkSpace(pipeline=name,plot_call=True,meta=meta)

def look(method=None,**kwargs):
	"""
	Inspect the workspace. Send a method name and we will run it for you.
	"""
	header_script = 'omni/base/header_look.py'
	#---! wish we could send more flags through without coding them here
	bash('python -iB %s %s'%(header_script,'null' if not method else method))

def store(obj,name,path,attrs=None,print_types=False,verbose=True):
	"""
	Use h5py to store a dictionary of data.
	"""
	import h5py
	#---! cannot do unicode in python 3. needs fixed
	if type(obj) != dict: raise Exception('except: only dictionaries can be stored')
	if os.path.isfile(path+'/'+name): raise Exception('except: file already exists: '+path+'/'+name)
	path = os.path.abspath(os.path.expanduser(path))
	if not os.path.isdir(path): os.mkdir(path)
	fobj = h5py.File(path+'/'+name,'w')
	for key in obj.keys(): 
		if print_types: 
			print('[WRITING] '+key+' type='+str(type(obj[key])))
			print('[WRITING] '+key+' dtype='+str(obj[key].dtype))
		#---python3 cannot do unicode so we double check the type
		#---! the following might be wonky
		if (type(obj[key])==np.ndarray and re.match('^str|^unicode',obj[key].dtype.name) 
			and 'U' in obj[key].dtype.str):
			obj[key] = obj[key].astype('S')
		try: dset = fobj.create_dataset(key,data=obj[key])
		except: 
			#---multidimensional scipy ndarray must be promoted to a proper numpy list
			try: dset = fobj.create_dataset(key,data=obj[key].tolist())
			except: raise Exception("failed to write this object so it's probably not numpy"+
				"\n"+key+' type='+str(type(obj[key]))+' dtype='+str(obj[key].dtype))
	if attrs != None: fobj.create_dataset('meta',data=np.string_(json.dumps(attrs)))
	if verbose: status('[WRITING] '+path+'/'+name)
	fobj.close()

def load(name,cwd=None,verbose=False,exclude_slice_source=False,filename=False):
	"""
	Get binary data from a computation.
	"""
	if not cwd: cwd,name = os.path.dirname(name),os.path.basename(name)
	cwd = os.path.abspath(os.path.expanduser(cwd))
	fn = os.path.join(cwd,name)
	if not os.path.isfile(fn): raise Exception('[ERROR] failed to load %s'%fn)
	data = {}
	import h5py
	rawdat = h5py.File(fn,'r')
	for key in [i for i in rawdat if i!='meta']: 
		if verbose:
			print('[READ] '+key)
			print('[READ] object = '+str(rawdat[key]))
		data[key] = np.array(rawdat[key])
	if 'meta' in rawdat: 
		if sys.version_info<(3,0): out_text = rawdat['meta'].value
		else: out_text = rawdat['meta'].value.decode()
		attrs = json.loads(out_text)
	else: 
		print('[WARNING] no meta in this pickle')
		attrs = {}
	if exclude_slice_source:
		for key in ['grofile','trajfile']:
			if key in attrs: del attrs[key]
	for key in attrs: data[key] = attrs[key]
	if filename: data['filename'] = fn
	rawdat.close()
	return data

def audit_plots(filename='audit.yaml'):
	"""
	Keeping track of plotting test sets
	"""
	#---retrieve
	with open(os.path.join('calcs','specs',filename)) as fp: audit_data = yaml.load(fp.read())
	plotnames = [re.match('^plot-(.+)\.py',os.path.basename(fn)).group(1) 
		for fn in glob.glob('calcs/plot-*') if re.match('^plot-(.+)\.py',os.path.basename(fn))]
	#---assume audits just have passing
	passing = audit_data.get('passing',{})
	asciitree(dict(passing=passing))
	#---not tested
	untested = sorted([i for i in plotnames if i not in passing])
	asciitree(dict(untested=untested))
