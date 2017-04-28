#!/usr/bin/env python

import os,sys,re,glob,copy,json,time,tempfile
from config import read_config,bash

from base.tools import catalog,delve,str_or_list,status
from base.hypothesis import hypothesis
from maps import NamingConvention,PostDat,ComputeJob,Calculation,Slice,SliceMeta,DatSpec,CalcMeta
from datapack import asciitree,delve,delveset
#---typical first encounger with super-python reqs so we warn the user if they have no good env yet
msg_needs_env = ('\n[ERROR] failed to load a key requirement (yaml) '
	'which means you probably need to source the environment. '
	'go to the factory root and run e.g. `source env/bin/activate py2`')
try: import yaml,h5py
except: raise Exception(msg_needs_env)
import numpy as np

str_types = [str,unicode] if sys.version_info<(3,0) else [str]

class WorkSpace:

	"""
	User-facing calculation management.
	"""

	#---hard-coded paths for specs files
	specs_path = 'calcs','specs','*.yaml'
	#---versioning manages fairly generic data structures written to disk
	versioning = {'spec_file':2}
	#---! currently hard-coded
	nprocs = 4

	def __init__(self,plot=None,plot_call=False,pipeline=None,meta=None,
		confirm_compute=False,cwd=None):
		"""
		Prepare the workspace.
		"""
		if not cwd: self.cwd = os.getcwd()
		else: self.cwd = os.path.join(cwd,'')
		if not os.path.isdir(self.cwd): raise Exception('invalid cwd for this WorkSpace: %s'%self.cwd)
		if plot and pipeline: raise Exception('only plot or pipeline')
		#---read the omnicalc config and specs files
		self.config = read_config(cwd=self.cwd)
		#---unpack the paths right into the workspace for calculation functions
		#---add paths here for backwards compatibility at the plotting stage
		self.paths = dict([(key,self.config[key]) for key in ['post_plot_spot','post_data_spot']])
		meta_incoming = meta
		#---check the config.py for this omnicalc to find restrictions on metafiles
		#---...note that this allows us to avoid using git branches and the meta flag in the CLI for 
		#---...managing multiple meta files.
		if not meta_incoming and self.config.get('meta_filter',None):
			#---set_config forces meta_filter to be a list. each can be a glob. the path is relative to 
			#---...the calcs/specs folder since that is the only acceptable location for meta files
			#---...we detect meta files here and send them as a list, otherwise read_specs gets a string
			#---...from the CLI which is a glob with the full path to calcs/specs
			meta_incoming = [i for j in [glob.glob(os.path.join(self.cwd,'calcs','specs',g)) 
				for g in self.config['meta_filter']] for i in j]
		#---read the specs according to incoming meta flags
		self.specs = self.read_specs(meta=meta_incoming)
		#---prepare a namer from the global omni_namer
		self.namer = NamingConvention(
			short_namer=self.meta.get('short_namer',None),
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
		self.slice_meta = SliceMeta(self.slices,work=self)
		#---get the right calculation order
		self.calc_order = self.infer_calculation_order()
		#---plot and pipeline skip calculations and call the target script
		self.plot_status,self.pipeline_status = plot,pipeline
		if not plot and not pipeline:
			#---match calculation codes with target slices
			self.jobs = self.prepare_calculations()
			self.compute(confirm=confirm_compute)
		elif plot: self.plot(plotname=plot,plot_call=plot_call,meta=meta_incoming)
		elif pipeline: self.pipeline(name=pipeline,plot_call=plot_call,meta=meta_incoming)

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

	def read_specs(self,meta=None,merge_method='careful'):
		"""
		Read and interpret calculation specs.
		Lifted directly from old workspace.load_specs.
		"""
		if merge_method!='careful': raise Exception('dev')
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
		if not specs_files: 
			raise Exception('cannot find meta files')
		allspecs = []
		for fn in specs_files:
			with open(fn) as fp: 
				if (merge_method != 'override_factory' or 
					not re.match('^meta\.factory\.',os.path.basename(fn))):
					allspecs.append(yaml.load(fp.read()))
		if not allspecs: return {}
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
		else: raise Exception('\n[ERROR] unclear meta specs merge method %s'%merge_method)
		self.vars = specs.get('variables',{})
		specs_unpacked = self.variable_unpacker(specs)
		self.meta = specs.get('meta',{})
		#---for backwards compatibility we put collections in vars
		if 'collections' in self.vars: raise Exception('collection already in vars')
		self.vars['collections'] = specs['collections']
		#---expose the plots
		self.plots = specs['plots']
		#---plots can be aliased to themselves
		for key,val in self.plots.items():
			if type(val) in str_types: 
				if key not in self.plots:
					raise Exception('plot alias from %s to %s is invalid'%(key,val))
				else: self.plots[key] = copy.deepcopy(self.plots[val])
		self.plotdir = self.paths['post_plot_spot']
		return specs_unpacked

	def infer_calculation_order(self):
		"""
		Needs tested and confirmed possibly with a safety check of some kind to avoid infinite recursion.
		Lifted directly from omnicalc workspace action function.
		"""
		#---infer the correct order for the calculation keys from their upstream dependencies
		upstream_catalog = [i for i,j in catalog(self.calcs) if 'upstream' in i]
		#---if there are no specs required to get the upstream data object the user can either 
		#---...use none/None as a placeholder or use the name as the key as in "upstream: name"
		for uu,uc in enumerate(upstream_catalog):
			if uc[-1]=='upstream': upstream_catalog[uu] = upstream_catalog[uu]+[delve(self.calcs,*uc)]
		depends = {t[0]:[t[ii+1] for ii,i in enumerate(t) if ii<len(t)-1 and t[ii]=='upstream'] 
			for t in upstream_catalog}
		calckeys = [i for i in self.calcs if i not in depends]
		#---check that the calckeys has enough elements 
		list(set(calckeys+[i for j in depends.values() for i in j]))			
		#---paranoid security check for infinite loop
		start_time = time.time()
		while any(depends):
			ii,i = depends.popitem()
			if all([j in calckeys for j in i]) and i!=[]: calckeys.append(ii)
			else: depends[ii] = i
			if time.time()>(start_time+10): 
				raise Exception('possibly loop in your graph of dependent calculations')
		return calckeys

	def unroll_loops_MOVED(self,details,return_stubs=False):
		"""
		The jobs list may contain loops. We "unroll" them here.
		"""
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
		if sweeps == []: new_calcs = [copy.deepcopy(details)]
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
		search = [fn for fn in fns if re.match('^%s\.py'%name,os.path.basename(fn))]
		if len(search)==0: 
			raise Exception('\n[ERROR] cannot find %s.py'%name)
		elif len(search)>1: raise Exception('\n[ERROR] redundant matches: %s'%str(search))
		#---manually import the function
		return search[0]

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
		if not hasattr(mod,calcname): raise Exception(('performing calculation "%s" and we found '+
			'%s but it does not contain a function named %s')%(calcame,script_name,calcname))
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

	def infer_group(self,calc):
		"""
		"""
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
		"""
		back_namer = dict([(self.namer.short_namer(key),key) for key in self.slices.keys()])
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
					#if calcname=='ion_binding_combinator':
					#	import ipdb;ipdb.set_trace()
					if sc['upstream'][calcname]:
						try:
							sc['upstream'][calcname].items()
						except:
							import ipdb;ipdb.set_trace()
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

	def chase_upstream_newish(self,specs,warn=False):
		"""
		"""
		refolded = {}
		unfolded = list(catalog(specs))
		for keylist,child_val in unfolded:
			if 'upstream' not in keylist: delveset(refolded,*keylist,value=child_val)
			elif 'upstream' in keylist and child_val==None: continue
			else:
				#---get the positions where we have an 'upstream'
				ups_inds = [ii for ii,i in enumerate(keylist) if i=='upstream']
				#---make sure they are above something in the dictionary
				if not all([len(keylist)>ii+1 and keylist[ii+1]!='upstream' for ii in ups_inds]):
					raise Exception('incorrect use of "upstream"')
				#---not sure how to handle raw specs in the upstream nesting
				if type(child_val) not in str_types: raise Exception('dev')
				#---we work backwards through the list of upstream signifiers
				for pivot in ups_inds[::-1]:
					#---at each encounter with "upstream" we find the upstream calculation
					#---! and hold it?
					calcname,keyword = keylist[pivot+1:]
					#---currently we expect all "upstream" references point to keywords that lie inside a
					#---...loop section of another calculation (however we could also try to make this
					#---...fully explicit). so we do not "unroll" the loop and instead use the keyword
					if calcname not in self.calcs: 
						raise Exception('searching for upstream data for spec "%s" '%specs+
							'but calculation %s is not in the metadata'%calcname)
					if 'specs' not in self.calcs[calcname]:
						raise Exception('searching for upstream data for spec "%s" '%specs+
							'and the calculation %s has no specs in the metadata'%calcname)
					if keyword not in self.calcs[calcname]['specs']:
						raise Exception('searching for upstream data for spec "%s" '%specs+
							'and the calculation %s has no spec named %s in the metadata'%(calcname,keyword))
					if keyword not in self.calcs[calcname]['specs']:
						raise Exception('searching for upstream data for spec "%s" '%specs+
							'and we cannot find the loop keyword under %s,%s '%(calcname,keyword)+
							'DEVNOTE: this is a development opportunity. associate-by-value instead of '+
							'just associate-by-reference!')
					if not child_val in self.calcs[calcname]['specs'][keyword]['loop']:
						raise Exception('searching for upstream data for spec "%s" '%specs+
							'and we could not find the reference %s in the loop for %s,%s'%(
							child_val,calcname,keyword))
					expansion = self.calcs[calcname]['specs'][keyword]['loop'][child_val]
					refolded.update(**expansion)
					#self.calcs[calcname]['specs'][keyword]
					#[item for item in self.unroll_loops(self.calcs[calcname]) if keyword in item['specs']]
				import ipdb;ipdb.set_trace()
				#---work up from the leaf of the tree
				#for pivot in ups_inds[::-1]:
				#	keys_not_upstream = [i for i in self.calcs[keylist[pivot+1]]['specs'] if i!='upstream']
				#	import ipdb;ipdb.set_trace()
				#---convert the upstream commands into instructions
				
	def store(self,obj,name,path,attrs=None,print_types=False,verbose=True):
		"""
		Use h5py to store a dictionary of data.
		"""
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
			if (type(obj[key])==np.ndarray and re.match('^str',obj[key].dtype.name) 
				and 'U' in obj[key].dtype.str):
				obj[key] = obj[key].astype('S')
			try: dset = fobj.create_dataset(key,data=obj[key])
			except: 
				raise Exception("failed to write this object so it's probably not numpy"+
					"\n"+key+' type='+str(type(obj[key]))+' dtype='+str(obj[key].dtype))
		if attrs != None: fobj.create_dataset('meta',data=np.string_(json.dumps(attrs)))
		if verbose: status('[WRITING] '+path+'/'+name)
		fobj.close()

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
				#---put everything in a calculation


				#---note that the calc object here includes the specs with out expansions
				#---...but they can be cross-referenced back to the stub to get the full calculation specs
				#import ipdb;ipdb.set_trace()
				####### YOU ARE HERE NOW. somehow 
				"""
				you need to get the calculation-metacalc pair from self.calc_meta
				then later you check incoming calcs from datspec files against e.g. calc_meta if they are 
				legacy.
				"""
				#import ipdb;ipdb.set_trace()
				#---get the calculation from the master listing in the CalcMeta class
				#...???
				#this_calculation = Calculation(work=self,name=calckey,group=group,
				#	**dict([(i,j) for i,j in calc.items() if i!='group']))

				#---loop over simulations
				for sn in sns:
					#---! previously: prepare a slice according to the specification in the calculation
					if False:
						request_slice = Slice(sn=sn,slice_name=slice_name,group=group,work=self)
					if False:
						#---! get a proper slice?
						if (slice_name,group) not in self.slice_meta.slices[sn]:
							asciitree(self.slice_meta.slices)
							raise Exception('see slices (meta) above. '+
								'cannot find slice for simulation %s: %s,%s'%(sn,slice_name,group))
						request_slice = self.slice_meta.slices[sn][(slice_name,group)]
					request_slice = self.slice_meta.get_slice(sn=sn,slice_name=slice_name,group=group)
					#---join the slice and calculation in a job
					jobs.append(ComputeJob(sl=request_slice,calc=calc,work=self))
		#---this function populates workspace.jobs but also has other uses
		#---! which other uses?
		return jobs

	def job_print(self,job):
		"""
		"""
		asciitree({job.sn:dict(**{job.calc.name:dict(
			specs=job.calc.specs['specs'],slice=job.slice.flat())})})

	def compute(self,confirm=False):
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
		if confirm:
			print('[NOTE] there are %d pending jobs'%len(pending))
			print('[QUESTION] okay to continue?')
			import ipdb;ipdb.set_trace()
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
			outgoing['upstream'][calcname] = self.load(
				name=self.postdat.toc[result].files['dat'],cwd=self.paths['post_data_spot'])
		#---for backwards compatibility we decorate the kwargs with the slice name and group
		outgoing.update(slice_name=job.slice.slice_name,group=job.slice.group)

		#---THE MOST IMPORTANT LINES IN THE WHOLE CODE (here we call the calculation function)
		if job.calc.specs['uptype']=='simulation':
			if job.slice.flat()['slice_type']=='standard':
				if not self.postdat.toc[job.slice.name].__dict__['namedat']['dat_type']=='gmx':
					raise Exception('dat_type is not gmx')
				struct_file,traj_file = [self.postdat.parser[('standard','gmx')]['d2n']%dict(
					suffix=suffix,**job.slice.flat()) for suffix in ['gro','xtc']]
				struct_file,traj_file = [os.path.join(self.paths['post_data_spot'],i) 
					for i in [struct_file,traj_file]]
				#---! use explicit kwargs to the function however it would be useful to 
				#---! ...introspect on the arguments e.g. grofile vs struct
				result,attrs = function(grofile=struct_file,trajfile=traj_file,**outgoing)
			elif job.slice.flat()['slice_type']=='readymade_namd':
				#---no dat_type for readymade_namd unlike standard/gmx
				struct_file = os.path.join(self.paths['post_data_spot'],job.slice.flat()['psf'])
				traj_file = [os.path.join(self.paths['post_data_spot'],i) for i in 
					str_or_list(job.slice.flat()['dcds'])]
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

		#---the following storage routine was previously known as "version 2" and is now the default. ...
		#---! finish this comment
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
		"""
		asciitree(dict([(key,val.specs['specs']) for key,val in self.postdat.posts().items()]))

	def plot(self,plotname,plot_call=False,meta=None):
		"""
		Plot something.
		! Get this out of the workspace.
		"""
		plots = self.specs['plots']
		if plotname not in plots: 
			raise Exception('cannot find plot %s in the plots section of the meta files'%plotname)
		plotspec = plots[plotname]
		#---we hard-code the plot script naming convention here
		script_name = self.find_script('plot-%s'%plotname)
		header_script = 'omni/base/header.py'
		meta_out = ' '.join(meta) if type(meta)==list else ('null' if not meta else meta)
		#---custom arguments passed to the header so it knows how to execute the plot script
		if plot_call: bash('./%s %s %s %s %s'%(header_script,script_name,plotname,'plot',meta_out))

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
		if plot_call: bash('./%s %s %s %s %s'%(header_script,script_name,name,'pipeline',meta_out))

	def load(self,name,cwd=None,verbose=False,exclude_slice_source=False,filename=False):
		"""
		Get binary data from a computation.
		"""
		if not cwd: cwd,name = os.path.dirname(name),os.path.basename(name)
		cwd = os.path.abspath(os.path.expanduser(cwd))
		fn = os.path.join(cwd,name)
		if not os.path.isfile(fn): raise Exception('[ERROR] failed to load %s'%fn)
		data = {}
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

	def plotload(self,plotname):
		"""
		"""
		#---get the calculations from the plot dictionary in the meta files
		plot_spec = self.plots.get(plotname,None)
		if not plot_spec: 
			raise Exception('cannot find plot %s in the plots section of the meta files'%plotname)
		calcnames = str_or_list(plot_spec['calculation'])
		#---instead of repeating the logic, we run the calculation prep to get the jobs out
		#---...this mimics the upstream_jobs section of compute_single above
		upstream_jobs = self.prepare_calculations(calcnames=calcnames)
		if any([not j.result for j in upstream_jobs]):
			#---! this exception does not route through tracebacker because we call python with bash in plot
			raise Exception('at least one of the jobs is missing a result. did you forget `make compute`?')

		#---load the data
		#data = dict([(sn,{}) for sn in sorted(list(set([j.sn for j in upstream_jobs])))])
		#---check for redundant incoming data, in which case the user needs to intervene in the plot meta.
		#---...we always loop over self.sns because it provides the simulations for this plot

		#---use the calculation finder to get the right calc
		#---! RYAN YOU HAVE TO ALLOW NO SPECS IN THE PLOT. TEST THIS !!!!!!!!!!!!!!!!!!
		#---! ... find_calculation needs a calculation, but calcnames may be longer than 1. DEVELOP THAT!
		#if len(calcnames)>1: 
		#	import ipdb;ipdb.set_trace()
		#	raise Exception('development error. need multiple calcs for a plot')
		#else: calcname = calcnames[0]
		#---! this is the part where we treat plots like calculations to get the right upstream data
		#---! note that the return below uses calcname as well!
		#---! moved below 
		#calc = self.calc_meta.find_calculation(calcname,plot_spec.get('specs',{}))

		#---prepare the outgoing data in data, indexed by calcname, but elevate it if only one, at the end
		data = dict([(calcname,dict([(sn,{}) for sn in self.sns()])) for calcname in calcnames])
		#---loop over data for each calculation
		for calcname in calcnames:
			calc = self.calc_meta.find_calculation(calcname,plot_spec.get('specs',{}))
			for snum,sn in enumerate(self.sns()):
				job_filter = [j for j in upstream_jobs if j.calc==calc and j.sn==sn]
				if len(job_filter)>1: raise Exception('multiple upstream jobs for plot %s and simulation %s. '%(
					plotname,sn)+'remember that plots resemble calculations. upstream loops require you to '+
					'specify the keyword for the item in the loop that you want.')
				elif len(job_filter)==0: 
					raise Exception('cannot locate upstream job for plot %s and simulation %s'%(plotname,sn))
				upstream_job = job_filter[0]
				status('reading %s'%self.postdat.toc[upstream_job.result].files['dat'],
					tag='load',i=snum,looplen=len(self.sns()))
				data[calcname][upstream_job.sn]['data'] = self.load(
					name=self.postdat.toc[upstream_job.result].files['dat'],
					cwd=self.paths['post_data_spot'])

		#---! NEED TO DO SELECTIONS HERE. RYAN DO THIS FOR LIPID AREAS ASAP!
		if False:
			for unum,upstream_job in enumerate(upstream_jobs):
				status('reading %s\n'%self.postdat.toc[upstream_job.result].files['dat'],
					tag='load',i=unum,looplen=len(upstream_jobs))
				data[upstream_job.sn]['data'] = self.load(
					name=self.postdat.toc[upstream_job.result].files['dat'],
					cwd=self.paths['post_data_spot'])

		#---for backwards compatibility we always send data with the plot spec however this is redundant
		#---! the plot spec points to the upstream data but they are always accessible in the workspace
		if len(calcnames)==1: return data[calcnames[0]],self.calcs[calcname]
		#---if we have multiple calcnames we send the data in a dictionary otherwise we promote
		else: return data,self.calcs


	def plotload2(self,plotname):
		"""
		"""
		#---get the calculations from the plot dictionary in the meta files
		plot_spec = self.plots.get(plotname,None)

		#---!!!!!!!!!!!
		plots_with_calcs = copy.deepcopy(self.calcs)
		plots_with_calcs.update(**self.plots)
		plot_meta = CalcMeta(plots_with_calcs,work=self)
		#import ipdb;ipdb.set_trace()

		if not plot_spec: 
			raise Exception('cannot find plot %s in the plots section of the meta files'%plotname)
		calcnames = str_or_list(plot_spec['calculation'])
		#---instead of repeating the logic, we run the calculation prep to get the jobs out
		#---...this mimics the upstream_jobs section of compute_single above
		upstream_jobs = self.prepare_calculations(calcnames=calcnames)
		if any([not j.result for j in upstream_jobs]):
			#---! this exception does not route through tracebacker because we call python with bash in plot
			raise Exception('at least one of the jobs is missing a result. did you forget `make compute`?')
		#---prepare the outgoing data in data, indexed by calcname, but elevate it if only one, at the end
		data = dict([(calcname,dict([(sn,{}) for sn in self.sns()])) for calcname in calcnames])
		#---loop over data for each calculation
		for calcname in calcnames:
			#---now search in plot_meta instead of self.calc_meta
			#calc = plot_meta.find_calculation(calcname,plot_spec.get('specs',{}))
			calc = self.calc_meta.find_calculation(calcname,plot_spec.get('specs',{}))
			for snum,sn in enumerate(self.sns()):
				job_filter = [j for j in upstream_jobs if j.calc==calc and j.sn==sn]
				if len(job_filter)>1: 
					raise Exception('multiple upstream jobs for plot %s and simulation %s. '%(
					plotname,sn)+'remember that plots resemble calculations. upstream loops require you to '+
					'specify the keyword for the item in the loop that you want.')
				elif len(job_filter)==0: 
					import ipdb;ipdb.set_trace()
					raise Exception('cannot locate upstream job for plot %s and simulation %s'%(plotname,sn)+
						'. note that the calc.specs is %s'%calc.specs)
				upstream_job = job_filter[0]
				status('reading %s'%self.postdat.toc[upstream_job.result].files['dat'],
					tag='load',i=snum,looplen=len(self.sns()))
				data[calcname][upstream_job.sn]['data'] = self.load(
					name=self.postdat.toc[upstream_job.result].files['dat'],
					cwd=self.paths['post_data_spot'])
		#---for backwards compatibility we always send data with the plot spec however this is redundant
		#---! the plot spec points to the upstream data but they are always accessible in the workspace
		if len(calcnames)==1: return data[calcnames[0]],self.calcs[calcname]
		#---if we have multiple calcnames we send the data in a dictionary otherwise we promote
		else: return data,self.calcs

	def plotload3(self,plotname):
		"""
		goal: 
		pseudocode
			1. get the plot subdictionary
			2. for each calculation, go get the calculation
				if specs then expand the calculation
			3. 
			end. run self.load with an upstream job which is accurate
		WHY WUZ THIS SOOO HARD????
		"""
		#---get the calculations from the plot dictionary in the meta files
		plot_spec = self.plots.get(plotname,None)
		if not plot_spec: 
			import ipdb;ipdb.set_trace()
		if type(plot_spec['calculation']) in str_types:
			calcs = {plot_spec['calculation']:self.calcs[plot_spec['calculation']]}
		#---loop over calculations in the plot
		elif type(plot_spec['calculation'])!=dict: raise Exception('dev')
		else: calcs = plot_spec['calculation']
		#---cache the upstream jobs for all calculations
		upstream_jobs = self.prepare_calculations(calcnames=calcs.keys())
		#---data indexed by calculation name then simulation name
		data = dict([(calc_name,{}) for calc_name in calcs.keys()])
		#---loop over calculations and dig up the right ones
		for calc_name,specs in calcs.items():
			calc = self.calc_meta.find_calculation(calc_name,specs)
			#---! correct to loop over this? is this set by the plotname?
			for sn in self.sns():
				job_filter = [j for j in upstream_jobs if j.calc==calc and j.sn==sn]
				if len(job_filter)!=1:
					print('job_filter fail... ')
					import ipdb;ipdb.set_trace()
				else: 
					job = job_filter[0]
					data[calc_name][job.sn] = {'data':self.load(
						name=self.postdat.toc[job.result].files['dat'],
						cwd=self.paths['post_data_spot'])}
		#---previous codes expect specs to hold the specs in the calcs from plotload
		calcs_reform = dict([(c,{'specs':v}) for c,v in calcs.items()])
		if len(calcs.keys())==1: return data[calcs.keys()[0]],calcs_reform[calcs.keys()[0]]
		else: return data,calcs_reform

	def plotload_manual(self,calcname,specs):
		"""
		Temporary fix to manually load the plotload stuff.
		"""
		data = {}
		for snum,sn in enumerate(self.sns()[:1]):
			upstream_jobs = self.prepare_calculations(calcnames=[calcname])
			if len(upstream_jobs)!=0:
				print('oops')
				import ipdb;ipdb.set_trace()
			else: job = upstream_jobs[0]
			data[sn] = self.load(name=self.postdat.toc[job.result].files['dat'],
				cwd=self.paths['post_data_spot'])
		import ipdb;ipdb.set_trace()

	def sns(self):
		"""
		For backwards compatibility with plot programs, we serve the list of simulations for a 
		particular plot using this function with no arguments.
		"""
		if not self.plot_status:
			raise Exception('you can only call WorkSpace.sns if you are plot')
		collections = str_or_list(self.plots[self.plot_status]['collections'])
		sns = sorted(list(set([i for j in [self.vars['collections'][k] 
			for k in collections] for i in j])))
		return sns

###---INTERFACE

def compute(meta=None,confirm=False):
	"""
	Expose the workspace to the command line.
	"""
	work = WorkSpace(meta=meta,confirm_compute=confirm)

def plot(name,meta=None):
	"""
	Plot something
	"""
	#---! avoid unnecessary calculations for the plot we want
	work = WorkSpace(plot=name,plot_call=True,meta=meta)

def pipeline(name,meta=None):
	"""
	Plot something
	"""
	#---! avoid unnecessary calculations for the plot we want
	work = WorkSpace(pipeline=name,plot_call=True,meta=meta)

def look(method=None):
	"""
	Inspect the workspace. Send a method name and we will run it for you.
	"""
	header_script = 'omni/base/header_look.py'
	bash('python -iB %s %s'%(header_script,'null' if not method else method))
