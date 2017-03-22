#!/usr/bin/env python

import os,sys,re,glob,copy,json,time
from config import read_config,bash
from base.tools import catalog,delve,str_or_list,status
from base.hypothesis import hypothesis
import yaml,h5py
import numpy as np

class WorkSpace:

	"""
	! note that I decided to make this a class when passing around slices etc got clumsy
	"""

	#---hard-coded paths for specs files
	specs_path = 'calcs','specs','*.yaml'
	#---identify any post-processing data
	post_regex = '^.+\.(dat|spec|gro|xtc)$'
	#---versioning manages fairly generic data structures written to disk
	versioning = {'spec_file':2}
	#---! currently hard-coded
	nprocs = 4

	def __init__(self,plot=None,plot_call=False,pipeline=None,meta=None):
		"""
		"""
		if plot and pipeline: raise Exception('only plot or pipeline')
		#---read the omnicalc config and specs files
		self.config = read_config()
		#---unpack the paths right into the workspace for calculation functions
		#---! check that all paths are added here for posterity/backwards compatibility
		self.paths = dict([(key,self.config[key]) for key in ['post_plot_spot','post_data_spot']])
		self.specs = self.read_specs(meta=meta)
		#---prepare a namer from the global omni_namer
		self.namer = NamingConventions(
			short_namer=self.meta.get('short_namer',None),
			short_names=self.meta.get('short_names',None))
		#---! source regular data
		#---! explicitly-named data
		#---catalog post-processing data
		self.postdat = self.read_postdat()
		#---CALCULATION LOOP
		self.calcs = self.specs.get('calculations',None)
		self.slices_meta = self.specs.get('slices',None)
		if not self.calcs: return
		if not self.calcs and self.slices_meta: 
			raise Exception('cannot continue to calculations without slices')
		#---get the right calculation order
		self.calc_order = self.infer_calculation_order()
		#---compile a listing of slices
		self.slice_catalog = self.get_slice_catalog()
		if not plot and not pipeline:
			#---match calculation codes with target slices
			calc_jobs = self.prepare_calculations()
			self.compute(calc_jobs)
		elif plot: self.plot(plotname=plot,plot_call=plot_call)
		elif pipeline: self.pipeline(name=pipeline,plot_call=plot_call)

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
			source = delve(self.vars,*sub.strip('+').split('/'))
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
		if not meta: specs_files = glob.glob(os.path.join(*self.specs_path))
		else: 
			#---in case the user gives the glob in calcs/specs we try this first
			specs_files = []
			try: specs_files = glob.glob(os.path.join(*(tuple(self.specs_path[:-1])+tuple([meta]))))
			#---otherwise the user should give a glob directly to calcs/specs/*.yaml
			except: pass
			if not specs_files: specs_files = glob.glob(meta)
		if not specs_files: raise Exception('cannot find meta files')
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
								import pdb;pdb.set_trace()
								raise Exception(
								('[ERROR] performing careful merge in the top-level specs dictionary "%s" '+
								' but there is already a child key "%s"')%(topkey,key))
		else: raise Exception('\n[ERROR] unclear meta specs merge method %s'%merge_method)
		self.vars = specs['variables']
		specs_unpacked = self.variable_unpacker(specs)
		self.meta = specs.get('meta',{})
		#---for backwards compatibility we put collections in vars
		if 'collections' in self.vars: raise Exception('collection already in vars')
		self.vars['collections'] = specs['collections']
		#---expose the plots
		self.plots = specs['plots']
		self.plotdir = self.paths['post_plot_spot']
		return specs_unpacked

	def read_postdat(self):
		"""
		Parse a directory of post-processed data.
		POSTDAT STRUCTURE REQUIRES dat_fn,spec_fn,spot,specs
		"""
		post_dn = self.config.get('post_data_spot',None)
		if not post_dn: raise Exception('config lacks "post_data_spot"')
		fns = [i for i in [
			os.path.basename(i) for i in glob.glob(os.path.join(post_dn,'*'))] 
			if re.match(self.post_regex,i)]
		#---classify each file in the post-processing dataset
		toc = dict([(re.match('^(.+)\.dat$',i).group(1),{'spot':post_dn,'dat_fn':i}) 
			for i in fns if re.match('^(.+)\.dat$',i)])
		#---remove dat files from consideration
		for fn in toc: fns.remove(fn+'.dat')
		#---check that each dat has a spec
		for fn in toc: 
			if fn+'.spec' not in fns: 
				raise Exception('in the post-processed data at '+
					'%s we found %s.dat with no corresponding %s.spec'%(post_dn,fn,fn))
			else: 
				fns.remove(fn+'.spec')
				toc[fn]['spec_fn'] = fn+'.spec'
		#---we should have removed all paired spec files so any remaining ones are lonely
		spec_unclaimed = [i for i in fns if re.match('^(.+)\.spec$',i)]
		if any(spec_unclaimed): raise Exception('found spec files without corresponding dat files '+
			'in post data at %s: %s'%(post_dn,spec_unclaimed))
		#---unpack spec files
		for fn in [i for i in toc if 'spec_fn' in toc[fn]]:
			toc[fn]['specs'] = json.load(open(os.path.join(toc[fn]['spot'],toc[fn]['spec_fn'])))
		#---! instead of parsing the slices, we simply add them to a grab-bag of slices. needs developed.
		#---! continue to parse the data
		self.post_grab_bag = fns
		return toc

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

	def get_slice_kind(self,spec):
		"""
		Figure out what kind of slice.
		"""
		if set(spec.keys())==set(['groups','slices']): return 'std'
		else: raise Exception('indeterminate slice')

	def get_slice_catalog(self):
		"""
		Match slices in the specs file with slices in the post-processed database.
		"""
		slice_catalog = {}
		#---preload slices from the grab bag
		grab_slices = dict([(fn,self.namer.check(fn)) for fn in self.post_grab_bag])
		#---we search for slices from the slice dictionary
		for sn,spec in self.slices_meta.items():
			kind = self.get_slice_kind(spec)
			#---standard slice request has a group and a name and uses gromacs
			if kind=='std':
				these_slices = {}
				for slice_name,sl in spec['slices'].items():
					#if 'groups' not in sl: 
					#	import ipdb;ipdb.set_trace()
					#---always copy the slice here because it might be an internal reference
					sl = dict(sl)
					for group in sl.pop('groups'):
						if sn not in slice_catalog: slice_catalog[sn] = []
						#---add this slice to the catalog
						new_slice = dict(slice_name=slice_name,group=group,sn=sn,**sl)
						#---! also save the post directory or highlander?
						#---for the standard slice we add the gro and xtc to the new_slice
						#---! more flexible data types
						for suffix in ['gro','xtc']:
							#---search the grab_slices for the slice data
							slice_fn = self.namer.slice(suffix=suffix,**new_slice)
							if slice_fn in grab_slices:
								new_slice[suffix] = dict(fn=slice_fn,**grab_slices[slice_fn])
								grab_slices.pop(slice_fn)
						slice_catalog[sn].append(new_slice)
			else: raise Exception('not sure how to find a %s slice'%kind)
		if grab_slices: 
			#import ipdb;ipdb.set_trace()
			print('grab_slices is not cleared')
		#---! pythonic?
		del self.__dict__['post_grab_bag']
		return slice_catalog

	def slices(self,sn,**kwargs):
		"""
		Get a slice from the catalog.
		"""
		#---! this is underspecified and needs development. consider the slice function from original omni?
		if sn not in self.slice_catalog: raise Exception('no slices for %s'%sn)
		slices = [i for i in self.slice_catalog[sn] 
			if all([key in i and i[key]==val for key,val in kwargs.items()])]
		return slices

	def slice(self,sn,**kwargs):
		"""
		Get a single slice from the catalog.
		"""
		slices = self.slices(sn,**kwargs)
		if len(slices)>1:
			import ipdb;ipdb.set_trace()
			raise Exception('found multiple slices for your request for %s: %s'%(sn,kwargs))
		elif len(slices)==0: raise Exception('cannot find a matching slice for %s: %s'%(sn,kwargs))
		else: return slices[0]

	def unroll_loops(self,details,return_stubs=False):
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

	def get_postdat_with_specs(self,**kwargs):
		"""
		Scan the postdat for data with particular specs.
		Note that this function takes a "job" in kwargs.
		! the "job" is rapidly becoming the most important component of this data structure
		"""
		job = kwargs
		#---spec files have flexible formats handled throughout the code according to self.versioning
		#---...so we match with postdat in a way that remains faithful to this versioning
		keys_by_versioning = {}

		#---version 1 specs files have raw specs written directly to the file and rely on the filename
		#---...to convey the metatdata e.g. simulation name
		base_name = self.job_to_name(calc=job['calc'],sl=job['slice'])
		keys_no_specs = [key for key in self.postdat if re.match(base_name,key)]
		keys_by_versioning[1] = [key for key in keys_no_specs if 
			self.postdat[key]['specs']==job['calc']['specs']]

		#---version 2 specs files have a specs sub-dictionary but also hold OTHER INFORMATION
		#---! which other information?
		#---! currently we still look to the base_name generated for version 1 to get this information
		keys_by_versioning[2] = [key for key in keys_no_specs
			if self.postdat[key]['specs'].get('specs',{})==job['calc']['specs']]

		#---get unique key matches
		keys_matches = [(val[0],key) for key,val in keys_by_versioning.items() if len(val)==1]
		if len(keys_matches)==1:
			this_key,this_version = keys_matches[0]
			print('[STATUS] matched postdat (v%s) %s'%(this_version,this_key))
			#job['result'] = this_key
			return this_key
			#print(666)
			#import ipdb;ipdb.set_trace()
			#return this_key
		#---no match means this calculation gets no result and hence will be pending
		#---! note that some versionings might have multiple matches but these must be invalid
		#else: pass

	def match_postdat_to_results(self,jobs):
		"""
		When the calculations are ready, we check the postdat for completed calculations.
		"""
		for job in jobs:
			result = self.get_postdat_with_specs(**job)
			if result: job['result'] = result

	def prepare_calculations(self,calcnames=None,sns=None):
		"""
		Match calculations with simulations.
		! assumes everything is ready. maybe call a validator? coder should not call this until validated
		This function prepares all pending calculations unless you ask for a specific one.
		"""
		calc_jobs = []
		#---loop over calculations
		for calckey in (self.calc_order if not calcnames else str_or_list(calcnames)):
			#---! the following is the standard calculation interpreter
			#---get the calculation spec
			calc_with_loops = self.calcs[calckey]
			#---unroll the calculation if loops
			calcs = self.unroll_loops(calc_with_loops)
			for calc in calcs:
				calc['calc_name'] = calckey
				#---get slice name
				slice_name = calc['slice_name']
				#---loop over simulations
				if not sns: sns = self.get_simulations_in_collection(*str_or_list(calc['collections']))
				#---custom simulation name request will whittle the sns list here
				else: sns = str_or_list(sns)
				#---get the group
				group = calc.get('group',None)
				#---group is absent if this is a downstream calculation
				if not group:
					def get_upstream_groups(*args):
						"""Recurse the dependency list for groups."""
						for arg in args:
							if 'group' in self.calcs[arg]: yield self.calcs[arg]['group']
							else: up_again.append(arg)
					groups = get_upstream_groups(calc['specs']['upstream'].keys())
					groups_consensus = list(set(groups))
					if len(groups_consensus)!=1: 
						print('cannot achieve upstream group consensus')
						import ipdb;ipdb.set_trace()
						raise Exception('cannot achieve upstream group consensus')
					group = groups_consensus[0]
				#---loop over simulations
				for sn in sns: 
					job = dict(sn=sn,slice=self.slice(sn,group=group,slice_name=slice_name))
					#---calculations do not require specs. empty specs gives empty specs file.
					#---...we add an empty dictionary so later this job can be matched against postdat
					if 'specs' not in calc: calc['specs'] = {}
					job['calc'] = calc
					calc_jobs.append(job)
		self.match_postdat_to_results(calc_jobs)
		return calc_jobs

	def compute(self,jobs):
		"""
		Run through computations.
		"""
		completed = [item for item in jobs if 'result' in item]
		pending = [item for item in jobs if 'result' not in item]
		for jnum,calc_job in enumerate(pending):
			print('[JOB] running calculation job %d/%d'%(jnum+1,len(pending)))
			self.compute_single(calc_job)

	def find_script(self,name,root='calcs'):
		"""
		Find a generic script somewhere in the calculations folder.
		"""
		#---find the script with the funtion
		fns = []
		for (dirpath, dirnames, filenames) in os.walk(root): 
			fns.extend([dirpath+'/'+fn for fn in filenames])
		search = [fn for fn in fns if re.match('^%s\.py'%name,os.path.basename(fn))]
		if len(search)==0: 
			import ipdb;ipdb.set_trace()
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
			for key in self.postdat if re.match(base_name,key)]
		if fns and not sorted([int(i) for i in fns])==list(range(len(fns))): 
			raise Exception('error in data numbering')
		index = len(fns)
		tag = '.n%d'%index
		#---! check if these files don't exist
		return [base_name+tag+i for i in ['.dat','.spec','']]

	def job_to_name(self,calc,sl):
		"""
		Take the calculation and slice specification and combine them for the namer.
		"""
		#---note that calc and slice are probably redundant
		combo = dict(calc)
		combo.update(**sl)
		return self.namer.slice(**combo)

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
			if re.match('^str',obj[key].dtype.name) and 'U' in obj[key].dtype.str:
				obj[key] = obj[key].astype('S')
			try: dset = fobj.create_dataset(key,data=obj[key])
			except: 
				raise Exception("failed to write this object so it's probably not numpy"+
					"\n"+key+' type='+str(type(obj[key]))+' dtype='+str(obj[key].dtype))
		if attrs != None: fobj.create_dataset('meta',data=np.string_(json.dumps(attrs)))
		if verbose: status('[WRITING] '+path+'/'+name)
		fobj.close()

	def compute_single(self,job):
		"""
		SINGLE COMPUTATION.
		Note that this replaces (most of) computer from the original omnicalc.
		"""
		#---retrieve the function
		function = self.get_calculation_function(job['calc']['calc_name'])
		#---prepare data for shipping to the function
		outgoing = {'calc':job['calc'],'workspace':self,'sn':job['sn']}

		#---THE MOST IMPORTANT LINES IN THE WHOLE CODE (here we call the calculation function)
		if job['calc']['uptype']=='simulation':
			#---! specific to the standard operation
			#---assume that all postdata are in one spot
			struct_file = os.path.join(self.paths['post_data_spot'],job['slice']['gro']['fn'])
			traj_file = os.path.join(self.paths['post_data_spot'],job['slice']['xtc']['fn'])
			result,attrs = function(struct_file,traj_file,**outgoing)
		elif job['calc']['uptype']=='post':
			#---acquire upstream data
			#---! multiple upstreams. double upstreams. loops. specs. etc. THIS IS REALLY COMPLICATED.
			upstreams = str_or_list(job['calc']['specs']['upstream'])
			outgoing['upstream'] = {}
			for upcalc in upstreams:
				#---get a jobs list for this single simulation since post data is one-simulation only
				upstream_jobs = self.prepare_calculations(calcnames=upstreams,sns=[job['sn']])
				if any(['result' not in j for j in upstream_jobs]):
					raise Exception('some upstream calculations are missing')
				for upstream_job in upstream_jobs:
					outgoing['upstream'][upcalc] = self.load(
						name=self.postdat[upstream_job['result']]['dat_fn'],
						cwd=self.postdat[upstream_job['result']]['spot'])
			#---for backwards compatibility we decorate the kwargs with the slice name and group
			outgoing.update(slice_name=job['slice']['slice_name'],group=job['slice']['group'])
			result,attrs = function(**outgoing)

			"""
			note some testing on old-school omnicalc
			ipdb> kwargs.keys()
			['group', 'slice_name', 'sn', 'workspace', 'upstream', 'calc']
			ipdb> kwargs.keys()
			['group', 'slice_name', 'sn', 'workspace', 'upstream', 'calc']
			ipdb> kwargs['upstream'].keys()
			['lipid_abstractor']
			ipdb> kwargs['upstream']['lipid_abstractor'].keys()
			[u'resnames', u'nframes', u'points', u'separator', u'vecs', u'resids', u'monolayer_indices', u'selector', u'nojumps']
			"""

		else: raise Exception('invalid uptype: %s'%job['calc']['uptype'])

		#---check the attributes against the specs so we don't underspecify the data in the spec file
		#---...if any calculation specifications are not in attributes we warn the user here
		if 'specs' in job['calc']: unaccounted = [i for i in job['calc']['specs'] if i not in attrs]
		else: unaccounted = []
		if 'upstream' in unaccounted and 'upstream' not in attrs: 
			status('automatically appending upstream data',tag='status')
			unaccounted.remove('upstream')
			attrs['upstream'] = job['calc']['specs']['upstream']
		if any(unaccounted):
			import textwrap
			print('\n'.join(['[ERROR] %s'%i for i in textwrap.wrap(computer_error_attrs_passthrough,width=80)]))
			raise Exception('some calculation specs were not saved: %s'%unaccounted)

		base_name = self.job_to_name(calc=job['calc'],sl=job['slice'])
		#---we must register the new files with postdat which tracks the contents of the post folder
		#---...otherwise get_new_dat_name will return the wrong answer
		if base_name in self.postdat: raise Exception('key %s in postdat already'%base_name)
		dat_fn,spec_fn,base_name_indexed = self.get_new_dat_name(base_name)
		#---save the results
		self.store(obj=result,name=dat_fn,path=self.paths['post_data_spot'],attrs=attrs)
		#---compile the specs according to a special versioning system
		if self.versioning['spec_file']==2:
			#---! should we also include other calculation data by default
			specs_out = {'specs':job['calc']['specs']}
			#---write a lightweight "spec" file, always paired with dat file
			with open(os.path.join(self.paths['post_data_spot'],spec_fn),'w') as fp: 
				fp.write(json.dumps(specs_out))
			#---in spec_file version 2 formatting, we register the entire spec file with the postdat. this 
			#---...contains a sub-dictionary under 'specs' as well as the metadata like simulation name 
			#---...included in the filename. this scheme means that you don't need to rely on the file 
			#---...name to include important metadata. everything is in the spec file for posterity.
			#---...multiply versioning schemes are handled throughout this code
			#---...postdat otherwise there would be a lot of redundancy
			self.postdat[base_name_indexed] = {
				'dat_fn':dat_fn,'spec_fn':spec_fn,'spot':self.paths['post_data_spot'],
				'specs':specs_out}
		else: 
			msg = 'requires spec_file versioning number 2'
			spec_fn_full,dat_fn_full = [
				os.path.join(self.paths['post_data_spot'],i) for i in [spec_fn,dat_fn]]
			if not os.path.isfile(spec_fn_full) and os.path.isfile(dat_fn_full):
				msg += '. note that we wrote %s without writing %s so you should delete the former'%(
					dat_fn_full,spec_fn_full)
			raise Exception(msg)

	def plot(self,plotname,plot_call=False):
		"""
		Plot something.
		! Get this out of the workspace.
		"""
		plots = self.specs['plots']
		if plotname not in plots: raise Exception('cannot find plot %s in the specs files'%plotname)
		plotspec = plots[plotname]
		#---we hard-code the plot script naming convention here
		script_name = self.find_script('plot-%s'%plotname)
		header_script = 'omni/base/header.py'
		#---custom arguments passed to the header so it knows how to execute the plot script
		if plot_call: bash('python -iB %s %s %s %s'%(header_script,script_name,plotname,'plot'))

	def pipeline(self,name,plot_call=False):
		"""
		Plot something.
		! Get this out of the workspace.
		"""
		#---we hard-code the pipeline script naming convention here
		script_name = self.find_script('pipeline-%s'%name)
		header_script = 'omni/base/header.py'
		#---custom arguments passed to the header so it knows how to execute the plot script
		if plot_call: bash('python -iB %s %s %s %s'%(header_script,script_name,name,'pipeline'))

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
		if any(['result' not in j for j in upstream_jobs]):
			#---! this exception does not route through tracebacker because we call python with bash in plot
			raise Exception('at least one of the jobs is missing a result. did you forget `make compute`?')
		"""
		#---backwards compatibility ...
		ipdb> data['simulation-v006-endophilin-A1-PIP2-bilayer-large'].keys()
		['group', 'fn_base', 'slice', 'data']
		ipdb> calc
		{'uptype': 'post', 'group': 'all', 'specs': {'grid_spacing': 1.0, 'upstream': 'lipid_abstractor'}, 'slice_name': 'current_protein', 'collections': 'all'}		
		"""
		if any(['result' not in j for j in upstream_jobs]):
			raise Exception('some upstream calculations are missing')
		#---load the data
		data = dict([(sn,{}) for sn in sorted(list(set([j['sn'] for j in upstream_jobs])))])
		for unum,upstream_job in enumerate(upstream_jobs):
			status('reading %s'%self.postdat[upstream_job['result']]['dat_fn'],
				tag='load',i=unum,looplen=len(upstream_jobs))
			data[upstream_job['sn']]['data'] = self.load(
				name=self.postdat[upstream_job['result']]['dat_fn'],
				cwd=self.postdat[upstream_job['result']]['spot'])
		#---for backwards compatibility we always send data with the plot spec however this is redundant
		#---! the plot spec points to the upstream data but they are always accessible in the workspace
		return data,self.calcs[plotname]

###---SUPPORT

computer_error_attrs_passthrough = """the calculation has a number of "specs" according to one of your meta files. 
after the data are saved, the plotloader function will use these specs to uniquely identify the upstream data from 
this calculation. however, all of the attributes must "pass through" the calculation function and end up in the attrs 
section. it looks like you failed to pass one of them through, but I couldn't tell until after the calculation was 
complete and we are ready to write the data. you can procede by removing the attribute from your calculation specs in 
the meta file or by adding it to the outgoing data via e.g. "attrs['my_spec'] = my_spec". recall also that the 
attribute/spec comes *in* to the calculation function in "kwargs['calc']['specs']". the incoming warning will tell you 
which attributes are causing the problem
"""

class NamingConventions:

	"""
	Organize the naming conventions for omnicalc.
	! note that namers is ordered
	"""

	common_types = {'wild':r'[A-Za-z0-9\-_]','float':r'\d+(?:(?:\.\d+))?','suffixes':'(gro|xtc)'}
	omni_namer = {
		'gmx_slice':{
			'namers':[
				('base',r'%(short_name)s.%(start)s-%(end)s-%(skip)s.%(group)s.pbc%(pbc)s'),
				('slice',r'%(short_name)s.%(start)s-%(end)s-%(skip)s.%(group)s.pbc%(pbc)s.%(suffix)s'),
				('post',r'%(short_name)s.%(start)s-%(end)s-%(skip)s.%(group)s.pbc%(pbc)s.%(calc_name)s')],
			'regex':
				'(?P<short_name>%(wild)s+)\.(?P<start>%(float)s)-(?P<end>%(float)s)-(?P<skip>%(float)s)\.'+
				'(?P<group>%(wild)s+)\.pbc(?P<pbc>%(wild)s+)\.(?P<suffix>%(suffixes)s)$',},}

	def __init__(self,**kwargs):
		"""
		Turn a set of specs into a namer.
		"""
		self.short_namer = kwargs.pop('short_namer',None)
		self.short_names = kwargs.pop('short_names',None)
		#---since the short_namer is the default if no explicit names we provide the identity function
		if not self.short_namer: self.short_namer = lambda x : x
		elif type(self.short_namer)!=str: 
			raise Exception('meta short_namer parameter must be a string: %s'%self.short_namer)
		#---compile the lambda function which comes in as a string
		else: self.short_namer = eval(self.short_namer)
		#---! validate the naming steps?
		if kwargs: raise Exception('unprocessed kwargs: %s'%kwargs)
		self.catalog,self.namers = {},[]
		for key,val in self.omni_namer.items():
			self.catalog[key] = val['regex']%self.common_types
			#---infer the keys required for the namer by the "%s(variable)" syntax, which is required
			for subkey,namer in val['namers']:
				self.namers.append(((key,subkey),{'reqs':re.findall(r'%\((.*?)\)',namer),'namer':namer}))

	def check(self,text):
		"""
		Try many regexes on some text and return the type.
		"""
		for key,val in self.catalog.items():
			if re.match(val,text): return {'kind':key,'data':re.match(val,text).groupdict()}

	def get_shortname(self,sn):
		"""
		Convert a full simulation name to a shorter name.
		"""
		#---the explicit short names list is top priority
		if self.short_names and sn in self.short_names: return self.short_names[sn]
		#---we consult a namer function if there is no explicit translation
		elif self.short_namer: return self.short_namer(sn)

	def slice(self,**kwargs):
		"""
		Name a slice.
		"""
		spec = dict(kwargs)
		#---add the short_name to the specs
		spec['short_name'] = self.get_shortname(spec['sn'])
		#---figure out the naming convention for this slice
		kinds = [key for key,namer in self.namers if all([k in spec for k in namer['reqs']])]
		if len(kinds)==0: raise Exception('cannot find a naming convention for %s'%spec)
		#---the namers are ordered and we choose the *last* match
		else: return self.namers[list(zip(*self.namers))[0].index(kinds[-1])][1]['namer']%spec

###---INTERFACE

def compute(meta=None):
	"""
	Expose the workspace to the command line.
	"""
	work = WorkSpace(meta=meta)

def plot(name):
	"""
	Plot something
	"""
	#---! avoid unnecessary calculations for the plot we want
	work = WorkSpace(plot=name,plot_call=True)

def pipeline(name):
	"""
	Plot something
	"""
	#---! avoid unnecessary calculations for the plot we want
	work = WorkSpace(pipeline=name,plot_call=True)
