#!/usr/bin/env python

import os,sys,glob,re,json,copy,time
from datapack import asciitree,delve,delveset,catalog
from base.hypothesis import hypothesis
from base.tools import status

###---SUPPORT

computer_error_attrs_passthrough = """the calculation has a number of "specs" according to one of your meta 
files. after the data are saved, the plotloader function will use these specs to uniquely identify the 
upstream data from this calculation. however, all of the attributes must "pass through" the calculation 
function and end up in the attrs section. it looks like you failed to pass one of them through, but I 
couldn't tell until after the calculation was complete and we are ready to write the data. you can procede 
by removing the attribute from your calculation specs in the meta file or by adding it to the outgoing data 
via e.g. "attrs['my_spec'] = my_spec". recall also that the attribute/spec comes *in* to the calculation 
function in "kwargs['calc']['specs']". the incoming warning will tell you which attributes are causing the 
problem
"""

str_types = [str,unicode] if sys.version_info<(3,0) else [str]

def json_type_fixer(series):
	"""Cast integer strings as integers, recursively. We also fix 'None'."""
	for k,v in series.items():
		if type(v) == dict: json_type_fixer(v)
		elif type(v)in str_types and v.isdigit(): series[k] = int(v)
		elif type(v)in str_types and v=='None': series[k] = None

class NamingConvention:

	"""
	Organize the naming conventions for omnicalc.
	omni_namer data structure:
		naming indexed by pairs: slice-type,data-type
			dictionary-to-name
			name-to-dictionary
		meta slice reading indexed by: slice-type
			keys required in the specs
	"""

	#---all n2d types in omni_namer get standard types
	common_types = {'wild':r'[A-Za-z0-9\-_]','float':r'\d+(?:(?:\.\d+))?',
		'gmx_suffixes':'(?:gro|xtc)'}
	omni_namer = [
		(('standard','gmx'),{
			'd2n':r'%(short_name)s.%(start)s-%(end)s-%(skip)s.%(group)s.pbc%(pbc)s.%(suffix)s',
			'n2d':'^(?P<short_name>%(wild)s+)\.'+
				'(?P<start>%(float)s)-(?P<end>%(float)s)-(?P<skip>%(float)s)\.'+
				'(?P<group>%(wild)s+)\.pbc(?P<pbc>%(wild)s+)\.%(gmx_suffixes)s$',}),
		(('standard','datspec'),{
			#---we append the dat/spec suffix and the nnum later
			'd2n':r'%(short_name)s.%(start)s-%(end)s-%(skip)s.%(group)s.'+
				r'pbc%(pbc)s.%(calc_name)s',
			'n2d':'^(?P<short_name>%(wild)s+)\.'+
				'(?P<start>%(float)s)-(?P<end>%(float)s)-(?P<skip>%(float)s)\.'+
				'(?P<group>%(wild)s+)\.pbc(?P<pbc>%(wild)s+)\.(?P<calc_name>%(wild)s+)'+
				'\.n(?P<nnum>\d+)\.(dat|spec)$',}),
		(('standard_obvious','datspec'),{
			#---we append the dat/spec suffix and the nnum later
			'd2n':r'%(short_name)s.%(start)s-%(end)s-%(skip)s.%(calc_name)s',
			'n2d':'^(?P<short_name>%(wild)s+)\.'+
				'(?P<start>%(float)s)-(?P<end>%(float)s)-(?P<skip>%(float)s)\.'+
				'(?P<calc_name>%(wild)s+)\.n(?P<nnum>\d+)\.(dat|spec)$',}),
		(('raw','datspec'),{
			#---we append the dat/spec suffix and the nnum later
			#---! should this include the number?
			'd2n':r'%(short_name)s.%(calc_name)s',
			'n2d':'^(?P<short_name>%(wild)s+)\.(?P<calc_name>%(wild)s+)\.n(?P<nnum>\d+)\.(dat|spec)$',}),]
	#---keys required in a slice in the meta file for a particular umbrella naming convention
	omni_slicer_namer = {
		'standard':{'slice_keys':['groups','slices']},
		'readymade_namd':{'slice_keys':['readymade_namd']},}
	#---alternate view of the namer
	parser = dict(omni_namer)

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

	def interpret_name(self,name):
		"""
		Given a post-processing data file name, extract data and infer the version.
		"""
		matches = []
		for (slice_type,dat_type),namespec in self.parser.items():
			if re.match(namespec['n2d']%self.common_types,name):
				matches.append((slice_type,dat_type))
		#---anything that fails to match goes into limbo of some kind
		if not matches: return None
		elif len(matches)>1: raise Exception('multiple filename interpretations: %s'%matches)
		slice_type,dat_type = matches[0]
		data = re.match(self.parser[(slice_type,dat_type)]['n2d']%self.common_types,name).groupdict()
		return {'slice_type':slice_type,'dat_type':dat_type,'body':data}

	def name_slice(self,kind,**kwargs):
		"""
		Generate a slice name from metadata according to the slice type.
		"""
		sn,group = kwargs['sn'],kwargs['group']
		if kind=='readymade_namd': slice_files = {'struct':kwargs['psf'],'traj':kwargs['dcds']}
		elif kind=='standard':
			if 'groups' in kwargs and group not in kwargs['groups']: 
				raise Exception('simulation %s does not have requested group %s'%(sn,group))
			name_data = dict(**kwargs)
			slice_files = {}
			for suffix,out in [('gro','struct'),('xtc','traj')]:
				name_data['short_name'] = self.short_namer(sn)
				name_data['suffix'] = suffix
				slice_files[out] = self.parser[('standard','gmx')]['d2n']%name_data
		elif kind=='datspec':
			#---first we try the standard datspec
			#---! could be more systematic about checking keys
			name = None
			for name_style in [('standard','datspec'),('raw','datspec')]:
				try:
					name = self.parser[name_style]['d2n']%dict(
						short_name=self.short_namer(kwargs['sn']),nnum=0,**kwargs)
				except: pass
			if not name: raise Exception('cannot generate datspec name')
			slice_files = name
		else: raise Exception('invalid slice kind: %s'%kind)
		return slice_files

class PostDat(NamingConvention):

	"""
	A library of post-processed data.
	"""

	def limbo(self): return dict([(key,val) for key,val in self.toc.items() if val=={}])
	def slices(self): return dict([(key,val) for key,val in self.toc.items() 
		if val.__class__.__name__=='Slice'])
	def posts(self): return dict([(key,val) for key,val in self.toc.items() 
		if val.__class__.__name__=='DatSpec'])

	def __init__(self,where,namer=None,work=None):
		"""
		Parse a post-processed data directory.
		"""
		self.work = work
		self.stable = [os.path.basename(i) for i in glob.glob(os.path.join(where,'*'))]
		#---! weird reference to the workspace namer?
		if namer: self.namer = namer
		self.toc = {}
		nfiles,nchars = len(self.stable),max([len(j) for j in self.stable])
		#---master classification loop
		while self.stable: 
			name = self.stable.pop()
			status(name,tag='import',i=nfiles-len(self.stable),looplen=nfiles+1,pad=nchars+2)
			#---interpret the name
			namedat = self.interpret_name(name)
			if not namedat: 
				#---this puts the slice in limbo
				#---! it would be useful to match these up later...
				self.toc[name] = {}
			else:
				#---if this is a datspec file we find its pair and read the spec file
				if namedat['dat_type']=='datspec':
					basename = self.get_twin(name,('dat','spec'))
					self.toc[basename] = DatSpec(fn=basename,dn=work.paths['post_data_spot'],work=self.work)
				#---everything else must be a slice
				#---! alternate check for different slice types?
				else: 
					#---ironically: "note that we could pair e.g. gro/xtc files but this has little benefit"
					basename = self.get_twin(name,('xtc','gro'))
					self.toc[basename] = Slice(name=basename,namedat=namedat)

	def search_slices(self,**kwargs):
		"""
		"""
		slices,results = self.slices(),[]
		#---flatten kwargs
		target = copy.deepcopy(kwargs)
		target.update(**target.pop('spec',{}))
		flats = dict([(slice_key,val.flat()) for slice_key,val in slices.items()])
		results = [key for key in flats if flats[key]==target]
		if not results:
			import ipdb;ipdb.set_trace()
			raise Exception('DEVELOPMENT ERROR. at this point we would normally make the slice for you. '+
				'failed to find slice: %s'%kwargs)
		return results

	def get_twin(self,name,pair):
		"""
		Many slices files have natural twins e.g. dat/spec and gro/xtc.
		This function finds them.
		"""
		this_suffix = re.match('^.+\.(%s)$'%'|'.join(pair),name).group(1)
		basename = re.sub('\.(%s)$'%'|'.join(pair),'',name)
		twin = basename+'.%s'%dict([pair,pair[::-1]])[this_suffix]
		if twin not in self.stable: raise Exception('cannot find dat-spec twin of %s. '%name+
			'this is typically due to a past failure to write the dat after writing the spec. '+
			'we recommend deleting the existing dat/spec file after you fix the error.')
		else: self.stable.remove(twin)
		return basename

	def get_slice(self,sn,slice_name,group):
		"""
		Find a slice.
		"""
		#---! require modifications to prepare the slice?
		#---! should we group gro/xtc at the level of the slice? probably.
		keys = [key for key in self.toc if self.toc[key].__class__.__name__=='Slice']
		import ipdb;ipdb.set_trace()

class DatSpec(NamingConvention):

	"""
	Parent class for identifying a piece of post-processed data.
	"""

	def __init__(self,fn=None,dn=None,job=None,work=None):
		"""
		We construct the DatSpec to mirror a completed result on disk OR a job we would like to run.
		"""
		self.work = work
		if fn and job: raise Exception('cannot send specs if you send a file')
		elif fn and not dn: raise Exception('send the post directory')
		elif fn and dn: self.from_file(fn,dn)
		elif job and fn: raise Exception('cannot send a file if you send specs')
		elif job: self.from_job(job)
		else: raise Exception('this class needs a file or a job')

	def from_file(self,fn,dn):
		"""
		"""
		path = os.path.join(dn,fn)
		self.files = {'dat':fn+'.dat','spec':fn+'.spec'}
		self.specs = json.load(open(path+'.spec'))
		#---for posterity we copy the specs before formatting everything
		self.specs_raw = copy.deepcopy(self.specs)
		json_type_fixer(self.specs_raw)
		json_type_fixer(self.specs)
		self.namedat = self.interpret_name(fn+'.spec')
		#---! why is this exception not in interpret_name?
		if not self.namedat: raise Exception('name interpreter failure')
		#---intervene here to handle backwards compatibility for VERSION 1 jobs
		spec_version_2 = all(['slice' in self.specs,'specs' in self.specs,
			'calc' in self.specs and 'calc_name' in self.specs['calc']])
		#---! note that version 2 also has short_name in the top level but this MIGHT BE REMOVED?
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
			#---! for some reason short_name is in the top?
			#self.specs['short_name'] = self.namedat['body']['short_name']
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
				name=self.specs['calc']['calc_name'],specs=self.specs['specs'])
		#---sometimes we hide calculations that are already complete because we are adding data
		except: 
			#---create a dummy calcspec if we cannot find the calculation in the meta
			#---! note that we may wish to populate this more. this error was found when trying to find a
			#---! ...match later, and the find_match function was trying to look in the CalcSpec for a 
			#---! ...calculation which had been removed from the meta
			#---we supply a name because find_match will be looking for one
			self.specs['calc'] = Calculation(name=None,specs={},stub=[])
			#---! NOTE THAT CALCSPEC IS NO MOR! REMOVE ITTTTTTTTT
		if False:
			#---first time in the execution that we encounter calculation specs
			import ipdb;ipdb.set_trace()
			self.specs['calc'] = CalcSpec(self.specs['calc'])
			if False:
				#---legacy spec files have "upstream" entries with pointers and not data
				#---...note that this was highly stupid. we replace the upstream information here, but 
				#---...obviously cannot guarantee that the data are consistent without checking the `dat` files
				#---...which get the final word on what's in the data
				if 'upstream' in self.specs['specs']:
					#print('FUUUUUUUUU')
					self.work.chase_upstream(self.specs['specs'])
					#import ipdb;ipdb.set_trace()

	def from_job(self,job):
		"""
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

	def flat_DEPRECATED(self):
		"""
		The "flat" view of this object is suitable for comparison.
		It folds data from the name into the body.
		"""
		me = dict(**self.specs)
		#---we fold in the namedat body and the slice_type and we drop nnum
		me.update(**self.namedat.get('body',{}))
		me.pop('nnum',None)
		fix_integers(me)
		return me

	def basename(self):
		"""
		Name this slice.
		"""
		#---! hard-coded VERSION 2 here because this is only called for new spec files
		slice_type = self.job.slice.flat()['slice_type']
		#---standard slice type gets the standard naming
		parser_key = {'standard':('standard','datspec'),
			'readymade_namd':('raw','datspec')}.get(slice_type,None)
		if not parser_key: raise Exception('unclear parser key')
		basename = self.parser[parser_key]['d2n']%dict(
			calc_name=self.job.calc.name,**self.job.slice.flat())
		#---note that the basename does not have the nN number yet (nnum)
		return basename

class Calculation_Deprecated:

	"""
	A calculation, including settings.
	"""

	def __init__(self,work=None,**kwargs):
		"""
		"""
		for key in ['name','uptype','group','slice_name','collections']:
			if key not in kwargs: 
				raise Exception('calculation requires %s'%key)
			self.__dict__[key] = kwargs.pop(key)
		self.specs = kwargs.pop('specs',{})
		if kwargs: raise Exception('unprocessed kwargs: %s'%kwargs)
		#---recursively fill in the specs when we generate the calculation
		json_type_fixer(self.specs)
		#---for posterity
		self.specs_raw = copy.deepcopy(self.specs)
		work.chase_upstream(self.specs)
		if False:
			specs_cursors = [copy.deepcopy(self.specs)]
			while specs_cursors:
				sc = specs_cursors.pop()
				if 'upstream' in sc:
					for calcname in sc['upstream']:
						for key,val in sc['upstream'][calcname].items():
							if type(val) in str_types:
								#---! this is pretty crazy. wrote it real fast pattern-matching
								expanded = work.calcs[calcname]['specs'][key]['loop'][
									sc['upstream'][calcname][key]]
								#---replace with the expansion
								self.specs[key] = copy.deepcopy(expanded)
								del self.specs['upstream'][calcname][key]
							#---! assert empty?
						del self.specs['upstream'][calcname]
					#import ipdb;ipdb.set_trace()		
				#if 'upstream' in self.specs:self.specs.get('upstream',None): raise Exception('failed to clear upstream')
			if 'upstream' in self.specs:
				if self.specs['upstream']: raise Exception('failed to clear upstream')
				else: del self.specs['upstream']
		#---! somehow the above works on one level. need check if it does the nesting. probably doesn't...

class CalcSpec:

	"""
	Calculation specs may have loops and/or upstream values in them. These are handled here.
	"""

	def __init__(self,incoming):
		self.body = incoming

class Calculation:

	"""
	A calculation, including settings.
	"""

	def __init__(self,name,specs,stub):
		"""
		We represent the calculation with the full specs as well as the stubs that, with the help of the 
		parent CalcMeta object which helps find them OR SOMETHING?
		"""
		self.name = name
		self.specs = specs
		self.specs_raw = copy.deepcopy(specs)
		self.stub = stub
		self.twined = None
		#---allow users to omit calculation specs
		if 'specs' not in self.specs: self.specs['specs'] = {}

class CalcMeta:

	"""
	Listing of calculations for cross-referencing.
	All calculations are identified by the name and the index of the "unrolled" version i.e. the specific
	calculation after loops are applied.
	"""

	def __init__(self,meta,**kwargs):
		"""
		"""
		self.work = kwargs.pop('work')
		if kwargs: raise Exception('unprocessed kwargs: %s'%kwargs)
		#---unroll each calculation and store the stubs because they map from the keyword used in the 
		#---...parameter sweeps triggered by "loop" and the full specs
		self.toc = {}
		for calcname,calc in meta.items():
			expanded_calcs,expanded_stubs = self.unroll_loops(calc,return_stubs=True)
			self.toc[calcname] = [Calculation(calcname,spec,stub) 
				for spec,stub in zip(expanded_calcs,expanded_stubs)]
		#---fix json
		for calcname in self.toc:
			for calc in self.toc[calcname]: json_type_fixer(calc.specs)
		#---instantiate internal references
		for calcname in self.toc:
			for cnum,calc in enumerate(self.toc[calcname]):
				if 'specs' not in calc.specs:
					print('???')
					raise Exception('???')
				calc.specs_linked = copy.deepcopy(calc.specs)
				upstream = calc.specs_linked['specs'].pop('upstream',None)
				ups = self.get_upstream(upstream) or []
				#---tag and link the upstream calculations
				for calc in ups: 
					self.toc[calcname][cnum].specs_linked['specs'][('up',calc.name)] = calc

	def get_upstream(self,specs):
		"""
		"""
		upstream_calcs = []
		if not specs: return specs
		#---if the get_upstream function receives a string, it is probably an "upstream: calcname" entry
		#---...which we typically write as {'upstream':{calcname:None}} in the past. we now allow the more 
		#---...compact syntax and return the string name because it refers to the upstream calculation
		elif type(specs) in str_types: 
			upstream_calcname = specs
			if not len(self.toc[upstream_calcname])==1: 
				raise Exception('the get_upstream function received a string "%s" indicating an upstream '+
					'calculation with no free parameters, however there are non-unique matches: %s'%
					self.toc[upstream_calcname])
			return [self.toc[upstream_calcname][0]]
		for key,val in specs.items():
			#---! upstream keys need to recurse or something?
			if key=='upstream':
				for key_up,val_up in val.items():
					#---! why has rpb not encountered this yet?
					raise Exception('???')
			else:
				#---previously we required that `i.stub['specs']==val` but this is too strict
				#---! val cannot be None below??
				matches = [i for ii,i in enumerate(self.toc[key]) 
					if val!=None and val.viewitems()<=i.stub['specs'].viewitems()]
				#---try to match the stubs. this will work if you point to an upstream calculation with the 
				#---...name of a subdictionary that represents a single calculation under a loop
				if len(matches)!=1:
					#---the None key implies there is only one calculation with no specs
					if not val:
						if len(self.toc[key])!=1: 
							raise Exception('received None for %s but there are %d calculations'%(
								key,len(self.toc[key])))
						else: upstream_calcs.append(self.toc[key][0])
					#---we can also identify upstream calculations by their specifications explicitly
					#---...by searching the toc. we allow the match to be a subset of the upstream 
					#---...calculations and only require that the match be unique
					else:
						explicit_matches = [i for ii,i in enumerate(self.toc[key]) 
							if i.specs['specs'].viewitems()>=val.viewitems()]
						if len(explicit_matches)==1: upstream_calcs.append(explicit_matches[0])
						else: 
							raise Exception('failed to locate upstream data')
				else: upstream_calcs.append(matches[0])
		return upstream_calcs

	def get_upstream1(self,key,val):
		"""
		"""
		upstream_calcs = []
		if val.keys()==['upstream']: 
			for subkey,subval in val['upstream'].items():
				print('subkey %s'%subkey)
				upstream_calcs.extend(self.get_upstream(subkey,subval))
		else:
			#---pick the right one
			print('getting key %s'%key)
			matches = [i for ii,i in enumerate(self.toc[key]) if i.stub['specs']==val]
			if len(matches)!=1:
				raise Exception('othershit happened')
			else: upstream_calcs.append(matches[0])
		return upstream_calcs

	def get_calcref(self,specs,cumulant):
		"""
		Receive specs starting with "upstream" and recurse the references.
		"""
		upstream = specs.pop('upstream',None)
		for key,val in upstream.items():
			raise Exception('???')

	def calcjobs(self,name):
		"""
		Return calculations by name, including loops.
		Called by prepare_calculations.
		"""
		if name not in self.toc: raise Exception('no calculation named %s'%name)
		else: return self.toc[name]

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

	def find_calculation(self,name,specs):
		"""
		Find a calculation in the master CalcMeta list by specs.
		"""
		if name not in self.toc: 
			raise Exception('calculation named %s is not in the CalcMeta table of contents: %s'%(
				name,self.toc.keys()))
		#---try to match calc specs explicitly
		matches = [calc for calc in self.toc[name] 
			if calc.specs['specs'].viewitems()>=specs.viewitems()]
		if len(matches)==1: return matches[0]
		else: 
			#---try to match the stub
			#---! ytf does the following fail?
			if False:
				matches = [calc for calc in self.toc[name] 
					if calc.stub['specs'].viewitems()>=specs.viewitems()]
			if len(self.toc[name])==1: return self.toc[name][0]
			#---! paranoid use of deepcopy below?
			matches = [calc for calc in self.toc[name] 
				if dict(copy.deepcopy(specs),**copy.deepcopy(calc.stub['specs']))==specs]
			if len(matches)==1: return matches[0]
			else:
				raise Exception('failed to find calculation %s with specs %s in the CalcMeta'%(name,specs))

	def find_calculation_internallywise_DEPRECATED_I_THINK(self,calcname,**kwargs):
		"""
		ONLY HANDLES INTERNAL POINTERS NOW.
		"""
		pointers = kwargs.pop('pointers',None)
		if kwargs: raise Exception('unprocessed kwargs: %s'%kwargs)
		if not pointers: raise Exception('dev')
		#---! pointers has to specify the whole specs but this seems unreasonable?
		matches = [ii for ii in range(len(self.toc[calcname]['stubs'])) 
			if self.toc[calcname]['stubs'][ii]['specs']==pointers]
		if len(matches)==1: return self.toc[calcname]['specs'][ii]
		else:
			raise Exception('too strict')

	def internal_referencer(self):
		"""
		SET ASIDE FOR NOW
		"""
		#---! internal reference example starts here
		spec = self.toc['lipid_areas2d']['specs'][1]
		cat = list(catalog(spec['specs']))
		self.find_calculation_internallywise('lipid_abstractor',pointers={'selector':'lipid_chol_com'})
		raise Exception('???')
		import ipdb;ipdb.set_trace()
		#[ii for ii,i in enumerate(self.toc_raw[calcname]['specs']) 
		# ... if all([key in i and 'loop' in i[key] for key,val in pointers.items()])]

class SliceMeta:

	"""
	This class JOINs a metadata slice dictionary with slices in the postdat so that the actual slice can be 
	retrieved for the calculation. It also calls the slicer.
	"""

	def __init__(self,meta,**kwargs):
		"""
		Convert a dictionary of slice requests into more formal Slices.
		This function mostly inverts the structure of the metadata, where the slices dictionary is indexed
		first by simulation name, then by type.
		"""
		self.work = kwargs.pop('work',None)
		if kwargs: raise Exception('unprocessed kwargs: %s'%kwargs)
		#---first we parse the meta (the slices dictionary from the metadata) into a dictionary
		#---the slices requests are keyed by sn, then slice,group couples
		self.slices = dict([(sn,{}) for sn in meta])
		for sn,sl in meta.items():
			for slice_type in [i for i in sl if i in ['readymade_namd','slices']]:
				slice_group = sl[slice_type]
				if slice_type=='readymade_namd':
					for slice_name,spec in slice_group.items():
						if slice_name in self.slices[sn]:
							raise Exception(
								'redundant slice named %s for simulation %s in the metadata'%(
								slice_name,sn))
						self.slices[sn][(slice_name,None)] = dict(slice_type='readymade_namd',
							dat_type='namd',spec=spec)
				elif slice_type=='slices':
					for slice_name,spec in slice_group.items():
						#---without deepcopy you will pop the dictionary and screw up the internal refs
						spec = copy.deepcopy(spec)
						if slice_name in self.slices[sn]:
							raise Exception(
								'redundant slice named %s for simulation %s in the metadata'%(
								slice_name,sn))
						#---loop over requested groups
						for group in spec.pop('groups',[]):
							if group not in sl.get('groups',[]):
								raise Exception(
									'cannot find corresponding group %s in slice %s in simulation %s'%
									(group,slice_name,sn))
							self.slices[sn][(slice_name,group)] = dict(
								slice_type='standard',dat_type='gmx',spec=dict(group=group,**spec))
		#---now we reproces each slice into a Slice instance for later comparison
		for sn in self.slices:
			for slice_name,group_name in self.slices[sn]:
				proto_slice = self.slices[sn][(slice_name,group_name)]
				#---some slices languish in limbo and we retrieve them here, since this is the stage at 
				#---...which requests become "mature" slices, linked in from the postdat
				#---...hence this is the earliest point that we could match request with files in limbo
				if proto_slice['slice_type']=='readymade_namd':
					name = 'dummy%d'%int(time.time())
					self.work.postdat.toc[name] = Slice(
						name=name,namedat={},slice_type='readymade_namd',dat_type='namd',
						spec=proto_slice['spec'],short_name=self.work.namer.short_namer(sn))
					psf = proto_slice['spec'].get('psf')
					if psf in self.work.postdat.toc: del self.work.postdat.toc[psf]
					for fn in proto_slice['spec'].get('dcds',[]):
						if fn in self.work.postdat.toc: del self.work.postdat.toc[fn]
					#---! ADD FILES HERE
				#---! previous idea was to make slices and then create a comparison operator
				#---search the slices for one with the right specs
				#---including simulation short_name is enforced here
				valid_slices = self.work.postdat.search_slices(
					short_name=self.work.namer.short_namer(sn),**proto_slice)
				if len(valid_slices)>1: raise Exception('multiple valid slices')
				elif len(valid_slices)==0: raise Exception('DEVELOPMENT. cannot find slice')
				else: self.slices[sn][(slice_name,group_name)] = self.work.postdat.toc[valid_slices[0]]
		#---we tag all slices with useful metadata
		for sn in self.slices:
			for slice_name,group_name in self.slices[sn]:
				self.slices[sn][(slice_name,group_name)].sn = sn
				self.slices[sn][(slice_name,group_name)].slice_name = slice_name
				self.slices[sn][(slice_name,group_name)].group = group_name

	def get_slice(self,sn,group,slice_name):
		"""
		Get slices. Group is permissive so we retrieve slices with this function.
		"""
		if sn not in self.slices: raise Excpetion('the slice (meta) object has no simulation %s'%sn)
		if (slice_name,group) in self.slices[sn]: 
			return self.slices[sn][(slice_name,group)]
		#---revert to group None if we cannot find it. this happens for no-group slices from e.g. NAMD
		elif (slice_name,None) in self.slices[sn]:
			return self.slices[sn][(slice_name,None)]
		else:
			asciitree(self.slices[sn])
			raise Exception('see slices (meta) above. '+
				'cannot find slice for simulation %s: %s,%s'%(sn,slice_name,group))

class Slice:

	"""
	Parent class which holds several different representations of what we call a "slice".
	"""

	def __init__(self,**kwargs):
		"""
		slice data structure:
			namedat
			slice_type
			dn
			files (by extension)
		...also
			figure out dat_type
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

		#---check for group/pbc for gmx
		#if 'slice_type' in me and me['slice_type']=='standard' and me['dat_type']=='gmx' and any(
		#	[i not in me for i in ['group','pbc']]):

			#import ipdb;ipdb.set_trace()
		return me

class ComputeJob:

	"""
	A computation job joins a calculation with a simulation slice.
	"""

	def __init__(self,calc,sl,**kwargs):
		"""
		"""
		self.calc = calc
		self.work = kwargs.pop('work',None)
		if kwargs: raise Exception('unprocessed kwargs: %s'%kwargs)
		#---search the slice requests list to get more information about the target slice
		#---...note that in the mock up, the slice requests list represents subtype "b", above is "c"
		slices_by_sn = self.work.slice_meta.slices.get(sl.sn,None)
		if not slices_by_sn: 
			print(self.work.namer.short_namer(sl.sn))
			#import ipdb;ipdb.set_trace()
			print(self.work.slice_meta.slices.get(self.work.namer.short_namer(sl.sn),None))
			if not slices_by_sn: raise Exception('cannot find simulation %s in slice requests'%sl.sn)
		name_group = None
		#---some slices cannot have names (e.g. readymade_namd) so we are permissive here
		for pair in [(sl.slice_name,sl.group),(sl.slice_name,None)]:
			if pair in slices_by_sn: 
				name_group = pair
				break
		if not name_group: 
			raise Exception('cannot find slice name %s for simulation %s for calculation %s'%
			(sl.slice_name,sl.sn,calc.name))
		self.slice = slices_by_sn[name_group] 
		#---keep the simulation name
		self.sn = sl.sn
		self.slice = sl
		#---match this job to a result
		self.result = self.match_result()

	def match_result(self):
		"""
		Check if this job is done.
		This function requires a JOIN between postdat items of the datspec variety and this job.
		Currently there are two ways that a job is completed, either if the computation just ran or if we 
		already linked the datspec with the job request. Both should result in the same object, namely, a 
		"result" which might be a good way to name this.
		"""
		#---assemble a VERSION 2 spec file for comparison
		#---this mirrors the creation of a VERSION 2 spec file at DatSpec.__init__
		#---! working on ptdins and needs reworked for banana. found that self.calc.specs is used as the 
		#---! ...specs below, but this includes the full calculation and we only need its specs
		#---! ...hence the following change which needs testing to self.calc.specs['specs']
		target = {'specs':self.calc.specs['specs'],
			'calc':{'calc_name':self.calc.name},
			'slice':self.slice.flat()}
		#---! hack to fix integers ??! should be done in Slice constructor
		for key,val in self.work.postdat.posts().items(): json_type_fixer(val.specs)
		#---we search for a result object by directly comparing the DatSpec.specs object
		#---! see the note above on the specs garbage.
		### matches = [key for key,val in self.work.postdat.posts().items() if val.specs==target]
		#---! switching to itemwise comparison
		#---! this is weird because it was working for ptdins ?!!?
		try: matches = [key for key,val in self.work.postdat.posts().items() if all([
			val.specs['slice']==target['slice'],
			val.specs.get('specs',None)==target['specs'],
			#---! calc spec objects sometimes have Calculation type or dictionary ... but why?
			val.specs['calc'].name==target['calc']['calc_name'],
			#---! ... 2017.4.19
			#val.specs['calc']['calc_name']==target['calc']['calc_name'],
			])]
		except:
			import pdb;pdb.set_trace()
		if len(matches)>1: 
			#---fallback to direct comparison of the raw specs
			#---! note that this is not sustainable! FIX IT!
			rematch = [key for key in matches if self.calc.specs_raw==self.work.postdat.toc[key].specs_raw]
			if len(rematch)==1: 
				print('[WARNING] ULTRAWARNING we had a rematch!')
				return rematch[0]
			import ipdb;ipdb.set_trace()
			raise Exception('multiple unique matches in the spec files. major error upstream?')
		elif len(matches)==1: return matches[0]

		#---! note that in order to port omnicalc back to ptdins, rpb added dat_type to Slice.flat()
		#---here we allow more stuff in the spec than you have in the meta file since the legacy
		#---...simulations added data to the specs
		#---!...I THINK !??!
		#matches2 = [dict(copy.deepcopy(target),**val.specs)==val in self.work.postdat.posts().items()]
		#---tried
		#[key for key,val in self.work.postdat.posts().items() if dict(val.specs,**target)==val.specs]
		"""
		ipdb> target
		{'slice': {'end': 108000, 'short_name': 'v509', 'skip': 100, 'start': 28000, 'pbc': 'mol', 'group': 'all', 'dat_type': 'gmx', 'slice_type': 'standard'}, 'calc': {'calc_name': 'hydrogen_bonding'}, 'specs': {'distance_cutoff': 3.4, 'angle_cutoff': 160.0}}
		ipdb> self.work.postdat.toc['v509.28000-108000-100.all.pbcmol.hydrogen_bonding.n0'].specs==target
		True
		ipdb> matches
		[]
		self.work.postdat.toc['v509.28000-108000-100.all.pbcmol.lipid_abstractor.n0'].specs
		"""
		#key = 'v509.28000-108000-100.all.pbcmol.electron_density_profile.n0'
		#asciitree({'have':self.work.postdat.toc[key].specs,'target':target})
		
		this_key = 'v003.0-400000-200.proteins.pbcmol.protein_abstractor.n0'
		this_key = 'v003.0-400000-200.all.pbcmol.lipid_abstractor.n2'
		#import ipdb;ipdb.set_trace()
		
		#---! upgrades to the data structures are such that calculations can be directly matched
		#---! ...however the slices need to be matched
		matches = [name for name,post in self.work.postdat.posts().items() 
			if post.specs['calc']==self.calc and self.slice.flat()==post.specs['slice']]
		if len(matches)==1: return matches[0]
		#---match failure returns nothing
		return

		#---! before making calculations into an explicit object
		if False:
			matches_sub = [key for key,val in self.work.postdat.posts().items() if 
				all([val.specs[subkey].viewitems()>=target[subkey].viewitems() for subkey in target.keys()])]
			if len(matches_sub)>1: 
				raise Exception('multiple unique SUB-matches in the spec files. major error upstream?')
			elif len(matches_sub)==1: 
				print('[WARNING] using subset match. you had more attributes than specs!')
				#import ipdb;ipdb.set_trace()
				return matches_sub[0]

	def make_name(self):
		"""
		"""
		#---each post-processing name has name data and spec data
		#---this function decides what kinds of information goes into each
		#---there are two cases: standard slice naming with gromacs slices gets name data
		#---...while everything else relies on the spec file
		slice_type = self.slice.flat()['slice_type']
		if slice_type=='standard':
			print('gmx')
		elif slice_type=='readymade_namd':
			print('namd')
		else: raise Exception('invalid slice type %s'%slice_type)
		namesss = self.work.namer.name_slice(kind='standard',sn=self.slice.flat()['short_name'],
			**dict([(key,self.slice.flat()[key]) for key in ['start','end','group','skip','pbc']]))
		import ipdb;ipdb.set_trace()
