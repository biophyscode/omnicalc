#!/usr/bin/env python

"""
OMNICALC DATA STRUCTURES
Note that this file "maps" much of the metadata onto objects in memory
and hence constitutes the core of the omnicalc functionality (other than omnicalc.py).
"""

import os,sys,glob,re,json,copy,time,collections
from datapack import asciitree,delve,delveset,catalog
from base.hypothesis import hypothesis
from base.tools import status
from slicer import make_slice_gromacs,edrcheck

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
	Several classes below inherit this to have easy access to the namers.
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
		self.work = kwargs.get('work',None)
		self.short_namer = kwargs.pop('short_namer',None)
		self.short_names = kwargs.pop('short_names',None)
		#---since the short_namer is the default if no explicit names we provide the identity function
		if not self.short_namer: self.short_namer = lambda sn,spot=None: sn
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
				spotname = self.work.raw.spotname_lookup(sn)
				name_data['short_name'] = self.short_namer(sn,spot=spotname)
				name_data['suffix'] = suffix
				slice_files[out] = self.parser[('standard','gmx')]['d2n']%name_data
		elif kind=='datspec':
			#---first we try the standard datspec
			#---! could be more systematic about checking keys
			name = None
			for name_style in [('standard','datspec'),('raw','datspec')]:
				try:
					spotname = self.work.raw.spotname_lookup(sn)
					name = self.parser[name_style]['d2n']%dict(
						short_name=self.short_namer(kwargs['sn'],spot=spotname),nnum=0,**kwargs)
				except: pass
			if not name: raise Exception('cannot generate datspec name')
			slice_files = name
		else: raise Exception('invalid slice kind: %s'%kind)
		return slice_files

class PostDat(NamingConvention):

	"""
	A library of post-processed data.
	This class mirrors the data in the post_spot (aka post_data_spot). It includes both post-processing 
	dat/spec file pairs, as well as sliced trajectories in gro/xtc or psf/dcd formats.
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
		if not self.stable: nfiles,nchars = 0,0
		else: nfiles,nchars = len(self.stable),max([len(j) for j in self.stable])
		#---master classification loop
		while self.stable: 
			name = self.stable.pop()
			status(name,tag='import',i=nfiles-len(self.stable)-1,looplen=nfiles,bar_width=10,width=65)
			#---interpret the name
			namedat = self.interpret_name(name)
			if not namedat: 
				#---this puts the slice in limbo. we ignore stray files in post spot
				self.toc[name] = {}
			else:
				#---if this is a datspec file we find its pair and read the spec file
				if namedat['dat_type']=='datspec':
					basename = self.get_twin(name,('dat','spec'))
					this_datspec = DatSpec(fn=basename,dn=work.paths['post_data_spot'],work=self.work)
					if this_datspec.valid: self.toc[basename] = this_datspec
				#---everything else must be a slice
				#---! alternate slice types (e.g. gro/trr) would go here
				else: 
					#---decided to pair gro/xtc because they are always made/used together
					basename = self.get_twin(name,('xtc','gro'))
					self.toc[basename] = Slice(name=basename,namedat=namedat)

	def search_slices(self,**kwargs):
		"""
		Find a specific slice.
		"""
		slices,results = self.slices(),[]
		#---flatten kwargs
		target = copy.deepcopy(kwargs)
		target.update(**target.pop('spec',{}))
		flats = dict([(slice_key,val.flat()) for slice_key,val in slices.items()])
		results = [key for key in flats if flats[key]==target]
		#---return none for slices that still need to be made
		if not results: return []
		return results

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

class DatSpec(NamingConvention):

	"""
	Parent class for identifying a piece of post-processed data.
	DatSpec instances can be picked up from a file on disk but also generated before running a job, which 
	will inevitably write the post-processed data anyway.
	"""

	def __init__(self,fn=None,dn=None,job=None,work=None):
		"""
		We construct the DatSpec to mirror a completed result on disk OR a job we would like to run.
		"""
		self.work = work
		self.valid = True
		if fn and job: raise Exception('cannot send specs if you send a file')
		elif fn and not dn: raise Exception('send the post directory')
		elif fn and dn: 
			try: self.from_file(fn,dn)
			except: self.valid = False
		elif job and fn: raise Exception('cannot send a file if you send specs')
		elif job: self.from_job(job)
		else: raise Exception('this class needs a file or a job')

	def from_file(self,fn,dn):
		"""
		DatSpec objects can be imported from file (including from previous versions of omnicalc)
		or they can be constructed in anticipation of finishing a calculation job and *making* the file 
		(see from_job below). This function handles most backwards compatibility.
		"""
		path = os.path.join(dn,fn)
		self.files = {'dat':fn+'.dat','spec':fn+'.spec'}
		self.specs = json.load(open(path+'.spec'))
		#---for posterity we copy the specs before formatting everything
		self.specs_raw = copy.deepcopy(self.specs)
		json_type_fixer(self.specs_raw)
		json_type_fixer(self.specs)
		self.namedat = self.interpret_name(fn+'.spec')
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
				name=self.specs['calc']['calc_name'],specs=self.specs['specs'])
		#---sometimes we hide calculations that are already complete because we are adding data
		except: 
			#---create a dummy calcspec if we cannot find the calculation in the meta
			#---! note that we may wish to populate this more. this error was found when trying to find a
			#---! ...match later, and the find_match function was trying to look in the CalcSpec for a 
			#---! ...calculation which had been removed from the meta
			#---we supply a name because find_match will be looking for one
			self.specs['calc'] = Calculation(name=None,specs={},stub=[])

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
			'readymade_namd':('raw','datspec')}.get(slice_type,None)
		if not parser_key: raise Exception('unclear parser key')
		basename = self.parser[parser_key]['d2n']%dict(
			calc_name=self.job.calc.name,**self.job.slice.flat())
		#---note that the basename does not have the nN number yet (nnum)
		return basename

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
		Represent the calculations from the metadata.
		"""
		if 'work' not in kwargs: raise Exception('sorry! send work manually in kwargs please')
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
		Get upstream calculations.
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
		#---if the upstream object is a list we get the calculation specs from there
		if type(specs)==list:
			for calcname in specs:
				upstream_calcs.append(self.toc[calcname][0])
		elif type(specs)==dict:
			for key,val in specs.items():
				#---! upstream keys need to recurse or something?
				if key=='upstream':
					for key_up,val_up in val.items():
						#---! why has rpb not encountered this yet?
						raise Exception('???')
				else:
					#---previously we required that `i.stub['specs']==val` but this is too strict
					#---! val cannot be None below??
					if key not in self.toc: 
						raise Exception('searching upstream data and cannot find calculation %s'%key)
					matches = [i for ii,i in enumerate(self.toc[key]) 
						if val!=None and val.viewitems()<=i.stub.get('specs',{}).viewitems()]
					#---try to match the stubs. this will work if you point to an upstream calculation 
					#---... with the name of a subdictionary that represents a single calculation under a loop
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
			match_len_first = len(matches)
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
				if len(self.toc[name])>0:
					print('[WARNING] here is a hint because we are excepting soon. '+
						'the first specs of the toc for this calculation is: %s'%self.toc[name][0].specs)
				else: print('[WARNING] here is a hint because we are excepting soon: there are no calcs')
				raise Exception('failed to find calculation %s with specs %s in the CalcMeta'%(name,specs)+
					'. it is likely that you need to *be more specific* (found %d then %d matches). '%
					(match_len_first,len(matches))+
					'remember that you can specify calculation specs as a dictionary in the plot request. '+
					'see the warning above for more details.')

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
		self.do_slices = kwargs.pop('do_slices',True)
		self.work = kwargs.pop('work',None)
		if kwargs: raise Exception('unprocessed kwargs: %s'%kwargs)
		#---first we parse the meta (the slices dictionary from the metadata) into a dictionary
		#---the slices requests are keyed by sn, then slice,group couples
		self.slices = dict([(sn,{}) for sn in meta])
		self.groups = dict([(sn,{}) for sn in meta])
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
					#---collect the groups
					self.groups[sn] = dict([(k,v) for k,v in sl.get('groups',{}).items()])
		#---parse the spots so we know them for the namer
		self.work.raw = ParsedRawData(self.work)
		#---now we reprocess each slice into a Slice instance for later comparison
		needs_slices = []
		for sn in self.slices:
			for slice_name,group_name in self.slices[sn]:
				proto_slice = self.slices[sn][(slice_name,group_name)]
				#---some slices languish in limbo and we retrieve them here, since this is the stage at 
				#---...which requests become "mature" slices, linked in from the postdat
				#---...hence this is the earliest point that we could match request with files in limbo
				if proto_slice['slice_type']=='readymade_namd':
					name = 'dummy%d'%int(time.time())
					#---! rare case where we require None if no spot
					spotname = self.work.raw.spotname_lookup(sn),
					self.work.postdat.toc[name] = Slice(
						name=name,namedat={},slice_type='readymade_namd',dat_type='namd',
						spec=proto_slice['spec'],short_name=self.work.namer.short_namer(sn,spot=spotname))
					psf = proto_slice['spec'].get('psf')
					if psf in self.work.postdat.toc: del self.work.postdat.toc[psf]
					for fn in proto_slice['spec'].get('dcds',[]):
						if fn in self.work.postdat.toc: del self.work.postdat.toc[fn]
					#---! ADD FILES HERE
				#---! previous idea was to make slices and then create a comparison operator
				#---search the slices for one with the right specs
				#---including simulation short_name is enforced here
				try: spotname = self.work.raw.spotname_lookup(sn)
				except: spotname = None
				slice_req = dict(short_name=self.work.namer.short_namer(sn,spot=spotname),**proto_slice)
				valid_slices = self.work.postdat.search_slices(**slice_req)
				if len(valid_slices)>1: raise Exception('multiple valid slices')
				elif len(valid_slices)==0: 
					#---save what we need to make the slice, and the mature slice, in pairs
					extras = dict(slice_type='standard',dat_type='gmx',
						group=group_name,slice_name=slice_name,sn=sn,spec=proto_slice['spec'])
					needs_slices.append((dict(sn=sn,**slice_req),extras))
				else: self.slices[sn][(slice_name,group_name)] = self.work.postdat.toc[valid_slices[0]]
		#---we tag all slices with useful metadata
		for sn in self.slices:
			#---only decorate proper slices, not dictionaries
			for slice_name,group_name in self.slices[sn]:
				if type(self.slices[sn][(slice_name,group_name)])==dict: 
					continue
				self.slices[sn][(slice_name,group_name)].sn = sn
				self.slices[sn][(slice_name,group_name)].slice_name = slice_name
				self.slices[sn][(slice_name,group_name)].group = group_name
		#---any slices which are not available are sent to the slicer
		if needs_slices and self.do_slices:
			print('[NOTE] there are %d slices we must make'%len(needs_slices))
			#---make_slice_gromacs requires a sequence from the ParsedRawData method
			for ns,(new_slice,extras) in enumerate(needs_slices):
				new_slice['sequence'] = self.work.raw.get_timeseries(new_slice['sn'])
				#---the prefixed sn goes to the make_slice_gromacs function to be used at the beginning of 
				#---...the name. the prefixer helps to distinguish simulations from different spots.
				#---...we also use a "shortnamer" if it is found in the meta to simplify names for later
				#---...the shortnamer is applied here. note that this is irreversible: using the shortnamer
				#---...once will cause slices to be written with those names, so you should not change it
				#---note also that the shortnamer in the meta dictionary of your metadata should be 
				#---...compatible with the "namer" in the definition of the spot (usually starts in the 
				#---...factory connection file and is then passed to omnicalc config.py). to recap: the 
				#---..."namer" with the spot renames incoming data from multiple spots (and takes the spot
				#---...name as an argument as well). the meta,short_namer in the metadata should mimic this
				#---...for cases where you import the post with no spots
				#---! development note: tested three naming conventions on 2017.5.15: ptdins, actinlink, and
				#---! ...banana with and without the identity function namer. this means that the data are
				#---! ...all backwards compatible now, however you must modify the short_namer in the meta
				#---! ...to accept the spotname as well as the simulation name, and tell users that the 
				#---! ...shortnamer must be compatible with all "namer" functions (a.k.a. the prefixer)
				#---! ...associated with each spot. only then will omnicalc be able to merge multiple spots
				#---! ...with redundant names into a single post-processing data set. then you would remove
				#---! ...the check, somewhere in maps.py or omnicalc.py, which prevents redundant simulation
				#---! ...names, and all internal naming will be unique. also remove this crazy comment.
				spotname = self.work.raw.spotname_lookup(new_slice['sn'])
				short_name = self.work.namer.short_namer(new_slice['sn'],spot=spotname)
				#---the following try-except loop handles both identity versus specific namers
				#---the following is the only call to prefixer anywhere in omnicalc (prefixer requires spots)
				try: new_slice['sn_prefixed'] = self.work.raw.prefixer(short_name)
				except: new_slice['sn_prefixed'] = self.work.raw.prefixer(new_slice['sn'])
				new_slice['tpr_keyfinder'] = self.work.raw.keyfinder((spotname,'tpr'))
				new_slice['traj_keyfinder'] = self.work.raw.keyfinder(
					(spotname,self.work.raw.trajectory_format))
				new_slice['gro_keyfinder'] = self.work.raw.keyfinder((spotname,'structure'))
			#---make slices
			#---! note that we make slices if they appear in the slices dictionary, regardless of whether we
			#---! need them or not. this is somewhat different than the current ethos here in maps.py
			for ns,(new_slice,extras) in enumerate(needs_slices):
				print('[SLICE] making slice %d/%d'%(ns+1,len(needs_slices)))
				asciitree({'slice':dict([(k,v) for k,v in new_slice.items() 
					if k not in ['sequence','traj_keyfinder','tpr_keyfinder','gro_keyfinder']])})
				#---collect group name and selection
				#---! should this always happen? or is there a no-group-means-all option?
				group_name = new_slice['spec']['group']
				group_selection = self.groups[new_slice['sn']][group_name]
				new_slice.update(group_name=group_name,group_selection=group_selection)
				#---retrieve the last starting structure for making groups REMOVE THIS COMMENT
				#---! note that this function was called get_last_start_structure in legacy omnicalc REMOVE THIS COMMENT
				new_slice['last_structure'] = self.work.raw.get_last_structure(new_slice['sn'])
				fn = make_slice_gromacs(postdir=self.work.postdir,**new_slice)
				namedat = self.work.postdat.interpret_name(fn+'.gro')
				mature_slice = Slice(name=fn,namedat=namedat,**extras)
				#---once we make the slice we update the postdat
				self.slices[new_slice['sn']][(extras['slice_name'],extras['group'])] = mature_slice
			#---we must refresh the post-processing data in order to find the slices we just made
			self.work.postdat = PostDat(where=self.work.config.get('post_data_spot',None),
				namer=self.work.namer,work=self.work)

	def get_slice(self,sn,group,slice_name):
		"""
		Get slices. Group is permissive so we retrieve slices with this function.
		"""
		if sn not in self.slices: raise Exception('the slice object from the metadata lacks a simulation '+
			'named %s.'%sn+' this probably means that you have not included this simulation '+
			'in the slices dictionary in your metadata.')
		if (slice_name,group) in self.slices[sn]: 
			return self.slices[sn][(slice_name,group)]
		#---revert to group None if we cannot find it. this happens for no-group slices from e.g. NAMD
		elif (slice_name,None) in self.slices[sn]:
			return self.slices[sn][(slice_name,None)]
		else:
			asciitree(dict([('%s,%s'%k,v.__dict__) for k,v in self.slices[sn].items()]))
			raise Exception('see slices (meta) above. '+
				'cannot find slice for simulation %s: %s,%s'%(sn,slice_name,group))

class ParsedRawData:

	"""
	Import raw simulation data.
	"""

	def __init__(self,work):
		"""
		This code was very quickly ported from workspace.py in the legacy omnicalc. 
		It is kept separate from the workspace for now.
		"""
		#---default
		self.trajectory_format = 'xtc'
		#---! note that at some point you should resolve the clumsy interconnectivity problems (work)
		self.work = work
		#---the "table of contents" holds all the data about our simulations
		#---get spots
		spots = self.work.config.get('spots',{})
		#---process the spots
		#---for each "spot" in the yaml file, we construct a template for the data therein
		#---the table of contents ("toc") holds one parsing for every part regex in every spot
		self.spots,self.toc = {},collections.OrderedDict()
		for name,details in spots.items():
			rootdir = os.path.join(details['route_to_data'],details['spot_directory'])
			if not os.path.isdir(rootdir):
				raise Exception('\n[ERROR] cannot find root directory %s'%rootdir)
			for part_name,part_regex in details['regexes']['part'].items():
				status('[STATUS] parsing %s'%part_name)
				spot = (name,part_name)
				self.toc[spot] = {}
				self.spots[spot] = {
					'rootdir':os.path.join(rootdir,''),
					'top':details['regexes']['top'],
					'step':details['regexes']['step'],
					'part':part_regex,
					'namer':eval(details['namer']),
					'namer_text':details['namer'],}
				self.spots[spot]['divy_keys'] = self.divy_keys(spot)
		#---run the treeparser on each spot
		for spotname,spot in self.spots.items(): self.treeparser(spotname,**spot)

	def divy_keys(self,spot):
		"""
		The treeparser matches trajectory files with a combined regex. 
		This function prepares a lambda that divides the combined regex into parts and reduces them to 
		strings if there is only one part. The resulting regex groups serve as keys in the toc.
		"""
		group_counts = [sum([i[0]=='subpattern' 
			for i in re.sre_parse.parse(self.spots[spot][key])]) 
			#---apply naming convention
			for key in ['top','step','part']]
		cursor = ([0]+[sum(group_counts[:i+1]) for i in range(len(group_counts))])
		slices = [slice(cursor[i],cursor[i+1]) for i in range(len(cursor)-1)]
		divy = lambda x: [y[0] if len(y)==1 else y for y in [x[s] for s in slices]]
		return divy

	def keyfinder(self,spotname):
		"""
		Decorate the keys_to_filename lookup function so it can be sent to e.g. slice_trajectory.
		If you are only working with a single spot, then this creates the file-name inference function
		for all data in that spot.
		"""
		def keys_to_filename(*args,**kwargs):
			"""
			After decomposing a list of files into keys that match the regexes in paths.yaml we often 
			need to reconstitute the original filename.
			"""
			strict = kwargs.get('strict',True)
			if not spotname in self.toc: raise Exception('need a spotname to look up keys')
			#---! it may be worth storing this as a function a la divy_keys
			#---follow the top,step,part naming convention
			try:
				backwards = [''.join(['%s' if i[0]=='subpattern' else chr(i[1]) 
					for i in re.sre_parse.parse(regex)]) for regex in [self.spots[spotname][key] 
					for key in ['top','step','part']]]
				fn = os.path.join(
					self.spots[spotname]['rootdir'],
					'/'.join([backwards[ii]%i for ii,i in enumerate(args)]))
			except Exception as e: 
				print(e)
				raise Exception('error making keys: %s,%s'%(str(spotname),str(args)))
			if strict: 
				if not os.path.isfile(fn): raise Exception('cannot find %s'%fn)
			return fn
		return keys_to_filename

	def spotname_lookup(self,sn_full):
		"""
		Find the spotname for a particular simulation.
		This is only used in a few places in maps.py: in the prefixer below and the portion of 
		SliceMeta.__init__ which makes slices.
		"""
		#---alias to the shortname (irreversible) here
		#---! naming is getting convoluted. the following try-except would be hard to debug
		try: sn = self.work.namer.short_namer(sn_full,spot=None)
		#---failure to run the shortnamer just passes the full simulation name
		except: sn = sn_full
		assert type(sn)==str
		#---the following or statement allows this to work for both full and short names
		#---! this needs further testing
		spotnames = [key for key,val in self.toc.items() if sn in val or sn_full in val]
		if not spotnames: 
			#---! development. needs tested. may only be used on near proto_slice in
			#---! ...readymade_namd near line 581. remove this and error message after testing
			#---! ...and note that the error below refers to refresh which is deprecated
			return None
			#---in case top diverges from prefixer we check the regexes
			top_regexes = [v['regexes']['top'] for v in self.work.paths['spots'].values()]
			if not any([re.match(top,sn) for top in top_regexes]):
				raise Exception('[ERROR] could not find "%s" in the toc *and* it fails to match '%sn+
					"any 'top' regexes in your paths. fix 'top' and make sure to run "+
					"'make refresh' to search for simulations again")
			raise Exception('[ERROR] could not find simulation "%s" in the toc'%sn)
		spotnames_unique = list(set(zip(*spotnames)[0]))
		if len(spotnames_unique) != 1: 
			raise Exception('[ERROR] you cannot have the same simulation in multiple spots.\n'+
				'simulation = "%s" and "%s"'%(sn,str(spotnames)))
		return spotnames_unique[0]

	def prefixer(self,sn):
		"""
		Choose a prefix for naming post-processing files.
		"""
		#---"spot" is a tuple of spotname and the part name
		#---namer takes the spotname (called spot in the yaml defn of namer) and the simulation name
		#---we include the partname when accessing self.spots
		try: this_spot = self.spotname_lookup(sn)
		except: raise Exception('cannot find the spot for %s'%sn)
		try:
			spot = spotname,partname = (this_spot,self.trajectory_format)
			#---new format for the namer requires arguments to be simulation name and then spot name
			#---note that the prefixer function is only called when we need slices hence there will always
			#---...be a spotname available. see SliceMeta.__init__ for these calls
			prefix = self.spots[spot]['namer'](sn,spot=spotname)
		except Exception as e: 
			raise Exception('[ERROR] prefixer failure on simulation "%s" (check your namer) %s'%(sn,e))
		return prefix

	def get_last_structure(self,sn,subtype='structure'):
		"""Get the most recent structure file."""
		#---! currently set for one-spot operation. need to apply namers for multiple spots
		spotname = self.spotname_lookup(sn)
		#---the toc is ordered but instead of getting the last one, we just
		#---...get all structures and use the mtimes
		steps = self.toc[(spotname,subtype)][sn]
		candidates = []
		for step_name,step in steps.items():
			for part_name,part in step.items():
				fn = self.keyfinder((spotname,subtype))(
					sn,step_name,part_name)
				#---hold the file if gro and we want structure (since this is the commonest use-case)
				#---...or always save it if we are looking for any other subtype e.g. tpr
				if re.match('^.+\.gro$',fn) or subtype!='structure': candidates.append(fn)
		#---return the newest
		return sorted(candidates,key=lambda x:os.path.getmtime(x))[-1]

	def treeparser(self,spot,**kwargs):
		"""
		This function parses simulation data which are organized into a "spot". 
		It writes the filenames to the table of contents (self.toc).
		"""
		spot_sub = kwargs
		rootdir = spot_sub['rootdir']
		#---start with all files under rootdir
		fns = [os.path.join(dirpath,fn) 
			for (dirpath, dirnames, filenames) 
			in os.walk(rootdir,followlinks=True) for fn in filenames]
		#---regex combinator is the only place where we enforce a naming convention via top,step,part
		#---note that we may wish to generalize this depending upon whether it is wise to have three parts
		regex = ('^%s\/'%re.escape(rootdir.rstrip('/'))+
			'\/'.join([spot_sub['top'],spot_sub['step'],spot_sub['part']])
			+'$')
		matches_raw = [i.groups() for fn in fns for i in [re.search(regex,fn)] if i]
		if not matches_raw: 
			status('no matches found for spot: "%s,%s"'%spot,tag='warning')
			return
		#---first we organize the top,step,part into tuples which serve as keys
		#---we organize the toc as a doubly-nested dictionary of trajectory parts
		#---the top two levels of the toc correspond to the top and step signifiers
		#---note that this procedure projects the top,step,part naming convention into the toc
		matches = [self.spots[spot]['divy_keys'](i) for i in matches_raw]
		self.toc[spot] = collections.OrderedDict()
		#---at this point we apply the irreversible transformation from long simulation names to short ones
		#---...by using the short namer.
		#---! removed short_namer on the top below for expedience, but this needs tested and confirmed
		matches = [(top,step,part) for top,step,part in matches]
		#---sort the tops into an ordered dictionary
		for top in sorted(set(zip(*matches)[0])): 
			self.toc[spot][top] = collections.OrderedDict()
		#---collect unique steps for each top and load them with the parts
		for top in self.toc[spot]:
			#---sort the steps into an ordered dictionary
			for step in sorted(set([i[1] for i in matches if i[0]==top])):
				#---we sort the parts into an ordered dictionary
				#---this is the leaf of the toc tree and we use dictionaries
				parts = sorted([i[2] for i in matches if i[0]==top and i[1]==step])
				self.toc[spot][top][step] = collections.OrderedDict([(part,{}) for part in parts])
		#---now the toc is prepared with filenames but subsequent parsings will identify EDR files

	def get_timeseries(self,sn_full):
		"""
		Typically EDR times are stored in the toc for a particular spot. 
		This function first figures out which spot you want and then returns the edr data.
		"""
		#---we apply the naming transformation to lookup the shortname in the toc, but below we will send out
		#---...the full name since the purpose of this function is to get filenames on disk for slicing
		sn = sn_full
		#---determine the spot, since the simulation could be in multiple spots
		spot_matches = [spotname for spotname,spot in self.spots.items() 
			if spotname[1]=='edr' and sn in self.toc[spotname]]
		if len(spot_matches)>1: 
			raise Exception('development. need a way to adjucate spots that have redundant simulations')
		elif len(spot_matches)==0:
			raise Exception('cannot find simulation %s in any of the spots: %s'%(sn,self.spots.keys()))
		else: spotname = spot_matches[0]
		edrtree = self.toc[spotname][sn]
		#---! current development only checks EDR files when they are needed instead of pre-populating
		for step in edrtree:
			for part in edrtree[step]:
				#---we have to send the full simulation name to the keyfinder
				fn = self.keyfinder(spotname)(sn_full,step,part)
				times = edrcheck(fn)
				keys = (spotname,sn,step,part)
				leaf = delve(self.toc,*keys)
				leaf['start'],leaf['stop'] = times
		#---naming convention
		#---according to the note above we pass the full simulation name back out for looking up files
		sequence = [((sn_full,step,part),tuple([edrtree[step][part][key] 
			for key in ['start','stop']]))
			for step in edrtree 
			for part in edrtree[step]]
		#---return a list of keys,times pairs
		return sequence

	def get_last(self,sn,subtype):
		"""
		Parse the toc to find the most recent file for a particular regex specified in the spots.
		"""
		#---this function is already mostly implemented above
		return self.get_last_structure(sn,subtype=subtype)
		#---! alternate method prototyped but not used
		spotname = self.spotname_lookup(sn)
		#---looking up the TPR with the standard method, which preassumes a top/step/part structure
		stepname = self.toc[(work.raw.spotname_lookup(sn),'tpr')][sn].keys()[-1]
		partname = self.toc[(work.raw.spotname_lookup(sn),'tpr')][sn][stepname].keys()[-1]
		fn = self.keyfinder((spotname,'tpr'))(sn,stepname,partname)

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

class ComputeJob:

	"""
	A computation job joins a calculation with a simulation slice.
	"""

	def __init__(self,calc,sl,**kwargs):
		"""
		Search for slices and match to a result.
		"""
		self.calc = calc
		self.work = kwargs.pop('work',None)
		if kwargs: raise Exception('unprocessed kwargs: %s'%kwargs)
		#---search the slice requests list to get more information about the target slice
		#---...note that in the mock up, the slice requests list represents subtype "b", above is "c"
		slices_by_sn = self.work.slice_meta.slices.get(sl.sn,None)
		if not slices_by_sn: 
			raise Exception('cannot find simulation %s in slice requests'%sl.sn)
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

	def slice_compare(self,this,that):
		"""
		Compare slices in the result matcher specifically ignoring some features which are irrelevant, namely
		the group. This allows downstream calculations to pull in slices derived from different groups.
		"""
		return all([this[key]==that[key] for key in this.keys()+that.keys() if key!='group'])

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
		#---convert strings to integers if possible
		for key,val in self.work.postdat.posts().items(): json_type_fixer(val.specs)
		#---we search for a result object by directly comparing the DatSpec.specs object
		#---! see the note above on the specs garbage.
		#---! switching to itemwise comparison
		#---! this is weird because it was working for ptdins
		matches = [key for key,val in self.work.postdat.posts().items() if all([
			self.slice_compare(val.specs['slice'],target['slice']),
			#---! recently (2017.07.29) changed the default for the specs to an empty dictionary from None
			#---! ...so that you can point to upstream calculations all cases
			val.specs.get('specs',{})==target.get('specs',{}),
			#---! calc spec objects sometimes have Calculation type or dictionary ... but why?
			val.specs['calc'].name==target['calc']['calc_name'],])]
		if len(matches)>1: 
			#---fallback to direct comparison of the raw specs
			#---! note that this is not sustainable! FIX IT!
			rematch = [key for key in matches if self.calc.specs_raw==self.work.postdat.toc[key].specs_raw]
			if len(rematch)==1: 
				print('[WARNING] ULTRAWARNING we had a rematch!')
				return rematch[0]
			raise Exception('multiple unique matches in the spec files. major error upstream?')
		elif len(matches)==1: 
			return matches[0]
		#---! note that in order to port omnicalc back to ptdins, rpb added dat_type to Slice.flat()
		#---here we allow more stuff in the spec than you have in the meta file since the legacy
		#---! removed lots of debugging notes here		
		#---! upgrades to the data structures are such that calculations can be directly matched
		#---! ...however the slices need to be matched
		matches = [name for name,post in self.work.postdat.posts().items() 
			if post.specs['calc']==self.calc and self.slice.flat()==post.specs['slice']]
		if len(matches)==1: 
			return matches[0]
		#---match failure returns nothing
		return

