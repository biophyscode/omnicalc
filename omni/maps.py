#!/usr/bin/env python

import os,sys,glob,re,json,copy,time
from datapack import asciitree

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

def fix_integers(series):
	"""Cast integer strings as integers, recursively."""
	for k,v in series.items():
		if type(v) == dict: fix_integers(v)
		elif type(v)in str_types and v.isdigit(): series[k] = int(v)

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
		#---master classification loop
		while self.stable: 
			name = self.stable.pop()
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
					self.toc[basename] = DatSpec(fn=basename,dn=work.paths['post_data_spot'])
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
		return results

	def get_twin(self,name,pair):
		"""
		Many slices files have natural twins e.g. dat/spec and gro/xtc.
		This function finds them.
		"""
		this_suffix = re.match('^.+\.(%s)$'%'|'.join(pair),name).group(1)
		basename = re.sub('\.(%s)$'%'|'.join(pair),'',name)
		twin = basename+'.%s'%dict([pair,pair[::-1]])[this_suffix]
		if twin not in self.stable: raise Exception('cannot find dat-spec twin of %s'%name)
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

	def __init__(self,fn=None,dn=None,job=None):
		"""
		We construct the DatSpec to mirror a completed result on disk OR a job we would like to run.
		"""
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

	def from_job(self,job):
		"""
		"""
		self.namedat = {}
		#---retain the pointer to the job
		self.job = job
		#---construct specs for a new job
		#---new specs VERSION 2 include top-level: specs, calc_name, slice
		self.specs = {'specs':self.job.calc.specs,
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

class Calculation:

	"""
	A calculation, including settings.
	"""

	def __init__(self,**kwargs):
		"""
		"""
		for key in ['name','uptype','group','slice_name','collections']:
			if key not in kwargs: raise Exception('calculation requires %s'%key)
			self.__dict__[key] = kwargs.pop(key)
		self.specs = kwargs.pop('specs',{})
		if kwargs: raise Exception('unprocessed kwargs: %s'%kwargs)

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
						self.slices[sn][(slice_name,None)] = dict(slice_type='readymade_namd',spec=spec)
				elif slice_type=='slices':
					for slice_name,spec in slice_group.items():
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
								slice_type='standard',spec=dict(group=group,**spec))
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

	def flat(self):
		"""
		Reduce a slice into a more natural form.
		"""
		slice_type = self.namedat.get('slice_type')
		if not slice_type: slice_type = self.__dict__.get('slice_type')
		if not slice_type: raise Exception('indeterminate slice type')
		me = dict(slice_type=slice_type,**self.namedat.get('body',{}))
		me.update(**self.__dict__.get('spec',{}))
		if 'short_name' not in me and 'short_name' in self.__dict__:
			me['short_name'] = self.short_name
		#---trick to fix integers
		fix_integers(me)
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
		self.result = self.match_result()
		#---keep the simulation name
		self.sn = sl.sn

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
		target = {'specs':self.calc.specs,
			'calc':{'calc_name':self.calc.name},
			'slice':self.slice.flat()}
		#---we search for a result object by directly comparing the DatSpec.specs object
		matches = [key for key,val in self.work.postdat.posts().items() if val.specs==target]
		if len(matches)>1: raise Exception('multiple unique matches in the spec files. major error upstream?')
		elif len(matches)==1: return matches[0]
		return

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
		namesss = self.work.namer.name_slice(kind='standard',sn=self.slice.flat()['short_name'],**dict([(key,self.slice.flat()[key]) for key in ['start','end','group','skip','pbc']]))
		import ipdb;ipdb.set_trace()
