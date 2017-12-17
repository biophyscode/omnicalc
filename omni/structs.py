#!/usr/bin/env python

"""
OMNICALC DATA STRUCTURES
~~~ "nothing (everything) is arbitrary"
annotations for data structures: "+++(build|translate|compare)"
"""

import re,copy

from base.tools import catalog
from datapack import asciitree,delveset,delve

def dictsub(subset,superset): 
	"""See if one dictionary is contained in another."""
	return all(item in superset.items() for item in subset.items())

class NoisyOmnicalcObject:
	def __repr__(self):
		"""Readable debugging."""
		asciitree({self.__class__.__name__:self.__dict__})
		return 'omnicalc %s object at %d'%(self.__class__.__name__,id(self))

class OmnicalcDataStructure:

	"""
	Parent class for flexible data structures.
	"""

	class StructureKey:
		"""Permissive object that serves as a name inside a flexible data structure."""
		def __init__(self,name):
			self.name = name
		def __eq__(self,other): 
			return True
		def __hash__(self): return id(self)

	class KeyCombo:
		"""Use a list to cross objects."""
		def __init__(self,name): self.name = name

	def classify(self,subject,strict=False):
		"""Identify a structure type using flextypes above."""
		# chart the subject
		routes = list(catalog(subject))
		candidates = []
		# get the relevant structures
		structs = getattr(self,'_flexible_structure_%s'%self.kind)
		# compare each structure to the subject
		for key,val in structs.items():
			template = list(catalog(val))
			# make sure that all routes match the datastructure
			# strict mode ensures the structures match exactly
			if ((not strict and all([r in zip(*template)[0] for r,v in routes]))or (strict and 
				set([tuple(j) for j in zip(*routes)[0]])==set([tuple(j) for j in zip(*template)[0]]))): 
				candidates.append(key)
		# retry in strict mode if we get too many matches
		if len(candidates)>1 and not strict: return self.classify(subject,strict=True)
		elif len(candidates)>1 and strict: raise Exception('matched multiple data structures to %s'%subject)
		elif len(candidates)==0: 
			raise Exception('failed to classify %s in %s'%(subject,self))
		else: return candidates[0]

	def cross(self,style,data):
		"""Turn a raw definition into multiple constituent components."""
		# chart the subject
		routes = list(catalog(data))
		# get the relevant structure and expand it
		structure = getattr(self,'_flexible_structure_%s'%self.kind)[style]
		template = list(catalog(structure))
		# hold the results and crosses
		toc,crosses = {},[]
		# loop over routes in the subject
		while routes:
			route,value = routes.pop()
			# find the matching route guaranteed by classify
			index, = [ii for ii,i in enumerate(zip(*template)[0]) if route==i]
			path,typ = template[index]
			# the identifier is the path up to the name
			hinge = max([ii for ii,i in enumerate(path) if i.__class__.__name__=='StructureKey'])
			# replace the StructureKey with the name found in the route at the hinge
			identifier = tuple(path[:hinge]+[route[hinge]])
			if identifier not in toc: toc[identifier] = {}
			# the subpath defines the route inside the final object
			subpath = tuple(path[hinge+1:])
			if typ.__class__.__name__=='KeyCombo':
				# if the terminus is a KeyCombo it will be crossed later
				crosses.append({'identifier':identifier,'subpath':subpath,'rename':typ.name,'values':value})
			else:
				if not subpath: toc[identifier] = value
				else: delveset(toc[identifier],*subpath,value=value)
		# apply crosses
		for cross in crosses:
			identifier = cross['identifier']
			subpath = cross['subpath']
			values = cross['values']
			rename = cross['rename']
			raw = toc.pop(identifier)
			for vv,val in enumerate(values):
				key = tuple(list(identifier)+[vv])
				toc[key] = copy.deepcopy(raw)
				delveset(toc[key],*tuple(list(subpath[:-1])+[rename]),value=val)
		return toc

	def test_equality(self,one,other,loud=False):
		"""
		Compare slices with flexible data structures.
		Set loud to figure out where you need to add comparisons.
		"""
		orderings = [(one,other),(other,one)]
		comps = ['_compare_%s_to_%s'%(a.style,b.style) for a,b in orderings]
		if all([hasattr(self,c) for c in comps]): raise Exception('redundant _compare_... functions!')
		compkeys = [cc for cc,c in enumerate(comps) if hasattr(self,c)]
		if len(compkeys)==2: raise Exception('redundant comparisons are available %s'%comps)
		# if we cannot compare the types they they are definitely not equivalent
		elif len(compkeys)==0: 
			if loud: print('[WARNING] cannot find comparison between: %s vs %s'%(one.style,other.style))
			return False
		# note that the argument order matters
		else: return getattr(self,comps[compkeys[0]])(*orderings[compkeys[0]])

class TrajectoryStructure(OmnicalcDataStructure):

	"""
	Abstract definition for "slices" i.e. trajectories in omnicalc.
	Initialization is handled by subclasses.
	dev: no typechecking yet,
	"""

	# the core structures provide the best detail
	_flexible_structure_request = {
		# structure definition for a standard slice made by omnicalc
		'standard_gromacs':{
			'slices':{OmnicalcDataStructure.StructureKey('slice_name'):{
				'pbc':'string','groups':OmnicalcDataStructure.KeyCombo('group'),
				'start':'number','end':'number','skip':'number'}},
			'groups':{OmnicalcDataStructure.StructureKey('group_name'):'string'}},}

	# the elements are individual components which may be derived from more than one request
	_flexible_structure_element = {
		'slice':{'key':'tuple','sn':'string',
			'val':{'start':'number','end':'number','skip':'number','group':'string','pbc':'string'}},
		'group':{'key':'tuple','val':'string','sn':'string'},}

	# alternate structures are used for parsing legacy data
	_flexible_structure_alternate = {
		'calculation_request':{'sn':'string','group':'string','slice_name':'string'},
		#! had to do something to distinguish spec_v3 via inference but could we be explicit ?!?
		'spec_v3':{
			'group':'string','sn':'string',
			'pbc':'string','start':'number','end':'number','skip':'number',},
		'legacy_spec_v2':{
			'group':'string','sn':'string',
			#! whittle dat_type and slice_type?
			'dat_type':'string','slice_type':'string',
			'short_name':'string','pbc':'string',
			'start':'number','end':'number','skip':'number',},
		'legacy_spec_v1_no_group':{
			'sn':'string','dat_type':'string','slice_type':'string',
			'short_name':'string','start':'number','end':'number','skip':'number',},}

	def __repr__(self):
		asciitree({self.__class__.__name__:self.__dict__})
		return 'omnicalc %s object at %d'%(self.__class__.__name__,id(self))

	def _compare_calculation_request_to_legacy_spec_v2(self,cr,ls):
		"""Match a calculation request to a legacy spec file (v2)."""
		#! generalize this? should this be in a function or structure?
		###### abandoned!!!!!!!!
		conditions = [cr.data['group']==ls.data['group'],]
		import ipdb;ipdb.set_trace()
		return all(conditions)

	def _compare_calculation_request_to_slice(self,cr,sl):
		conditions = [
			#! knowledge of the keys required here
			sl.data['key'][0]=='slices',cr.data['slice_name']==sl.data['key'][1],
			cr.data['sn']==sl.data['sn'],
			cr.data['group']==sl.data['val']['group'],]
		return all(conditions)

	def _compare_legacy_spec_v2_to_slice(self,ls,sl):
		conditions = [
			# the slice type is a gromacs standard so we make sure the legacy spec agrees
			ls.data.get('dat_type',None)=='gmx',ls.data.get('slice_type',None)=='standard',
			dictsub(sl.data['val'],ls.data),sl.data['sn']==ls.data['sn']]
		return all(conditions)


"""
prototyping a new spec data structure
	for standard standard gromacs data
		has the usual name from the slice
		slice is written by the TS below OR a struct above?
		data structure is
			slice
				stuff from the raw data
				what about dat type and slice type? or should this go in metadata?
					... do not want crosstalk
			calculation
				calc_name
				specs
			metadata (optionals)
				short name
				spec version
later replace the namingconvention or fold it in somehow?
"""

class NamingConvention:
	"""
	Organize the naming conventions for omnicalc.
	Several classes below inherit this to have easy access to the namers.
	"""
	# all n2d types in omni_namer get standard types
	common_types = {'wild':r'[A-Za-z0-9\-_]','float':r'\d+(?:(?:\.\d+))?',
		'gmx_suffixes':'(?:gro|xtc)'}
	omni_namer = [
		(('standard','gmx'),{
			'd2n':r'%(short_name)s.%(start)s-%(end)s-%(skip)s.%(group)s.pbc%(pbc)s.%(suffix)s',
			'n2d':'^(?P<short_name>%(wild)s+)\.'+
				'(?P<start>%(float)s)-(?P<end>%(float)s)-(?P<skip>%(float)s)\.'+
				'(?P<group>%(wild)s+)\.pbc(?P<pbc>%(wild)s+)\.%(gmx_suffixes)s$',}),
		(('standard','datspec'),{
			# we append the dat/spec suffix and the nnum later
			'd2n':r'%(short_name)s.%(start)s-%(end)s-%(skip)s.%(group)s.'+
				r'pbc%(pbc)s.%(calc_name)s',
			'n2d':'^(?P<short_name>%(wild)s+)\.'+
				'(?P<start>%(float)s)-(?P<end>%(float)s)-(?P<skip>%(float)s)\.'+
				'(?P<group>%(wild)s+)\.pbc(?P<pbc>%(wild)s+)\.(?P<calc_name>%(wild)s+)'+
				'\.n(?P<nnum>\d+)\.(dat|spec)$',}),
		(('standard_obvious','datspec'),{
			# we append the dat/spec suffix and the nnum later
			'd2n':r'%(short_name)s.%(start)s-%(end)s-%(skip)s.%(calc_name)s',
			'n2d':'^(?P<short_name>%(wild)s+)\.'+
				'(?P<start>%(float)s)-(?P<end>%(float)s)-(?P<skip>%(float)s)\.'+
				'(?P<calc_name>%(wild)s+)\.n(?P<nnum>\d+)\.(dat|spec)$',}),
		(('raw','datspec'),{
			# we append the dat/spec suffix and the nnum later
			#! should this include the number?
			'd2n':r'%(short_name)s.%(calc_name)s',
			'n2d':'^(?P<short_name>%(wild)s+)\.(?P<calc_name>%(wild)s+)\.n(?P<nnum>\d+)\.(dat|spec)$',}),]
	# keys required in a slice in the meta file for a particular umbrella naming convention
	omni_slicer_namer = {
		'standard':{'slice_keys':['groups','slices']},
		'readymade_namd':{'slice_keys':['readymade_namd']},
		'readymade_gmx':{'slice_keys':['readymade_gmx']},
		'readymade_meso_v1':{'slice_keys':['readymade_meso_v1']},}
	# alternate view of the namer
	parser = dict(omni_namer)

	def __init__(self,**kwargs):
		"""Turn a set of specs into a namer."""
		self.work = kwargs.get('work',None)
		self.short_namer = kwargs.pop('short_namer',None)
		self.short_names = kwargs.pop('short_names',None)
		# since the short_namer is the default if no explicit names we provide the identity function
		if not self.short_namer: self.short_namer = lambda sn,spot=None: sn
		elif type(self.short_namer)!=str: 
			raise Exception('meta short_namer parameter must be a string: %s'%self.short_namer)
		# compile the lambda function which comes in as a string
		else: self.short_namer = eval(self.short_namer)

	def interpret_name(self,name):
		"""Given a post-processing data file name, extract data and infer the version."""
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

	def basename(self,job,style):
		"""
		Name a piece of data according to our rules.
		"""
		# map slice style to the right namer
		#! note that we should replace the couplets above because they are redundant and match this to below
		#! spotname here. also need to formalize the name_reqs
		#! it would also be good to move sn up somewhere standard? some kind of get function on the slice ob?
		basename = self.parser[style]['d2n']%dict(calc_name=job.calc.name,suffix='dat',
			short_name=self.short_namer(job.slice.data['sn'],None),**job.slice.data['val'])
		return basename

