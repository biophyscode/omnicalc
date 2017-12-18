#!/usr/bin/env python

"""
OMNICALC DATA STRUCTURES
~~~ "nothing (everything) is arbitrary"
annotations for data structures: "+++(build|translate|compare)"
"""

import re,copy

from base.tools import catalog
from datapack import asciitree,delveset,delve,str_types

def dictsub(subset,superset): 
	"""See if one dictionary is contained in another."""
	return all(item in superset.items() for item in subset.items())

class NoisyOmnicalcObject:
	def __repr__(self):
		"""Readable debugging."""
		asciitree({self.__class__.__name__:self.__dict__})
		return 'omnicalc %s object at %d'%(self.__class__.__name__,id(self))

class OmnicalcDataStructure(NoisyOmnicalcObject):

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

	def __init__(self,data):
		"""
		Uniform constructor for all data types.
		"""
		self.data = data
		self.style = self.classify(self.data)
		if self.style not in ['calculation_request','post_spec_v2','slice_request_named']:
			import ipdb;ipdb.set_trace()

	types_map = {'string':str_types,'number':[int,float],'bool':[bool,None]}
	def type_checker(self,v,t): 
		# lists restruct is to specific options
		if type(t)==list: return v in t
		elif t in self.types_map: return type(v) in self.types_map[t]
		else: raise Exception('failed to type-check %s'%t)
	def _routes_typecheck(self,template,routes):
		template_to_type = dict([(tuple(i),j) for i,j in template])
		match_types = [self.type_checker(v,template_to_type[tuple(r)]) for r,v in routes]
		return all(match_types)
	def _routes_equality(self,template,routes,check_types=False):
		template_routes,template_types = zip(*template)
		routes_routes,routes_types = zip(*routes)
		match_routes = set([tuple(j) for j in routes_routes])==set([tuple(j) for j in template_routes])
		if not check_types: return match_routes
		elif not match_routes: return False
		else: return match_routes and self._routes_typecheck(template,routes)
	def _routes_subset(self,template,routes,check_types=False):
		template_routes,template_types = zip(*template)
		match_routes = all([r in template_routes for r,v in routes])
		if not check_types: return match_routes
		elif not match_routes: return False
		else: return match_routes and self._routes_typecheck(template,routes)

	def classify(self,subject):
		"""Identify a structure type using flextypes above."""
		# chart the subject
		routes = list(catalog(subject))
		candidates = []
		# get the relevant structures
		structs = self._structures
		# compare each structure to the subject
		for key,struct in structs.items():
			strict_this = struct.get('meta',{}).get('strict',False)
			check_types = struct.get('meta',{}).get('check_types',True)
			template = list(catalog(struct['struct']))
			# make sure that all routes match the data structure
			if strict_this and self._routes_equality(template,routes,check_types=check_types):
				candidates.append(key)
			elif not strict_this and self._routes_subset(template,routes,check_types=check_types):
				candidates.append(key)
		#! removed a strict keyword that applied to all classifications and ran after multiple 
		#! ... matches were made in order to find a more specific one. this was too much inference!
		if len(candidates)>1: 
			raise Exception('matched multiple data structures to %s'%subject)
		elif len(candidates)==0: 
			import ipdb;ipdb.set_trace()
			raise Exception('failed to classify %s in %s'%(subject,self))
		else: return candidates[0]

	def cross(self,style,data):
		"""Turn a raw definition into multiple constituent components."""
		# chart the subject
		routes = list(catalog(data))
		# get the relevant structure and expand it
		structure = self._structures[style]['struct']
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

	def __eq__(self,other):
		"""Supervise comparisons."""
		if self.style==other.style: return self.data==other.data
		# all comparisons happen with special functions instead of the usual __eq__
		#! more elegant way to handle comparison orderings? perhaps a dictionary or something?
		orderings = [(self,other),(other,self)]
		function_names = ['_eq_%s_%s'%(a.style,b.style) for a,b in orderings]
		#! no protection against contradicting equivalence relations or functions
		for name,(first,second) in zip(function_names,orderings):
			if hasattr(self,name): return getattr(self,name)(a,b)
		if hasattr(self,'_equivalence'):
			for first,second in orderings:
				if (first.style,second.style) in self._equivalence:
					return all([first.data[key]==second.data[key] 
						for key in self._equivalence[(first.style,second.style)]])
		print(self)
		print(other)
		raise Exception('see structures above. '
			'cannot find a comparison function or equivalence relation for %s,%s'%(self.style,other.style))

class TrajectoryStructure(OmnicalcDataStructure):

	"""
	Abstract definition for "slices" i.e. trajectories in omnicalc.
	Initialization is handled by subclasses.
	dev: no typechecking yet,
	"""

	# abstract trajectory structure
	_structures = {
		# calculations which request a slice and a group
		'calculation_request':{
			'struct':{'sn':'string','group':'string','slice_name':'string'},
			'meta':{'strict':True}},
		'post_spec_v2':{
			'struct':{
				'group':'string','sn':'string',
				'dat_type':['gmx'],
				'slice_type':['standard'],
				'short_name':'string','pbc':'string',
				'start':'number','end':'number','skip':'number'},
			'meta':{'strict':True,'check_types':True}},
		'slices_request':{
			# cannot be strict and cannot check types for this style
			'meta':{'strict':False,'check_types':False},
			'struct':{
				'slices':{OmnicalcDataStructure.StructureKey('slice_name'):{
					'pbc':'string','groups':OmnicalcDataStructure.KeyCombo('group'),
					'start':'number','end':'number','skip':'number'}},
				'groups':{OmnicalcDataStructure.StructureKey('group_name'):'string'}}},
		'slice_request_named':{
			'meta':{'strict':True,'check_types':True},
			'struct':{
				'sn':'string','group':'string','slice_name':'string','pbc':'string',
				'start':'number','end':'number','skip':'number'}},}

	_equivalence = {
		('slice_request_named','calculation_request'):['slice_name','sn','group'],
		('post_spec_v2','slice_request_named'):['sn','group','pbc','start','end','skip'],}

	#def _eq_calculation_request_slice_request_named(self,calc_request,slice_request):
	#	import ipdb;ipdb.set_trace()

	if False:
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

	if False:

		def __repr__(self):
			asciitree({self.__class__.__name__:self.__dict__})
			return 'omnicalc %s object at %d'%(self.__class__.__name__,id(self))

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

#!!! RIPE FOR REFACTOR
######################
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
			short_name=self.short_namer(job.slice.data['sn'],None),**job.slice.data)
		return basename
