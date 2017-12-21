#!/usr/bin/env python

"""
OMNICALC DATA STRUCTURES
"""

import os,sys,re,copy,glob

from base.tools import catalog
from datapack import asciitree,delveset,delve,str_types,dictsub,json_type_fixer

class NoisyOmnicalcObject:
	def __repr__(self):
		"""Readable debugging."""
		asciitree({self.__class__.__name__:self.__dict__})
		return '[STATUS] omnicalc %s object at %d'%(self.__class__.__name__,id(self))

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

	types_map = {'string':str_types,'number':[int,float],'bool':[bool,None],'list':[list]}
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
		function_names = ['_eq_%s_to_%s'%(a.style,b.style) for a,b in orderings]
		#! no protection against contradicting equivalence relations or functions
		for name,(first,second) in zip(function_names,orderings):
			if hasattr(self,name): return getattr(self,name)(a,b)
		if hasattr(self,'_unequal') and set([self.style,other.style]) in self._unequal: return False
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
	"""

	# abstract trajectory structure
	_structures = {
		# calculations which request a slice and a group
		'calculation_request':{
			'struct':{'sn':'string','group':'string','slice_name':'string'},
			'meta':{'strict':True}},
		'post_spec_v2':{
			'struct':{
				'sn':'string','short_name':'string',
				'dat_type':['gmx'],'slice_type':['standard'],
				'group':'string','pbc':'string',
				'start':'number','end':'number','skip':'number'},
			'meta':{'strict':True,'check_types':True}},
		'post_spec_v2_basic':{
			'struct':{
				'sn':'string','short_name':'string',
				'dat_type':['gmx'],'slice_type':['standard'],
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
				'start':'number','end':'number','skip':'number'}},
		'gromacs_group':{
			'meta':{'strict':True,'check_types':True},
			'struct':{'group_name':'string','selection':'string','sn':'string'}},
		'gromacs_slice':{
			'meta':{'strict':True,'check_types':True},
			'struct':{
				'sn':'string',
				'body':{'short_name':'string','pbc':'string','group':'string',
				'start':'number','end':'number','skip':'number'},
				'suffixes':'list','basename':'string',
				'dat_type':['gmx'],'slice_type':['standard']}},}

	_unequal = [{'calculation_request','gromacs_group'}]
	_equivalence = {
		('slice_request_named','calculation_request'):['slice_name','sn','group'],
		('post_spec_v2','slice_request_named'):['sn','group','pbc','start','end','skip'],
		('post_spec_v2_basic','slice_request_named'):['sn','start','end','skip'],}

	def _eq_gromacs_slice_to_slice_request_named(self,a,b):
		checks = ([a.data[k]==b.data['body'][k] for k in ['start','end','skip','pbc','group']]+
			[a.data['sn']==b.data['sn']])
		return all(checks)

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

class Calculation:
	def __init__(self,**kwargs):
		"""Construct a calculation object."""
		self.name = kwargs.pop('name')
		#! note that we forgo the OmnicalcDataStructure here for more fine-grained control
		# the top-level calculation specs mirrors the text of the calculation request
		calc_specs = kwargs.pop('specs',{})
		# construct a raw calculation object
		self.raw = dict(
			uptype=calc_specs.pop('uptype','post'),
			group_name=calc_specs.pop('group',None),
			slice_name=calc_specs.pop('slice_name',None),
			collections=calc_specs.pop('collections',None))
		# hold the specs separately
		self.specs = calc_specs.pop('specs',{})
		if calc_specs: raise Exception('unprocessed inputs to the calculation: %s'%calc_specs)
		# copy any upstream references for later
		self.upstream = copy.deepcopy(self.specs.get('upstream',{}))
		# save the stubs for later lookups
		self.stubs = kwargs.pop('stubs',[])
		if kwargs: raise Exception('unprocessed kwargs %s'%kwargs)
	def __eq__(self,other):
		#! note that calculations are considered identical if they have the same specs
		#! ... we disregard the calc_specs because they include collections, slice names (which might change)
		#! ... and the group name. we expect simulation and group and other details to be handled on 
		#! ... slice comparison
		return self.specs==other.specs and self.name==other.name

class CalculationOLD(NoisyOmnicalcObject):
	"""
	A calculation, including settings.
	Note that this class is customized rather than derived from OmnicalcDataStructure
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
		#! consolidate calls to the type fixer?
		json_type_fixer(self.specs)

	def __eq__(self,other):
		"""See if calculations are equivalent."""
		#! note that calculations are considered identical if they have the same specs
		#! ... we disregard the calc_specs because they include collections, slice names (which might change)
		#! ... and the group name. we expect simulation and group and other details to be handled on 
		#! ... slice comparison
		return self.specs==other.specs

class NamingConvention:
	"""
	Create a file-naming cnvention.
	"""
	# name the types in the parser
	common_types = {'wild':r'[A-Za-z0-9\-_]','float':r'\d+(?:(?:\.\d+))?',
		'gmx_suffixes':'(?:gro|xtc)'}
	# all n2d types in omni_namer get standard types
	parser = dict([
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
			'n2d':'^(?P<short_name>%(wild)s+)\.(?P<calc_name>%(wild)s+)\.n(?P<nnum>\d+)\.(dat|spec)$',}),])

class NameManager(NamingConvention):
	"""
	Manage file name creation and interpretation.
	"""
	def __init__(self,**kwargs):
		self.short_namer = kwargs.pop('short_namer',None)
		if not self.short_namer: self.short_namer = lambda sn,spot:sn
		else: self.short_namer = eval(self.short_namer)
		self.spots = kwargs.pop('spots',{})
		# if no spots are defined then we have no way of using a spotname in any naming scheme
		# ... hence we save them as None here and require that names in post are unique
		if not self.spots: self.spotnames = {}
		# if we have spots then we note the simulation locations in order to hold their spots
		#! note minor redundancy with ParsedRawData but we avoid that because it is slow, requires spots
		else:
			self.spotnames = {}
			for spot,details in self.spots.items():
				dn_top = os.path.join(details['route_to_data'],details['spot_directory'])
				dns = [os.path.basename(dn) 
					for dn in glob.glob(os.path.join(dn_top,'*')) if os.path.isdir(dn)]
				dns_match = [dn for dn in dns if re.match(details['regexes']['top'],dn)]
				for dn in dns_match:
					if dn not in self.spotnames: self.spotnames[dn] = [spot]
					else: self.spotnames[dn].append(spot)
		if kwargs: raise Exception('unprocessed kwargs %s'%kwargs)

	def get_spotname(self,sn):
		"""Return a spot name in case simulations with the same name come from identical spots."""
		# failure to identify a spot gives a generic result so users can see when they need to add spot info
		spotnames = self.spotnames.get(sn,['spotname'])
		if len(spotnames)!=1: 
			raise Exception('DEV. need to decided between spotnames for simulation %s: %s'%(sn,spotnames))
		else: return spotnames[0]

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

	def alias(self,sn):
		"""Combine a spotname lookup and short-name alias in one function."""
		spotname = self.get_spotname(sn)
		return self.short_namer(sn,spotname)

	def basename(self):
		"""Name this slice."""
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
