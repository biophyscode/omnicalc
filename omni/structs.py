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
		return '<%s instance at 0x%x>'%(self.__class__.__name__,id(self))

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

	types_map = {'string':str_types,'number':[int,float],
		'bool':[bool,None],'list':[list],'str_or_list':str_types+[list]}
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
			raise Exception('failed to classify %s in %s'%(subject,self.__dict__))
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
		if ((self.style=='readymade' or other.style=='readymade') and 
			self.data['sn']=='membrane-v8421' and other.data['sn']=='membrane-v8421'):
			pass
			#import ipdb;ipdb.set_trace()
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
        #'calculation_request_basic':{
		#	'struct':{'sn':'string','slice_name':'string'},
		#	'meta':{'strict':True},},
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
		'readymade_request':{
			# cannot be strict and cannot check types for this (nested?) style
			'meta':{'strict':False,'check_types':False},
			'struct':{OmnicalcDataStructure.StructureKey('slice_name'):{
				'structure':'string','trajectory':'str_or_list'}},},
		'readymade':{
			'meta':{'strict':False,'check_types':True},
			'struct':{'structure':'string','trajectory':'str_or_list','slice_name':'string','sn':'string',
			'name_style':['readymade_datspec']}},
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
				#! deprecated 'dat_type':['gmx'],'slice_type':['standard'],
				'name_style':['standard_gmx']}},}

	_unequal = [{'calculation_request','gromacs_group'},{'slice_request_named','gromacs_group'},
		{'slice_request_named','calculation_request_basic'},{'gromacs_group','calculation_request_basic'},
		{'slice_request_named','readymade'},{'post_spec_v2','readymade'},{'post_spec_v2_basic','readymade'},
		{'gromacs_slice','readymade'},{'gromacs_group','readymade'}]
	_equivalence = {
		('slice_request_named','calculation_request'):['slice_name','sn','group'],
		('post_spec_v2','slice_request_named'):['sn','group','pbc','start','end','skip'],
		('post_spec_v2_basic','slice_request_named'):['sn','start','end','skip'],
		('readymade','calculation_request'):['sn','slice_name'],}

	def _eq_gromacs_slice_to_slice_request_named(self,a,b):
		checks = ([a.data[k]==b.data['body'][k] for k in ['start','end','skip','pbc','group']]+
			[a.data['sn']==b.data['sn']])
		return all(checks)

class Calculation(NoisyOmnicalcObject):
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
		self.name_style = calc_specs.pop('name_style',None)
		# we protect against extra unprocessed data in the calculations here
		if calc_specs: raise Exception('unprocessed inputs to the calculation: %s'%calc_specs)
		# copy any upstream references for later
		self.upstream = copy.deepcopy(self.specs.get('upstream',{}))
		# save the stubs for later lookups
		self.stubs = kwargs.pop('stubs',[])
		# some jobs have specific requests for a naming scheme
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
	# note that we previously used a dat_type,slice_type couple to describe these but that has been removed
	# previous options included ('standard','gmx'), ('standard','datspec'), ('standard_obvious','datspec')
	# ... and ('raw','datspec')
	parser = dict([
		# standard GMX slices include a suffix and a PBC mode and a group name
		('standard_gmx',{
			'd2n':r'%(short_name)s.%(start)s-%(end)s-%(skip)s.%(group)s.pbc%(pbc)s.%(suffix)s',
			'n2d':'^(?P<short_name>%(wild)s+)\.'+
				'(?P<start>%(float)s)-(?P<end>%(float)s)-(?P<skip>%(float)s)\.'+
				'(?P<group>%(wild)s+)\.pbc(?P<pbc>%(wild)s+)\.%(gmx_suffixes)s$',}),
		# datspec with only the simulation alias, times, and calculation name
		('standard_datspec',{
			# we append the dat/spec suffix and the nnum later
			'd2n':r'%(short_name)s.%(start)s-%(end)s-%(skip)s.%(calc_name)s',
			'n2d':'^(?P<short_name>%(wild)s+)\.'+
				'(?P<start>%(float)s)-(?P<end>%(float)s)-(?P<skip>%(float)s)\.'+
				'(?P<calc_name>%(wild)s+)\.n(?P<nnum>\d+)\.(dat|spec)$',}),
		# datspec name which retains the PBC and group from the standard_gmx slice
		('standard_datspec_pbc_group',{
			# we append the dat/spec suffix and the nnum later
			'd2n':r'%(short_name)s.%(start)s-%(end)s-%(skip)s.%(group)s.'+
				r'pbc%(pbc)s.%(calc_name)s',
			'n2d':'^(?P<short_name>%(wild)s+)\.'+
				'(?P<start>%(float)s)-(?P<end>%(float)s)-(?P<skip>%(float)s)\.'+
				'(?P<group>%(wild)s+)\.pbc(?P<pbc>%(wild)s+)\.(?P<calc_name>%(wild)s+)'+
				'\.n(?P<nnum>\d+)\.(dat|spec)$',}),
		# naming convention for results from readymade slices
		('readymade_datspec',{
			# we append the dat/spec suffix and the nnum later
			#! should this include the number?
			'd2n':r'%(short_name)s.readymade.%(slice_name)s.%(calc_name)s',
			'n2d':'^(?P<short_name>%(wild)s+)\.readymade\.(?P<slice_name>%(wild)s+)'+
				'\.(?P<calc_name>%(wild)s+)\.n(?P<nnum>\d+)\.(dat|spec)$',}),
		#??? unused
		('raw_datspec',{
			# we append the dat/spec suffix and the nnum later
			#! should this include the number?
			'd2n':r'%(short_name)s.%(calc_name)s',
			'n2d':'^(?P<short_name>%(wild)s+)\.(?P<calc_name>%(wild)s+)\.n(?P<nnum>\d+)\.(dat|spec)$',}),
		])

class NameManager(NamingConvention):
	"""
	Manage file name creation and interpretation.
	"""
	def __init__(self,**kwargs):
		self.short_namer = kwargs.pop('short_namer',None)
		if not self.short_namer: self.short_namer = lambda sn,spot:sn
		else: 
			# allow lambda or functions
			try: short_namer = eval(self.short_namer)
			except: 
				try:
					extract_short_namer = {}
					exec(self.short_namer,{'re':re},extract_short_namer)
					# if you use a function it must be named "renamer"
					short_namer = extract_short_namer['renamer']
				except: 
					print(self.short_namer)
					raise Exception('failed to interpret the alias function with eval or exec')
			def careful_naming():
				def short_namer_careful(*args,**kwargs):
					try: return short_namer(*args,**kwargs)
					except: raise Exception(
						'short_namer/renamer function failed on args %s, kwargs %s'%(args,kwargs))
				return short_namer_careful
			self.short_namer = careful_naming()
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
		for name_key,namespec in self.parser.items():
			if re.match(namespec['n2d']%self.common_types,name):
				matches.append(name_key)
		# anything that fails to match goes into limbo of some kind
		if not matches: return None
		elif len(matches)>1: raise Exception('multiple filename interpretations: %s'%matches)
		else: name_style = matches[0]
		data = re.match(self.parser[name_style]['n2d']%self.common_types,name).groupdict()
		return {'name_style':name_style,'body':data}

	def alias(self,sn):
		"""Combine a spotname lookup and short-name alias in one function."""
		spotname = self.get_spotname(sn)
		return self.short_namer(sn,spotname)

	def basename(self,**kwargs):
		"""
		Name this slice.
		This function has a critical role in preparing job specs for the namer functions. 
		It would be useful to make this more automatic.
		"""
		job = kwargs.pop('job',None)
		name_style = kwargs.pop('name_style',None)
		if not name_style: raise Exception('basename needs a name_style')
		if not job: raise Exception('basename needs a job')
		if name_style not in self.parser: raise Exception('cannot find style in the parser: %s'%name_style)
		# +++TRANSFORM a job into a name
		if name_style=='standard_datspec':
			request = dict([(k,job.slice.data[k]) for k in ['start','end','skip']])
			request.update(short_name=self.names_short[job.slice.data['sn']],calc_name=job.calc.name)
		elif name_style=='standard_datspec_pbc_group':
			request = dict([(k,job.slice.data[k]) for k in ['start','end','skip','group','pbc']])
			request.update(short_name=self.names_short[job.slice.data['sn']],calc_name=job.calc.name)
		elif name_style=='standard_gmx':
			request = dict([(k,job.slice.data[k]) for k in ['start','end','skip','group','pbc']])
			request.update(short_name=self.names_short[job.slice.data['sn']])
		elif name_style=='readymade_datspec':
			request = dict(short_name=self.names_short[job.slice.data['sn']],calc_name=job.calc.name,
				slice_name=job.slice.data['slice_name'])
		else: raise Exception('cannot pepare data for the namer for name_style %s'%name_style)
		return self.parser[name_style]['d2n']%request
