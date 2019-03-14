#!/usr/bin/env python

#! this is deprecated. try `make make` actually not so sure maybe it really is main_alt.py
#! do needs a proper tracebacker instead of the "If you suspect this is an IPython bug"

import os,re,glob,json
import yaml
import time
import copy,itertools

from ortho import json_type_fixer
from ortho import Handler
from ortho import treeview
from ortho import catalog
from ortho import delveset,delve

# put this in config?
old_config = {'spots': {'sims': {'namer': 'lambda name,spot=None: name',
'regexes': {'part': {'edr': 'md\\.part([0-9]{4})\\.edr',
'structure': '(system|system-input|structure)\\.(gro|pdb)',
'tpr': 'md\\.part([0-9]{4})\\.tpr',
'trr': 'md\\.part([0-9]{4})\\.trr',
'xtc': 'md\\.part([0-9]{4})\\.xtc'},
'step': '([stuv])([0-9]+)-([^\\/]+)',
'top': '(.+)'},
'route_to_data': '/mnt/store-omicron/major.factory/data/ptdins',
'spot_directory': 'sims'},
'source': {'namer': 'lambda name,spot=None: re.match("^membrane-v(\\d+)$",name).group(1)',
'regexes': {'part': {'edr': 'md\\.part([0-9]{4})\\.edr',
'structure': '(system|system-input|structure)\\.(gro|pdb)',
'tpr': 'md\\.part([0-9]{4})\\.tpr',
'trr': 'md\\.part([0-9]{4})\\.trr',
'xtc': 'md\\.part([0-9]{4})\\.xtc'},
'step': '([stuv])([0-9]+)-([^\\/]+)',
'top': '(.+)'},
'route_to_data': '/home/rpb/omicron',
'spot_directory': 'dataset-project-ptdins'}}}

import ortho

def dirdive(base,level=1,subsel=None):
	"""Walk directories to a certain level with a regex-and-return."""
	# via https://stackoverflow.com/a/234329/3313859
	base = base.rstrip(os.path.sep)
	if not os.path.isdir(base): 
		raise Exception('must be a directory: %s'%base)
	num_sep = base.count(os.path.sep)
	for root,dns,fns in os.walk(base):
		detail = subsel(base=base,root=root) if subsel else None
		if detail: yield dict(root=root,dns=dns,fns=fns,detail=detail)
		elif subsel==None: yield root,dns,fns
		num_sep_this = root.count(os.path.sep)
		seps = root.count(os.path.sep)
		# delete in place for desired os.walk usage
		if num_sep + level <= num_sep_this: del dns[:]

#! ortho.conf.get('spots')
ortho.treeview(dict(old=old_config))

#! an example
sn = 'membrane-v563'

# starting point for parsing
detail = dict(sep=os.path.sep,root=os.path.join(
	'/home/rpb/omicron','dataset-project-ptdins'))
source = '%(root)s%(sep)s'%detail

# default hook for parsing simulations
regex_targets = r'^(?P<sn>.+)%ss(?P<step_no>\d+)-(?P<step_name>.+)'%os.path.sep
def valid_targets(base,root,**kwargs): 
	"""Selecting valid targets for analysis."""
	path = os.path.relpath(root,base)
	match = re.match(regex_targets,os.path.relpath(root,base))
	if match: return match.groupdict()

# STEP 1: parse the files
raw = list(dirdive(source,level=2,subsel=valid_targets))

"""
pseudocode for a new loop:
	atomics:
		1. slice to kernel slice
		2. calculation to ???
pseudocode for reading data stores?
	objective: read in a dataset and compare it systematically to requests
journal of  the dev process
	started by getting the raw parsed files above
	this was independent of interpreting the files in the post spot
	now it is 2019.1.29
	collected the post files and started with a single regex for the datspec
	started writing a handler for unpacking the files
		note that the handler takes the place of a way of morphone one datastructure to another
		and I am in favor of it because if feels automagical (in a good way, like rails)
	need to twin the 
	design of the code is taking some shape, so time to reflect
		we are repeating a small loop
			prepare a data structure
			pare it down
			run a loop over its  items
		so far, the data structures are
			A. the raw files from MD
			B. the list of post files
		before writing a post-file-to-spec transformer, we need to pair them
			but the pairing with dat/spec is an operation that happens on the data structure of files
		it would be useful to eventually make everything functional style
		made a stupid mistake in hard-coding the dat extension then trying to twin things with no spec!
"""

# STEP 2: parse the post data

post_data_spot = ortho.conf['post_data_spot']
post_files = [
	# read file objects with a directory and name
	{'fn':os.path.basename(i),'dn':post_data_spot} 
	# files are globbed from the post_data_spot
	for i in glob.glob(os.path.join(post_data_spot,'*'))]

regex_post = (
	r'^(?P<short_name>.+)\.'
	r'(?P<start>\d+)-(?P<end>\d+)-(?P<step>\d+)'
	r'\.(.+)\.n(?P<version>\d+)\.(?P<ext>.+)$')
regex_post_datspec = (
	r'^(?P<short_name>.+)\.'
	r'(?P<start>\d+)-(?P<end>\d+)-(?P<step>\d+)'
	r'\.(.+)\.n(?P<version>\d+)$')

# get post data that matches regex_post
post_files_basic = [i for i in post_files if re.match(regex_post,i['fn'])]

# STEP 3: pair dat/spec files

def reduce_pairs_dat_spec(incoming):
	"""..."""
	pattern = r'(dat|spec)'
	regex = r'^(?P<base>.+)\.(?P<ext>%s)$'%pattern
	for ii,item in enumerate(incoming):
		match = re.match(regex,item['fn'])
		if match: 
			incoming[ii].update(**match.groupdict())
	#! previously did a loop with regexes in the double loop and it took 0.3s
	#! new loop is 0.0013 to regex once through, then 0.0098 total to twin them
	#! doubtful this can be improved
	global twins #!
	twins = dict([sorted((ii,ii+jj+1))
		for ii,i in enumerate(incoming) 
		for jj,j in enumerate(incoming[ii+1:])
		if i['base']==j['base']])
	# collapse items
	for ii,i in enumerate(incoming):
		if ii in twins.keys(): 
			incoming[twins[ii]]['datspec'] = True
			del incoming[twins[ii]]['fn']
	incoming = [i for ii,i in enumerate(incoming) if ii not in twins.keys()]
	#! assertion or exception?
	assert not set.intersection(set(twins.keys()),set(twins.values()))
	return incoming #!?!?!? why is it necessary to return!?

post_files_basic = reduce_pairs_dat_spec(post_files_basic)

if False:
	this = next(i for i in post_files_basic if i.get('datspec',False))
	name_data = re.match(regex_post_datspec,this['base']).groupdict()
	json_type_fixer(name_data)

class FluxFileToData(Handler):
	kinds = ('datspec','orphan',)
	def __repr__(self):
		treeview({str(id(self)):self.dat})
		return "FluxFileToData [%s] [at %d]"%(self.kind,id(self))
	def triage_dat_spec(self,dn,base,ext,datspec):
		#! figure out a better way to handle booleans?
		if datspec!=True: raise Exception('datspec flag was misused!')
		self.kind = 'datspec'
		# get information from the name
		name_data = re.match(regex_post_datspec,base).groupdict()
		json_type_fixer(name_data)		
		# get information from the spec file
		with open(os.path.join(dn,'%s.spec'%base)) as fp:
			spec_data = json.load(fp)
		# package
		self.dat = dict(dn=dn,base=base,name_data=name_data,spec_data=spec_data)
	def orphan(self,dn,base,fn):
		self.dat = dict(fn=fn)
		self.kind = 'orphan'

# STEP 4: resolve each datspec file into a new list

post_files_resolved = []
for item in post_files_basic:
	resolved = FluxFileToData(**item)
	post_files_resolved.append(resolved)

posts = dict([(k,list(filter(lambda x:x.kind==k,post_files_resolved))) 
	for k in FluxFileToData.kinds])

that = posts['datspec'][0]

class Specs:
	def __init__(self,spot):
		self.spot = os.path.abspath(os.path.expanduser(spot))
		self.fns = glob.glob(os.path.join(self.spot,'*.yaml'))
		meta_filter = ortho.listify(ortho.conf.get('meta_filter',[]))
		# filter specs files if we have a list or string of files or globs
		if meta_filter:
			self.fns = [i for i in self.fns if i in [m for n in 
				[glob.glob(os.path.join(self.spot,j)) 
				for j in meta_filter] for m in n]]
		# read all yaml files
		sources = {}
		for fn in self.fns:
			with open(fn) as fp: 
				sources[fn] = yaml.load(fp)
		st = time.time()
		self.raw = self.merge(sources)
		print(time.time()-st) #! slow?
	def merge(self,sources):
		"""Merge YAML dictionaries without overlaps (that is, strictly)."""
		sources_unravel = {}
		for fn in sources:
			sources_unravel[fn] = list(catalog(sources[fn]))
		# perfect check for unique paths
		# note that this seems expensive but only taks 0.0017s 
		#   and then 0.0062s with the merge at the end
		paths_all = [tuple(i) for j in 
			[list(zip(*i))[0] for i in sources_unravel.values()] for i in j]
		# note that this exception does not care if you have e.g. slices: {} 
		#   and a nonempty slices elsewhere which is the desired behavior. 
		#   we only want to check for paths that overwrite each other
		#! ignore paths that overwrite with the same value
		if len(paths_all)!=len(set(paths_all)):
			from collections import Counter
			redundant_paths = [k for k,v in Counter(paths_all).items() if v>1]
			raise Exception('found redundant paths: %s'%redundant_paths)
		# merge the dictionaries
		raw = {}
		for data in sources_unravel.values():
			for key,val in data:
				delveset(raw,*key,value=val)
		return raw

#! hardcoded path below	
specs = Specs(spot='~/omicron/factory/calc/ptdins/calcs/specs')

# STEP 5: build a list of calculation dependencies 

calcs_meta = specs.raw['calculations']

def infer_calculation_order(calcs_meta):
	"""
	Resolve dependencies in the calculation list.
	Note that this function is a verbatim port from original omnicalc.
	This function encodes the data structure implicitly, and should be 
	replaced with a more modular version.
	"""
	upstream_catalog = [i for i,j in catalog(calcs_meta) if 'upstream' in i]
	# if there are no specs required to get the upstream data object the user can either 
	#   use none/None as a placeholder or use the name as the key as in "upstream: name"
	for uu,uc in enumerate(upstream_catalog):
		if uc[-1]=='upstream': upstream_catalog[uu] = upstream_catalog[uu]+[delve(calcs_meta,*uc)]
	depends = {}
	# formulate a list of dependencies while accounting for multiple upstream dependencies
	for t in upstream_catalog:
		if t[0] not in depends: depends[t[0]] = []
		depends[t[0]].extend([t[ii+1] 
			for ii,i in enumerate(t) 
			if ii<len(t)-1 and t[ii]=='upstream'])
	calckeys = [i for i in calcs_meta if i not in depends]
	# if the calculation uses an upstream list instead of dictionary we flatten it
	depends = dict([(k,(v if not all([type(i)==list for i in v]) else 
		[j for k in v for j in k])) for k,v in depends.items()])
	# check that the calckeys has enough elements 
	_ = list(set(calckeys+[i for j in depends.values() for i in j]))
	# paranoid security check for infinite loop. you cannot use popitem
	time_limit = 10 
	depends = list(depends.items())
	start_time = time.time()
	while any(depends):
		ii,i = depends.pop()
		if all([j in calckeys for j in i]) and i!=[]: calckeys.append(ii)
		else: depends.insert(0,(ii,i))
		if time.time()>(start_time+time_limit): 
			raise Exception(
				'It is taking too long to resolve the dependency graph. '
				'You might have a loop in your graph of dependencies.')
	return calckeys

calc_order = infer_calculation_order(calcs_meta)

"""
next steps:
	refactor actually completely rewrite the thing that interprets the yaml files
	anticipate the data in a calculation result
	compare to the posts['datspect'] to generate a list of pending jobs
		the code to do this resolution will be complex
sidetracked:
	started working on the above and realized it would be nice to not hard-code the relationships between calculations
	idea: make all relations into calss relations
		basically each item in a calculation gets slotted into a class object and we can subclass 
		and inherit to make larger classes
		this is somewhat the opposite of abstraction
		the problem is that abstraction requires handlers that require hard-coding
		so the handlers themselves are no longer abstract
		basically, a bunch of python classes might be a better abstraction mechanism
		than a two-level meta-class kind of thing where I write the handler in some dictionary
		then write a meta-handler to turn that into a set of code
		basically the python code is both the data structure and the handler
		anyway, this sounds like I'm chiefing over here so time to get to work
		the Handler parent class already does this a lot so I will start there
		the handler can be the interpreter of the calculations and then code up the relations too
		note that we also need some kind of nesting
	needs for Handler
		fix the name issue soon!
			consider removing name so everything is totally generic
			this might require a lot of adjustment in other parts of the code?
			possibly needs a switch to handle it
		ensure that when we look at a Handler subclass instance 
			we can tell what is "novel" and what is part of the base class 
			i.e. if I set self.specs inside the method, how can I distinguish it from self.name
commit 
"""

def resolve_loops(tree):
	"""
	Generic function for resolving the "loop" keyword in a nested dictionary.
	The loop keyword can point to a dictionary or a list of values.
	In both cases, the product of all values (if dict) or list items (if list)
	is generated and a unique tree is produced for each combination in the 
	product.
	#! what to do about keys?
	"""
	paths = list(catalog(tree))
	# get the pivots for the loops
	loop_base = set([tuple(path[:path.index('loop')]) for path,val in paths if 'loop' in path])
	loop_spots = set([tuple(path[:path.index('loop')+2]) for path,val in paths if 'loop' in path])
	loop_full = set([tuple(path) for path,val in paths if 'loop' in path])
	loop_term = [i for i in loop_full if i[-1]=='loop']
	# collect all terminal loop values organized by path
	loop_term_values = dict([(i[:-1],delve(tree,*i)) for i in loop_term])
	splits = {}
	aliases = {}
	# the non-terminal loops have full dictionaries
	for item in loop_full:
		if item[-1]=='loop': 
			downstream = delve(tree,*item)
			if not isinstance(downstream,dict) and not isinstance(downstream,list):
				raise Exception(
					('Cannot loop the following path: %s with value: %s')%(
						item,str(downstream)))
			splits[item[:-1]] = downstream
	for item in loop_spots:
		if item[-2]=='loop': 
			path = item[:item.index('loop')]
			tail = item[:item.index('loop')+2]
			alias = (path,tail[-1])
			if path not in splits: 
				splits[path] = []
				aliases[path] = []
			# we index aliases and splits together to later reconstruct the alias
			splits[path].append(delve(tree,*tail))
			aliases[path].append(alias)
	routes,values = splits.keys(),splits.values()
	# get the counts of the items in each loop in the order of routes above
	counts = [len(i) for i in values]
	# each index below is a distinct item in the loop
	indices = list(itertools.product(*(range(i) for i in counts)))
	# each item in the loop gets raw data and any aliases
	#! previously: combos = [dict(zip(splits.keys(),i)) for i in list(itertools.product(*splits.values()))]
	# combos is a list of the raw data and an alias
	# the alias tells you what the "name" of the item in the loop is, if constructed from a hash
	# note that loops over lists will not have a name, and hance will not have an alias
	combos = [dict(
		raw=dict(zip(routes,[list(values)[ii][i] 
			for ii,i in enumerate(index)])),
		# the aliases are saved by route, and indexed by i in the same order as the values of splits[path]
		alias=[aliases[list(routes)[ii]][i] 
			for ii,i in enumerate(index) 
			# we only look up an alias if the route is in the alias dictionary
			if list(routes)[ii] in aliases],
		) for index in indices]
	if not combos: return dict(multi=[copy.deepcopy(tree)])
	multiplexed = []
	for mod in combos:
		this = copy.deepcopy(tree)
		alias = mod['alias']
		for key,val in mod['raw'].items():
			delveset(this,*key,value=val)
		multiplexed.append(dict(raw=this,alias=alias))
	return dict(multi=multiplexed)

class Specs(Handler):
	def upstream(self,upstream,**kwargs):
		self.upstream = upstream
		self.default(**kwargs)
		return "I AM UPSTREAM"
	def default(self,**kwargs):
		self.raw = kwargs
		# resolve loops here ...!!!

class Calculation(Handler):
	def item(self,slice_name,collections,specs,group=None,uptype='post'):
		#! this is almost an edge in our calculation
		self.specs = Specs(**specs)

class CalculationLayer(Handler):
	def item(self,raw,alias=None):
		self.alias = alias
		self.calc = Calculation(**raw)

class CalculationSet(Handler):
	#! you cannot name the function _item below because of handler
	def item(self,many):
		self.many = many
		for key,val in self.many.items():
			calcs = resolve_loops(val)
			self.many[key] = dict(calcs=[CalculationLayer(**i) for i in calcs['multi']])

"""
resolving loop structures
	each Calculation object has a self.raw attribute which could have loops in it
	each time we encounter a loop, we need to make a copy of everything "above"
	and then each copy gets a different value for everything "below"
"""

them = CalculationSet(**dict(many=calcs_meta))
eg = them.many['lipid_abstractor']['calcs'][0]
