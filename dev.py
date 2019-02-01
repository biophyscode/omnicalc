#!/usr/bin/env python

#! do needs a proper tracebacker instead of the "If you suspect this is an IPython bug"

import os,re,glob
import time
from ortho import json_type_fixer
from ortho import Handler

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
fns = [
	# read file objects with a directory and name
	{'fn':os.path.basename(i),'dn':post_data_spot} 
	# files are globbed from the post_data_spot
	for i in glob.glob(os.path.join(post_data_spot,'*'))]

# STEP 2b: pairing functions
#! needs developed


regex_basic = (
	r'^(?P<short_name>.+)\.'
	r'(?P<start>\d+)-(?P<end>\d+)-(?P<step>\d+)'
	r'\.(.+)\.n(?P<version>\d+)\.(?P<ext>.+)$')

#! development start by reading a single item
incoming = [i for i in fns if re.match(regex_basic,i['fn'])]
this = incoming[0]

name_data = re.match(regex_basic,this['fn']).groupdict()
json_type_fixer(name_data)

"""
class FluxFileToData(Handler):
	def triage_dat_spec(self,fn,dn):
		if datspec: raise Exception('dev error: this has to be null!')
		print(fn)
		self.kind = 'datspec'

file_resolved = []
for item in incoming:
	try: 
		resolved = FluxFileToData(**item)
		file_resolved.append(resolved)
	except: pass

that = file_resolved[0].kind
"""
def posts_to_twin_symmetric(fn,peers,**kwargs):
	"""Modify a peers list in-place to identify twins."""
	pattern = kwargs.pop('pattern')
	if kwargs: raise Exception('unprocessed kwargs: %s'%kwargs)
	regex = '^(?P<base>.*?)%s$'%pattern
	candidates = []
	for ii,item in enumerate(peers):
		match = re.match(regex,item)
		if match:
			# save the index and groupdict
			candidates.append((ii,item.groupdict()))
	if len(candidates)==1: 
		index,reduced = candidates
		# add the file names back to the groupdict
		reduced['via'] = (fn,item,)
		if False:
			# remove the candidate from the list of peers
			# remove from list fast: https://stackoverflow.com/a/5746071/3313859
			peer = [i for ii,i in enumerate(peer) if ii==index]
		return reduced
	else: return

#! takes a file 0.3s so too slow
if False:
	t = time.time()
	# identify twins in a double loop over files
	twins = []
	pattern = r'\.(dat|spec)'
	regex = r'^(?P<base>.+)%s$'%pattern
	for ii,i in enumerate(incoming):
		for jj,j in enumerate(incoming[ii+1:]):
			match_l = re.match(regex,i['fn'])
			match_r = re.match(regex,j['fn'])
			if match_l and match_r and match_l['base']==match_r['base']:
				twins.append((match_l.group('base'),ii,ii+jj))
	print(time.time()-t)

t = time.time()
pattern = r'\.(dat|spec)'
regex = r'^(?P<base>.+)%s$'%pattern
for ii,item in enumerate(incoming):
	match = re.match(regex,item['fn'])
	if match: incoming[ii]['base'] = match['base']
print(time.time()-t);t = time.time()
#! previously did a loop with regexes in the double loop and it took 0.3s
#! new loop is 0.0013 to regex once through, then 0.0098 total to twin them
#! doubtful this can be improved
twins = dict([sorted((ii,ii+jj+1))
	for ii,i in enumerate(incoming) 
	for jj,j in enumerate(incoming[ii+1:])
	if i['base']==j['base']])
print(time.time()-t);t = time.time()
# collapse items
for ii,i in enumerate(incoming):
	if ii in twins.keys(): 
		incoming[twins[ii]]['datspec'] = True
incoming = [i for ii,i in enumerate(incoming) if ii not in twins.keys()]
print(time.time()-t);t = time.time()
assert not set.intersection(set(twins.keys()),set(twins.values()))
print(time.time()-t);t = time.time()

