#!/usr/bin/env python

import re,os
import ortho

#! make set_hook omnicalc_main="\"{'s':'omni/main_alt.py','f':'main_alt'}\""

real_code = """
# we have to inject the code here, we cannot simply run it

"""

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

def dirdive(base,level=1,subsel=None):
	"""Walk directories to a certain level with a regex-and-return."""
	# via https://stackoverflow.com/a/234329/3313859
	base = base.rstrip(os.path.sep)
	if not os.path.isdir(base): 
		raise Exception('must be a directory: %s'%base)
	num_sep = base.count(os.path.sep)
	for root,dns,fns in os.walk(base):
		if subsel==None: yield root,dns,fns
		else:
			detail = subsel(base=base,root=root)
			if detail: yield dict(root=root,dns=dns,fns=fns,detail=detail)
		num_sep_this = root.count(os.path.sep)
		seps = root.count(os.path.sep)
		# delete in place for desired os.walk usage
		if num_sep + level <= num_sep_this: del dns[:]

def main_alt():
	"""
	Development zone!
	run with `make make` or `make interact`
	"""

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

	targets = list(dirdive(source,level=2,subsel=valid_targets))

	import ipdb;ipdb.set_trace()
	# ^^^
