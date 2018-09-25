#!/usr/bin/env python

"""
Omnicalc command-line interface.
"""

from __future__ import print_function

# expose interface functions from omnicalc.py as well
__all__ = [
	'locate','set_config','setup','clone_calcs',
	'blank_meta','audit','go',
	# interface functions from omnicalc
	'compute','plot','look','clear_stale',
	#! temporary
	'default_spot']

import os,sys,re
from ortho import read_config,write_config,bash

# note that omni/cli.py is imported by ortho and hence requires
#   the following absolute import from the root
from omni.logo import logo

from omni.omnicalc import compute,plot,look,go,clear_stale

default_config = {'commands': ['omni/cli.py'],'commands_aliases': [('set','set_config')]}

def locate(keyword):
	"""
	Find a function.
	"""
	os.system((r'find ./ -name "*.py" | '
		r'xargs egrep --color=always "(def|class) \w*%s\w*"'%keyword))

def setup():
	"""
	Check for required settings.
	"""
	config = read_config()
	required_settings = ['post_data_spot','post_plot_spot']
	needs_keys = [i for i in required_settings if i not in config]
	if any(needs_keys): 
		print('note','setting is incomplete until you add: %s. use `make set key="val"`.'%needs_keys)

def clone_calcs(source):
	"""
	Clone a calculations repository.
	"""
	config = read_config()
	if 'calculations_repo' in config and not os.path.isdir('calcs/.git'):
		raise Exception('config has a calculations repo registered but we cannot find calcs/.git')
	elif not 'calculations_repo' in config and os.path.isdir('calcs/.git'):
		raise Exception('found calcs/.git but no calculations_repo in the config')
	elif 'calculations_repo' in config and os.path.isdir('calcs/.git'):
		raise Exception('you already have a calculations repo at calcs')
	# clone and register
	bash('git clone %s calcs'%source)
	config['calculations_repo'] = source
	write_config(config)

#! deprecated
if False:
	#! should be deprecated
	calcs_template = """
	variables: {}
	collections: {}
	plots: {}
	""".strip()

	def blank_meta(make_template=True):
		"""
		Set up an empty specs container. You can opt to only make the specs folder in the event that
		you do not want a blank meta file (the factory does this because the GUI tells the user how to make one).
		"""
		if not os.path.isdir('calcs/specs'): os.mkdir('calcs/specs')
		if make_template:
			if not os.path.isfile('calcs/specs/meta.yaml'):
				with open('calcs/specs/meta.yaml','w') as fp: fp.write(calcs_template)

def audit(debug=False,source='calcs/auditor.py'):
	"""
	Command-line interface to an auditor for tracking the status of different calculations.
	"""
	if not os.path.isfile(source): raise Exception('requires source code at %s'%source)
	else: 
		from makeface import import_remote
		auditor = import_remote(source)
		auditor['CalcsAuditor'](debug=debug)

def default_spot(spot):
	#! temporary until a better method
	config = read_config()
	spots = {'sims': {'route_to_data': '/Users/rpb/worker/factory/data/demo', 'regexes': {'step': '([stuv])([0-9]+)-([^\\/]+)', 'part': {'edr': 'md\\.part([0-9]{4})\\.edr', 'trr': 'md\\.part([0-9]{4})\\.trr', 'xtc': 'md\\.part([0-9]{4})\\.xtc', 'tpr': 'md\\.part([0-9]{4})\\.tpr', 'structure': '(system|system-input|structure)\\.(gro|pdb)'}, 'top': '(.+)'}, 'namer': 'lambda name,spot=None: name', 'spot_directory': 'sims'}}
	if not os.path.isdir(spot): raise Exception
	spots['sims']['route_to_data'] = os.path.dirname(spot)
	spots['sims']['spot_directory'] = os.path.basename(spot)
	config['spots'] = spots
	write_config(config)
