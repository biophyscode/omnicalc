#!/usr/bin/env python

"""
Omnicalc command-line interface.
"""

#---expose interface functions from omnicalc.py as well
# refactor: __all__ = ['locate','set_config','nuke','setup','compute','plot','pipeline','clone_calcs','look','blank_meta','audit_plots']

__all__ = ['locate','set_config','nuke','setup','clone_calcs','blank_meta',
	#---interface functions from omnicalc
	'compute','plot']

import os,sys,re
from config import read_config,write_config,is_terminal_command,bash,abspath,set_config
# refactor: from omnicalc import compute,plot,pipeline,look,WorkSpace,audit_plots
from omnicalc import compute,plot # refactor: ,pipeline,look,WorkSpace,audit_plots

default_config = {'commands': ['omni/cli.py'],'commands_aliases': [('set','set_config')]}

def nuke(sure=False):
	"""
	Reset this copy of omnicalc. Be careful!
	"""
	if sure or all(re.match('^(y|Y)',(input if sys.version_info>(3,0) else raw_input)
		('[QUESTION] %s (y/N)? '%msg))!=None for msg in 
		['`nuke` deletes everything. okay?','confirm']):
		#---reset procedure starts here
		write_config(default_config)
		#---! other resets?

def locate(keyword):
	"""
	Find a function.
	"""
	os.system('find ./ -name "*.py" | xargs egrep --color=always "(def|class) \w*%s\w*"'%keyword)

def setup():
	"""
	"""
	if not os.path.isfile('config.py'):	write_config(default_config)
	config = read_config()
	required_settings = ['post_data_spot','post_plot_spot']
	needs_keys = [i for i in required_settings if i not in config]
	if any(needs_keys): 
		print('[NOTE] setting is incomplete until you add: %s. use `make set key="val"`.'%needs_keys)

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
	#---clone and register
	bash('git clone %s calcs'%source)
	config['calculations_repo'] = source
	write_config(config)

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
