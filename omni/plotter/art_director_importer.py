#!/usr/bin/env python

"""
Import the "art director", a scheme for setting the aesthetics of different plots.
"""

import os,sys,re

#---variables which are expected by plotting functions by default
#---art words are set to None by default
protected_art_words = """
colors
labels
""".strip().split()

def import_art_director(fn,cwd='calcs'):
	"""
	Read the art director file.
	"""
	#---filename comes from a yaml file and points to a file with `calcs` as the implicit root 
	fn_abs = os.path.join(os.getcwd(),cwd,fn)
	if not os.path.isfile(fn_abs): 
		raise Exception('cannot find art director file given by variables,art_director: %s'%fn_abs)
	hold_path = list(sys.path)
	sys.path.insert(0,os.path.dirname(fn_abs))
	mod_name = re.match('^(.+)\.py$',os.path.basename(fn_abs)).group(1)
	mod = __import__(mod_name)
	#---! still need to define custom gopher functions for e.g. the number of proteins ...
	keys_to_globals = protected_art_words + mod.__dict__.get('_art_words',[])
	return dict([(k,mod.__dict__[k]) for k in keys_to_globals if k in mod.__dict__])
