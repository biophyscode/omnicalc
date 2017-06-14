#!/usr/bin/env python

"""
Compile documentation. 
Forked from the automacs version with live documentation removed.
The automacs documentation pushes to a separate github because it represents the most complex use-case in 
which the main documentation contains "live" snapshots of documentation for other optional modules.
Since the omnicalc documentation does not document the calculation modules, we use a simpler procedure, in 
which the documentation can be generated from the master branch, and a gh-pages branch in the same
repository pushes the rendered docs up to github. This means that the documentation can be found on the
BioPhysCode github at http://biophyscode.github.io/omnicalc. Administrators can refresh these docs using 
instructions provided below.

To initially set up documentation for github-pages (gh-pages) from a subdirectory of the repository you must 
first create the branch via ``git checkout -b gh-pages``. Then, add and commit the folder that contains 
``index.html``. Push the branch up to gh-pages using the following command.

``git subtree push --prefix omni/docs/build_all/DOCS origin gh-pages``

When you are done, you can ``git checkout master``. This method allows you to serve the documentation from 
the same repository as the code, but users do not automatically get their own copy of the compiled docs.

Currently you must repeat the procedure to update the docs. It would be useful to know how to track the 
remote in the subdirectory in order to properly refresh the docs.
"""

import os,sys,re,glob,subprocess,shutil,datetime
from makeface import strip_builtins
from datapack import asciitree
from sphinx_auto import makefile,conf_py,conf_master_py

__all__ = ['docs']

def write_rst_toctree(items,name,spacer='-',infotext=''):
	"""..."""
	text = "%s\n%s\n\n%s\n\n"%(name,spacer*len(name),infotext)
	text += ".. toctree::\n\t:maxdepth: 4\n\n"+''.join(["\t%s\n"%i for i in items])
	return text

def write_rst_automodule(title,name,spacer='-'):
	text = "%s\n%s\n\n"%(name,spacer*len(name))
	text += ".. automodule:: %s\n\t:members:\n\t:undoc-members:\n\t:show-inheritance:\n\n"%name
	return text

def docs(refresh=False,clean=False):
	"""
	Compile the documentation.
	"""
	if refresh: 
		docs_refresh()
		return

	docs_dn = 'build_all'
	build_dn = os.path.join(os.path.dirname(__file__),docs_dn)
	#---style directory holds custom themes
	style_dn = os.path.join(os.path.dirname(__file__),'style')

	#---cleanup
	if clean and os.path.isdir(build_dn):
		print('[NOTE] cleaning docs')
		shutil.rmtree(build_dn)
		return 
	elif clean:
		print('[NOTE] no docs to clean')
		return

	if not os.path.isdir(build_dn): os.mkdir(build_dn)
	else: raise Exception('build directory already exists %s. try `make docs clean` first'%build_dn)

	#---copy the walkthrough files
	for fn in glob.glob(os.path.join(os.path.dirname(__file__),'walkthrough','*')): shutil.copy(fn,build_dn)
	#---write the master configuration
	master_import_text = ['import os,sys']
	#---we must import any modules which are automatically documented in modules.rst via sphinx-autodoc
	master_import_text.extend([
		'sys.path.insert(0,os.path.abspath("../../../omni"))',
		'sys.path.insert(0,os.path.abspath("../../../omni/base"))',
		'sys.path.insert(0,os.path.abspath("../../../"))',
		'import omnicalc,cli,config',])
	#---write the makefile
	#---this section mimics a single iteration of the live documentation loop in automacs
	fns = [('Makefile',makefile),('conf.py',conf_py%os.path.abspath('omni'))]
	#---write the configuration and makefile
	#---! the Makefile may be superfluous
	for fn,text in fns:
		with open(os.path.join(build_dn,fn),'w') as fp: fp.write(text)
	master_dn = 'DOCS'
	shutil.copytree(style_dn,os.path.join(build_dn,'_static/'))
	proc = subprocess.check_call('sphinx-build . %s'%master_dn,shell=True,cwd=build_dn)
	print('[NOTE] documentation available at "file://%s"'%
		os.path.join(build_dn,master_dn,'index.html'))

def docs_refresh():
	"""
	Refresh the documentation if it already exists.
	This is mostly meant to make the documentation development faster.
	"""
	docs_dn = 'build_all'
	master_dn = 'DOCS'
	build_dn = os.path.join(os.path.dirname(__file__),docs_dn)
	if not os.path.isdir(build_dn): raise Exception('[ERROR] cannot find build directory %s. '%build_dn+
		'make the docs from scratch instead, with `make docs`.')
	subprocess.check_call('rsync -ariv ../walkthrough/* ./',cwd=build_dn,shell=True)
	subprocess.check_call('sphinx-build . %s'%master_dn,shell=True,cwd=build_dn)
