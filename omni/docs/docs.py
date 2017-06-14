#!/usr/bin/env python

"""
Compile documentation. 
Forked from the automacs version with live documentation removed.
"""

import os,sys,re,glob,subprocess,shutil,datetime
from makeface import strip_builtins
from datapack import asciitree
from sphinx_auto import makefile,conf_py,conf_master_py

__all__ = ['docs','publish_docs']

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

def publish_docs(to=''):
	"""
	Prepare documentation for push to github pages. Administrator usage only.
	WARNING! Make sure you compile the docs manually before you run this!

	NOTES:
	-----
	This function will set up the repo to track the github repo.
	We used a similar procedure to update the docs, and eventually replaced it with the current
	set of commands to handle the newer versions of git.	
	The first commit to the repo was created as follows (saved here for posterity):
		git init .
		git commit -m 'initial commit' --allow-empty
		git branch gh-pages
		git checkout gh-pages
		touch .nojekyll
		git add .
		git commit -am 'added files'
		git remote add origin <destination>
		git push -u origin gh-pages
	"""
	html_source_path = 'build_all/DOCS'
	if not to: raise Exception('send destination for documentation via the "to" argument to make')
	dropspot = os.path.join(os.path.dirname(__file__),html_source_path,'')
	print('[WARNING] you must make sure the docs are up-to-date before running this!')
	timestamp = '{:%Y.%m.%d.%H%M}'.format(datetime.datetime.now())
	cmds = [
		'git init .',
		'git checkout -b new_changes',
		'git add .',
		'git commit -m "refreshing docs"',
		'git remote add origin "%s"'%to,
		'git fetch origin gh-pages',
		'git checkout gh-pages',
		('git merge -X theirs -m "refreshing docs" new_changes',
			'git merge -X theirs --allow-unrelated-histories -m "refreshing docs" new_changes'),
		'git commit -m "refreshing docs"',
		'git push --set-upstream origin gh-pages',
		][:-1]
	for cmd in cmds: 
		if type(cmd)==tuple: run_cmds = cmd
		else: run_cmds = [cmd]
		for try_num,this_cmd in enumerate(run_cmds):
			try: subprocess.call(this_cmd,cwd=dropspot,shell=True)		
			except: 
				if try_num==0: continue 
				else: raise Exception('[ERROR] both command options failed!')
	print('[NOTE] tracking github pages from "%s"'%dropspot)
	print('[NOTE] admins can push from there to publish documentation changes')
	print('[NOTE] run "git push --set-upstream origin gh-pages" from there')
