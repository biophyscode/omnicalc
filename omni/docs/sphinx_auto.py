#!/usr/bin/env python 

makefile = """
# Makefile for Sphinx documentation
#

# You can set these variables from the command line.
SPHINXOPTS    =
SPHINXBUILD   = sphinx-build
PAPER         =
BUILDDIR      = _build

# Internal variables.
PAPEROPT_a4     = -D latex_paper_size=a4
PAPEROPT_letter = -D latex_paper_size=letter
ALLSPHINXOPTS   = -d $(BUILDDIR)/doctrees $(PAPEROPT_$(PAPER)) $(SPHINXOPTS) .
# the i18n builder cannot share the environment and doctrees with the others
I18NSPHINXOPTS  = $(PAPEROPT_$(PAPER)) $(SPHINXOPTS) .

.PHONY: clean
clean:
	rm -rf $(BUILDDIR)/*

.PHONY: html
html:
	$(SPHINXBUILD) -b html $(ALLSPHINXOPTS) $(BUILDDIR)/html
	@echo
	@echo "Build finished. The HTML pages are in $(BUILDDIR)/html."

.PHONY: dirhtml
dirhtml:
	$(SPHINXBUILD) -b dirhtml $(ALLSPHINXOPTS) $(BUILDDIR)/dirhtml
	@echo
	@echo "Build finished. The HTML pages are in $(BUILDDIR)/dirhtml."

.PHONY: singlehtml
singlehtml:
	$(SPHINXBUILD) -b singlehtml $(ALLSPHINXOPTS) $(BUILDDIR)/singlehtml
	@echo
	@echo "Build finished. The HTML page is in $(BUILDDIR)/singlehtml."
"""

conf_base = """
import sys
import os
import shlex

sys.dont_write_bytecode = True

extensions = ['sphinx.ext.autodoc','numpydoc']

autodoc_docstring_signature = True
templates_path = ['_templates']
source_suffix = '.rst'
master_doc = 'index'

#---project information
project = u'omni'
html_show_copyright = False
html_show_sphinx = False
author = u'BioPhysCode'
version = ''
release = ''
language = 'en'
today_fmt = '%%Y.%%B.%%d'
exclude_patterns = ['_build', 'Thumbs.db', '.DS_Store']
pygments_style = 'sphinx'
todo_include_todos = False

#---paths for custom themes
html_theme = 'bizstyle-custom'
html_theme_path = ['_static/']
html_title = "OMNICALC Documentation"
html_short_title = "OMNI docs"
html_logo = 'omnicalc.png'
html_static_path = ['_static']
htmlhelp_basename = 'omnidoc'

from sphinx.ext import autodoc

class SimpleDocumenter(autodoc.MethodDocumenter):
  objtype = "simple"
  #---do not add a header to the docstring
  def add_directive_header(self, sig): pass

def setup(app):
    app.add_autodocumenter(SimpleDocumenter)

#---variable paths
#---! get these exactly from modules
rst_prolog = '\\n'.join([
	'.. |path_runner| replace:: ../../../runner/',
	])

"""

conf_py = conf_base + '\n'+ r"import sys;sys.path.insert(0,'%s')"
conf_master_py = conf_base + '\n'+ r"%s"
