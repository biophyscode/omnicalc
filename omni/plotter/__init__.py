#!/usr/bin/env python

import matplotlib as mpl 
import matplotlib.pyplot as plt
#---common imports
from mpl_toolkits.axes_grid1.inset_locator import inset_axes

if False:
	#---note that this works on clean openSuSE 42.2 installation, then install texlive then texlive-sfmath
	#---we require latex because too many plots already use latex commands. see dockerfiles for help installing 
	#---...the latex packages. note that this is the point at which latex would need to be turned off

	import matplotlib as mpl 
	import matplotlib.pyplot as plt
	#---common imports
	from mpl_toolkits.axes_grid1.inset_locator import inset_axes


	from distutils.spawn import find_executable
	has_latex = find_executable('latex')!=None
	#---! read a latex flag from the configuration, or have it passed through?
	mpl.rcParams['text.usetex'] = has_latex
	mpl.rcParams['text.latex.preamble'] = [
		r'\usepackage{sfmath}',
		r'\usepackage{amsmath}',
		r'\usepackage{siunitx}',
		r'\sisetup{detect-all}',
		r'\usepackage{helvet}',
		r'\usepackage{sansmath}',
		r'\sansmath',
		][:]

	mpl.rc('font',**{'family':'sans-serif','sans-serif':
		['Helvetica','Avant Garde','Computer Modern Sans serif'][2]})

