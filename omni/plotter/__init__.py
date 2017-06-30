#!/usr/bin/env python

#---note that this works on clean openSuSE 42.2 installation, then install texlive then texlive-sfmath

import matplotlib as mpl 
import matplotlib.pyplot as plt

mpl.rcParams['text.usetex'] = True
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

