#!/usr/bin/env python

def errorbar(ax,x,d,m,s,color='k'):
	"""
	Plot a nice error bar with rounded bars and a central dot.
	"""
	ax.plot([x-d,x+d],[m-s,m-s],linewidth=4,solid_capstyle='round',color=color,zorder=1)
	ax.plot([x-d,x+d],[m+s,m+s],linewidth=4,solid_capstyle='round',color=color,zorder=1)
	ax.plot([x,x],[m-s,m+s],linewidth=4,solid_capstyle='round',color=color,zorder=1)
	ax.scatter([x],[m],color='w',alpha=1.,marker='o',s=80,zorder=2)
	ax.scatter([x],[m],color='k',alpha=1.,marker='o',s=30,zorder=3)

