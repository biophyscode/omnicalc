#!/usr/bin/env python

def zoom_figure(fig,zoom=3.0):
	w, h = fig.get_size_inches()
	fig.set_size_inches(w * zoom, h * zoom)
