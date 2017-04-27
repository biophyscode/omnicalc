#!/usr/bin/env python

import os,sys,re,inspect,subprocess,time,collections,traceback
import yaml

def flatten(k):
	while any([type(j)==list for j in k]): k = [i for j in k for i in j] 
	return k
def unique(k): return list(set(k))
def delve(o,*k): return delve(o[k[0]],*k[1:]) if len(k)>1 else o[k[0]]

unescape = lambda x: re.sub(r'\\(.)',r'\1',x)
argsort = lambda seq : [x for x,y in sorted(enumerate(seq), key = lambda x: x[1])]
tupleflat = lambda x: [j for k in [list([i]) if type(i)!=tuple else list(i) for i in x] for j in k]

def unpacker(fn,name=None):
	"""
	Read a py or yaml file and return a variable by name.
	"""
	if re.match('^.+\.py',os.path.basename(fn)):
		incoming = {}
		execfile(fn,incoming)
		return incoming if not name else incoming[name]
	elif re.match('^.+\.yaml',os.path.basename(fn)):
		with open(fn) as fp: incoming = yaml.load(fp.read())
		return incoming if not name else incoming[name]
	else: raise Exception('unpacker can only read py or yaml files, not %s'%fn)

def catalog(base,path=None):
	"""
	Traverse all paths in a nested dictionary.
	"""
	if not path: path=[]
	if isinstance(base,dict):
		for x in base.keys():
			local_path = path[:]+[x]
			for b in catalog(base[x],local_path): yield b
	else: yield path,base

def framelooper(total,start=None,text='frame'):
	"""
	When performing parallel calculations with joblib we pass a generator to count the number of 
	tasks and report the time.
	"""
	for fr in range(total):
		status(text,i=fr,looplen=total,tag='parallel',start=start)
		yield fr

def str_or_list(incoming):
	"""
	Promote a string to a list. Useful for allowing both strings and lists in meta files.
	"""
	if type(incoming)==str: return [incoming]
	elif type(incoming)!=list: raise Exception('str_or_list received neither a string nor a list')
	else: return incoming

def status(string,i=0,looplen=None,bar_character=None,width=25,tag='',start=None,pad=None):
	"""
	Show a status bar and counter for a fixed-length operation.
	Taken from AUTOMACS to work in python 2 and 3.
	!NOTE need to fix the thing where lines get shorter and garbage is left behind...
	"""
	#---! it would be useful to receive a signal here to suppress the status bar from 
	#---! ...printing to the log file on backrun.
	#---use unicode if not piping to a log file
	logfile = sys.stdout.isatty()==False
	#---use of equals sign below is deprecated when we suppress status bars in the log file below
	if not logfile: 
		left,right,bb = u'\u2590',u'\u258C',(u'\u2592' if bar_character==None else bar_character)
	else: left,right,bb = '|','|','='
	string = '[%s] '%tag.upper()+string if tag != '' else string
	if pad: string = ('%-'+str(int(pad))+'s')%string
	if not looplen:
		if not logfile: sys.stdout.write(string+'\n')
		else: sys.stdout.write(string+'\n')
	#---suppress progress bar in the log file except on the last item
	elif looplen and logfile and i < looplen-1: pass
	else:
		if start != None:
			esttime = (time.time()-start)/(float(i+1)/looplen)
			timestring = ' %s minutes'%str(abs(round((esttime-(time.time()-start))/60.,1)))
			width = 15
		else: timestring = ''
		countstring = str(i+1)+'/'+str(looplen)
		bar = ' %s%s%s '%(left,int(width*(i+1)/looplen)*bb+' '*(width-int(width*(i+1)/looplen)),right)
		if not logfile: 
			output = u'\r'+string+bar+countstring+timestring+' '
			if sys.version_info<(3,0): output = output.encode('utf-8')
			sys.stdout.flush()
			sys.stdout.write(output)
		else: 
			#---suppressed progress bar in the logfile avoids using carriage return
			sys.stdout.write('[STATUSBAR] '+string+bar+countstring+timestring+' ')
		if i+1<looplen: sys.stdout.flush()
		else: sys.stdout.write('\n')
