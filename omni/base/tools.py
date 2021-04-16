#!/usr/bin/env python

import os,sys,re,inspect,subprocess,time,collections,traceback,importlib
str_types = [str,unicode] if sys.version_info<(3,0) else [str]
try: import yaml
except: print('[WARNING] no yaml so environment is not ready')

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
	#---this case handles terminal blank dictionaries. added 2017.08.07 and needs propagated
	if base=={}: yield path,{}
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

def status(string,i=0,looplen=None,bar_character=None,width=None,spacer='.',
	bar_width=25,tag='status',start=None,pad=None,refresh=True):
	"""
	Show a status bar and counter for a fixed-length operation.
	Taken from AUTOMACS to work in python 2 and 3.
	!NOTE need to fix the thing where lines get shorter and garbage is left behind...
	"""
	#---! it would be useful to receive a signal here to suppress the status bar from 
	#---! ...printing to the log file on backrun.
	#---use unicode if not piping to a log file
	logfile = (not hasattr(sys.stdout,'isatty')) or sys.stdout.isatty()==False
	#---use of equals sign below is deprecated when we suppress status bars in the log file below
	if not logfile: 
		left,right,bb = u'\u2590',u'\u258C',(u'\u2592' if bar_character==None else bar_character)
	else: left,right,bb = '|','|','='
	string = '[%s] '%tag.upper()+string if tag != '' else string
	if width: string = string.ljust(width,spacer)[:width]
	if pad: string = ('%-'+str(int(pad))+'s')%string
	if not looplen:
		if not logfile: sys.stdout.write(string+'\n')
		else: sys.stdout.write(string+'\n')
	elif looplen and logfile and i==0: sys.stdout.write('[STATUS] running a loop ')
	#---suppress progress bar in the log file except on the last item
	elif looplen and logfile and i>0 and i<looplen-1: sys.stdout.write('.')
	else:
		if start != None:
			esttime = (time.time()-start)/(float(i+1)/looplen)
			timestring = ' %s minutes'%str(abs(round((esttime-(time.time()-start))/60.,1)))
			bar_width = 15
		else: timestring = ''
		countstring = str(i+1)+'/'+str(looplen)
		bar = ' %s%s%s '%(left,int(bar_width*(i+1)/looplen)*bb+' '*\
			(bar_width-int(bar_width*(i+1)/looplen)),right)
		if not logfile: 
			output = (u'\r' if refresh else '')+string+bar+countstring+timestring+' '
			if sys.version_info<(3,0): output = output.encode('utf-8')
			if refresh:
				sys.stdout.flush()
				sys.stdout.write(output)
			else: print(output)
		else: 
			#---suppressed progress bar in the logfile avoids using carriage return
			sys.stdout.write('[STATUSBAR] '+string+bar+countstring+timestring+' ')
		if i+1<looplen: sys.stdout.flush()
		else: sys.stdout.write('\n')

def dictsum(*args):
	"""Merge dictionaries sequentially."""
	if not all([type(d)==dict for d in args]):
		raise Exception('dictsum can only accept dict objects')
	return dict([(k,v) for d in args for k,v in d.items()])

def gopher(spec,module_name='module',variable_name='function'):
	"""Load an external module. Useful for changing the workflow without changing the code."""
	mod = importlib.import_module(spec[module_name])
	target = mod.__dict__.get(spec[variable_name],None)
	if not target: raise Exception('add %s and %s to the specs'%(module_name,variable_name))
	return target

def backrun(command=None,cwd='.',log='log-back'):
    """
    Run a command in the background and generate a kill switch.

    Parameters
    ----------
    command : string
        A terminal command used to execute the script in the background e.g. ``./script-protein.py``. You
        can also use other targets in the command e.g. ``make back command="make metarun <name>"``.
    """
    cmd = "nohup %s > %s 2>&1 &"%(command,log)
    print('[STATUS] running the background via "%s"'%cmd)
    job = subprocess.Popen(cmd,shell=True,cwd=cwd,preexec_fn=os.setsid)
    ask = subprocess.Popen('ps xao pid,ppid,pgid,sid,comm',
        shell=True,stdout=subprocess.PIPE,stderr=subprocess.PIPE)
    com = ask.communicate()
    if sys.version_info>=(3,0): ret = '\n'.join([j.decode() for j in com])
    else: ret = '\n'.join(com)
    if sys.version_info>=(3,0):
        pgid = next(int(i.split()[2]) for i in ret.splitlines() if re.match('^\s*%d\s'%job.pid,i))
    else: pgid = next(int(i.split()[2]) for i in ret.splitlines() if re.match('^\s*%d\s'%job.pid,i))
    kill_script = 'script-stop-job.sh'
    term_command = 'pkill -TERM -g %d'%pgid
    with open(kill_script,'w') as fp: fp.write(term_command+'\n')
    os.chmod(kill_script,0o744)
    print('[STATUS] if you want to terminate the job, run "%s" or "./%s"'%(term_command,kill_script))
    job.communicate()

class Observer(object):
	"""Watch locals and return them. The umpteenth metaprogramming trick for nice environments!"""
	# via https://stackoverflow.com/questions/9186395/
	# ... python-is-there-a-way-to-get-a-local-function-variable-from-within-a-decorator
	def __init__(self,function):
		self._locals = {}
		self.function = function
	def __call__(self,*args,**kwargs):
		def tracer(frame,event,arg):
			if event=='return': 
				self._locals = frame.f_locals.copy()
				# it is unwise to modify locals so dynamic variables can drop to _locals
				self._locals.update(**self._locals.get('__locals__',{}))
		# tracer is activated on next call, return or exception
		sys.setprofile(tracer)
		# trace the function call
		try: res = self.function(*args,**kwargs)
		# disable tracer and replace with old one
		finally: sys.setprofile(None)
	def clear_locals(self): self._locals = {}
	@property
	def locals(self): return self._locals

def unique_ordered(seq):
	"""Return unique items maintaining the order."""
	vals = set()
	return [x for x in seq if not (x in vals or vals.add(x))]
