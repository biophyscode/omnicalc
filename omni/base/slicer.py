#!/usr/bin/env python

"""
GROMACS slicer
make samples of a trajectory in GROMACS
"""

import os,sys,time,re,subprocess
#! from config import bash
from ortho import bash
from omni.base.tools import status

#---only load gmxpaths once when it is used
#! gmxpaths = None

#! removed automacs-style checking
gmxpaths = {'edrcheck':'gmx edrcheck','gmxcheck':'gmx check','trjconv':'gmx trjconv','trjcat':'gmx trjcat'}

def make_slice_gromacs(**kwargs):
	"""
	Make a slice that does not exist yet.
	Note that this function makes contact with omnicalc (maps.py) in only one place: in the constructor for 
	the SliceMeta class which interprets slices from the metadata and holds them in memory for other 
	functions to access. 
	"""
	spec_in = kwargs.get('spec',None)
	if not spec_in: raise Exception('send slice details in a dict called "spec"')
	req_keys = 'start end skip group'.split()
	missing_keys = [k for k in req_keys if k not in spec_in]
	if any(missing_keys): 
		raise Exception('slice maker for GROMACS is missing items in kwargs[\'specs\']: %s'%missing_keys)
	#---prepare specification for the slicer
	spec = dict([(k,spec_in[k]) for k in req_keys])
	#---get the PBC
	spec['pbc'] = spec_in.get('pbc',None)
	#---sequence uses the EDR files to figure out which parts we need to slice
	spec['sequence'] = kwargs['sequence']
	sn_prefixed = kwargs['sn_prefixed']
	#---name the slices
	pbc_suffix = '' if not spec['pbc'] else '.pbc%s'%spec['pbc']
	spec['outkey'] = '%s.%d-%d-%d.%s%s'%(
		sn_prefixed,spec['start'],spec['end'],spec['skip'],spec['group'],pbc_suffix)
	spec['postdir'] = kwargs['postdir']
	spec['tpr_keyfinder'] = kwargs['tpr_keyfinder']
	spec['traj_keyfinder'] = kwargs['traj_keyfinder']
	#---create the group
	if spec_in['group']:
		if spec_in['group']!=kwargs['group_name']:
			raise Exception('group_name %s does not match the slice group %s'%(
				spec_in['group'],kwargs['group_name']))
		spec_group = dict(sn=kwargs['sn'],group=spec_in['group'],
			select=kwargs['group_selection'],simkey=spec['outkey'])
		#import ipdb;ipdb.set_trace()
		#---get the latest starting structure
		#spec['tpr_keyfinder']('EGFR_active_L747P_MD_2', ('s', '01', 'protein'), '0001')
		group_fn = create_group(postdir=kwargs['postdir'],structure=kwargs['last_structure'],**spec_group)
		spec['group_fn'] = group_fn
	#---call the slice maker
	slice_trajectory(**spec)
	#---return the name for storage in the postdat
	return spec['outkey']

def edrcheck(fn,debug=False):
	"""
	Given the path of an EDR file we return its start and end time.
	!!! Perhaps store the EDR data in a more comprehensive format.
	"""
	global gmxpaths
	if gmxpaths==None: 
		print('\n[STATUS] getting gromacs paths')
		gmxpaths = get_gmx_paths()
	start,end = None,None
	cmd = gmxpaths['gmxcheck']+' -e %s'%fn
	p = subprocess.Popen(cmd,stdout=subprocess.PIPE,stdin=subprocess.PIPE,stderr=subprocess.PIPE,shell=True)
	catch = p.communicate(input=None)
	log = re.sub('\\\r','\n','\n'.join(catch)).split('\n')
	start,end = None,None
	try: 
		start = map(lambda y:re.findall('^.+time\s*([0-9]+\.?[0-9]+)',y)[0],
			filter(lambda z:re.match('\s*(R|r)eading energy frame',z),log))[0]
		end = map(lambda y:re.findall('^.+time\s*([0-9]+\.?[0-9]+)',y)[0],
			filter(lambda z:re.match('\s*(L|l)ast energy',z),log))[0]
	except: pass
	start = float(start) if start!=None else start
	end = float(end) if end!=None else end
	return start,end

def gmxread(grofile,trajfile=None):
	"""
	Read a simulation trajectory via MDAnalysis.
	"""
	if trajfile == None: uni = MDAnalysis.Universe(grofile)
	else: uni = MDAnalysis.Universe(grofile,trajfile)
	return uni

def mdasel(uni,select): 
	"""
	Make a selection in MDAnalysis regardless of version.
	"""
	if hasattr(uni,'select_atoms'): return uni.select_atoms(select)
	else: return uni.selectAtoms(select)

def infer_parts_to_slice_legacy(start,end,skip,sequence):
	"""
	Legacy method for turning an EDR sequence in (sn,(s,N,name),part number)
	"""
	sources = []
	for key,span in sequence:
		if None in span and any([i<=end and i>=start for i in span if i!=None]):
			#---lax requirements for adding to sources in the event of corrupted EDR files
			t0 = int(span[0]/float(skip)+1)*float(skip)
			sources.append((key,t0))
		elif any([
			(start <= span[1] and start >= span[0]),
			(end <= span[1] and end >= span[0]),
			(start <= span[0] and end >= span[1])]):
			#---! why is there a skip+1 below? it is wrong at both the beginning and end
			#---! this needs fixed/made sensible
			t0 = int(span[0]/float(skip)+0)*float(skip)
			sources.append((key,t0))
	return sources	

def infer_parts_to_slice(start,end,skip,sequence):
	"""Stopgap method for resolving step number ambiguity."""
	try:
		# protect from ambiguous step names which occurs when the time stamp starts at zero on a new step 
		# ... and the original method cannot tell which step to use. typically the last step is the only 
		# ... relevant one since preceding steps are usualy preparatory e.g. with restraints. users who wish
		# ... to have more control are welcome to code up something more specific. the slicer is due for an
		# ... overhaul anyway. for now, we just try to get the right sequence by restricting attention to
		# ... the last step. since the toc is sorted this is easy.
		# all steps have the same sn and they should be ordered from the toc so we filter by the last one
		last_step = sequence[-1][0][1]
		sequence_alt = [s for s in sequence if s[0][1]==last_step]
		slice_target = infer_parts_to_slice_legacy(start,end,skip,sequence_alt)
	# fall back to the original method
	except: slice_target = infer_parts_to_slice_legacy(start,end,skip,sequence)
	return slice_target

def create_group(**kwargs):
	"""
	Create a group.
	"""
	sn = kwargs['sn']
	name = kwargs['group']
	select = kwargs['select']
	simkey = kwargs['simkey']
	postdir = kwargs['postdir']
	structure = kwargs['structure']
	cols = 100 if 'cols' not in kwargs else kwargs['cols']
	#---naming convention holds that the group names follow the prefix and we suffix with ndx
	fn = '%s.ndx'%simkey
	fn_abs = os.path.join(postdir,fn)
	#---see if we need to make this group
	if os.path.isfile(fn_abs): return fn_abs
	#---! removed a self.confirm_file function from legacy omnicalc
	print('[STATUS] creating group %s'%simkey)
	#---read the structure
	import MDAnalysis
	uni = MDAnalysis.Universe(structure)
	sel = uni.select_atoms(select)
	#---write NDX 
	import numpy as np
	iii = sel.indices+1	
	rows = [iii[np.arange(cols*i,cols*(i+1) if cols*(i+1)<len(iii) else len(iii))] 
		for i in range(0,len(iii)/cols+1)]
	with open(fn_abs,'w') as fp:
		fp.write('[ %s ]\n'%name)
		for line in rows:
			fp.write(' '.join(line.astype(str))+'\n')
	return fn_abs

def slice_trajectory(**kwargs):
	"""
	Make a trajectory slice.
	The keyfinders are lambda functions that take keys and return the correct filename.
	Grafted in from classic factory almost verbatim.
	"""
	global gmxpaths
	if gmxpaths==None: gmxpaths = get_gmx_paths()
	call = bash
	#---process kwargs
	start,end,skip,sequence = [kwargs[k] for k in 'start end skip sequence'.split()]
	tpr_keyfinder,traj_keyfinder = kwargs['tpr_keyfinder'],kwargs['traj_keyfinder']
	outkey = kwargs['outkey']
	postdir = kwargs['postdir']
	output_format = kwargs.get('output_format','xtc')
	pbc = kwargs.get('pbc',None)
	group_fn = kwargs.get('group_fn',None)

	#---commands to create sub-slices
	sources = infer_parts_to_slice(start,end,skip,sequence)
	sn = sources[0][0][0]
	group_flag = '' if not group_fn else ' -n '+group_fn
	pbc_flag = '' if not pbc else ' -pbc %s'%pbc
	cmdlist = []
	for num,source in enumerate(sources):
		keys,t0 = source
		sn = keys[0]
		#---get tpr exist use the previous one (or fail on first source)
		try: 
			tpr = tpr_keyfinder(*keys,strict=False)
		except: 
			import ipdb;ipdb.set_trace()
			raise Exception('development error. could not locate a TPR: %s'%kwargs)
		#---assume cursor points to the trajectory we want
		try: 
			traj = traj_keyfinder(*keys)
		except Exception as e: 
			raise Exception('could not locate trajectory for %s,%s,%s'%keys+': %s'%e)
		outfile = 'trjconv%d.%s'%(num,output_format)
		"""
		note on timestamps: if you ask for time beyond the end of a simulation, the slicer will fail with
		blank outputs from `gmx trjconv`. in one misadventure, the author misattributed this to problems
		with the interval of the samples, since the dt flag causes trjconv to only save frames with times
		which are zero modulo dt, and copied the begin flag to t0 to fail through the problem silently. 
		a better alternative is to treat trjconv failures more seriously and check the time stamps with
		`make look times`. the slicer is designed to ignore problems of jitter. if a new XTC starts on
		a non-even or non-integer time, the slicer should continue as normal and rely on dt to find the next
		valid time. ... ???
		"""
		tail = ' -b %d -e %d -dt %d -s %s -f %s -o %s%s%s'%(
			t0 if t0>start else start,end,skip,tpr,traj,
			outfile,group_flag,pbc_flag)
		cmdlist.append((outfile,gmxpaths['trjconv']+tail))

	#---make a GRO file of the first frame for reference
	keys,t0 = sources[0]
	sn,sub,fn = keys
	traj = traj_keyfinder(*keys)
	tail = ' -dump %d -s %s -f %s -o %s.gro%s'%(start,tpr,traj,outkey,group_flag)
	if pbc != None: tail = tail + ' -pbc %s'%pbc
	bash(gmxpaths['trjconv']+tail,cwd=postdir,inpipe='0\n',scroll=False)
	
	#---convert relevant trajectories
	start = time.time()
	for ii,(outfile,cmd) in enumerate(cmdlist):
		status('slicing trajectory',i=ii,looplen=len(cmdlist),start=start,tag='SLICE')
		bash(cmd,cwd=postdir,inpipe='0\n',scroll=False)
	
	#---concatenate remaining steps with no errors
	valid_parts = range(len(cmdlist))
	bash(gmxpaths['trjcat']+' -o %s.%s -f '%(outkey,output_format)+
		' '.join(zip(*cmdlist)[0]),cwd=postdir,scroll=False)

	#---delete extraneous files
	#---! consider using a temporary directory although it's nice to have things onsite
	for outfile in zip(*cmdlist)[0]:
		os.remove(postdir+'/%s'%outfile)
