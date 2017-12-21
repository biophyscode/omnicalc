#!/usr/bin/env python

"""
GROMACS slicer
make samples of a trajectory in GROMACS
"""

import os,sys,time,re,subprocess
from config import bash
from base.tools import status

#---only load gmxpaths once when it is used
gmxpaths = None

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

def get_machine_config(hostname=None):
	"""
	Use AUTOMACS format for getting GROMACS paths.
	Copied from amx/calls.py.
	!!! NOTE THIS NEEDS TO BE UPDATED.
	"""
	machine_config = {}
	#---!
	config_fn = 'gromacs_config.py'
	print('[STATUS] reading %s'%config_fn)
	if not os.path.isfile(os.path.expanduser(config_fn)):
		config_fn = os.path.expanduser('~/.automacs.py')
		print('[STATUS] reading %s'%config_fn)
		if not os.path.isfile(config_fn):
			raise Exception('cannot find either a local (gromacs_config.py) or a global (~/.automacs.py) '
				'gromacs configuration. make one with `make gromacs_config (local|home)`')
	with open(os.path.expanduser(config_fn)) as fp: exec(fp.read(),machine_config)
	#---most of the machine configuration file are headers that are loaded into the main dictionary
	machine_config = machine_config['machine_configuration']
	this_machine = 'LOCAL'
	if not hostname:
		hostnames = [key for key in machine_config 
			if any([varname in os.environ and (
			re.search(key,os.environ[varname])!=None or re.match(key,os.environ[varname]))
			for varname in ['HOST','HOSTNAME']])]
	else: hostnames = [key for key in machine_config if re.search(key,hostname)]
	#---select a machine configuration according to the hostname
	if len(hostnames)>1: raise Exception('[ERROR] multiple machine hostnames %s'%str(hostnames))
	elif len(hostnames)==1: this_machine = hostnames[0]
	else: this_machine = 'LOCAL'
	print('[STATUS] setting gmxpaths for machine: %s'%this_machine)
	machine_config = machine_config[this_machine]
	#---! previously did some ppn calculations here
	return machine_config

def modules_load(machine_config):
	"""
	Interact with environment modules to load software.
	"""
	#---modules in LOCAL configuration must be loaded before checking version
	import importlib
	if 'module_path' in machine_config: module_path = machine_config['module_path']
	else:
		module_parent = os.environ.get('MODULESHOME','/usr/share/Modules/default')
		module_path = os.path.join(module_parent,'init','python.py')
	incoming = {}
	if sys.version_info<(3,0): execfile(module_path,incoming)
	else: exec(open(module_path).read(),incoming)
	#---note that modules that rely on dynamically-linked C-code must use EnvironmentModules
	modlist = machine_config['modules']
	if type(modlist)==str: modlist = modlist.split(',')
	for mod in modlist:
		#---always unload gromacs to ensure correct version
		incoming['module']('unload','gromacs')
		print('[STATUS] module load %s'%mod)
		incoming['module']('load',mod)

def get_gmx_paths(override=False,gmx_series=False,hostname=None):
	"""
	Copied from amx/calls.py.
	!!! NOTE THIS NEEDS TO BE UPDATED.
	"""
	gmx4paths = {'grompp':'grompp','mdrun':'mdrun','pdb2gmx':'pdb2gmx','editconf':'editconf',
		'genbox':'genbox','make_ndx':'make_ndx','genion':'genion','genconf':'genconf',
		'trjconv':'trjconv','tpbconv':'tpbconv','vmd':'vmd','gmxcheck':'gmxcheck','gmx':'gmxcheck',
		'trjcat':'trjcat'}
	gmx5paths = {'grompp':'gmx grompp','mdrun':'gmx mdrun','pdb2gmx':'gmx pdb2gmx',
		'editconf':'gmx editconf','genbox':'gmx solvate','make_ndx':'gmx make_ndx',
		'genion':'gmx genion','trjconv':'gmx trjconv','genconf':'gmx genconf',
		'tpbconv':'gmx convert-tpr','gmxcheck':'gmx check','vmd':'vmd','solvate':'gmx solvate','gmx':'gmx',
		'trjcat':'gmx trjcat'}
	#---note that we tacked-on "gmx" so you can use it to find the share folder using get_gmx_share
	machine_config = get_machine_config(hostname=hostname)
	#---check the config for a "modules" keyword in case we need to laod it
	if 'modules' in machine_config: modules_load(machine_config)
	#---basic check for gromacs version series
	suffix = '' if 'suffix' not in machine_config else machine_config['suffix']
	check_gmx = subprocess.Popen('gmx%s'%suffix,shell=True,executable='/bin/bash',
		stdout=subprocess.PIPE,stderr=subprocess.PIPE).communicate()
	if override and 'gmx_series' in machine_config: gmx_series = machine_config['gmx_series']
	elif not gmx_series:
		#---! is this the best way to search?
		if not re.search('command not found',str(check_gmx[1])): gmx_series = 5
		else:
			output = subprocess.Popen('mdrun%s -g /tmp/md.log'%suffix,shell=True,
				executable='/bin/bash',stdout=subprocess.PIPE,stderr=subprocess.PIPE).communicate()
			if sys.version_info<(3,0): check_mdrun = ''.join(output)
			else: check_mdrun = ''.join([i.decode() for i in output])
			if re.search('VERSION 4',check_mdrun): gmx_series = 4
			elif not override: raise Exception('gromacs is absent. make sure it is installed. '+
				'if your system uses the `module` command, try loading it with `module load gromacs` or '+
				'something similar. you can also add `modules` in a list to the machine configuration dictionary '+
				'in your gromacs config file (try `make gromacs_config` to see where it is).')
			else: print('[NOTE] preparing gmxpaths with override')
	if gmx_series == 4: gmxpaths = dict(gmx4paths)
	elif gmx_series == 5: gmxpaths = dict(gmx5paths)
	else: raise Exception('gmx_series must be either 4 or 5')
	#---! need more consistent path behavior here
	#---modify gmxpaths according to hardware configuration
	config = machine_config
	if suffix != '': 
		if gmx_series == 5:
			for key,val in gmxpaths.items():
				gmxpaths[key] = re.sub('gmx ','gmx%s '%suffix,val)
		else: gmxpaths = dict([(key,val+suffix) for key,val in gmxpaths.items()])
	if 'nprocs' in machine_config and machine_config['nprocs'] != None: 
		gmxpaths['mdrun'] += ' -nt %d'%machine_config['nprocs']
	#---use mdrun_command for quirky mpi-type mdrun calls on clusters
	if 'mdrun_command' in machine_config: gmxpaths['mdrun'] = machine_config['mdrun_command']
	#---if any utilities are keys in config we override and then perform uppercase substitutions from config
	utility_keys = [key for key in gmxpaths if key in machine_config]
	if any(utility_keys):
		for name in utility_keys:
			gmxpaths[name] = machine_config[name]
			for key,val in machine_config.items(): 
				gmxpaths[name] = re.sub(key.upper(),str(val),gmxpaths[name])
		del name
	#---even if mdrun is customized in config we treat the gpu flag separately
	if 'gpu_flag' in machine_config: gmxpaths['mdrun'] += ' -nb %s'%machine_config['gpu_flag']	
	#---export the gmxpaths to the state
	if 'state' in globals(): state.gmxpaths = gmxpaths
	return gmxpaths
	
def edrcheck(fn,debug=False):
	"""
	Given the path of an EDR file we return its start and end time.
	!!! Perhaps store the EDR data in a more comprehensive format.
	"""
	global gmxpaths
	if gmxpaths==None: gmxpaths = get_gmx_paths()
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

def infer_parts_to_slice(start,end,skip,sequence):
	"""
	We collect start/stop times from EDR files before slicing because it's faster than parsing the
	trajectories themselves. But since the timestamps in EDR files are not 1-1 with the trajectories
	we have to infer which trajectory files to use, in a non-strict way.
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
	bash(gmxpaths['trjconv']+tail,cwd=postdir,inpipe='0\n')
	
	#---convert relevant trajectories
	start = time.time()
	for ii,(outfile,cmd) in enumerate(cmdlist):
		status('slicing trajectory',i=ii,looplen=len(cmdlist),start=start,tag='SLICE')
		bash(cmd,cwd=postdir,inpipe='0\n')
	
	#---concatenate remaining steps with no errors
	valid_parts = range(len(cmdlist))
	bash(gmxpaths['trjcat']+' -o %s.%s -f '%(outkey,output_format)+
		' '.join(zip(*cmdlist)[0]),cwd=postdir)

	#---delete extraneous files
	#---! consider using a temporary directory although it's nice to have things onsite
	for outfile in zip(*cmdlist)[0]:
		os.remove(postdir+'/%s'%outfile)
