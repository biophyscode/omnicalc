#!/usr/bin/env python

import os,collections,re

from base.tools import status,delve
from .slicer import edrcheck

class ParsedRawData:

	"""
	Import raw simulation data.
	"""

	def __init__(self,spots):
		"""Parse simulation data on disk into a usable structure."""
		# default trajectory format is GROMACS XTC
		self.trajectory_format = 'xtc'
		# process the spots
		# for each "spot" in the yaml file, we construct a template for the data therein
		# the table of contents ("toc") holds one parsing for every part regex in every spot
		self.spots,self.toc = {},collections.OrderedDict()
		for name,details in spots.items():
			status('parsing data from spot "%s"'%name,tag='parse')
			rootdir = os.path.join(details['route_to_data'],details['spot_directory'])
			if not os.path.isdir(rootdir):
				raise Exception('\n[ERROR] cannot find root directory %s'%rootdir)
			for pnum,(part_name,part_regex) in enumerate(details['regexes']['part'].items()):
				status('parsing data type "%s"'%part_name,i=pnum,
					looplen=len(details['regexes']['part']),tag='parse')
				spot = (name,part_name)
				self.toc[spot] = {}
				self.spots[spot] = {
					'rootdir':os.path.join(rootdir,''),
					'top':details['regexes']['top'],
					'step':details['regexes']['step'],
					'part':part_regex,
					'namer':eval(details['namer']),
					'namer_text':details['namer'],}
				self.spots[spot]['divy_keys'] = self.divy_keys(spot)
		#---run the treeparser on each spot
		for snum,(spotname,spot) in enumerate(self.spots.items()):
			status('running the treeparser: %s,%s'%spotname,
				i=snum,looplen=len(self.spots),tag='parse',width=65)
			self.treeparser(spotname,**spot)

	def divy_keys(self,spot):
		"""
		The treeparser matches trajectory files with a combined regex. 
		This function prepares a lambda that divides the combined regex into parts and reduces them to 
		strings if there is only one part. The resulting regex groups serve as keys in the toc.
		"""
		group_counts = [sum([i[0].name=='SUBPATTERN' 
			for i in re.sre_parse.parse(self.spots[spot][key])]) 
			#---apply naming convention
			for key in ['top','step','part']]
		cursor = ([0]+[sum(group_counts[:i+1]) for i in range(len(group_counts))])
		slices = [slice(cursor[i],cursor[i+1]) for i in range(len(cursor)-1)]
		divy = lambda x: [y[0] if len(y)==1 else y for y in [x[s] for s in slices]]
		return divy

	def keyfinder(self,spotname):
		"""
		Decorate the keys_to_filename lookup function so it can be sent to e.g. slice_trajectory.
		If you are only working with a single spot, then this creates the file-name inference function
		for all data in that spot.
		"""
		def keys_to_filename(*args,**kwargs):
			"""
			After decomposing a list of files into keys that match the regexes in paths.yaml we often 
			need to reconstitute the original filename.
			"""
			strict = kwargs.get('strict',True)
			if not spotname in self.toc: raise Exception('need a spotname to look up keys')
			#---! it may be worth storing this as a function a la divy_keys
			#---follow the top,step,part naming convention
			try:
				backwards = [''.join(['%s' if i[0].name=='SUBPATTERN' else chr(i[1]) 
					for i in re.sre_parse.parse(regex)]) for regex in [self.spots[spotname][key] 
					for key in ['top','step','part']]]
				fn = os.path.join(
					self.spots[spotname]['rootdir'],
					'/'.join([backwards[ii]%i for ii,i in enumerate(args)]))
			except Exception as e: 
				print(e)
				raise Exception('error making keys: %s,%s'%(str(spotname),str(args)))
			if strict: 
				if not os.path.isfile(fn): raise Exception('cannot find %s'%fn)
			return fn
		return keys_to_filename

	def spotname_lookup(self,sn_full):
		"""
		Find the spotname for a particular simulation.
		This is only used in a few places in maps.py: in the prefixer below and the portion of 
		SliceMeta.__init__ which makes slices.
		"""
		#---alias to the shortname (irreversible) here
		#---! naming is getting convoluted. the following try-except would be hard to debug
		try: sn = self.work.namer.short_namer(sn_full,spot=None)
		#---failure to run the shortnamer just passes the full simulation name
		except: sn = sn_full
		assert type(sn)==str
		#---the following or statement allows this to work for both full and short names
		#---! this needs further testing
		spotnames = [key for key,val in self.toc.items() if sn in val or sn_full in val]
		if not spotnames: 
			#---! development. needs tested. may only be used on near proto_slice in
			#---! ...readymade_namd near line 581. remove this and error message after testing
			#---! ...and note that the error below refers to refresh which is deprecated
			return None
			#---in case top diverges from prefixer we check the regexes
			top_regexes = [v['regexes']['top'] for v in self.work.paths['spots'].values()]
			if not any([re.match(top,sn) for top in top_regexes]):
				raise Exception('[ERROR] could not find "%s" in the toc *and* it fails to match '%sn+
					"any 'top' regexes in your paths. fix 'top' and make sure to run "+
					"'make refresh' to search for simulations again")
			raise Exception('[ERROR] could not find simulation "%s" in the toc'%sn)
		spotnames_unique = list(set(list(zip(*spotnames))[0]))
		if len(spotnames_unique) != 1: 
			raise Exception('[ERROR] you cannot have the same simulation in multiple spots.\n'+
				'simulation = "%s" and "%s"'%(sn,str(spotnames)))
		return spotnames_unique[0]

	def prefixer(self,sn):
		"""
		Choose a prefix for naming post-processing files.
		"""
		#---"spot" is a tuple of spotname and the part name
		#---namer takes the spotname (called spot in the yaml defn of namer) and the simulation name
		#---we include the partname when accessing self.spots
		try: this_spot = self.spotname_lookup(sn)
		except: raise Exception('cannot find the spot for %s'%sn)
		try:
			spot = spotname,partname = (this_spot,self.trajectory_format)
			#---new format for the namer requires arguments to be simulation name and then spot name
			#---note that the prefixer function is only called when we need slices hence there will always
			#---...be a spotname available. see SliceMeta.__init__ for these calls
			prefix = self.spots[spot]['namer'](sn,spot=spotname)
		except Exception as e: 
			raise Exception('[ERROR] prefixer failure on simulation "%s" (check your namer) %s'%(sn,e))
		return prefix

	def get_last_structure(self,sn,subtype='structure'):
		"""Get the most recent structure file."""
		#---! currently set for one-spot operation. need to apply namers for multiple spots
		spotname = self.spotname_lookup(sn)
		#---the toc is ordered but instead of getting the last one, we just
		#---...get all structures and use the mtimes
		steps = self.toc[(spotname,subtype)][sn]
		candidates = []
		for step_name,step in steps.items():
			for part_name,part in step.items():
				fn = self.keyfinder((spotname,subtype))(
					sn,step_name,part_name)
				#---hold the file if gro and we want structure (since this is the commonest use-case)
				#---...or always save it if we are looking for any other subtype e.g. tpr
				if re.match('^.+\.gro$',fn) or subtype!='structure': candidates.append(fn)
		#---return the newest
		return sorted(candidates,key=lambda x:os.path.getmtime(x))[-1]

	def treeparser(self,spot,**kwargs):
		"""
		This function parses simulation data which are organized into a "spot". 
		It writes the filenames to the table of contents (self.toc).
		"""
		spot_sub = kwargs
		rootdir = spot_sub['rootdir']
		#---start with all files under rootdir
		fns = [os.path.join(dirpath,fn) 
			for (dirpath, dirnames, filenames) 
			in os.walk(rootdir,followlinks=True) for fn in filenames]
		#---regex combinator is the only place where we enforce a naming convention via top,step,part
		#---note that we may wish to generalize this depending upon whether it is wise to have three parts
		regex = ('^%s\/'%re.escape(rootdir.rstrip('/'))+
			'\/'.join([spot_sub['top'],spot_sub['step'],spot_sub['part']])
			+'$')
		matches_raw = [i.groups() for fn in fns for i in [re.search(regex,fn)] if i]
		if not matches_raw: return
		#---first we organize the top,step,part into tuples which serve as keys
		#---we organize the toc as a doubly-nested dictionary of trajectory parts
		#---the top two levels of the toc correspond to the top and step signifiers
		#---note that this procedure projects the top,step,part naming convention into the toc
		matches = [self.spots[spot]['divy_keys'](i) for i in matches_raw]
		self.toc[spot] = collections.OrderedDict()
		#---at this point we apply the irreversible transformation from long simulation names to short ones
		#---...by using the short namer.
		#---! removed short_namer on the top below for expedience, but this needs tested and confirmed
		matches = [(top,step,part) for top,step,part in matches]
		#---sort the tops into an ordered dictionary
		for top in sorted(set(list(zip(*matches))[0])): 
			self.toc[spot][top] = collections.OrderedDict()
		#---collect unique steps for each top and load them with the parts
		for tnum,top in enumerate(self.toc[spot]):
			#---sort the steps into an ordered dictionary
			sorted_matches = sorted(set([i[1] for i in matches if i[0]==top]))
			for snum,step in enumerate(sorted_matches):
				#---we sort the parts into an ordered dictionary
				#---this is the leaf of the toc tree and we use dictionaries
				parts = sorted([i[2] for i in matches if i[0]==top and i[1]==step])
				self.toc[spot][top][step] = collections.OrderedDict([(part,{}) for part in parts])
		#---now the toc is prepared with filenames but subsequent parsings will identify EDR files

	def get_timeseries(self,sn_full):
		"""
		Typically EDR times are stored in the toc for a particular spot. 
		This function first figures out which spot you want and then returns the edr data.
		"""
		#---we apply the naming transformation to lookup the shortname in the toc, but below we will send out
		#---...the full name since the purpose of this function is to get filenames on disk for slicing
		sn = sn_full
		#---determine the spot, since the simulation could be in multiple spots
		spot_matches = [spotname for spotname,spot in self.spots.items() 
			if spotname[1]=='edr' and sn in self.toc[spotname]]
		if len(spot_matches)>1: 
			raise Exception('development. need a way to adjucate spots that have redundant simulations')
		elif len(spot_matches)==0:
			import ipdb;ipdb.set_trace()
			raise Exception('cannot find simulation %s in any of the spots: %s'%(sn,self.spots.keys()))
		else: spotname = spot_matches[0]
		edrtree = self.toc[spotname][sn]
		#---! current development only checks EDR files when they are needed instead of pre-populating
		for step in edrtree:
			for part in edrtree[step]:
				#---we have to send the full simulation name to the keyfinder
				fn = self.keyfinder(spotname)(sn_full,step,part)
				times = edrcheck(fn)
				keys = (spotname,sn,step,part)
				leaf = delve(self.toc,*keys)
				leaf['start'],leaf['stop'] = times
		#---naming convention
		#---according to the note above we pass the full simulation name back out for looking up files
		sequence = [((sn_full,step,part),tuple([edrtree[step][part][key] 
			for key in ['start','stop']]))
			for step in edrtree 
			for part in edrtree[step]]
		#---return a list of keys,times pairs
		return sequence

	def get_last(self,sn,subtype):
		"""
		Parse the toc to find the most recent file for a particular regex specified in the spots.
		"""
		#---this function is already mostly implemented above
		return self.get_last_structure(sn,subtype=subtype)
		#---! alternate method prototyped but not used
		spotname = self.spotname_lookup(sn)
		#---looking up the TPR with the standard method, which preassumes a top/step/part structure
		stepname = self.toc[(work.raw.spotname_lookup(sn),'tpr')][sn].keys()[-1]
		partname = self.toc[(work.raw.spotname_lookup(sn),'tpr')][sn][stepname].keys()[-1]
		fn = self.keyfinder((spotname,'tpr'))(sn,stepname,partname)
