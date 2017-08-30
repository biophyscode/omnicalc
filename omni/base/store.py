#!/usr/bin/env python

"""
Storage functions. Require a workspace in globals so do an import/export.
"""

import os,sys,re,glob,json,collections,importlib
import matplotlib as mpl
import matplotlib.pyplot as plt
from base.tools import str_or_list,status
from PIL import Image
from PIL import PngImagePlugin
import numpy as np

def picturesave(savename,directory='./',meta=None,extras=[],backup=False,
	dpi=300,form='png',version=False,pdf=False,tight=True,pad_inches=0,figure_held=None,loud=True):
	"""
	Function which saves the global matplotlib figure without overwriting.
	!Note that saving tuples get converted to lists in the metadata so if you notice that your plotter is not 
	overwriting then this is probably why.
	"""
	#---intervene here to check the wordspace for picture-saving "hooks" that apply to all new pictures
	if 'work' in globals() and 'picture_hooks' in work.vars:
		extra_meta = work.vars['picture_hooks']
		#---redundant keys are not allowed: either they are in picture_hooks or passed to picturesave
		redundant_extras = [i for i in extra_meta if i in meta]
		if any(redundant_extras):
			raise Exception(
				'keys "%r" are incoming via meta but are already part of picture_hooks'
				%redundant_extras)
	#---if version then we choose savename based on the next available index
	if version:
		#---check for this meta
		search = picturefind(savename,directory=directory,meta=meta,loud=loud)
		if not search:
			if meta == None: raise Exception('[ERROR] versioned image saving requires meta')
			fns = glob.glob(os.path.join(directory,savename+'.v*'))
			nums = [int(re.findall('^.+\.v([0-9]+)\.png',fn)[0]) for fn in fns 
				if re.match('^.+\.v[0-9]+\.png',fn)]
			ind = max(nums)+1 if nums!=[] else 1
			savename += '.v%d'%ind
		else: savename = re.findall('(.+)\.[a-z]+',os.path.basename(search))[0]
	#---backup if necessary
	savename += '.'+form
	base_fn = os.path.join(directory,savename)
	if loud: status('saving picture to %s'%savename,tag='store')
	if os.path.isfile(base_fn) and backup:
		for i in range(1,100):
			latestfile = '.'.join(base_fn.split('.')[:-1])+'.bak'+('%02d'%i)+'.'+base_fn.split('.')[-1]
			if not os.path.isfile(latestfile): break
		if i == 99 and os.path.isfile(latestfile):
			raise Exception('except: too many copies')
		else: 
			if loud: status('backing up '+base_fn+' to '+latestfile,tag='store')
			os.rename(base_fn,latestfile)
	#---intervene to use the PDF backend if desired
	#---...this is particularly useful for the hatch-width hack 
	#---...(search self.output(0.1, Op.setlinewidth) in 
	#---...python2.7/site-packages/matplotlib/backends/backend_pdf.py and raise it to e.g. 3.0)
	if pdf:
		alt_name = re.sub('.png$','.pdf',savename)
		#---holding the figure allows other programs e.g. ipython notebooks to show and save the figure
		(figure_held if figure_held else plt).savefig(alt_name,dpi=dpi,bbox_extra_artists=extras,
			bbox_inches='tight' if tight else None,pad_inches=pad_inches if pad_inches else None)
		#---convert pdf to png
		os.system('convert -density %d %s %s'%(dpi,alt_name,base_fn))
		os.remove(alt_name)
	else: 
		(figure_held if figure_held else plt).savefig(base_fn,dpi=dpi,bbox_extra_artists=extras,
			bbox_inches='tight' if tight else None,pad_inches=pad_inches if pad_inches else None)
	plt.close()
	#---add metadata to png
	if meta != None:
		im = Image.open(base_fn)
		imgmeta = PngImagePlugin.PngInfo()
		imgmeta.add_text('meta',json.dumps(meta))
		im.save(base_fn,form,pnginfo=imgmeta)

def picturedat(savename,directory='./',bank=False):
	"""
	Read metadata from figures with identical names.
	"""
	directory = os.path.join(directory,'')
	if not bank: 
		if os.path.isfile(directory+savename): 
			return json.loads(Image.open(directory+savename).info['meta'])
		else: return
	else:
		dicts = {}
		if os.path.isfile(directory+savename):
			dicts[directory+savename] = Image.open(directory+savename).info
		for i in range(1,100):
			base = directory+savename
			latestfile = '.'.join(base.split('.')[:-1])+'.bak'+('%02d'%i)+'.'+base.split('.')[-1]
			if os.path.isfile(latestfile): dicts[latestfile] = json.loads(Image.open(latestfile).info)
		return dicts

def lowest_common_dict_denominator(data):
	"""..."""
	if isinstance(data,basestring): return str(data)
	elif isinstance(data,collections.Mapping): 
		return dict(map(lowest_common_dict_denominator,data.iteritems()))
	elif isinstance(data,collections.Iterable): 
		return type(data)(map(lowest_common_dict_denominator,data))
	else: return data

def compare_dicts(a,b):
	"""Compare dictionaries with unicode strings."""
	return lowest_common_dict_denominator(a)==lowest_common_dict_denominator(b)

def picturefind(savename,directory='./',meta=None,loud=True):
	"""
	Find a picture in the plot repository.
	"""
	if loud: status('searching pictures',tag='store')
	regex = '^.+\.v([0-9]+)\.png'
	fns = glob.glob(directory+'/'+savename+'.v*')
	nums = map(lambda y:(y,int(re.findall(regex,y)[0])),filter(lambda x:re.match(regex,x),fns))
	matches = [fn for fn,num in nums if 
		compare_dicts(meta,picturedat(os.path.basename(fn),directory=directory))]
	if len(matches)>1 and meta!=None: 
		print('[ERROR] multiple matches found for %s'%savename)
		raise Exception('???')
	if matches==[] and meta==None:
		return dict([(os.path.basename(fn),
			picturedat(os.path.basename(fn),directory=directory)) for fn,num in nums]) 
	return matches if not matches else matches[0]

def plotload(plotname,specfile=None,choice_override=None,use_group=False,whittle_calc=None):
	"""
	Wrapper around WorkSpace.plotload method for backwards compatibility with plot scripts, which 
	expect to find this function in globals to get data.
	"""
	data,calc = work.plotload(plotname,whittle_calc=whittle_calc)
	return data,calc

def datmerge(kwargs,name,key,same=False):
	"""
	Incoming upstream data are sometimes taken from multiple pickles.
	This function stitches together the key from many of these pickles.
	"""
	#---if there is only one upstream object with no index we simply lookup the key we want
	if name in kwargs.get('upstream',[]): return kwargs['upstream'][name][key]
	else:
		#---! this function seems to require upstream data so we except here
		if 'upstream' not in kwargs: raise Exception('datmerge needs upstream pointers')
		#---get indices for the upstream object added by computer
		inds = [int(re.findall(name+'(\d+)',i)[0]) for i in kwargs['upstream'] if re.match(name,i)]
		collected = [kwargs['upstream']['%s%d'%(name,i)][key] for i in sorted(inds)]
		if not same:
			if collected==[]: raise Exception('collected is empty, argument to datmerge is wrong')
			if type(collected[0])==list: return [i for j in collected for i in j]
			elif type(collected[0])==numpy.ndarray: 
				#---sometimes single numbers are saved as 0-dimensional arrays
				if numpy.shape(collected[0])==(): return numpy.array([float(i) for i in collected])
				else: return numpy.concatenate(collected)
			else: raise Exception('\n[ERROR] not sure how to concatenate')
		else:
			#---the same flag forces a check that the key is the same in each item of the collected data
			if any([any(collected[0]!=c) for c in collected[1:]]): 
				raise Exception('\n[ERROR] objects not same')
			else: return collected[0]

def alternate_module(**kwargs):
	"""
	Systematic way to retrieve an alternate module from within a calculation code by consulting the meta.
	"""
	module_name = kwargs.get('module',None)
	variable_name = kwargs.get('variable',None)
	if not variable_name:
		raise Exception('you must set `variable` in the alternate module')
	if not module_name: 
		raise Exception('you must set `module` in the alternate module')
	try:
		mod = importlib.import_module(module_name)
		result = mod.__dict__.get(variable_name,None)
	except Exception as e:
		raise Exception('failed to import module "%s" and variable "%s" with exception %s'%(
			module_name,variable_name,e))
	return result

def uniquify(array):
    """Get unique rows in an array."""
    #---contiguous array trick
    alt = np.ascontiguousarray(array).view(
        np.dtype((np.void,array.dtype.itemsize*array.shape[1])))
    unique,idx,counts = np.unique(alt,return_index=True,return_counts=True)
    #---sort by count, descending
    idx_sorted = np.argsort(counts)[::-1]
    return idx[idx_sorted],counts[idx_sorted]
