#!/usr/bin/env python

"""
Storage functions. Require a workspace in globals so do an import/export.
More generic data manipulations are found in datapack.
"""

import os,sys,re,glob,json,collections,importlib
from base.tools import str_or_list,status
from PIL import Image
from PIL import PngImagePlugin
import numpy as np

def picturesave(savename,directory='./',meta=None,extras=[],backup=False,
	dpi=300,form='png',version=False,pdf=False,tight=True,pad_inches=0,figure_held=None,loud=True,
	redacted=False):
	"""
	Function which saves the global matplotlib figure without overwriting.
	!Note that saving tuples get converted to lists in the metadata so if you notice that your plotter is not 
	overwriting then this is probably why.
	"""
	#---automatically share images with group members (note that you could move this to config)
	os.umask(0o002)
	#---earlier import allows users to set Agg so we import here, later
	import matplotlib as mpl
	import matplotlib.pyplot as plt
	#---intervene here to check the wordspace for picture-saving "hooks" that apply to all new pictures
	#---! is it necessary to pass the workspace here?
	if 'work' in globals() and 'picture_hooks' in work.metadata.variables:
		extra_meta = work.metadata.variables['picture_hooks']
		#---redundant keys are not allowed: either they are in picture_hooks or passed to picturesave
		redundant_extras = [i for i in extra_meta if i in meta]
		if any(redundant_extras):
			raise Exception(
				'keys "%r" are incoming via meta but are already part of picture_hooks'
				%redundant_extras)
	#---redacted figures have blurred labels
	if redacted:
		directory_redacted = os.path.join(directory,'REDACTED')
		if not os.path.isdir(directory_redacted): os.mkdir(directory_redacted)
		directory = directory_redacted
		status('you have requested redacted figures, so they are saved to %s'%directory,tag='warning')
		import random
		color_back,color_front = '','#696969'
		scrambler = lambda x,max_len=12:''.join([chr(ord('a')+random.randint(0,25)) for i in x][:max_len])
		#---best just to come right out and say it
		scrambler = lambda x,max_len=12: ('redacted...'*3)[:max(max_len,len(x))]
		scrambler = lambda x:'redacted'
		num_format = re.compile("^[\-]?[1-9][0-9]*\.?[0-9]+$")
		isnumber = lambda x:re.match(num_format,x)
		for obj in [i for i in plt.findobj() if type(i)==mpl.text.Text]:
		    text_this = obj.get_text()
		    if text_this!='' and not isnumber(text_this):
		        obj.set_text(scrambler(text_this))
		        if color_back: obj.set_backgroundcolor(color_back)
		        obj.set_color(color_front)
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
	if pdf and form!='png': raise Exception('can only use PDF conversion when writing png')
	elif pdf:
		alt_name = re.sub('.png$','.pdf',savename)
		#---holding the figure allows other programs e.g. ipython notebooks to show and save the figure
		(figure_held if figure_held else plt).savefig(alt_name,dpi=dpi,bbox_extra_artists=extras,
			bbox_inches='tight' if tight else None,pad_inches=pad_inches if pad_inches else None,format=form)
		#---convert pdf to png
		os.system('convert -density %d %s %s'%(dpi,alt_name,base_fn))
		os.remove(alt_name)
	else: 
		(figure_held if figure_held else plt).savefig(base_fn,dpi=dpi,bbox_extra_artists=extras,
			bbox_inches='tight' if tight else None,pad_inches=pad_inches if pad_inches else None,format=form)
	plt.close()
	#---add metadata to png
	if form=='png' and meta!=None:
		im = Image.open(base_fn)
		imgmeta = PngImagePlugin.PngInfo()
		imgmeta.add_text('meta',json.dumps(meta))
		im.save(base_fn,form,pnginfo=imgmeta)
	else: print('[WARNING] you are saving as %s and only png allows metadata-versioned pictures'%form)

def picturesave_redacted(*args,**kwargs):
	"""Wrap picturesave with redacted plots."""
	return picturesave_original(*args,redacted=True,**kwargs)

def picturedat(savename,directory='./',bank=False):
	"""
	Read metadata from figures with identical names.
	"""
	directory = os.path.join(directory,'')
	if not bank: 
		if os.path.isfile(directory+savename): 
			try: return json.loads(Image.open(directory+savename).info['meta'])
			except: raise Exception('failed to load metadata from (possibly corrupted) image %s'%(
				directory+savename))
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
    #! this is likely deprecated. see art_ptdins.py for a newer version and not that unique now accepts axis
    #---contiguous array trick
    alt = np.ascontiguousarray(array).view(
        np.dtype((np.void,array.dtype.itemsize*array.shape[1])))
    unique,idx,counts = np.unique(alt,return_index=True,return_counts=True)
    #---sort by count, descending
    idx_sorted = np.argsort(counts)[::-1]
    return idx[idx_sorted],counts[idx_sorted]

def load(name,cwd=None,verbose=False,exclude_slice_source=False,filename=False):
	"""
	Get binary data from a computation.
	"""
	if not cwd: cwd,name = os.path.dirname(name),os.path.basename(name)
	cwd = os.path.abspath(os.path.expanduser(cwd))
	fn = os.path.join(cwd,name)
	if not os.path.isfile(fn): raise Exception('[ERROR] failed to load %s'%fn)
	data = {}
	import h5py
	rawdat = h5py.File(fn,'r')
	for key in [i for i in rawdat if i!='meta']: 
		if verbose:
			print('[READ] '+key)
			print('[READ] object = '+str(rawdat[key]))
		data[key] = np.array(rawdat[key])
	if 'meta' in rawdat: 
		if sys.version_info<(3,0): out_text = rawdat['meta'].value
		else: out_text = rawdat['meta'].value.decode()
		attrs = json.loads(out_text)
	else: 
		print('[WARNING] no meta in this pickle')
		attrs = {}
	if exclude_slice_source:
		for key in ['grofile','trajfile']:
			if key in attrs: del attrs[key]
	for key in attrs: data[key] = attrs[key]
	if filename: data['filename'] = fn
	rawdat.close()
	return data

def store(obj,name,path,attrs=None,print_types=False,verbose=True):
	"""
	Use h5py to store a dictionary of data.
	"""
	import h5py
	#---! cannot do unicode in python 3. needs fixed
	if type(obj) != dict: raise Exception('except: only dictionaries can be stored')
	if os.path.isfile(path+'/'+name): raise Exception('except: file already exists: '+path+'/'+name)
	path = os.path.abspath(os.path.expanduser(path))
	if not os.path.isdir(path): os.mkdir(path)
	fobj = h5py.File(path+'/'+name,'w')
	for key in obj.keys(): 
		if print_types: 
			print('[WRITING] '+key+' type='+str(type(obj[key])))
			print('[WRITING] '+key+' dtype='+str(obj[key].dtype))
		#---python3 cannot do unicode so we double check the type
		#---! the following might be wonky
		if (type(obj[key])==np.ndarray and re.match('^str|^unicode',obj[key].dtype.name) 
			and 'U' in obj[key].dtype.str):
			obj[key] = obj[key].astype('S')
		try: dset = fobj.create_dataset(key,data=obj[key])
		except: 
			#---multidimensional scipy ndarray must be promoted to a proper numpy list
			try: dset = fobj.create_dataset(key,data=obj[key].tolist())
			except: raise Exception("failed to write this object so it's probably not numpy"+
				"\n"+key+' type='+str(type(obj[key]))+' dtype='+str(obj[key].dtype))
	if attrs != None: 
		try: fobj.create_dataset('meta',data=np.string_(json.dumps(attrs)))
		except Exception as e: raise Exception('failed to serialize attributes: %s'%e)
	if verbose: status('[WRITING] '+path+'/'+name)
	fobj.close()