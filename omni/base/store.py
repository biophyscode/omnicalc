#!/usr/bin/env python

from __future__ import print_function
import os,sys,json,re,glob
import numpy as np
from ortho import requires_python,status,compare_dicts

@requires_python('h5py')
def load(name,cwd=None,verbose=False,exclude_slice_source=False,filename=False):
	"""
	Get binary data from a computation.
	"""
	if not cwd: cwd,name = os.path.dirname(name),os.path.basename(name)
	cwd = os.path.abspath(os.path.expanduser(cwd))
	fn = os.path.join(cwd,name)
	if not os.path.isfile(fn): raise Exception('failed to load %s'%fn)
	data = {}
	import h5py
	try: rawdat = h5py.File(fn,'r')
	except:
		print('error failed to read %s'%fn)
		raise
	for key in [i for i in rawdat if i!='meta']: 
		if verbose:
			print('status','read '+key)
			print('status','object = '+str(rawdat[key]))
		data[key] = np.array(rawdat[key])
		# python 3 convert bytes to strings
		if not sys.version_info<(3,0) and data[key].dtype.kind=='S':
			data[key] = np.array(data[key]).astype(str)
		#! note that we are still getting 'O' objects in python 3
		#!   and this means everything has to be converted back to strings
		#!   when they come back in. it's easy to do this in the analysis 
		#!   scripts however it is an unfortunate hassle
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

@requires_python('h5py')
def store(obj,name,path,attrs=None,print_types=False,verbose=True):
	"""
	Use h5py to store a dictionary of data.
	"""
	import h5py
	#! cannot do unicode in python 3. needs fixed
	if type(obj) != dict: 
		raise Exception('except: only dictionaries can be stored')
	if os.path.isfile(path+'/'+name): 
		raise Exception('except: file already exists: '+path+'/'+name)
	path = os.path.abspath(os.path.expanduser(path))
	if not os.path.isdir(path): os.mkdir(path)
	fobj = h5py.File(path+'/'+name,'w')
	for key in obj.keys(): 
		if print_types: 
			print('[WRITING] '+key+' type='+str(type(obj[key])))
			print('[WRITING] '+key+' dtype='+str(obj[key].dtype))
		# removed type checking for ndarray and dtype.name with U however
		#   recent vintage numpy in python 3 returns an "object" dtype
		#   which is usually a string. fixed with a try to convert to string
		# note that we do try-continue-except to handle the strings and lists
		try:
			# most objects are converted here
			fobj.create_dataset(key,data=obj[key])
			continue
		except: pass
		try:
			# strings should be saved with special type from bytes
			fobj.create_dataset(key,data=obj[key],dtype=h5py.special_dtype(vlen=bytes))
			continue
		except: pass
		try:
			# if you have mixed integers and strings, we need to recast as bytes
			fobj.create_dataset(key,data=obj[key].astype(bytes),dtype=h5py.special_dtype(vlen=str))
			continue
		except: pass
		try:
			# multidimensional scipy ndarray must be promoted to a numpy list
			fobj.create_dataset(key,data=obj[key].tolist())
			continue
		except: pass
		# failures above cause an exception
		import ipdb;ipdb.set_trace()
		raise Exception(
			"failed to write this object so it's probably not numpy"+
			"\n"+key+' type='+str(type(obj[key]))+
			' dtype='+str(obj[key].dtype))
	if attrs != None: 
		try: fobj.create_dataset('meta',data=np.string_(json.dumps(attrs)))
		except Exception as e: 
			raise Exception('failed to serialize attributes: %s'%e)
	if verbose: print('status','writing'+path+'/'+name)
	fobj.close()

def picturedat(savename,directory='./',bank=False):
	"""
	Read metadata from figures with identical names.
	"""
	# previously we did import PIL as Image but perhaps a version change
	from PIL import Image
	directory = os.path.join(directory,'')
	if not bank: 
		if os.path.isfile(directory+savename): 
			try: 
				return json.loads(Image.open(directory+savename).info['meta'])
			except: 
				raise Exception('failed to load metadata from (possibly corrupted) image %s'%(
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

def picturefind(savename,directory='./',meta=None,loud=True):
	"""
	Find a picture in the plot repository.
	"""
	if loud: status('searching pictures',tag='store')
	regex = r'^.+\.v([0-9]+)\.png'
	fns = glob.glob(directory+'/'+savename+'.v*')
	nums = map(lambda y:(y,int(re.findall(regex,y)[0])),filter(lambda x:re.match(regex,x),fns))
	#! using unicode-to-string compare_dicts function from historical version but need to confirm necessary
	matches = [fn for fn,num in nums if 
		compare_dicts(meta,picturedat(os.path.basename(fn),directory=directory))]
	if len(matches)>1 and meta!=None: 
		print('[ERROR] multiple matches found for %s'%savename)
		raise Exception('???')
	if matches==[] and meta==None:
		return dict([(os.path.basename(fn),
			picturedat(os.path.basename(fn),directory=directory)) for fn,num in nums]) 
	return matches if not matches else matches[0]

@requires_python('PIL')
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
		color_back = work.metadata.director.get('redacted_background_color','')
		color_fore = work.metadata.director.get('redacted_foreground_color','k')
		if 'redacted_scrambler' in work.metadata.director:
			scrambler_code = work.metadata.director['redacted_scrambler']
			try: 
				scrambler = eval(scrambler_code)
				scrambler('test text')
			except: raise Exception(
				'failed to evaluate your `redacted_scrambler` from the director: `%s`'%scrambler_code)
		else: 
			#! method below is deprecated because it looks silly. best to use hashes
			if False: scrambler = lambda x,max_len=12:''.join([
				chr(ord('a')+random.randint(0,25)) for i in x][:max_len])
			scrambler = lambda x,max_len=10:('#'*len(x))[:max_len]
		num_format = re.compile("^[\-]?[1-9][0-9]*\.?[0-9]+$")
		isnumber = lambda x:re.match(num_format,x)
		for obj in [i for i in plt.findobj() if type(i)==mpl.text.Text]:
		    text_this = obj.get_text()
		    if text_this!='' and not isnumber(text_this):
		        obj.set_text(scrambler(text_this))
		        if color_back: obj.set_backgroundcolor(color_back)
		        obj.set_color(color_fore)
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
		from PIL import Image
		from PIL import PngImagePlugin
		im = Image.open(base_fn)
		imgmeta = PngImagePlugin.PngInfo()
		imgmeta.add_text('meta',json.dumps(meta))
		im.save(base_fn,form,pnginfo=imgmeta)
	else: print('[WARNING] you are saving as %s and only png allows metadata-versioned pictures'%form)

def picturesave_redacted(*args,**kwargs):
	"""Wrap picturesave with redacted plots."""
	return picturesave(*args,redacted=True,**kwargs)