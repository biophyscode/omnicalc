#!/usr/bin/env python

"""
Storage functions. Require a workspace in globals so do an import/export.
"""

import os,sys,re,glob,json
import matplotlib as mpl
import matplotlib.pyplot as plt
from base.tools import str_or_list,status
from PIL import Image
from PIL import PngImagePlugin

def picturesave(savename,directory='./',meta=None,extras=[],backup=False,
	dpi=300,form='png',version=False,pdf=False,tight=True,pad_inches=0):
	"""
	Function which saves the global matplotlib figure without overwriting.
	"""
	status('saving picture',tag='store')
	#---intervene here to check the wordspace for picture-saving "hooks" that apply to all new pictures
	################### this was highly stupid: from base.header import work
	if 'picture_hooks' in work.vars:
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
		search = picturefind(savename,directory=directory,meta=meta)
		if not search:
			if meta == None: raise Exception('[ERROR] versioned image saving requires meta')
			fns = glob.glob(os.path.join(directory,savename,'.v*'))
			nums = [int(re.findall('^.+\.v([0-9]+)\.png',fn)[0]) for fn in fns 
				if re.match('^.+\.v[0-9]+\.png',fn)]
			ind = max(nums)+1 if nums != [] else 1
			savename += '.v%d'%ind
		else: savename = re.findall('(.+)\.[a-z]+',os.path.basename(search))[0]
	#---backup if necessary
	savename += '.'+form
	base_fn = os.path.join(directory,savename)
	if os.path.isfile(base_fn) and backup:
		for i in range(1,100):
			latestfile = '.'.join(base_fn.split('.')[:-1])+'.bak'+('%02d'%i)+'.'+base_fn.split('.')[-1]
			if not os.path.isfile(latestfile): break
		if i == 99 and os.path.isfile(latestfile):
			raise Exception('except: too many copies')
		else: 
			status('backing up '+base_fn+' to '+latestfile,tag='store')
			os.rename(base_fn,latestfile)
	#---intervene to use the PDF backend if desired
	#---...this is particularly useful for the hatch-width hack 
	#---...(search self.output(0.1, Op.setlinewidth) in 
	#---...python2.7/site-packages/matplotlib/backends/backend_pdf.py and raise it to e.g. 3.0)
	if pdf:
		alt_name = re.sub('.png$','.pdf',savename)
		fig = plt.gcf()
		fig.savefig(alt_name,dpi=dpi,bbox_extra_artists=extras,
			bbox_inches='tight' if tight else None,pad_inches=pad_inches if pad_inches else None)
		#---convert pdf to png
		os.system('convert -density %d %s %s'%(dpi,alt_name,base_fn))
		os.remove(alt_name)
	else: 
		fig = plt.gcf()
		fig.savefig(base_fn,dpi=dpi,bbox_extra_artists=extras,
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

def picturefind(savename,directory='./',meta=None):
	"""
	Find a picture in the plot repository.
	"""
	status('searching pictures',tag='store')
	regex = '^.+\.v([0-9]+)\.png'
	fns = glob.glob(directory+'/'+savename+'.v*')
	nums = map(lambda y:(y,int(re.findall(regex,y)[0])),filter(lambda x:re.match(regex,x),fns))
	matches = [fn for fn,num in nums if meta==picturedat(os.path.basename(fn),directory=directory)]
	if len(matches)>1 and meta!=None: 
		print('[ERROR] multiple matches found for %s'%savename)
		raise Exception('???')
		import pdb;pdb.set_trace()
	if matches==[] and meta==None:
		return dict([(os.path.basename(fn),
			picturedat(os.path.basename(fn),directory=directory)) for fn,num in nums]) 
	return matches if not matches else matches[0]

def plotload(plotname,specfile=None,choice_override=None,use_group=False):
	"""
	Wrapper around WorkSpace.plotload method for backwards compatibility with plot scripts, which 
	expect to find this function in globals to get data.
	"""
	data,calc = work.plotload(plotname)
	return data,calc

def datmerge(kwargs,name,key,same=False):
	"""
	Incoming upstream data are sometimes taken from multiple pickles.
	This function stitches together the key from many of these pickles.
	"""
	#---if there is only one upstream object with no index we simply lookup the key we want
	if name in kwargs['upstream']: return kwargs['upstream'][name][key]
	else:
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
