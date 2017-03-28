#!/usr/bin/env python

"""
Look at the workspace and possibly run a method.
"""

exec(open('omni/base/pythonrc.py').read())

import os,sys,re

#---we set the file name so tracebacks make sense
__file__,method = sys.argv[:2]

#---this script is called from root and expects omni to be present
if not os.path.isdir('omni'): raise Exception('cannot find `omni` folder')
sys.path.insert(0,'calcs')
sys.path.insert(0,'omni')
from omnicalc import WorkSpace

work = WorkSpace()

if method!='null' and not hasattr(work,method): 
	raise Exception('no WorkSpace method called %s'%method)
elif method!='null' and hasattr(work,method): 
	getattr(work,method)()

