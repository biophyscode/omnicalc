#!/usr/bin/python

"""
Track the elapsed time across submodules.
"""

from base.tools import status
import time
script_start_time = time.time()
def checktime(): status('%.2f'%(1./60*(time.time()-script_start_time))+' minutes',tag='time')
