#!/usr/bin/env python

import os

from ortho import Handler

class HookHandler(Handler):
	# when calling this handler, send the kwargs from the compute function
	#   through to the meta kwarg so the hook can access everything in compute
	taxonomy = {'standard':{'import_target','function'}}
	def standard(self,import_target,function):
		from ortho import importer
		# assume import targets are in calcs
		mod = importer(os.path.join('calcs',import_target))
		func = mod[function]
		return func(kwargs=self.meta)
