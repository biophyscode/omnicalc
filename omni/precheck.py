#!/usr/bin/env python

"""
Make sure we are in the right environment.
"""

#---IMPORT CHECKER
import sys
from makeface import tracebacker
#---load some common requirements or warn the user. this is the *first* time we source an external dependency
message_env = """Since we could not import yaml, it is extremely likely you are in the wrong environment.
If you are running this omnicalc in a factory, you may need to run `source ../../env/bin/activate py2` from
here. This command will source the python 2.7 environment (MDAnalysis is not ready for python 3) which has
been prepared by the factory setup program (run that with `make setup` from the factory root at ../.. from 	
this directory. We recommend running the source command mentioned above before running your analysis. You
could also run `make set activate_env="../../env/bin/activate py2"` and we will run it automatically before
each `make` command, although this is kind of redundant. You could then return to normal with
`make unset activate_env` to get back to normal. Good luck!"""
message_env_bold = 'failed to import a requirement. are you sure you are in the right environment?'
try: import yaml
except Exception as e:
	import textwrap
	print('\n'.join(['[HINT] %s'%i for i in textwrap.wrap(message_env,width=80)]))
	#---we suppress the flamboyant output if the keyword `set` is in the arguments
	#---...because sometimes you want to use the `set` argument, aliased to `set_config` to 
	#---...update the paths with e.g. activate_env in order to cue up the right environment after
	#---...which time the prechecker will pass
	if 'set' not in sys.argv: 
		tracebacker(e)
		tracebacker(message_env_bold)
		raise Exception(message_env_bold)
	else: print(e+'\n'+message_env_bold)
