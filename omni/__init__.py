
#__all__ = ['compute']

from ortho import status

from ortho import requires_python_check
from .cli import required_python_packages 
requires_python_check(*required_python_packages)
from .omnicalc import WorkSpace
from .base.store import load,store
from .base.utils import get_automacs,uniquify
from .base.compute_loops import framelooper,basic_compute_loop
from .base.geometry import *
from .base.hooks import HookHandler
from .base.store import picturesave
from .base.utils import subdivide_trajectory
from .plotter.utils import zoom_figure
from .base.utils import PostAccumulator

# track the start time
import time
script_start_time = time.time()

def checktime(): 
	"""Report the time anywhere in the calculation workflow."""
	status('%.2f'%(1./60*(time.time()-script_start_time))+' minutes',tag='time')
