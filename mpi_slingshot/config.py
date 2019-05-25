from __future__ import absolute_import
###################
### LEAVE THIS LINE
CONFIG={} #########
###################

import os

try:
	from .local import CONFIG
except ImportError:
	# CONSTANTS
	CONFIG['PATH_SLINGS'] = os.path.abspath(os.path.join(os.path.dirname(__file__),'..','slings'))
	CONFIG['PATH_PATHLISTS'] = os.path.abspath(os.path.join(os.path.dirname(__file__),'..','pathlists'))
	CONFIG['SLING_EXT'] = ['py','R','ipynb']
	CONFIG['PATH_KEY'] = '_path'
	CONFIG['PATH_EXT']='txt'
	CONFIG['NBIMPORTER_ONLY_DEFS'] = False
	##

ENV_PREFIX='SLINGSHOT_'
for k,v in list(os.environ.items()):
	if k.startswith(ENV_PREFIX):
		key=k[len(ENV_PREFIX):]
		CONFIG[key]=v
