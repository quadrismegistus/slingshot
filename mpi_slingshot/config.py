###################
### LEAVE THIS LINE
CONFIG={} #########
###################

import os

try:
	from .local import CONFIG
except ImportError:
	# CONSTANTS
	CONFIG['PATH_SLINGS'] = os.path.join(os.path.dirname(__file__),'..','slings')
	CONFIG['PATH_PATHLISTS'] = os.path.join(os.path.dirname(__file__),'..','pathlists')
	CONFIG['SLING_EXT'] = ['py','R']
	CONFIG['KEY_PATH'] = '_path'
	##

ENV_PREFIX='SLINGSHOT_'
for k,v in os.environ.items():
	if k.startswith(ENV_PREFIX):
		key=k[len(ENV_PREFIX):]
		CONFIG[key]=v
