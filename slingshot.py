##
# Constants
cache_dir = '/Users/ryan/cache/slingshot'
default_dir = '/Users/ryan/DH/lit/corpus/chadwyck/_txt_chadwyck/Early_English_Prose_Fiction'
#default_dir = '/Users/ryan/DH/lit/corpus/chadwyck/_txt_chadwyck'
default_ext = '.txt'
##

import os,sys,codecs,json,numpy as np
from datetime import datetime as dt
from mpi4py import MPI


def get_all_paths_from_folder(rootdir,ext='.txt'):
	for root, subdirs, files in os.walk(rootdir):
		for fn in files:
			if fn.endswith(ext):
				yield os.path.join(root,fn)

def print_path(path):
	print path

def slingshot(rock=None,paths=None,limit=None,path_source=default_dir,path_ext=default_ext,cache_results=True,cache_path=None):
	if not rock:
		print '!! rock must be a function'
		return
	if not paths:
		paths=list(get_all_paths_from_folder(path_source,path_ext))[:limit] if path_source else None
		if not paths:
			print '!! no paths given or found at %s' % path_source if path_source else ''

	if cache_results and not cache_path:
		cache_path=os.path.join(cache_dir,rock.__name__)

	# Start MPI
	t1 = dt.now()
	comm = MPI.COMM_WORLD
	size = comm.Get_size()
	rank = comm.Get_rank()
	print '>> SLINGSHOT: initializing MPI with size %s and rank %s' % (size,rank)


	# Am I the seed process?
	if rank == 0:
		if not os.path.exists(cache_path): os.makedirs(cache_path)

		segments = np.array_split(paths,size) if size>1 else [paths]
		print '>> SLINGSHOT: %s paths divided into %s segments' % (len(paths), len(segments))
		t2 = dt.now()
		print '>> SLINGSHOT: %s seconds have passed...' % (t2-t1).total_seconds()
	# Or am I a process created by the seed?
	else:
		segments = None

	# Scatter the segments (if rank is 0?)
	segment = comm.scatter(segments, root=0)

	# Parse the segment.
	paths = segment
	#print rank, len(paths), paths[0]


	#"""
	# This method is by path, we ask the rock to slingshot each path
	results={}
	for i,path in enumerate(paths):
		#print 'DOING SOMETHING:',i,path

		#################################################
		# THIS IS WHERE THE ROCK FITS INTO THE SLINGSHOT
		result=rock(path)
		if result is not None:
			#results+=[(path,result)]
			results[path]=result
		#################################################
	#"""

	# This method we slingshot the rock at the list of paths and ask it to store the results
	#results=rock(paths)

	# cache results?
	if cache_results:
		cache_fn = 'results.rank=%s.json' % str(rank).zfill(4)
		cache_fnfn = os.path.join(cache_path,cache_fn)
		with open(cache_fnfn,'wb') as cache_f:
			json.dump(results,cache_f)
			print '>> saved:',cache_fnfn

	RESULTS = comm.gather(results, root=0)

	if rank == 0:
		t3 = dt.now()
		print '>> SLINGSHOT: Finished in %s seconds.' % (t3-t1).total_seconds()
		print RESULTS
		return RESULTS


# etc
def now(now=None,seconds=True):
	import datetime as dt
	if not now:
		now=dt.datetime.now()
	elif type(now) in [int,float,str]:
		now=dt.datetime.fromtimestamp(now)

	return '{0}{1}{2}-{3}{4}{5}'.format(now.year,str(now.month).zfill(2),str(now.day).zfill(2),str(now.hour).zfill(2),str(now.minute).zfill(2),'-'+str(now.second).zfill(2) if seconds else '')
###












#### MAIN EVENT ###

if __name__ == '__main__':
	args=sys.argv
	rock_function=None
	paths=None
	num_args=len(args)
	if num_args<2:
		print '!! Usage: [mpiexec -n numprocs] python [-m mpi4py] slingshot.py ROCK_FILENAME'
		exit()

	### ROCK
	rock_filename = sys.argv[1]
	if not os.path.exists(rock_filename):
		print '!! Nothing exists at %s' % rock_filename
		exit()

	# Python rocks
	if rock_filename.endswith('.py'):
		import imp
		rock_module = imp.load_source('rock', rock_filename)
		rock_function = rock_module.rock_function

	# R rocks
	if rock_filename.endswith('.R'):
		# @TODO
		pass

	if not rock_function:
		print '!! No rock function loaded'
		exit()
	###


	### GOLIATH
	path_source = args[2] if num_args>2 and args[2] and os.path.exists(args[2]) and os.path.isdir(args[2]) else None
	path_ext = args[3] if num_args>3 and args[3] else None
	###


	## THROW!
	# Throw the slingshot against the goliath
	slingshot(rock=rock_function,path_source=path_source,path_ext=path_ext)

	
