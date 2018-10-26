##
# Constants
cache_dir = '/Users/ryan/cache/slingshot'
default_dir = '/Users/ryan/DH/lit/corpus/chadwyck/_txt_chadwyck/Early_English_Prose_Fiction'
#default_dir = '/Users/ryan/DH/lit/corpus/chadwyck/_txt_chadwyck'
default_ext = '.txt'
##

import os,sys,codecs,json,numpy as np,random
from datetime import datetime as dt
from mpi4py import MPI


def get_all_paths_from_folder(rootdir,ext='.txt'):
	for root, subdirs, files in os.walk(rootdir):
		for fn in files:
			if fn.endswith(ext):
				yield os.path.join(root,fn)

def print_path(path):
	print path

def slingshot(rock=None,paths=None,limit=None,path_source=default_dir,path_ext=default_ext,cache_results=False,cache_path=None,shuffle_paths=True):
	if not rock:
		print '!! rock must be a function'
		return
	if not paths:
		if path_source:
			if os.path.isdir(path_source):
				paths=list(get_all_paths_from_folder(path_source,path_ext)) if path_source else None
			elif os.path.exists(path_source):
				with open(path_source) as pf:
					paths=[line.strip() for line in pf]
					paths=[x for x in paths if x]
	if not paths:
		print '!! no paths given or found at %s' % path_source if path_source else ''
		return
	if shuffle_paths:
		random.shuffle(paths)
	paths=paths[:limit]


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
		if cache_results and not os.path.exists(cache_path): os.makedirs(cache_path)

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
		if not i%100: print '>> rank #%s is process #%s of %s paths...' % (rank,i+1,len(paths))

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
import imp,argparse

"""
class ArgParser(argparse.ArgumentParser):
	def error(self, message):
		sys.stderr.write('error: %s\n' % message)
		self.print_help()
		sys.exit(2)
"""

if __name__ == '__main__':
	# parse arguments
	parser = argparse.ArgumentParser()
	parser.add_argument('-sling',help="path to the python or R file of code (ending in .py or .R)")
	parser.add_argument('-rock',help='the name of the function in the code that takes a string filepath')
	parser.add_argument('-pathlist',help='a text file with a path per line')
	parser.add_argument('-path',help='a directory where files ending with -ext EXT will be considered the paths [is recursive]')
	parser.add_argument('-ext',help='the file extension files in -path PATH must have in order to be considered [default = "%s"]' % default_ext)
	parser.add_argument('-limit',help='how many paths to process')
	args = parser.parse_args()

	# load code

	# Load slingshot
	if not args.sling or not args.rock:
		if not args.sling: print "error: -sling SLING must be specified"
		if not args.rock: print "error: -rock ROCK must be specified"
		print
		parser.print_help()
		sys.exit(1)

	# Construct Goliath
	path_ext=None
	if args.pathlist:
		path_source = args.pathlist
	elif args.path:
		path_source = args.path
		path_ext = args.ext if args.ext else default_ext
	else:
		print "error: neither -pathlist PATHLIST nor -path PATH specified"
		parser.print_help()
		sys.exit(1)

	# Other options
	limit = int(args.limit) if args.limit else None

	# Execute!
	sling = imp.load_source('sling', args.sling)
	rock = getattr(sling,args.rock)
	slingshot(rock=rock, path_source=path_source,path_ext=path_ext,limit=limit)
