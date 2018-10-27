import os,sys,codecs,json,numpy as np,random,imp
from datetime import datetime as dt
from mpi4py import MPI

def get_all_paths_from_folder(rootdir,ext='.txt'):
	for root, subdirs, files in os.walk(rootdir):
		for fn in files:
			if fn.endswith(ext):
				yield os.path.join(root,fn)

def slingshot(sling=None,rock=None,paths=None,limit=None,path_source=None,path_ext=None,cache_results=False,cache_path=None,save_results=True,results_dir=None,shuffle_paths=True):
	if not sling or not rock:
		print '!! sling or rock not specified'
		return

	sling = imp.load_source('sling', sling)
	rock = getattr(sling,rock)

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
		#cache_path=os.path.join(cache_dir,rock.__name__)
		if results_dir: cache_path=os.path.join(results_dir,'cache')


	# Start MPI
	t1 = dt.now()
	comm = MPI.COMM_WORLD
	size = comm.Get_size()
	rank = comm.Get_rank()
	print '>> SLINGSHOT: initializing MPI with size %s and rank %s' % (size,rank)


	# Am I the seed process?
	if rank == 0:
		if cache_results and cache_path and not os.path.exists(cache_path): os.makedirs(cache_path)

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
		#if not i%100:
		print '>> rock #%s has knocked down #%s of %s paths...' % (rank,i,len(paths))

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
	if cache_results and cache_path:
		cache_fn = 'results.rank=%s.json' % str(rank).zfill(4)
		cache_fnfn = os.path.join(cache_path,cache_fn)
		with open(cache_fnfn,'wb') as cache_f:
			json.dump(results,cache_f)
			print '>> saved:',cache_fnfn

	RESULTS = comm.gather(results, root=0)

	if rank == 0:
		t3 = dt.now()
		print '>> SLINGSHOT: Finished in %s seconds.' % (t3-t1).total_seconds()
		if save_results and results_dir:
			if not os.path.exists(results_dir): os.makedirs(results_dir)
			results_fn='results.json'
			results_fnfn=os.path.join(results_dir,results_fn)
			with codecs.open(results_fnfn,'wb') as results_f:
				json.dump(RESULTS,results_f)
				print '>> saved:',results_fnfn
		return RESULTS
