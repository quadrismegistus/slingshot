import os,codecs,numpy as np
from datetime import datetime as dt
from mpi4py import MPI

def get_all_paths_from_folder(rootdir,ext='.txt'):
	for root, subdirs, files in os.walk(rootdir):
		for fn in files:
			if fn.endswith('.txt'):
				yield os.path.join(root,fn)

def print_path(path):
	print path

def count_words(path):
	with codecs.open(path,encoding='utf-8') as f:
		count=len(f.read().split())
	return {'data1':10, 'data2':count}

def booknlp(path):
	import os
	basename=os.path.basename(path)
	os.system('cd /Users/ryan/DH/charspace/book-nlp && ./runjava \
			novels/BookNLP -doc %s \
			-printHTML -p /Users/ryan/DH/charspace/output/%s \
			-tok /Users/ryan/DH/charspace/output/%s/%s.tokens -f' %  (path, basename, basename, basename) )

def slingshot(rock=count_words,limit=None):
	#rootdir = '/home/heuser/oak/corpora/new/_txt_chadwyck/Early_English_Prose_Fiction/ee50010.01.txt'
	#rootdir = '/Users/ryan/DH/lit/corpus/chadwyck/_txt_chadwyck/Early_English_Prose_Fiction'
	rootdir = '/Users/ryan/DH/lit/corpus/chadwyck/_txt_chadwyck'

	t1 = dt.now()
	comm = MPI.COMM_WORLD
	size = comm.Get_size()
	rank = comm.Get_rank()

	#
	print '>> SLINGSHOT: initializing MPI with size %s and rank %s' % (size,rank)


	if rank == 0:
		paths = list(get_all_paths_from_folder(rootdir))[:limit]
		segments = np.array_split(paths,size) if size>1 else [paths]
		print '>> SLINGSHOT: %s paths divided into %s segments' % (len(paths), len(segments))
		t2 = dt.now()
		print '>> SLINGSHOT: %s seconds have passed...' % (t2-t1).total_seconds()
	else:
		segments = None

	# Scatter the segments (if rank is 0?)
	segment = comm.scatter(segments, root=0)

	# Parse the segment.
	paths = segment
	#print rank, len(paths), paths[0]

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

	RESULTS = comm.gather(results, root=0)

	if rank == 0:
		t3 = dt.now()
		print '>> SLINGSHOT: Finished in %s seconds.' % (t3-t1).total_seconds()
		print RESULTS
		return RESULTS
