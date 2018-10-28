import os,sys,codecs,json,numpy as np,random,imp

try:
	import ujson as json
except ImportError:
	import json

import unicodecsv as csv
from datetime import datetime as dt
from mpi4py import MPI
import slingshot_config
CONFIG={} if not slingshot_config.CONFIG else slingshot_config.CONFIG

def get_all_paths_from_folder(rootdir,ext='.txt'):
	for root, subdirs, files in os.walk(rootdir):
		for fn in files:
			if fn.endswith(ext):
				yield os.path.join(root,fn)

def rconvert(robj):
	import rpy2
	#if type(robj) == rpy2.robjects.vectors.ListVector:
	try:
		return { key : robj.rx2(key)[0] for key in robj.names }
	except AttributeError:
		print "!! forcing conversion for:",robj
		from rpy2.robjects import pandas2ri
		pandas2ri.activate()
		return pandas2ri.ri2py(robj)

def load_stone_in_sling(path_sling,stone_name):
	if not os.path.exists(path_sling):
		print "!!",path,"does not exist"
		return
	if path_sling.endswith('.py'):
		sling = imp.load_source('sling', path_sling)
		stone = getattr(sling,stone_name)
		return stone

	if path_sling.endswith('.R'):
		from rpy2.robjects import r as R
		#from rpy2.robjects import pandas2ri
		#pandas2ri.activate()
		# load all source
		with open(path_sling) as f:
			code=f.read()
			R(code)
			#stone = lambda x: pandas2ri.ri2py(R[stone_name](x))
			stone = lambda x: rconvert(R[stone_name](x))
			return stone

def slingshot(sling=None,stone=None,paths=None,limit=None,path_source=None,path_ext=None,cache_results=False,cache_path=None,save_results=True,results_dir=None,shuffle_paths=True,stream_results=True,save_txt=True):
	if not sling or not stone:
		print '!! sling or stone not specified'
		return

	stone=load_stone_in_sling(sling,stone)

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
	paths=paths[:limit]
	if shuffle_paths:
		random.shuffle(paths)



	if cache_results and not cache_path:
		#cache_path=os.path.join(cache_dir,stone.__name__)
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
	# This method is by path, we ask the stone to slingshot each path
	# cache results?
	writer=None
	if cache_results and cache_path:
		cache_fn = 'results.rank=%s.json' % str(rank).zfill(4)
		cache_fnfn = os.path.join(cache_path,cache_fn)
		#with open(cache_fnfn,'wb') as cache_f:
		#	json.dump(results,cache_f)
		#	print '>> saved:',cache_fnfn

		#writer=JSONAutoArray.ArrayWriter(f)
		writer=JSONStreamWriter(cache_fnfn)



	results=[]
	for i,path in enumerate(paths):
		#print 'DOING SOMETHING:',i,path
		#if not i%100:
		print '>> stone #%s has knocked down #%s of %s paths...' % (rank,i,len(paths))

		#################################################
		# THIS IS WHERE THE stone FITS INTO THE SLINGSHOT
		result=stone(path)
		if result is not None:
			path_result=(path,result)
			if not stream_results: results+=[path_result]
			#print ">> RESULT FOR PATH '%s': %s" % (path,result)
			if writer: writer.write(path_result)
		#################################################
	#"""
	if writer: writer.close()

	# This method we slingshot the stone at the list of paths and ask it to store the results
	#results=stone(paths)



	RESULTS = comm.gather(results, root=0)


	def stream_cached_jsons(cache_path=cache_path):
		for fn in os.listdir(cache_path):
			if not fn.endswith('.json'): continue
			if obj in iterload(os.path.join(cache_path,fn)):
				yield obj

	if rank == 0:
		t3 = dt.now()
		print '>> SLINGSHOT: Finished in %s seconds.' % (t3-t1).total_seconds()
		if save_results and results_dir:

			if not os.path.exists(results_dir): os.makedirs(results_dir)

			# Save JSON
			results_fn='results.json'
			results_fnfn=os.path.join(results_dir,results_fn)
			if not stream_results:
				with codecs.open(results_fnfn,'wb') as results_f:
					json.dump(RESULTS,results_f)
					print '>> saved:',results_fnfn
			else:
				# Stream JSON write
				Writer=JSONAutoArray.ArrayWriter(results_fnfn)
				for obj in stream_cached_jsons():
					Writer.write(obj)
				Writer.close()

			# Stream-save TSV
			if save_txt:
				# First find KEYS
				KEYS=set()
				for path,result in iterload(results_fnfn):
					if hasattr(result,'keys'):
						KEYS|=set(result.keys())

				# Then loop again to write
				header=['_path']+sorted(list(KEYS))
				results_fnfn_txt=os.path.join(results_dir,'results.txt')
				with open(results_fnfn_txt,'wb') as results_f_txt:
					writer = csv.DictWriter(results_f_txt,delimiter='\t',fieldnames=header)
					writer.writeheader()
					for obj in
							pathd['_path']=path
							writer.writerow(pathd)
							LD+=[pathd]
					print '>> saved:',results_fnfn_txt

		return LD


class JSONStreamWriter(object):
	def __init__(self,filename,encoding='utf-8'):
		self.file=codecs.open(filename,'w',encoding=encoding)

	def write(self,obj):
		oline=json.dumps(obj)
		oline=oline.replace('\n','\\n').replace('\r','\\r').replace('\t','\\t')
		self.file.write(oline+'\n')

	def close(self):
		self.file.close()

def iterload(filename):
	with codecs.open(filename,'r',encoding=encoding) as f:
		for line in f:
			yield json.loads(line)
