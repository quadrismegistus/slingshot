import os,sys,codecs,numpy as np,random,imp,time,random
try:
	import ujson as json
except ImportError:
	import json
from datetime import datetime as dt
import unicodecsv as csv
from mpi4py import MPI
from collections import defaultdict,Counter
from .config import CONFIG
KEY_PATH=CONFIG.get('KEY_PATH','_path')



def slingshot(path_sling=None,stone_name=None,paths=None,limit=None,path_source=None,key_path=KEY_PATH,path_ext=None,cache_results=True,cache_path=None,save_results=True,results_dir=None,shuffle_paths=True,stream_results=True,save_txt=True,txt_maxcols=10000):
	"""
	Main function
	"""

	# Load code-sling and stone-function
	stone=load_stone_in_sling(path_sling,stone_name)

	# Load paths
	all_paths = load_paths(path_source,path_ext,limit,shuffle_paths) if not paths else paths

	# Break if these weren't returned
	if not stone or not all_paths:
		print '!!',[path_sling,stone_name,path_source,path_ext]
		return

	# Save cache dir
	if cache_results and not cache_path:
		if results_dir: cache_path=os.path.join(results_dir,'cache')


	# Start MPI
	t1 = dt.now()
	comm = MPI.COMM_WORLD
	size = comm.Get_size()
	rank = comm.Get_rank()
	print '>> SLINGSHOT: initializing MPI with size %s and rank %s' % (size,rank)


	# Am I the seed process?
	if rank == 0:
		# Make cache folder
		if cache_results and cache_path and not os.path.exists(cache_path): os.makedirs(cache_path)

		# Farm out paths to other processes
		segments = np.array_split(all_paths,size) if size>1 else [all_paths]
		print '>> SLINGSHOT: %s paths divided into %s segments' % (len(all_paths), len(segments))

	# Or am I a process created by the seed?
	else:
		segments = None

	# Scatter the segments (if rank is 0?)
	segment = comm.scatter(segments, root=0)

	# Parse the segment.
	paths = segment

	# cache results?
	cache_writer=None
	if cache_results and cache_path:
		cache_fn = 'results.rank=%s.json' % str(rank).zfill(4)
		cache_fnfn = os.path.join(cache_path,cache_fn)
		cache_writer=JSONStreamWriter(cache_fnfn)

	# let's go! loop over the paths
	results=[]
	num_paths=len(paths)
	pronoun='their'
	zlen=len(str(num_paths))
	zlen_rank=len(str(size))
	for i,path in enumerate(paths):
		#################################################
		# THIS IS WHERE THE STONE FITS INTO THE SLINGSHOT
		result=stone(path)
		if result is not None:
			path_result=(path,result)
			if not stream_results: results+=[path_result]
			if cache_writer: cache_writer.write(path_result)
		#################################################
		print '>> Clone #%s slings %s at #%s of %s %s enemies!' % (str(rank).zfill(zlen_rank),stone_name,str(i+1).zfill(zlen),pronoun,num_paths)
	if cache_writer: cache_writer.close()

	# Gather the results
	RESULTS = comm.gather(results, root=0)

	# If I am the seed process again
	if rank == 0:
		t3 = dt.now()
		print '>> SLINGSHOT: Finished parsing in %s seconds.' % (t3-t1).total_seconds()

		# Save results...
		if save_results and results_dir:
			# Make dir...
			if not os.path.exists(results_dir): os.makedirs(results_dir)

			# Copy pathlist
			results_fnfn_pathlist = os.path.join(results_dir,'pathlist.txt')
			results_fnfn_metadata = os.path.join(results_dir,'metadata.txt')
			save_results_pathlist(results_fnfn_pathlist,results_fnfn_metadata,all_paths,path_source)

			# Save JSON
			results_fnfn_json = os.path.join(results_dir,'results.json')
			save_results_json(results_fnfn_json,cache_results,cache_path,stream_results)

			# Stream-save TSV
			results_fnfn_txt = os.path.join(results_dir,'results.txt')
			if save_txt: save_results_txt(results_fnfn_txt,results_fnfn_json,txt_maxcols)

		# Exit
		t4 = dt.now()
		print '>> SLINGSHOT: Finished everything in %s seconds!' % (t4-t1).total_seconds()




def save_results_pathlist(results_fnfn_pathlist,results_fnfn_metadata,paths,path_source):
	with open(results_fnfn_pathlist,'w') as of:
		for path in paths:
			of.write(path+'\n')

	path_pathlists = CONFIG.get('PATH_PATHLISTS','')
	pathlist_path_source = os.path.join(path_pathlists,path_source)

	if os.path.exists(path_source) and is_csv(path_source):
		from shutil import copyfile
		copyfile(path_source, results_fnfn_metadata)
	elif os.path.exists(pathlist_path_source) and is_csv(path_source):
		from shutil import copyfile
		copyfile(path_source, results_fnfn_metadata)





def load_stone_in_sling(path_sling,stone_name):
	if not path_sling or not stone_name:
		print '!! sling or stone not specified'
		return
	if not os.path.exists(path_sling):
		in_PATH_STRINGS=os.path.join(CONFIG['PATH_SLINGS'],path_sling)
		if not os.path.exists(in_PATH_STRINGS):
			print "!!",path,"does not exist"
			return
		path_sling=in_PATH_STRINGS
	if path_sling.endswith('.py'):
		sling = imp.load_source('sling', path_sling)
		stone = getattr(sling,stone_name)
		return stone

	if path_sling.endswith('.R'):
		from rpy2.robjects import r as R
		# load all source
		with open(path_sling) as f:
			code=f.read()
			R(code)
			stone = lambda x: rconvert(R[stone_name](x))
			return stone


def get_all_paths_from_folder(rootdir,ext='.txt'):
	for root, subdirs, files in os.walk(rootdir):
		for fn in files:
			if fn.endswith(ext):
				yield os.path.join(root,fn)




def get_paths_from_csv(_fnfn,key_path=KEY_PATH,sep='\t'):
	paths=[]
	#with codecs.open(_fnfn,encoding='utf-8') as pf:
	with open(_fnfn) as pf:
		reader=csv.DictReader(pf,delimiter=sep)
		for dx in reader:
			path=dx.get(key_path,'')
			if path: paths+=[path]
	return paths

def is_csv(_fnfn,sep='\t'):
	if not os.path.exists(_fnfn): return False
	if os.path.isdir(_fnfn): return False
	with open(_fnfn) as pf:
		first_line=pf.readline()
		return sep in first_line

def get_paths_from_pathlist(_fnfn,sep='\t',key_path=KEY_PATH):
		if is_csv(_fnfn,sep=sep):
			return get_paths_from_csv(_fnfn,key_path=key_path,sep=sep)
		else:
			with open(_fnfn) as pf:
				paths=[line.strip() for line in pf]
				paths=[x for x in paths if x]
				return paths



def load_paths(path_source,path_ext,limit,shuffle_paths,key_path=KEY_PATH):
	if path_source:
		if os.path.isdir(path_source):
			paths=list(get_all_paths_from_folder(path_source,path_ext)) if path_source else None
		elif os.path.exists(path_source):
			paths=get_paths_from_pathlist(path_source,key_path=key_path)
		else:
			path_pathlists = CONFIG.get('PATH_PATHLISTS','')
			pathlist_path_source = os.path.join(path_pathlists,path_source)
			if os.path.exists(pathlist_path_source):
				paths=get_paths(pathlist_path_source)
	if not paths:
		print '!! no paths given or found at %s' % path_source if path_source else ''
		return
	paths=paths[:limit]
	if shuffle_paths:
		random.shuffle(paths)
	return paths


def save_results_json(results_fnfn,cache_results,cache_path,stream_results):
	if cache_results and cache_path and stream_results:
		with codecs.open(results_fnfn,'w',encoding='utf-8') as results_f:
			results_f.write('[\n')
			for fn_c in sorted(os.listdir(cache_path)):
				if not fn_c.endswith('.json'): continue
				fnfn_c=os.path.join(cache_path,fn_c)
				with codecs.open(fnfn_c,encoding='utf-8') as f_c:
					for line in f_c:
						if len(line)<3: continue
						results_f.write(line.strip()+'\n')
					#results_f.seek(-2,1)
					results_f.write(',\n')
				# delete cache file
				os.unlink(fnfn_c)
			results_f.seek(-3,1)
			results_f.write('\n]\n\n')
	else:
		with codecs.open(results_fnfn,'wb') as results_f:
			json.dump(RESULTS,results_f)
	print '>> saved:',results_fnfn



def save_results_txt(results_fnfn_txt,results_fnfn_json,txt_maxcols):
	now=time.time()

	# First find KEYS
	KEYS=set()
	if txt_maxcols: Count=Counter()
	for path,result in iterload(results_fnfn_json):
		if hasattr(result,'keys'):
			if txt_maxcols:
				Count.update(result.keys())
			else:
				KEYS=set(result.keys())
	if txt_maxcols: KEYS={x for x,y in Count.most_common(txt_maxcols)}

	then,now=now,time.time()
	print '>> save_txt: found keys in %ss' % int(now-then)
	#print KEYS

	if KEYS:
		# Then loop again to write
		header=['_path']+sorted([unicode(x) for x in KEYS])
		with codecs.open(results_fnfn_txt,'w',encoding='utf-8') as results_f_txt:
			results_f_txt.write('\t'.join(header) + '\n')
			for path,result in iterload(results_fnfn_json):
				result['_path']=path
				orow=[unicode(result.get(h,'')) for h in header]
				results_f_txt.write('\t'.join(orow) + '\n')
			print '>> saved:',results_fnfn_txt
			then,now=now,time.time()
			print '>> save_txt: saved in %ss' % int(now-then)





class JSONStreamWriter(object):
	def __init__(self,filename,encoding='utf-8'):
		self.file=codecs.open(filename,'w',encoding=encoding)
		self.file.write('[\n')

	def write(self,obj):
		oline=json.dumps(obj)
		oline=oline.replace('\n','\\n').replace('\r','\\r').replace('\t','\\t')
		self.file.write(oline+',\n')

	def close(self):
		self.file.seek(-2, 1)
		self.file.write('\n]\n')
		self.file.close()

def iterload(filename,encoding='utf-8'):
	with codecs.open(filename,'r',encoding=encoding) as f:
		for i,line in enumerate(f):
			line = line[:-2] if line[-2:]==',\n' else line
			try:
				x=json.loads(line)
				yield x
			except ValueError as e:
				pass



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
