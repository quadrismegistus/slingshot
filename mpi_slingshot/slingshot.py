import os,sys,codecs,numpy as np,random,imp,time,random
import jsonlines
import ujson as json
from datetime import datetime as dt
import unicodecsv as csv
from collections import defaultdict,Counter
from .config import CONFIG
DEFAULT_PATH_KEY='_path'
DEFAULT_EXT = 'txt'

PATH_KEY=CONFIG.get('PATH_KEY','')
PATH_EXT=CONFIG.get('PATH_EXT','').replace('.','')

if not PATH_KEY: PATH_KEY=DEFAULT_PATH_KEY
if not PATH_EXT: PATH_EXT=DEFAULT_PATH_EXT


def slingshot(path_sling=None,stone_name=None,paths=None,limit=None,path_source=None,stone=None,path_key=PATH_KEY,path_ext=None,path_prefix='',path_suffix='',cache_results=True,cache_path=None,save_results=True,results_dir=None,shuffle_paths=True,stream_results=True,save_txt=True,txt_maxcols=10000,sling_args=[],sling_kwargs={},num_runs=1):
	"""
	Main function
	"""

	# Load code-sling and stone-function
	if not stone:
		stone=load_stone_in_sling(path_sling,stone_name)

	# Load paths
	all_paths = load_paths(path_source,path_ext,limit,shuffle_paths,path_key,path_prefix,path_suffix) if not paths else paths

	# Multiply paths by runs
	all_paths = [(path,run+1) for path in all_paths for run in range(num_runs)]

	# Break if these weren't returned
	if not stone or not all_paths:
		#print '!!',[path_sling,stone_name,path_source,path_ext]
		return

	# Save cache dir
	if cache_results and not cache_path:
		if results_dir: cache_path=os.path.join(results_dir,'cache')


	# Start MPI
	from mpi4py import MPI
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
		cache_fn = 'results.rank=%s.jsonl' % str(rank).zfill(4)
		cache_fnfn = os.path.join(cache_path,cache_fn)
		cache_writer=codecs.open(cache_fnfn, mode='w',encoding='utf-8')



	# let's go! loop over the paths
	results=[]
	num_paths=len(paths)
	pronoun='their'
	zlen=len(str(num_paths))
	zlen_rank=len(str(size))

	for i,(path,run) in enumerate(paths):
		#################################################
		# THIS IS WHERE THE STONE FITS INTO THE SLINGSHOT

		sling_kwargs2=dict(sling_kwargs.items())
		sling_kwargs2['results_dir']=results_dir
		if num_runs>1: sling_kwargs2['run']=run

		try:
			result=stone(path,*sling_args,**sling_kwargs2)
		except TypeError:
			result=stone(path,*sling_args,**sling_kwargs)

		if result is not None:
			path_result=(path,result)
			if not stream_results: results+=[path_result]
			#if cache_writer:
			if cache_writer:
				#cache_writer.write(path_result) # when using jsonlines
				#try:
				jsonl=json.dumps(path_result)
				cache_writer.write(jsonl + '\n')
				#except:
				#	print "!! could not write to results file!"
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
			results_fnfn_json = os.path.join(results_dir,'results.jsonl')
			save_results_json(results_fnfn_json,cache_results,cache_path,stream_results)

			# Stream-save TSV
			results_fnfn_txt = os.path.join(results_dir,'results.txt')
			if save_txt: save_results_txt(results_fnfn_txt,results_fnfn_json,txt_maxcols)

		# Exit
		t4 = dt.now()
		print '>> SLINGSHOT: Finished everything in %s seconds!' % (t4-t1).total_seconds()




def save_results_pathlist(results_fnfn_pathlist,results_fnfn_metadata,paths,path_source):
	with open(results_fnfn_pathlist,'w') as of:
		for (path,run) in paths:
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
		from rpy2.robjects import r as Rno
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




def get_paths_from_csv(_fnfn,path_key=PATH_KEY,path_ext=PATH_EXT,path_prefix='',path_suffix='',sep='\t'):
	paths=[]
	#with codecs.open(_fnfn,encoding='utf-8') as pf:
	if not path_key: path_key=DEFAULT_PATH_KEY
	with open(_fnfn) as pf:
		reader=csv.DictReader(pf,delimiter=sep)
		for dx in reader:
			path=dx.get(path_key,'')
			if not path: continue
			if path_prefix: path=os.path.join(path_prefix,path)
			if path_suffix: path=path+path_suffix
			#path_from_fnfn = os.path.join(os.path.dirname(_fnfn),path)
			#path_from_fnfn_plus_ext = '.'.join(path_from_fnfn.split('.')+[path_ext])
			#path_exists=os.path.exists(path)
			#print [path,path_from_fnfn,path_from_fnfn_plus_ext]
			# if not path_exists:
			# 	if os.path.exists(path_from_fnfn):
			# 		path=os.path.abspath(path_from_fnfn)
			# 	elif os.path.exists(path_from_fnfn_plus_ext):
			# 		path=os.path.abspath(path_from_fnfn_plus_ext)
			# 	else:
			# 		continue
			if path: paths+=[path]
	return paths

def is_csv(_fnfn,sep='\t'):
	if not os.path.exists(_fnfn): return False
	if os.path.isdir(_fnfn): return False
	with open(_fnfn) as pf:
		first_line=pf.readline()
		return sep in first_line

def get_paths_from_pathlist(_fnfn,sep='\t',path_key=PATH_KEY,path_prefix='',path_suffix=''):
	_fnfn_is_csv=is_csv(_fnfn,sep=sep)
	if _fnfn_is_csv:
		return get_paths_from_csv(_fnfn,path_key=path_key,sep=sep,path_prefix=path_prefix,path_suffix=path_suffix)
	else:
		with open(_fnfn) as pf:
			paths=[line.strip() for line in pf]
			paths=[x for x in paths if x]
			if path_prefix: paths=[os.path.join(path_prefix,x) for x in paths]
			if path_suffix: paths=[path+path_suffix for x in paths]
			return paths



def load_paths(path_source,path_ext,limit,shuffle_paths,path_key=PATH_KEY,path_prefix='',path_suffix=''):
	if path_source:
		if os.path.isdir(path_source):
			paths=list(get_all_paths_from_folder(path_source,path_ext)) if path_source else None
		elif os.path.exists(path_source):
			paths=get_paths_from_pathlist(path_source,path_key=path_key,path_prefix=path_prefix,path_suffix=path_suffix)
		else:
			path_pathlists = CONFIG.get('PATH_PATHLISTS','')
			pathlist_path_source = os.path.join(path_pathlists,path_source)
			if os.path.exists(pathlist_path_source):
				paths=get_paths(pathlist_path_source)
	paths=paths[:limit]
	#paths=[p for p in paths if os.path.exists(p)]
	if not paths:
		print '!! no paths given or found at %s' % path_source if path_source else ''
		return
	if shuffle_paths:
		random.shuffle(paths)
	return paths





def save_results_jsonl(results_fnfn,cache_results,cache_path,stream_results):
	if cache_results and cache_path and stream_results:
		with codecs.open(results_fn, 'w', encoding='utf-8') as of:
			for fn_c in sorted(os.listdir(cache_path)):
				if not fn_c.endswith('.jsonl'): continue
				fnfn_c=os.path.join(cache_path,fn_c)
				with codecs.open(fnfn_c,encoding='utf-8') as f_c:
					for line in f_c:
						of.write(line)


def save_results_json(results_fnfn,cache_results,cache_path,stream_results):
	if cache_results and cache_path and stream_results:
		with codecs.open(results_fnfn,'w',encoding='utf-8') as results_f:
			results_f.write('[\n')
			for fn_c in sorted(os.listdir(cache_path)):
				if not fn_c.endswith('.jsonl'): continue
				fnfn_c=os.path.join(cache_path,fn_c)
				with codecs.open(fnfn_c,encoding='utf-8') as f_c:
					for line in f_c:
						if len(line)<3: continue
						results_f.write(line.strip()+'\n')
	else:
		with codecs.open(results_fnfn,'wb') as results_f:
			json.dump(RESULTS,results_f)
	print '>> saved:',results_fnfn

def save_results_json_v1(results_fnfn,cache_results,cache_path,stream_results):
	if cache_results and cache_path and stream_results:
		with codecs.open(results_fnfn,'w',encoding='utf-8') as results_f:
			results_f.write('[\n')
			for fn_c in sorted(os.listdir(cache_path)):
				if not fn_c.endswith('.jsonl'): continue
				fnfn_c=os.path.join(cache_path,fn_c)
				with codecs.open(fnfn_c,encoding='utf-8') as f_c:
					for line in f_c:
						if len(line)<3: continue
						results_f.write(line.strip()+'\n')
					#results_f.seek(-2,1)
					results_f.write(',\n')
				# delete cache file
				#os.unlink(fnfn_c)
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
	with jsonlines.open(results_fnfn_json) as reader:
		for path,result in reader.iter(skip_invalid=True):
			if hasattr(result,'keys'):
				if txt_maxcols:
					Count.update(result.keys())
				else:
					KEYS=set(result.keys())
	if txt_maxcols: KEYS={x for x,y in Count.most_common(txt_maxcols)}

	then,now=now,time.time()
	print '>> save_txt: found keys in %ss' % int(now-then)
	print KEYS

	if KEYS:
		# Then loop again to write
		header=['_path']+sorted([unicode(x) for x in KEYS])
		with codecs.open(results_fnfn_txt,'w',encoding='utf-8') as results_f_txt, jsonlines.open(results_fnfn_json) as reader:
			results_f_txt.write('\t'.join(header) + '\n')
			for path,result in reader.iter(skip_invalid=True):
				result['_path']=path
				orow=[unicode(result.get(h,'')) for h in header]
				results_f_txt.write('\t'.join(orow) + '\n')
			print '>> saved:',results_fnfn_txt
			then,now=now,time.time()
			print '>> save_txt: saved in %ss' % int(now-then)



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


### Loading results
def stream_results_json(path_cache,ext='.json'):
	import ujson as json
	for fn in os.listdir(path_cache):
		if not fn.endswith(ext): continue
		fnfn=os.path.join(path_cache,fn)
		with open(fnfn) as f:
			for i,ln in enumerate(f):
				line=ln[:-2]
				if not line: continue
				try:
					path,data=json.loads(line)
				except ValueError as e:
					print '!!',e
					print "!!",line[:200]
					continue
				yield (path,data)

def stream_results(path_cache,ext='.jsonl'):
	if 'jsonl' in os.path.basename(path_cache).split('.'):
		for path,data in stream_jsonl:
			#if '.ipynb' in path: continue
			yield (path,data)
	else:
		for fn in os.listdir(path_cache):
			if not fn.endswith(ext): continue
			fnfn=os.path.join(path_cache,fn)
			for path,data in stream_jsonl(fnfn):
				#if '.ipynb' in path: continue
				yield (path,data)

def stream_jsonl(fn):
	import json_lines
	with json_lines.open(fn) as reader:
		for x in reader:
			yield x


def writegen(fnfn,generator,header=None,args=[],kwargs={}):
	if 'jsonl' in fnfn.split('.'): return writegen_jsonl(fnfn,generator,args=args,kwargs=kwargs)
	with codecs.open(fnfn,'w',encoding='utf-8') as of:
		for i,dx in enumerate():
			if not header: header=sorted(dx.keys())
			if not i: of.write('\t'.join(header) + '\n')
			of.write('\t'.join([unicode(dx.get(h,'')) for h in header]) + '\n')
	print '>> saved:',fnfn
