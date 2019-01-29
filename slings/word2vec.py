import os,codecs

STONES = ['save_skipgrams_from_txt_paths','gen_word2vec_model_from_skipgrams']

### TOKENIZER
def tokenize_fast(line):
	import re
	return re.findall("[A-Z]{2,}(?![a-z])|[A-Z][a-z]+(?=[A-Z])|[\'\w\-]+",line.lower())



### SKIPGRAMS
def yield_skipgrams_from_text(text,skipgram_size=10,lowercase=True):
	skipgram=[]
	for token in tokenize_fast(text):
		skipgram+=[token]
		if len(skipgram)>=skipgram_size:
			yield skipgram[:skipgram_size]
			skipgram=[]

def save_skipgrams_from_txt_paths(path_to_list_of_txt_paths,results_dir='./',skipgram_size=10,lowercase=True):
	output_fnfn=os.path.join(results_dir,'skipgrams',os.path.basename(path_to_list_of_txt_paths).replace('.txt','')) + '.txt'
	output_path = os.path.dirname(output_fnfn)
	if not os.path.exists(output_path):
		try:
			os.makedirs(output_path)
		except OSError:
			pass
	print '>> saving skipgrams to',output_fnfn,'...'
	with codecs.open(path_to_list_of_txt_paths,encoding='utf-8') as f, codecs.open(output_fnfn,'w',encoding='utf-8') as of:
		for ln in f:
			path_txt=ln.strip()
			with codecs.open(path_txt,encoding='utf-8',errors='ignore') as path_f:
				txt=path_f.read()
				for sg in yield_skipgrams_from_text(txt,skipgram_size=skipgram_size,lowercase=lowercase):
					of.write(' '.join(sg)+'\n')

def save_skipgrams_from_list_of_lit_ids(path_to_list_of_ids,results_dir=None,skipgram_size=10,lowercase=True):
	import os,codecs
	corpus_name,group_name=path_to_list_of_ids.split(os.path.sep)[-2:]
	group_name=group_name.replace('.txt','')

	import lit
	corpus = lit.load_corpus(corpus_name)

	output_fnfn=os.path.join(results_dir,'skipgrams',corpus_name,group_name)+'.txt'
	output_path = os.path.dirname(output_fnfn)
	if not os.path.exists(output_path): os.makedirs(output_path)
	with codecs.open(path_to_list_of_ids,encoding='utf-8') as f, codecs.open(output_fnfn,'w',encoding='utf-8') as of:
		for ln in f:
			idx=ln.strip()
			text = corpus.textd[idx]
			txt = text.text_plain()
			for sg in yield_skipgrams_from_text(txt,skipgram_size=skipgram_size,lowercase=lowercase):
				of.write(' '.join(sg)+'\n')




class SkipgramsSampler(object):
	def __init__(self, fn, num_skips_wanted):
		self.fn=fn
		self.num_skips_wanted=num_skips_wanted
		self.num_skips=self.get_num_lines()
		self.line_nums_wanted = set(random.sample(list(range(self.num_skips)), num_skips_wanted))

	def get_num_lines(self):
		then=time.time()
		print '>> [SkipgramsSampler] counting lines in',self.fn
		with gzip.open(self.fn,'rb') as f:
			for i,line in enumerate(f):
				pass
		num_lines=i+1
		now=time.time()
		print '>> [SkipgramsSampler] finished counting lines in',self.fn,'in',int(now-then),'seconds. # lines =',num_lines,'and num skips wanted =',self.num_skips_wanted
		return num_lines

	def __iter__(self):
		i=0
		with gzip.open(self.fn,'rb') as f:
			for i,line in enumerate(f):
				if i in self.line_nums_wanted:
					yield line.split()


### WORD2VEC

# STONE
def gen_word2vec_model_from_skipgrams(path_to_skipgram_file,results_dir='./',skipgram_size=10,run=None,
									num_skips_wanted=None, num_workers=8, min_count=10, num_dimensions=100, sg=1, num_epochs=None):

	"""
	STONE CERTIFIED
	Throw this at skipgram files to make word2vec models.
	"""
	import gensim,logging
	logging.basicConfig(format='%(asctime)s : %(levelname)s : %(message)s', level=logging.INFO)

	# Load skipgrams
	skips = gensim.models.word2vec.LineSentence(path_to_skipgram_file) if not num_skips_wanted else SkipgramsSampler(path_to_skipgram_file, num_skips_wanted)

	# Generate model
	model = gensim.models.Word2Vec(skips, workers=num_workers, sg=sg, min_count=min_count, size=num_dimensions, window=skipgram_size)

	# Output filename
	ofn_l = [os.path.splitext(os.path.basename(path_to_skipgram_file))[0]]
	if run: ofn_l+=['run=%s' % str(run).zfill(2)]
	ofn = '.'.join(ofn_l) + '.txt'
	ofnfn=os.path.join(results_dir,'word2vec_models',ofn)
	ofnfn_vocab=os.path.splitext(ofnfn)[0]+'.vocab.txt'
	ofolder=os.path.dirname(ofnfn)
	ofnfn=ofnfn+'.gz'						# add gzip compression for model
	if not os.path.exists(ofolder):
		try:
			os.makedirs(ofolder)
		except OSError:
			pass

	# Save model
	model.wv.save_word2vec_format(ofnfn, ofnfn_vocab)
	print ">> saved:",ofnfn
	print ">> saved:",ofnfn_vocab
