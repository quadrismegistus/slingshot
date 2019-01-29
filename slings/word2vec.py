import os,codecs

STONES = ['save_skipgrams_from_txt_paths']

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

def save_skipgrams_from_txt_paths(path_to_list_of_txt_paths,results_dir='./'):
	output_fnfn=os.path.join(results_dir,'skipgrams',path_to_list_of_txt_paths)+'.txt'
	output_path = os.path.dirname(output_fnfn)
	if not os.path.exists(output_path): os.makedirs(output_path)
	print '>> saving skipgrams to',output_fnfn,'...'
	with codecs.open(path_to_list_of_ids,encoding='utf-8') as f, codecs.open(output_fnfn,'w',encoding='utf-8') as of:
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

def gen_word2vec_model_from_skipgrams(path_to_skipgram_file,results_dir=None,skipgram_size=10,
									num_skips_wanted=None, num_workers=8, min_count=10, num_dimensions=100, sg=1, num_epochs=None):

	import gensim
	if not self.num_skips_wanted:
		skips = gensim.models.word2vec.LineSentence(self.skipgram_fn)
	else:
		skips = SkipgramsSampler(self.skipgram_fn, self.num_skips_wanted)

	model = gensim.models.Word2Vec(skips, workers=num_workers, sg=sg, min_count=min_count, size=num_dimensions, window=skipgram_size)
