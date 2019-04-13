from __future__ import absolute_import
from __future__ import print_function
import six
from six.moves import zip
# coding: utf-8
# Syntax parsing slingshot functions with spacy

# set stones aside for sling
STONES = ['parse_path', 'postprocess','save_as_individual_text_csvs']
MAX_LEN = 1000000 # spacy

# imports
import os,sys,codecs
from xopen import xopen

# Load spacy
import spacy
try:
	nlp = spacy.load('en_core_web_sm')
except IOError:
	nlp = spacy.load('en')

"""
Principal functions
"""

def path2txt(path):
	with xopen(path) as f:
		return f.read().decode('utf-8')

def parse_path(path):
	return list(parse(path2txt(path)))

# Main parser
import time
def parse(txt,max_len=MAX_LEN):
	txt=txt.replace('\r\n','\n').replace('\r','\n')
	txt=txt.replace('@ @ @ @ @ @ @ @ @ @','\n\n') # hack for COHA
	paras=txt.split('\n\n')

	#doc=nlp(txt)
	now=time.time()
	num_tokens=0
	for pi,para in enumerate(paras):
		#if not pi%10: print('>>',pi,'...')
		para=para.strip()
		if len(para)>max_len: continue
		doc=nlp(para)
		for token in doc:
			num_tokens+=1
			td={'word':token.text,
				'lemma':token.lemma_,
				'tag':token.tag_,
				'para':pi+1,
				'i':token.i,
				'pos':token.pos_,
				'dep':token.dep_,
				'head':token.head.text,
				'head_pos':token.head.pos_,
				'head_lemma':token.head.lemma_,
				'head_tag':token.head.tag_,
				'sent_start':token.sent.start
				#'children':'|'.join([child.text for child in token.children])
			}
			yield td
	nownow=time.time()
	duration=nownow-now
	rate=num_tokens/duration
	print('>> FINISHED PROCESSING IN %s seconds (%s wps)' % (round(duration,1),rate))




#Postprocess

def iterate_syntax(results_cache_dir_or_jsonl_file):
	import os
	from mpi_slingshot import stream_results_json

	I=0
	for path,data in stream_results_json(results_cache_dir_or_jsonl_file):
		if '.ipynb' in path: continue
		#fn=os.path.split(path)[-1]
		for dx in data:
			if not I%100000: print('>>',I,'...')
			dx['_i']=I
			dx['_path']=path
			I+=1
			yield dx

def save_as_individual_text_csvs(results_cache_dir_or_jsonl_file,odir=None):
	import codecs,csv,pandas as pd
	from llp import tools
	of=None
	path_now=None
	if not odir: odir='results_postprocess_individual_csv'
	old=[]
	for dx in iterate_syntax(results_cache_dir_or_jsonl_file):
		if path_now!=dx['_path']:
			#opath=dx['_path'] if not dx['_path'].startswith('/') else dx['_path'][1:]
			if path_now:
				opath=path_now
				opath=opath if not opath.startswith('/') else opath[1:]
				opath_full = os.path.join(odir,os.path.split(opath)[0])
				if not os.path.exists(opath_full): os.makedirs(opath_full)
				ofn=os.path.splitext(os.path.basename(path_now))[0] + '.txt'
				ofnfn=os.path.join(opath_full, ofn)
				#if of: of.close()
				#of=codecs.open(ofnfn,'w',encoding='utf-8')
				#of=open(ofnfn,'w')
				#header=sorted(dx.keys())
				#header.remove('_path')
				#writer=csv.DictWriter(of,fieldnames=header,delimiter='\t')
				#writer.writeheader()
				#tools.write2(ofnfn,old)
				#print ofnfn,len(old)
				for d in old:
					for k,v in list(d.items()):
						d[k]=six.text_type(v).replace('\r\n',"\\n").replace('\r','\\n').replace('\n','\\n')
				#pd.DataFrame(old).to_csv(ofnfn,sep='\t',encoding='utf-8')
				tools.write(ofnfn,old,toprint=True)
				#print '>> saved:',ofnfn
			old=[]
			path_now=dx['_path']

		del dx['_path']
		old+=[dx]

		#writer.writerow(dx)
	#of.close()




def postprocess(results_cache_dir_or_jsonl_file,only_words=set(),only_pos=set(),only_rels=set(),lemma=False,output_fn=None,limit=None):
	if not output_fn:
		if '.jsonl' in results_cache_dir_or_jsonl_file:
			output_fn=results_cache_dir_or_jsonl_file.replace('.jsonl','.postprocessed.txt')
		else:
			output_folder=os.path.abspath(os.path.join(results_cache_dir_or_jsonl_file,'..'))
			output_fn=os.path.join(output_folder,'results.postprocessed.txt')
	kwargs=dict(list(zip(['only_words','only_pos','only_rels','lemma','limit'], [only_words,only_pos,only_rels,lemma,limit])))
	writegen(output_fn,postprocess_iter,args=[results_cache_dir_or_jsonl_file],kwargs=kwargs)

def postprocess_iter(results_jsonl_fn,only_words=set(),only_pos=set(),only_rels=set(),lemma=False,limit=None):
	import pandas as pd,os
	from mpi_slingshot import stream_results
	wnum=-1
	if only_words: only_words=set(only_words)
	if only_pos: only_pos=set(only_pos)
	if only_rels: only_rels=set(only_rels)
	for ipath,(path,data) in enumerate(stream_results(results_jsonl_fn)):
		if limit and ipath>=limit: break
		if '.ipynb' in path: continue
		sent_ld=[]
		num_sent=0
		fn=os.path.split(path)[-1]
		odx=None
		for dx in data:
			wnum+=1
			dx['_i']=wnum
			if not wnum%10000: print('>>',wnum,ipath,path,odx,'...')
			if sent_ld and dx['sent_start']!=sent_ld[-1]['sent_start']:
				old=postprocess_sentence(sent_ld,only_words=only_words,only_pos=only_pos,only_rels=only_rels,lemma=lemma)
				num_sent+=1
				for odx in old:
					odx['num_sent']=num_sent
					odx['fn']=fn
					yield odx
					sent_ld=[]
			sent_ld+=[dx]

def postprocess_sentence(sent_ld,only_words=set(),only_pos=set(),only_rels=set(),lemma=False):
	"""
	Modifiers
	Nouns possessed by characters: poss
	Adjectives modifying characters:
	Verbs of which character is a subject
	Verbs of which character is an object

	rels = {'poss':'Possessive',
		   'nsubj':'Subject',
		   'dobj':'Object',
		   'amod':'Modifier'}
	"""

	old=[]
	for dx in sent_ld:
		word=dx['lemma'] if lemma else dx['word']
		rel=dx['dep']
		head=dx['head_lemma'] if lemma else dx['head']
		pos=dx['pos']
		word,head=word.lower(),head.lower()
		if only_words and not word in only_words: continue
		if only_pos and not pos in only_pos: continue
		if only_rels and not rel in only_rels: continue
		word_dx={'head':head,'word':word,'rel':rel, '_i':dx['_i']}
		old+=[word_dx]
	return old



"""
Appendices
"""
def gleanPunc2(aToken):
	aPunct0 = ''
	aPunct1 = ''
	while(len(aToken) > 0 and not aToken[0].isalnum()):
		aPunct0 = aPunct0+aToken[:1]
		aToken = aToken[1:]
	while(len(aToken) > 0 and not aToken[-1].isalnum()):
		aPunct1 = aToken[-1]+aPunct1
		aToken = aToken[:-1]

	return (aPunct0, aToken, aPunct1)



def writegen(fnfn,generator,header=None,args=[],kwargs={}):
	import codecs,csv
	if 'jsonl' in fnfn.split('.'): return writegen_jsonl(fnfn,generator,args=args,kwargs=kwargs)
	iterator=generator(*args,**kwargs)
	first=next(iterator)
	if not header: header=sorted(first.keys())
	with open(fnfn, 'w') as csvfile: # , encoding='utf-8', errors='ignore')
		writer = csv.DictWriter(csvfile,fieldnames=header,delimiter='\t')
		writer.writeheader()
		for i,dx in enumerate(iterator):
			for k,v in list(dx.items()):
				dx[k]=six.text_type(v).encode('utf-8',errors='ignore')
			writer.writerow(dx)
	print('>> saved:',fnfn)
