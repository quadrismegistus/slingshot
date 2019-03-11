# coding: utf-8
# Syntax parsing slingshot functions with spacy

# set stones aside for sling
STONES = ['parse_path']
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
		if not pi%10: print '>>',pi,'...'
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
	print '>> FINISHED PROCESSING IN %s seconds (%s wps)' % (round(duration,1),rate)


def postprocess(results_jsonl_fn,words=set(),only_pos=set(),only_rel=set(),lemma=True):
	import pandas as pd,os
	from mpi_slingshot import stream_results
	wnum=-1
	for path,data in enumerate(stream_results(fn)):
		if '.ipynb' in path: continue
		sent_ld=[]
		num_sent=0
		fn=os.path.split(path)[-1]
		for dx in data:
			wnum+=1
			if sent_ld and dx['sent_start']!=sent_ld[-1]['sent_start']:
				old=postprocess_sentence(sent_ld,pos_only=only_pos,lemma=lemma)
				num_sent+=1
				for odx in old:
					odx['_i']=wnum
					odx['num_sent']=num_sent
					odx['fn']=fn
					yield odx
					sent_ld=[]
			sent_ld+=[dx]


def postprocess_sentence(sent_ld,pos_only={},lemma=False):
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
		if not word in all_words or not pos in pos_only: continue
		word_dx={'head':head,'word':word,'rel':rel}
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
