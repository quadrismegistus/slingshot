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
