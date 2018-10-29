STONES = ['count_words','get_text_length']
import codecs,re


def get_text_length(path):
	with codecs.open(path,encoding='utf-8') as f:
		count=len(f.read().split())
	return {'count':count}

def count_words_fast(path):
	from future_builtins import map
	from collections import Counter
	from itertools import chain
	with codecs.open(path,encoding='utf-8') as f:
		return Counter(chain.from_iterable(map(tokenize_fast, f)))

def tokenize_fast(line):
	return re.findall("[A-Z]{2,}(?![a-z])|[A-Z][a-z]+(?=[A-Z])|[\'\w\-]+",line.lower())


###
count_words=count_words_fast
