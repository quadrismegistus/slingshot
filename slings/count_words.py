STONES = ['count_words']
import codecs,re


def count_words_simple(path):
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
