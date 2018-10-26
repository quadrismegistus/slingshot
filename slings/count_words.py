print '>> IMPORT COUNT_WORDS ROCK MECHANICS'

import codecs

def count_words(path):
	with codecs.open(path,encoding='utf-8') as f:
		count=len(f.read().split())
	return {'data1':10, 'data2':count}

rock_function=count_words
