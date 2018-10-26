print '>> IMPORT COUNT_WORDS ROCK MECHANICS'

import codecs

def count_words(path):
	with codecs.open(path,encoding='utf-8') as f:
		count=len(f.read().split())
	return {'count':count}

rock_function=count_words
