
def booknlp(path):
	import os
	basename=os.path.basename(path)
	os.system('cd /Users/ryan/DH/charspace/book-nlp && ./runjava \
			novels/BookNLP -doc %s \
			-printHTML -p /Users/ryan/DH/charspace/output/%s \
			-tok /Users/ryan/DH/charspace/output/%s/%s.tokens -f' %  (path, basename, basename, basename) )
