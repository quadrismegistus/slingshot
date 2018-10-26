# encoding=utf-8
print '>> IMPORT PROSODIC ROCK MECHANICS'

import prosodic as p,codecs
import bs4

DEFAULT_METER ='meter_ryan'


def parse_chadwyck(path,meter=DEFAULT_METER):
	print path
	with codecs.open(path,encoding='utf-8') as f:
		xml_string=f.read()
		txt = chadwyck_xml_to_txt(xml_string)
		text = p.Text(txt)
		text.parse(meter=meter)
		res=list(text.stats_lines(save=False))
		print '>> RESULT for path "%s": %s' % (path,res)

rock_function=parse_chadwyck


## Supporting functions
def chadwyck_xml_to_txt(xml_string, OK=['l','lb'], BAD=['note'], body_tag='poem'):
	STANZA_TAGS = ['stanza','versepara','pdiv']
	LINE_TAGS = ['l','lb']
	REPLACEMENTS={
		'&indent;':'    ',
		'&hyphen;':'-',
		u'\u2014':' -- ',
		u'\u2013':' -- ',
	}
	txt=[]
	dom = bs4.BeautifulSoup(xml_string)

	for tag in BAD:
		[x.extract() for x in dom.findAll(tag)]

	# standardize lines:
	for tag in LINE_TAGS:
		if tag=='l': continue
		for ent in dom(tag):
			ent.name='l'

	## replace stanzas
	num_stanzas=0
	for tag in STANZA_TAGS:
		for ent in dom(tag):
			ent.name='stanza'
			num_stanzas+=1
	txt=[]
	if not num_stanzas:
		for line in dom('l'):
			txt+=[line.text]
	else:
		for stanza in dom('stanza'):
			for line in stanza('l'):
				txt+=[line.text.strip()]
			txt+=['']

	txt='\n'.join(txt).replace(u'∣','').strip()
	for k,v in REPLACEMENTS.items():
		txt=txt.replace(k,v)
	return txt
