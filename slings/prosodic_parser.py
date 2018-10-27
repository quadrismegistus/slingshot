# encoding=utf-8

## CONSTANTS
DEFAULT_METER ='meter_ryan'
LINE_LIM = 10

## IMPORTS
import prosodic as p,codecs,csv,os
p.config['en_TTS_ENGINE']='none'
import bs4


### PROCESS
def path2opath(path):
	return path.replace('/xml/','/prosodic/').replace('.xml','.txt')

def parse_chadwyck(path,meter=DEFAULT_METER,ofnfn=None,save_as=path2opath,save=True,return_result=False,force=True):
	if save:
		ofnfn=save_as(path) if not ofnfn else ofnfn
		if not force and os.path.exists(ofnfn) and os.stat(ofnfn).st_size:
			# Already done!
			print '>> already saved:',ofnfn
			return

	with codecs.open(path,encoding='utf-8') as f:
		xml_string=f.read()
		txt = chadwyck_xml_to_txt(xml_string)
		txt = txt.split('\n')[:LINE_LIM]
		text = p.Text(txt)
		print '##',len(txt.split('\n')),len(text.lines()),LINE_LIM
		text.parse(meter=meter,line_lim=LINE_LIM)
		res=text.stats_lines_ot(save=False,meter=meter)
		#print '>> RESULT for path "%s": %s' % (path,res)
		if save:
			opath=os.path.split(ofnfn)[0]
			if not os.path.exists(opath): os.makedirs(opath)
			with codecs.open(ofnfn,'w',encoding='utf-8') as of:
				header=text.stats_lines_ot_header(meter=meter)
				dw=csv.DictWriter(of,header,delimiter='\t',extrasaction='ignore')
				dw.writeheader()
				for dx in res:
					for k,v in dx.items():
						if type(v) == str:
							dx[k]=v.decode('utf-8',errors='ignore')
					try:
						dw.writerow(dx)
					except UnicodeEncodeError:
						print "!! UnicodeEncodeError:",dx
						print '!! aborting file:',ofnfn
						os.unlink(ofnfn)
						return
				print '>> saved:',ofnfn
		if return_result:
			return list(res)

rock_function=parse_chadwyck




#### POSTPROCESS
def read_processed_data_from_path(path):
	opath=path2opath(path)
	if not os.path.exists(opath): return
	with codecs.open(opath,encoding='utf-8') as f:
		ld=list(csv.DictReader(f,delimiter='\t'))
		for d in ld:
			for k,v in d.items():
				if k!='line' and not v:
					d[k]=0
				try:
					d[k]=float(v)
				except ValueError:
					pass
		return ld


def postprocess_chadwyck(path):
	import pandas as pd,numpy as np
	ld=read_processed_data_from_path(path)
	if not ld: return
	#dld=ld2dld(ld,'num_line')
	#for num_line,lld in dld.items(): #sorted(dld.items(),key=lambda (k,v): float(k)):
	df=pd.DataFrame(ld)

	## add w/s/ww/ss
	df['num_ww']=[ meter.count('ww') for meter in df['meter'] ]
	df['num_w']=[ meter.count('w') - meter.count('ww') for meter in df['meter'] ]
	df['num_s']=[ meter.count('s') - meter.count('ss') for meter in df['meter'] ]
	df['num_ss']=[ meter.count('ss') for meter in df['meter'] ]
	num_sylls = df['num_sylls']=[ len(meter) for meter in df['meter'] ]

	df_numeric = df.select_dtypes('number')

	# Normalize by number of syllables: normalize per 10 syllables
	df_numeric = df_numeric.divide(num_sylls,axis=0) * 10
	df_numeric['num_sylls'] = num_sylls

	pivot_numeric = df_numeric.pivot_table(index='num_line', aggfunc=np.mean)
	pivot_num_parses = df.pivot_table(index='num_line',aggfunc={'parse':len}).rename(columns={'parse':'num_parses'})
	pivot = pivot_numeric.join(pivot_num_parses)
	final_data = pivot.agg(np.mean)
	return dict(final_data)



### PREPROCESS
def make_path_list(ofn='paths.chadwyck.1600_1900.txt',min_dob=1600,max_dob=1900):
	from lit.corpus.chadwyck_poetry import ChadwyckPoetry
	corpus=ChadwyckPoetry()
	with open(ofn,'w') as of:
		for t in corpus.texts():
			dob=t.meta['author_dob']
			if dob:
				try:
					dob=float(dob)
					if dob<min_dob: continue
					if dob>max_dob: continue
					of.write(t.fnfn_xml+'\n')
				except ValueError:
					pass




### SUPPORTING FUNCTIONS
def chadwyck_xml_to_txt(xml_string, OK=['l','lb'], BAD=['note'], body_tag='poem', line_lim=LINE_LIM):
	STANZA_TAGS = ['stanza','versepara','pdiv']
	LINE_TAGS = ['l','lb']
	REPLACEMENTS={
		'&indent;':'    ',
		'&hyphen;':'-',
		u'\u2014':' -- ',
		u'\u2013':' -- ',
		u'\u2018':"'",
		u'\u2019':"'",
		u'\xc9':'E', #u'É',
		u'\xe9':'e', #u'é',
		u'\xc8':'E',#u'È',
		u'\u201d':'"',
		u'\u201c':'"',
	}
	txt=[]
	dom = bs4.BeautifulSoup(xml_string,'lxml')

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
	num_lines=0
	if not num_stanzas:
		for line in dom('l'):
			txt+=[line.text.strip()]
			num_lines+=1
			if line_lim and num_lines>=line_lim: break
	else:
		for stanza in dom('stanza'):
			for line in stanza('l'):
				txt+=[line.text.strip()]
				num_lines+=1
			if line_lim and num_lines>=line_lim: break
			txt+=['']

	txt=txt[:line_lim]
	txt='\n'.join(txt).replace(u'∣','').strip()
	for k,v in REPLACEMENTS.items():
		txt=txt.replace(k,v)
	return txt
