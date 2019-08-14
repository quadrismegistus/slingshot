from __future__ import absolute_import
from __future__ import print_function
import os,imp,argparse,sys
from .logos import SLINGSHOT
from .config import CONFIG
from .slingshot import *
from six.moves import input
import imp,inspect
longest_line = max(len(line.rstrip()) for line in SLINGSHOT.split('\n'))
HR='\n'+'-'*longest_line+'\n'






def interactive(parser, SLING_EXT=['py','R','ipynb']):
	slings=None
	import readline
	from .tab_completer import tabCompleter
	tabber=tabCompleter()
	args=parser.parse_args()
	args.sling=args.code
	args.stone=args.func
	args.stone_args=args.args


	readline.set_completer_delims('\t')
	if 'libedit' in readline.__doc__:
		readline.parse_and_bind("bind ^I rl_complete")
	else:
		readline.parse_and_bind("tab: complete")

	arg2help=dict([(x.dest,x.help) for x in parser.__dict__['_actions']])

	opener_printed=False

	def print_opener():
		print(SLINGSHOT)
		longest_line = max(len(line.rstrip()) for line in SLINGSHOT.split('\n'))
		HR='\n'+'-'*longest_line+'\n'
		#print "### SLINGSHOT v0.1 ###"
		print("## SLINGSHOT v0.1: interactive mode (see \"slingshot --help\" for more)")
		#print parser.format_help()
		opener_printed=True

	try:
		functions_str=''
		if not args.llp_method:
			# SLING
			path_slings = CONFIG.get('PATH_SLINGS','')
			SLING_EXT = CONFIG.get('SLING_EXT','')
			if path_slings and os.path.exists(path_slings) and os.path.isdir(path_slings):
				slings=sorted([fn for fn in os.listdir(path_slings) if fn.split('.')[-1] in SLING_EXT])
				#sling_str='  '.join(['(%s) %s' % (si+1, sl) for si,sl in enumerate(slings)])
				sling_str='  '+'\n            '.join(['(%s) %s' % (si+1, sl) for si,sl in enumerate(slings)])
			while not args.sling:
				readline.set_completer(tabber.pathCompleter)
				if not opener_printed: print_opener()
				print('\n>> CODE ("Sling"): '+arg2help['code'])
				if path_slings and slings:
					print('          [numerical shortcuts for slings found in\n          [{dir}]\n          {slings}'.format(dir=path_slings, slings=sling_str))
				sling = input('>> ').strip()
				if sling.isdigit() and 0<=int(sling)-1<len(slings):
					args.sling=os.path.join(path_slings,slings[int(sling)-1])
				elif not os.path.exists(sling):
					print("!! filename does not exist")
				elif not sling.split('.')[-1] in SLING_EXT:
					print("!! filename does not end in one of the acceptable file extensions [%s]" % ', '.join(SLING_EXT))
				else:
					args.sling=sling

			# STONE
			longest_line = max(len(line.rstrip()) for line in SLINGSHOT.split('\n'))
			HR='\n'+'-'*longest_line+'\n'
			print(HR)
			if args.sling.endswith('.py'):
				sling = imp.load_source('sling', args.sling)
				# functions = sling.STONES if hasattr(sling,'STONES') and sling.STONES else sorted([x for x,y in inspect.getmembers(sling, inspect.isfunction)])
				# functions_str='  '.join(['(%s) %s' % (si+1, sl) for si,sl in enumerate(functions)])
				# tabber.createListCompleter(functions)
			elif args.sling.endswith('.ipynb'):
				import nbimporter
				nbimporter.options['only_defs'] = CONFIG.get('NBIMPORTER_ONLY_DEFS',False)
				ppath,pfn = os.path.split(args.sling)
				pname,pext = os.path.splitext(pfn)
				NBL = nbimporter.NotebookLoader(path=[ppath])
				sling = NBL.load_module(pname)
			else:
				sling = None
				functions_str=''

			if sling:
				functions = sling.STONES if hasattr(sling,'STONES') and sling.STONES else sorted([x for x,y in inspect.getmembers(sling, inspect.isfunction)])
				functions_str='  '.join(['(%s) %s' % (si+1, sl) for si,sl in enumerate(functions)])
				tabber.createListCompleter(functions)


			while not args.stone:

				#prompt='\n>> STONE: {help}\noptions: {opts}\n>>'.format(help=arg2help['stone'], opts=', '.join(functions))
				#prompt='\n>> STONE: {help}\n>>'.format(help=arg2help['stone'])
				print('>> FUNC ("Stone"): '+arg2help['func'])
				#print '          [options]: '+functions_str
				if functions_str:
					readline.set_completer(tabber.listCompleter)
					print('          '+functions_str)
				stone = input('>> ').strip()
				if stone.isdigit() and 0<=int(stone)-1<len(functions):
					args.stone=functions[int(stone)-1]
				elif functions_str and not stone in functions:
					print("!! function not in file")
				else:
					args.stone=stone

		# PATH

		if not args.llp_corpus:
			print(HR)
			num2cname={}

			try:
				import llp
				import pandas as pd
				#print('>> CORPUS: Type the number or name of an LLP corpus')
				print('[LLP Corpus Library] (showing corpora with txt or xml folders installed)')
				#print()

				cnum=0
				for ci,(corpus,cdx) in enumerate(sorted(llp.corpus.load_manifest().items())):
					if not os.path.exists(cdx['path_txt']) and not os.path.exists(cdx['path_xml']): continue
					cnum+=1
					num2cname[cnum]=corpus
					print('\t({num}) {name} ({desc})'.format(num=str(cnum), desc=cdx['desc'], name=cdx['name']))  #.zfill(2).replace('0','')
				#pd.options.display.max_colwidth = 100
				#print(pd.DataFrame(llp.corpus.load_manifest()).T[['desc']])
				print()
				"""llp_input = input('>> ').strip()
				if llp_input.strip().isdigit():
					cnum=int(llp_input.strip())
					if cnum in num2cname:
						cname=num2cname[cnum]
						if corpus: args.llp_corpus=cname
				else:
					#corpus=llp.load_corpus(llp_input)
					#if corpus: args.llp_corpus=corpus
					args.llp_corpus=llp_input.strip()"""

			except ImportError:
				pass


			#print(HR)

			opener='>> CORPUS/PATH: '
			opener_space=' '*len(opener)
			pathlists_str=''
			while not (args.path or args.llp_corpus):
				readline.set_completer(tabber.pathCompleter)
				print(opener+'Please enter either ...\n'\
				'	* a number to refer to an LLP corpus above\n' \
				'	* a path to a folder, to find all files in that folder with a certain extension\n'\
				'	* a path to a file, each line of which is the absolute path to another text file\n')
				path = input('>> ').strip()
				if path.isdigit():
					cnum=int(path.strip())
					if cnum in num2cname:
						cname=num2cname[cnum]
						if corpus: args.llp_corpus=cname
				elif not os.path.exists(path):
					print("!! filename or directory does not exist")
				elif os.path.isdir(path):
					args.ext = input('\n>> EXT: '+arg2help['ext']+'\n>> ').strip()
					args.path=path
				elif is_csv(path):
					args.path=path
					args.pathkey=input('\n>> COLUMN: '+arg2help['pathkey']+'\n>> ').strip()
					args.pathprefix=input('\n>> PREFIX: '+arg2help['pathkey']+'\n>> ').strip()
					args.pathsuffix=input('\n>> SUFFIX: '+arg2help['pathkey']+'\n>> ').strip()
				else:
					args.path=path


		if opener_printed:
			longest_line = max(len(line.rstrip()) for line in SLINGSHOT.split('\n'))
			HR='\n'+'-'*longest_line+'\n'
			#print(HR)
			print('OPTIONAL SECTION')
		module='.'.join(os.path.basename(args.sling).split('.')[:-1])
		#default_savedir='/'.join(['results_slingshot',module,args.stone,now()])
		default_savedir='/'.join(['data_slingshot',args.stone+'_'+(args.llp_corpus if args.llp_corpus else os.path.basename(args.path))])

		#args.nosave = input('\n>> SAVE: Save results? [Y]\n>> (Y/N) ').strip().lower()=='n'
		args.nosave = False

		if not args.nosave:
			args.savedir = iter_filename(input('\n>> SAVEDIR: Directory to store results in [%s]' % default_savedir  + '\n>> ').strip())
			#args.cache = input('\n>> CACHE: Cache partial results? [Y]\n>> (Y/N) ').strip().lower()=='y'
			args.cache = True
			#mfw = input('\n>> MFW: %s' % arg2help['mfw'] + '\n>> ').strip()
			mfw=None
			args.mfw=mfw if mfw else parser.get_default('mfw')

		#args.quiet = input('\n>> QUIET: %s? [N]\n>> (Y/N) ' % arg2help['quiet']).strip().lower()=='y'
		args.quiet = False
		args.limit = input('\n>> LIMIT: '+arg2help['limit']+' [None]\n>> ').strip()

		args.sbatch = input('\n>> SBATCH: Add to the SLURM/Sherlock process queue via sbatch? [N]\n>> (Y/N) ').strip().lower()=='y'

		args.parallel = input('\n>> PARALLEL: '+arg2help['parallel']+' [4]\n>> ').strip()
		if args.sbatch:
			hours = input('\n>> HOURS: '+arg2help['hours']+' [1]\n>> ').strip()
			hours = ''.join([x for x in hours if x.isdigit()])
			args.hours = parser.get_default('hours') if not hours else hours

			mem = input('\n>> MEMORY: '+arg2help['mem']+' [2G]\n>> ').strip()
			args.mem = parser.get_default('mem') if not mem else mem
		else:
			#args.debug = input('\n>> DEBUG: %s? [N]\n>> (Y/N) ' % arg2help['debug']).strip().lower()=='y'
			args.debug = False

		print()

	except (KeyboardInterrupt,EOFError) as e:
		print('\n>> goodbye')
		exit()
	
	return args


def now(now=None,seconds=False):
	import datetime as dt
	if not now:
		now=dt.datetime.now()
	elif type(now) in [int,float,str]:
		now=dt.datetime.fromtimestamp(now)

	return '{0}{1}{2}-{3}{4}{5}'.format(now.year,str(now.month).zfill(2),str(now.day).zfill(2),str(now.hour).zfill(2),str(now.minute).zfill(2),'-'+str(now.second).zfill(2) if seconds else '')
