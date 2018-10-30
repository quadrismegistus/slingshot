import os,imp,argparse,sys
from .logos import SLINGSHOT
from .config import CONFIG


def interactive(parser, SLING_EXT=['py','R']):
	slings=None
	import readline
	from tab_completer import tabCompleter
	tabber=tabCompleter()
	args=parser.parse_args()
	readline.set_completer_delims('\t')
	if 'libedit' in readline.__doc__:
		readline.parse_and_bind("bind ^I rl_complete")
	else:
		readline.parse_and_bind("tab: complete")

	arg2help=dict([(x.dest,x.help) for x in parser.__dict__['_actions']])

	print SLINGSHOT
	longest_line = max(len(line.rstrip()) for line in SLINGSHOT.split('\n'))
	HR='\n'+'-'*longest_line+'\n'
	#print "### SLINGSHOT v0.1 ###"
	print "## SLINGSHOT v0.1: interactive mode (see \"slingshot --help\" for more)"
	#print parser.format_help()

	try:
		# SLING
		path_slings = CONFIG.get('PATH_SLINGS','')
		SLING_EXT = CONFIG.get('SLING_EXT','')
		if path_slings and os.path.exists(path_slings) and os.path.isdir(path_slings):
			slings=sorted([fn for fn in os.listdir(path_slings) if fn.split('.')[-1] in SLING_EXT])
			sling_str='  '.join(['(%s) %s' % (si+1, sl) for si,sl in enumerate(slings)])
		while not args.sling:
			readline.set_completer(tabber.pathCompleter)
			print '\n>> SLING: '+arg2help['sling']
			if path_slings and slings:
				print '          [numerical shortcuts for slings found in\n          [{dir}]\n          {slings}'.format(dir=path_slings, slings=sling_str)
			sling = raw_input('>> ').strip()
			if sling.isdigit() and 0<=int(sling)-1<len(slings):
				args.sling=os.path.join(path_slings,slings[int(sling)-1])
			elif not os.path.exists(sling):
				print "!! filename does not exist"
			elif not sling.split('.')[-1] in SLING_EXT:
				print "!! filename does not end in one of the acceptable file extensions [%s]" % ', '.join(SLING_EXT)
			else:
				args.sling=sling

		# STONE
		print HR
		if args.sling.endswith('.py'):
			import imp,inspect
			sling = imp.load_source('sling', args.sling)
			functions = sling.STONES if hasattr(sling,'STONES') and sling.STONES else sorted([x for x,y in inspect.getmembers(sling, inspect.isfunction)])
			functions_str='  '.join(['(%s) %s' % (si+1, sl) for si,sl in enumerate(functions)])
			tabber.createListCompleter(functions)
		else:
			functions_str=''
		while not args.stone:

			#prompt='\n>> STONE: {help}\noptions: {opts}\n>>'.format(help=arg2help['stone'], opts=', '.join(functions))
			#prompt='\n>> STONE: {help}\n>>'.format(help=arg2help['stone'])
			print '>> STONE: '+arg2help['stone']
			#print '          [options]: '+functions_str
			if functions_str:
				readline.set_completer(tabber.listCompleter)
				print '          '+functions_str
			stone = raw_input('>> ').strip()
			if stone.isdigit() and 0<=int(stone)-1<len(functions):
				args.stone=functions[int(stone)-1]
			elif functions_str and not stone in functions:
				print "!! function not in file"
			else:
				args.stone=stone

		# PATH
		print HR
		path_pathlists = CONFIG.get('PATH_PATHLISTS','')
		opener='>> PATH: '
		opener_space=' '*len(opener)
		pathlists_str=''
		if path_pathlists and os.path.exists(path_pathlists) and os.path.isdir(path_pathlists):
			pathlists=[fn for fn in os.listdir(path_pathlists) if not fn.startswith('.')]
			joiner='\n'+opener_space
			pathlists_str=''.join(['\n%s(%s) %s' % (opener_space,si+1, sl) for si,sl in enumerate(pathlists)])
		while not args.path:
			readline.set_completer(tabber.pathCompleter)
			#print opener+arg2help['stone']
			print opener+'Enter a path either to a pathlist text file, or to a directory of texts'
			if path_slings and slings:
				print '{space}[numerical shortcuts for pathlists found in\n{space}[{dir}]{pathlists}'.format(dir=path_slings, pathlists=pathlists_str,space=opener_space)
			path = raw_input('>> ').strip()
			if path.isdigit() and 0<=int(path)-1<len(pathlists):
				args.path=os.path.join(path_pathlists,pathlists[int(path)-1])
			elif not os.path.exists(path):
				print "!! filename or directory does not exist"
			elif os.path.isdir(path):
				args.ext = raw_input('\n>> EXT: '+arg2help['ext']+'\n>> ').strip()
				args.path=path
			else:
				args.path=path

		print HR
		print 'OPTIONAL SECTION'
		module='.'.join(os.path.basename(args.sling).split('.')[:-1])
		#default_savedir='/'.join(['results_slingshot',module,args.stone,now()])
		default_savedir='/'.join(['results_slingshot',module,args.stone])

		args.sbatch = raw_input('\n>> SBATCH: Add to the SLURM/Sherlock process queue via sbatch? [N]\n>> (Y/N) ').strip().lower()=='y'
		if args.sbatch:
			args.parallel = raw_input('\n>> PARALLEL: '+arg2help['parallel']+' [4]\n>> ').strip()

			hours = raw_input('\n>> HOURS: '+arg2help['hours']+' [1]\n>> ').strip()
			hours = ''.join([x for x in hours if x.isdigit()])
			args.hours = parser.get_default('hours') if not hours else hours

			mem = raw_input('\n>> MEMORY: '+arg2help['mem']+' [2G]\n>> ').strip()
			args.mem = parser.get_default('mem') if not mem else mem
		else:
			args.debug = raw_input('\n>> DEBUG: %s? [N]\n>> (Y/N) ' % arg2help['debug']).strip().lower()=='y'

		args.nosave = raw_input('\n>> SAVE: Save results? [Y]\n>> (Y/N) ').strip().lower()=='n'
		if not args.nosave:
			args.savedir = raw_input('\n>> SAVEDIR: Directory to store results in [%s]' % default_savedir  + '\n>> ').strip()
			args.cache = raw_input('\n>> CACHE: Cache partial results? [Y]\n>> (Y/N) ').strip().lower()=='y'
			mfw = raw_input('\n>> MFW: %s' % arg2help['mfw'] + '\n>> ').strip()
			args.mfw=mfw if mfw else parser.get_default('mfw')

		args.quiet = raw_input('\n>> QUIET: %s? [N]\n>> (Y/N) ' % arg2help['quiet']).strip().lower()=='y'
		args.limit = raw_input('\n>> LIMIT: '+arg2help['limit']+' [None]\n>> ').strip()

		print

	except (KeyboardInterrupt,EOFError) as e:
		print '\n>> goodbye'
		exit()

	return args


def now(now=None,seconds=False):
	import datetime as dt
	if not now:
		now=dt.datetime.now()
	elif type(now) in [int,float,str]:
		now=dt.datetime.fromtimestamp(now)

	return '{0}{1}{2}-{3}{4}{5}'.format(now.year,str(now.month).zfill(2),str(now.day).zfill(2),str(now.hour).zfill(2),str(now.minute).zfill(2),'-'+str(now.second).zfill(2) if seconds else '')
