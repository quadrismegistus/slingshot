# mpi-slingshot
A Python wrapper to "slingshot" a "rock" (function) across thousands of files using MPI.

## Slaying the Goliath of big data
We’re David. Our data is Goliath. How can we slay Goliath by extending our code over hundreds of thousands of texts? To do so, we write a little function, a “stone,” and then load it into the slingshot: the program then takes care of “throwing” the stone at hundreds of thousands of text files. It works by cloning David, basically, so that 4, 8, or 128 Davids are all pelting Goliath at the same time. More specifically, the “stone” is a function that accepts a path to text file; that function then reads and processes the text in any way at all; and then returns data in any form. That data will then be collected together and saved in both JSON and, where possible, as a TSV, the latter of which can be opened directly in Python/Pandas, R, and Excel.

In more technical lingo, slingshot is basically a minimalist map/reduce framework: we “map” a function onto many texts simultaneously, and then “reduce” the data back into a single representation. But I never found “MapReduce” a very evocative image or a good analysis of what’s really happening in the process. Whereas: David-clones fighting Goliath! Much more accurate.

## Running interactively (start here)
Run in your Terminal:

	slingshot

You should see a prompt:

             :o+s/+:                           .///`
           `d-o`+:/h+`                       .+y-``+o.
            d/+++/++om+:-                   +ho+yyo.`y-
           :y+oo+:/++hdoso/-               yh+/+/osy:s+
           -yy-/+o++osm``+o:+-           `/osyso+ooyd-
            -do/oo-:.`h.  .+::+`        /+//o-`-hhdms-
             -om.-`-- :h    :o-o-     .o-+-s-.. ss-s+++`
               s+ :`:- ss    .s-s.   /o`+.o../.ho   :ooo/
               `d- -`/. yo    `s-s `o/.+-+.-o/h:     `:soo-
                .h. - + `y+    `y:yo-:::/-:+ys`        `:oso.
                 -y``-`/ `s+`   /ho::-/+:-+y:            `+os/
                  -s`.--: `/s++ys+::++/.:s/`               -sss`
                   -s`::.+` .:/::-- `-/s/`                  `soh.
                    -o`-`:+ ``-..:`-ssh`                     `y/d`
                     .s- .s    `.:yds:s`                      -y+y
                      +h- /  - `+/N:-h-+                       h`N`
                      m-` ``o. ` ss  oos.                      y.N-
                     :d-:.:o- `:.m`   o+s`                     h-N`
                     h/o`/+-  +`y+     oss`   `````           :soy
                    -h-//-/  :-:h       oss:/+////+o+:.     `/s:m.
                    d-+--+  `+`d.       `hsyy.`     .:+s/` -y+-d-
                   /s `+ +  o.y:        oo./sh:       `/dyoy/+y.
                  `d` o- / -/+o         d:  ..       .ydyysso:
                  /s +/. .+-.y          :y-`         //sN:`
                  d.o+`  `s y.           `+so/:--...-:sh-
                 :yo:...:+.+-               -/++ooo++:.
                 hoo::ooo-:y
                .m/s::/s/.d.
                `yoo///:.h-
                  .ossoos.

	## SLINGSHOT v0.1: interactive mode (see "slingshot --help" for more)

	>> SLING: Path to the python or R file of code (ending in .py or .R)
	          [numerical shortcuts for slings found in /oak/stanford/groups/malgeehe/code/mpi-slingshot/slings]
	          (1) booknlp.py  (2) count_words.R  (3) count_words.py  (4) prosodic_parser.py
	>>

You’re being prompted for the “**sling**,” the file of python or R code. Type a number to select from some built-in slings, or type the path to a file of your own code. After selecting the “sling,” you’ll be asked for the “stone”:

	>> STONE: The name of the function in the code that takes a string filepath
	          (1) parse_chadwyck  (2) postprocess_chadwyck
	>>

Type either the appropriate # (if available [rn only for Python]), or the name of the function that is the “**stone**.” The stone is the function inside the code, or sling, that is to be slingshot onto the texts. Its only required argument is an absolute path to a text file: this function will take that path, load the text, and return some data, any data. The data will then be collected together at the end.

But which texts should we slingshot this function at? We now need to select a list of filepaths.

	>> PATH: Enter a path either to a pathlist text file, or to a directory of texts
	         [numerical shortcuts for pathlists found in /oak/stanford/groups/malgeehe/code/mpi-slingshot/slings]
	         (1) paths_sherlock.chicago.txt
	         (2) paths_sherlock.fanfic.txt
	         (3) paths_sherlock.chadwyck_poetry.txt
	         (4) paths_sherlock.chadwyck.1600_1900.txt
	         (5) paths_ryan.chadwyck.1600_1900.txt
	         (6) paths_sherlock.dime-westerns.txt
	>>

Now we provide a list of files to slingshot at (a **path** or **pathlist**). We can do this either by:

* Typing a number for a pre-defined pathlist (those found in the default pathlist folder).
* Typing out the path to a file (hit tab for autocomplete, double-tap tab to list files). This file must have one absolute path per line, nothing more.
* Typing out the path to a directory (hit tab for autocomplete), and then supplying a file extension (e.g. “txt”, “xml”): in this case, the directory will be recursively searched, and any file matching that extension will be included in the list of filepaths.

That’s all we need! The other options are optional:

	OPTIONAL SECTION

	>> SBATCH: Add to the SLURM/Sherlock process queue via sbatch? [N]
	>> (Y/N)
	
	>> DEBUG: Do not run on MPI and do not submit with sbatch? [N]
	>> (Y/N)
	
	>> SAVE: Save results? [Y]
	>> (Y/N)
	
	>> SAVEDIR: Directory to store results in [results_slingshot/prosodic_parser/parse_chadwyck]
	>>
	
	>> CACHE: Cache partial results? [Y]
	>> (Y/N)
	
	>> QUIET: Print nothing to screen? [N]
	>> (Y/N)
	
	>> LIMIT: Limit the number of paths to process to this number [None]
	>>

If we hit enter the rest of the way, this is what will happen:

* MPI will run the default number of CPUs [4] to accomplish applying the provided function to all the texts included in the pathlist. Outputs will be printed to screen.
* A new folder will be created in your current working directory, with the name results_slingshot/[sling]/[stone]. In that folder is output.txt, which is a log of the output printed to screen; and cmd.txt, which is the actual command that the interactive slingshot created.
* Eventually, when the process is completed, we will also see a **results.json** and a **results.txt**. These represent the total result of the process, collected together and indexed by the original path.

### Results files

#### results.json

We should now also have two results files in the folder: results.json, which is a JSON file which looks like this:

	[
	["/...blah.../00022180.txt", {"count": 310725}],
	["/...blah.../00004615.txt", {"count": 70321}],
	["/...blah.../00021819.txt", {"count": 88483}],
	]

#### results.txt

And results.txt, which has the same data but formatted as a TSV file. It reads:

	_path	count
	/...blah.../00022180.txt	310725
	/...blah.../00004615.txt	70321
	/...blah.../00021819.txt	88483

But results.txt is different in one regard: to make results.txt, a tab-separated dataframe with (in this case) words as columns and texts as rows, we need to prune the number of columns, otherwise we’d have millions of them, and the file would become fat with empty cells (tab characters). By default, slingshot will limit the columns to the N most frequently found present in the data (in this case the N most frequent words). N can be set using the >> MFW prompt; it defaults to 10,000.