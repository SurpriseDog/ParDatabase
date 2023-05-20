#!/usr/bin/python3

# A deduplicating hash and par2 table to verify and repair a directory even when files are changed.

import os
import sys
import time
import shutil

import sd.tree as tree
import sd.easy_args as ea
from database import Database
from sd.format_number import rfs
from sd.cds import ConvertDataSize




# Run self with ionice if available
try:
    import psutil
    psutil.Process().ionice(psutil.IOPRIO_CLASS_IDLE)
except ModuleNotFoundError:
    pass


def sort_by_key(dic):
    '''Return dict values sorted by key name'''
    return [dic[key] for key in sorted(dic.keys())]

def parse_args():
    "Parse arguments"

    positionals = [\
    ["target", '', str, '.'],
    "Target Directory to generate par2",
    ]

    # Get rid of leading dash after --options
    flag = False
    for count, arg in enumerate(sys.argv):
        if arg.lower().startswith('--option'):
            flag = True
            continue
        if flag:
            sys.argv[count] = sys.argv[count].lstrip('-')


    # Main arguments including program mode
    myargs = [\
    ['basedir', '', str],
    "Base directory to put the par2 database, (Defaults to the target directory)",
    ['clean', '', bool],
    "Delete old unused .par2 files from the database.",
    ['dryrun', '', bool],
    "Show what action would be done without doing it.",
    ['nice', '', int, 8],
    "Run program with a nice level, 0=disable",
    ['singlecharfix', '', bool],
    "Temporarily rename files to fix a bug in par2.",
    "Clean database of extraneous files",
    ['options', '', str],
    '''Options passed onto par2 program, use quotes:
    For example "-r5" will set the target recovery percentage at 5%
    More options can be found by typing: man par2''',
    ['sequential', '', bool],
    "Hash the file before running par2 (instead of running in parallel)",
    ['delay', '', float],
    "Wait for (delay * read_time) after every read to keep drive running cooler.",
    ['verify', '', bool],
    "Verify existing files by comparing the hash",
    ['repair', '', str],
    "Repair an existing file",
    ['verbose', '', bool],
    "Useful for debugging.",
    ]


    # Parity Arguments
    parity_args = ea.dict_args([\
    ['minsize', 'min_size', str, '1M'],
    '''
    Minimum file size to produce par2 files. Par2 works best with larger files and can be extremely inefficient to the point of producing parity files bigger than the source with very small files. Since larger files are more likely to contain bad sectors, the minimum size is set to 1 megabyte by default. - Smaller files are still scanned and hashed unless the --minscan option is set otherwise.
    Example: --minsize 4K''',   # pylint: disable=C0301
    ['maxsize', 'max_size', str],
    '''
    Maximum file size to produce par2 files
    Example: --maxsize 1G
    ''',
    # ["skip_exts", '', list, ['.par2']],
    # '''
    # File extensions to ignore
    # By default, existing .par2 files are skipped.
    # '''
    ])

    # Overwrite tree_args defaults with some of my own
    parity_args = {**ea.dict_args(tree.TREE_ARGS), **parity_args}


    # Scan arguments with no limitations
    scan_args = ea.dict_args([\
    ['minsize', 'min_size', str, '1'],
    "Minimum scan size",
    ['maxsize', 'max_size', str],
    "Maximum scan size",
    '',
    ], hidden=True)
    scan_args = {**ea.dict_args(tree.TREE_ARGS, hidden=True), **scan_args}

    # Prepend scan args with the word 'scan'
    for arg in scan_args.values():
        arg['alias'] = 'scan' + arg['alias']
        if arg['varname']:
            arg['varname'] = 'scan' + arg['varname']


    # Parse!
    am = ea.ArgMaster(\
                      usage='<Target Directory>, options...',
                      description='Create a database of .par2 files for a directory',
                     )
    am.update(positionals, positionals=True, hidden=True)
    am.update(myargs, title="\nMain Arguments\n")
    am.update(argdicts=sort_by_key(parity_args),
              title='''\nParity Arguments: These determine which files have a parity created for them.\nTo apply these arguments to files that are scanned, but no parity made, you can add the word 'scan' to any argument.\nFor example: --scanmaxsize = 1G to limit scanning to files over 1 gigabyte.\n'''  # pylint: disable=C0301
              )
    am.update(argdicts=sort_by_key(scan_args), hidden=True)
    return vars(am.parse())


def fix_args(args):
    '''Verify that basedir and target dir are okay and convert user sizes'''
    basedir = args['basedir']
    target = args['target']

    if basedir:
        if not os.path.exists(basedir):
            print("Could not find path:", basedir)
            return False
        if not os.path.isdir(basedir):
            print("Base directory must be a folder")
            return False
    else:
        basedir = target

    if not os.path.isdir(target) or not os.access(basedir, os.R_OK):
        print("Target directory must be a readable folder")
        return False

    if not os.access(basedir, os.W_OK):
        print("Base directory must be writeable")
        return False

    args['basedir'] = os.path.realpath(basedir)
    args['target'] = os.path.realpath(target)


    # Convert user data sizes
    for arg in 'min_size max_size scanmin_size scanmax_size'.split():
        if args[arg]:
            args[arg] = ConvertDataSize()(args[arg])

    return args


def spanning(files):
    data2process = sum(info.size for info in files)
    return str(len(files)) + ' files spanning ' + rfs(data2process)


def main():
    uargs = parse_args()            # User arguments
    uargs = fix_args(uargs)
    if not uargs:
        return False

    def dryrun():
        if uargs['dryrun']:
            print("\nDryrun: No files hashed. No parity created.")
            sys.exit(0)

    os.nice(uargs['nice'])
    db = Database(uargs['basedir'], uargs['target'],)
    db.delay = uargs['delay'] if uargs['delay'] else db.delay

    # Make sure the database exists for key operations
    if any([uargs[key] for key in uargs if key in ('repair', 'verify', 'clean')]):
        if not db.files:
            print("There is no pardatabase for", db.target)
            print("Run again without any --options to generate one.")
            return False



    if uargs['repair']:
        # Repair single file and quit, if requested
        dryrun()
        return db.repair(uargs['repair'])

    if uargs['verify']:
        # Verify files in database
        dryrun()
        return db.verify()

    if uargs['clean']:
        # Check for files deleted from database
        dryrun()
        print("\n\nRunning database cleaner...")
        db.cleaner()
        db.save()
        print("Done.")
        return True


    # Walk through file tree looking for files that need to be processed
    scan_args = {key:uargs['scan' + key] for key in tree.Tree.default_args if 'scan' + key in uargs}
    parity_args = {key:uargs[key] for key in tree.Tree.default_args if key in uargs}

    # Always skip .par2 files and pardatabase folders
    for args in (scan_args, parity_args):
        args['skip_dirs'].append('.pardatabase')
        args['skip_exts'].append('.par2')

    # print('\nuargs', uargs)
    if uargs['verbose']:
        print('\nScan_args:', scan_args)
        print('\nParity_args:', parity_args)
    newpars, newhashes = db.scan(scan_args, parity_args)

    if (newpars and newhashes) or uargs['dryrun']:
        print("\nBased on the options selected:")
        print(spanning(newhashes), "will be hashed without parity")
        print(spanning(newpars), "will be both hashed and have parity files created")

    dryrun()
    # Hash files without creating parity
    if newhashes:
        db.gen_hashes(newhashes)

    # Generate new parity files
    if newpars:
        db.gen_pars(newpars,
                    sequential=uargs['sequential'],
                    singlecharfix=uargs['singlecharfix'],
                    par2_options=uargs['options'])

    if newhashes or newpars:
        db.save()
    return True


def super_main():
    "Wrapper for main"
    if not shutil.which('par2'):
        print("Please install par2 to continue")
        return False

    start_time = time.time()
    status = main()
    if status and 'psutil' not in sys.modules and time.time() - start_time > 60:
        print("\n\nInstall psutil with: pip3 install psutil")
        print("to automatically reduce the io impact of this program.")
    return status


if __name__ == "__main__":
    sys.exit(not super_main())
