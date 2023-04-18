#!/usr/bin/python3

# A deduplicating hash and par2 table to verify and repair a directory even when files are changed.

import os
import sys
import time
import shutil
import hashlib


from database import Database
from sd.easy_args import easy_parse
from sd.common import ConvertDataSize


# Run self with ionice if available
try:
    import psutil
    psutil.Process().ionice(psutil.IOPRIO_CLASS_IDLE)
except ModuleNotFoundError:
    pass


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


    args = [\
    ['basedir', '', str],
    "Base directory to put the par2 database, (Defaults to the target directory)",
    # ['quiet', '', bool],
    # "Don't print so much",
    # ['hash', '', str, 'sha512'],
    # "Hash function to use",
    ['clean', '', bool],
    "Delete old unused .par2 files from the database.",

    ['min', 'minpar', str, '4k'],
    '''
    Minimum file size to produce par2 files
    Example: --min 4k
    ''',
    ['max', 'maxpar', str],
    '''
    Maximum file size to produce par2 files
    Example: --max 1G
    ''',

    ['minscan', '', str],
    '''
    Minimum file size to scan
    Example: --minscan 4k
    ''',
    ['maxscan', '', str],
    '''
    Maximum file size to scan
    Example: --maxscan 1G
    ''',
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
    ]

    args = easy_parse(args,
                      positionals,
                      usage='<Target Directory>, options...',
                      description='Create a database of .par2 files for a directory')

    args = vars(args)

    # Choose hashing algorithm
    hashes = sorted(list(hashlib.algorithms_guaranteed))
    if args['hash'] not in hashes:
        print("Available hashes are:", hashes)
        return False


    # Verify that basedir and target dir are okay
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
    for arg in 'minpar maxpar minscan maxscan'.split():
        if args[arg]:
            args[arg] = ConvertDataSize()(args[arg])

    return args


def main():
    uargs = parse_args()            # User arguments
    if not uargs:
        return False

    os.nice(uargs['nice'])
    db = Database(uargs['basedir'], uargs['target'],)
    db.delay = uargs['delay'] if uargs['delay'] else db.delay

    # Make sure the database exists for key operations
    if any([uargs[key] for key in uargs if key in ('repair', 'verify', 'clean')]):
        if not db.files:
            print("There is no pardatabase for", db.target)
            print("Run again without the '--verify' to generate one.")
            return False

    if uargs['repair']:
        # Repair single file and quit, if requested
        return db.repair(uargs['repair'])

    if uargs['verify']:
        # Verify files in database
        return db.verify()

    if uargs['clean']:
        # Check for files deleted from database
        print("\n\nRunning database cleaner...")
        db.cleaner()
        db.save()
        print("Done.")
        return True


    # Walk through file tree looking for files that need to be processed
    newpars, newhashes = db.scan(minscan=uargs['minscan'],
                                 maxscan=uargs['maxscan'],
                                 minpar=uargs['minpar'],
                                 maxpar=uargs['maxpar'],
                                 )
    if newpars and newhashes:
        print("\nBased on the options selected:")
        print(len(newhashes), "files will be hashed without parity and")
        print(len(newpars), "files will be both hashed and have parity files created")

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
