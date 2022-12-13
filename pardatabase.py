#!/usr/bin/python3

# A deduplicating hash and par2 table to verify and repair a directory even when files are changed.

import os
import sys
import time
import shutil
import signal
import hashlib
import subprocess
from time import perf_counter as tpc
from hexbase import HexBase
from sd.file_progress import FileProgress, tprint
from sd.easy_args import easy_parse
from sd.common import fmt_time, rfs, ConvertDataSize


# Run self with ionice if available
try:
    import psutil
    psutil.Process().ionice(psutil.IOPRIO_CLASS_IDLE)
except ModuleNotFoundError:
    print("Install psutil with: pip3 install psutil")
    print("to automatically reduce the io impact of this program.\n\n")


def qprint(*args, **kargs):
    "Hide printing if user specifies --q"
    if not UARGS['quiet']:
        print(*args, **kargs)


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
    ['quiet', '', bool],
    "Don't print so much",
    ['hash', '', str, 'sha512'],
    "Hash function to use",
    ['clean', '', bool],
    "Delete old unused .par2 files from the database.",

    ['min', '', str],
    '''
    Minimum file size to produce par2 files
    Example: --min 4k
    ''',
    ['max', '', str],
    '''
    Maximum file size to produce par2 files
    Example: --max 1G
    ''',

    ['minscan', '', str],
    '''
    Minimum file size to scan
    Example: --min 4k
    ''',
    ['maxscan', '', str],
    '''
    Maximum file size to scan
    Example: --max 1G
    ''',
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
                      description='Create a database of .par2 files for a directory.')

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
    for arg in 'min max minscan maxscan'.split():
        if args[arg]:
            args[arg] = ConvertDataSize()(args[arg])

    return args


def get_hash(path):
    "Hash a file and optionall sleep for delay * read_time"
    delay = UARGS['delay']
    if not delay:
        return DATABASE.get_hash(path)
    else:
        start = tpc()
        result = DATABASE.get_hash(path)
        delay = (tpc() - start) * delay
        tprint("Sleeping for...", fmt_time(delay))
        time.sleep(delay)
        return result



class Info:
    "Info on filepaths within system"

    def __init__(self, pathname=None, load=None):
        self.tmpname = '.pardatabase_tmp_file'          # Temporary file used when generating .par2

        if load:
            # Load from json dict
            for key, val in load.items():
                setattr(self, key, val)
        else:
            self.pathname = pathname        # Relative path (can be changed between runs)
            self.hash = None
            self.mtime = None
            self.size = None
            self.update()

    def tojson(self,):
        "Return neccesary variables as compact json dict."
        out = dict(vars(self,))
        out.pop('tmpname')
        return out



    def fullpath(self,):
        "Return full path of self.pathname"
        return os.path.join(UARGS['target'], self.pathname)

    def verify(self,):
        "Verify file is correct or generate new one"
        return bool(self.hash == get_hash(self.fullpath()))


    def update(self,):
        "Update file size and mtime"
        self.mtime = os.path.getmtime(self.fullpath())
        self.size = os.path.getsize(self.fullpath())


    def rehash(self,):
        "Rehash the file"
        self.hash = get_hash(self.fullpath())


    def repair(self):
        "Attempt to repair a file with the files found in the database"
        if self.hash not in DATABASE.pfiles:
            print("No par2 files found for:", rel_path(self.fullpath()))
            return False

        cwd = os.path.dirname(self.fullpath())
        dest_files = DATABASE.get(self.hash, cwd)
        if dest_files:
            cmd = "par2 repair".split() + [sorted(dest_files)[0]] + [self.fullpath()]

            print(' '.join(cmd))
            ret = subprocess.run(cmd, check=False, cwd=cwd)
            status = not ret.returncode
            if status:
                for file in dest_files:
                    os.remove(file)
            return status
        else:
            return False

    def find_tmp(self, cwd):
        "Find existing par2 tmp files"
        for name in os.listdir(cwd):
            if name.endswith('.par2') and name.startswith(self.tmpname):
                yield name

    def remove_existing(self, cwd):
        "Delete leftover .par2 files"
        for name in self.find_tmp(cwd):
            qprint("Removing existing par2 file:", rel_path(os.path.join(cwd, name)))
            os.remove(os.path.join(cwd, name))


    def generate(self, sequential=False):
        '''Generate par2 or find existing, return True on new files
            quiet = Be less verbose
            sequential = Hash the file first, before running par2 (instead of in parallel)
        '''
        cwd = os.path.dirname(self.fullpath())
        ret = None
        old_name = self.pathname                        # Original base filename
        new_name = old_name                             # Modified name
        if UARGS['singlecharfix'] and len(old_name) == 1:
            new_name = os.path.join(os.path.dirname(os.path.basename(self.pathname)),
                                    old_name + '.pardatabase.tmp.rename')


        def interrupt(*_):
            print("\n\nCaught ctrl-c!")
            rename(new_name, old_name)
            if ret:
                ret.kill()
            print("Saving database, please wait...")
            save_database()
            self.remove_existing(cwd)
            print("Done")
            sys.exit(0)

        def run_par2():
            "Run par2 command"
            self.remove_existing(cwd)
            cmd = "par2 create -n1 -qq".split()
            options = UARGS['options']
            if options:
                cmd.extend(('-'+options).split())
            cmd += ['-a', self.tmpname + '.par2', '--', new_name]
            return subprocess.Popen(cmd, stdout=subprocess.DEVNULL, cwd=cwd)

        def rename(old, new, verbose=False):
            "Swap name old for new"
            if old != new:
                os.rename(old, new)
                self.pathname = new
                if verbose:
                    print("File name restored:", new)

        # Finish before get_hash in sequential mode or run in parallel
        signal.signal(signal.SIGINT, interrupt)     # Catch Ctrl-C
        rename(old_name, new_name)                  # Fix 1 char filenames (if needed)
        if sequential:
            self.hash = get_hash(self.fullpath())
            if self.hash in DATABASE.pfiles:
                rename(new_name, old_name)
                return False, []
            code = run_par2().wait()
        else:
            ret = run_par2()
            self.hash = get_hash(self.fullpath())
            if self.hash in DATABASE.pfiles:
                ret.terminate()
                self.remove_existing(cwd)
                rename(new_name, old_name)
                return 'PARALLEL_EARLY_QUIT', []
            else:
                code = ret.wait()
        rename(new_name, old_name)                  # Swap name back
        signal.signal(signal.SIGINT, lambda *args: sys.exit(1))

        # File read error or par2 error
        if code:
            print("par2 returned code:", code)
        if not self.hash or code:
            return False, []

        return True, [os.path.join(cwd, name) for name in self.find_tmp(cwd)]


class NewPars:
    "New parity files to create"

    def __init__(self, minimum, maximum):
        self.new_pars = []                  # Files to calculate .par2
        self.data2process = 0               # Data left to process into .par2
        self.minimum = minimum or 1         # Minimum size to parity is at least 1 byte
        self.maximum = maximum or None


    def gen_pars(self, sequential=False):
        "Rehash files and Generate new .par2 files"

        if not self.new_pars:
            return False

        print("\nCreating parity for", len(self.new_pars), 'files spanning', rfs(self.data2process))


        last_save = time.time() # Last time database was saved
        fp = FileProgress(len(self.new_pars), self.data2process)
        results = []
        for count, info in enumerate(self.new_pars):

            if info.hash and info.hash in DATABASE.pfiles:
                qprint("Found existing par2 for:", info.pathname)
                return 0

            # + = multi - = sequential     '+-'[sequential],
            tprint("File", fp.progress(info.size)['default'], ':', info.pathname)

            status, files = info.generate(sequential=sequential)
            results.append(status)

            for name in files:
                # total_size += os.path.getsize(name)
                DATABASE.put(name, info.hash, name=os.path.basename(name.replace(info.tmpname, '')))


            if not sequential and results[-5:] == ['PARALLEL_EARLY_QUIT'] * 5:
                print("Too many files with existing .par2... switch to sequential mode.")
                sequential = True


            # Save every hour
            if not count % 10 and time.time() - last_save >= 3600:
                save_database()
                last_save = time.time()

            if len(results) > 5:
                results.pop(0)

        tprint("Done. Processed", fp.done()['msg'])
        return True

    def new(self, info):
        "Do user arguments allow parity to be created for file?"
        size = info.size
        if self.minimum and size < self.minimum:
            return False
        if self.maximum and size > self.maximum:
            return False

        self.new_pars.append(info)
        self.data2process += size
        return True



def walk(dirname, exclude, minimum=1, maximum=0):
    "Walk through directory returning entry and pathname"
    minimum = max(minimum, 1)       # Min size must be 1 byte to work
    for entry in os.scandir(dirname):
        if entry.is_symlink():
            continue
        pathname = os.path.join(dirname, entry.name)
        if pathname.endswith('.par2'):
            continue
        if not os.access(pathname, os.R_OK):
            print("Could not access", pathname)
            continue
        if entry.is_dir():
            if not pathname == exclude:
                yield from walk(pathname, exclude, minimum, maximum)

        else:
            stat = entry.stat()
            size = stat.st_size
            if size < minimum:
                continue
            if maximum and size > maximum:
                continue
            yield stat, pathname



def repair(files, name):
    "Given a filename, attempt a repair"

    if not os.path.exists(name):
        print("Error, Can't find filename:", name)
        return False

    if name not in files:
        print("Error, No record of filename in database.")
        return False

    info = files[name]
    print("Attempting repair...")
    if info.repair():
        info.rehash()
        info.update()
        print("File fixed!\n\n")
        return True
    return False


def verify(files):
    "Verify files in directory"
    file_errors = []

    fp = FileProgress(len(files), sum(info.size for info in files.values()))
    for relpath, info in files.items():
        fullpath = info.fullpath()
        if not os.path.exists(fullpath):
            continue
        tprint(fp.progress(filename=fullpath)['default'])

        if not info.verify():
            print("\n\nError in file!", relpath)
            file_errors.append(relpath)
        else:
            info.update()
    tprint("Done. Hashed", fp.done()['msg'])
    return file_errors


def rel_path(pathname):
    "Return path of files relative to target directory"
    return os.path.relpath(pathname, UARGS['target'])



def cleaner(files, visited):
    '''Clean database of non existant files, assumes that every file has actually been tried'''

    # Build list of hashes to info (could be multiples)
    hashes = dict()
    for info in files.values():
        hashes.setdefault(info.hash, []).append(info)

    # Look for hashes in the database that no longer correspond to .par2 files and delete those par2
    deleted = 0
    for pathname in list(files.keys()):
        fullpath = os.path.join(UARGS['target'], pathname)
        if pathname not in visited and not os.path.exists(fullpath):
            info = files[pathname]
            hashes[info.hash].remove(info)
            print('\nRemoving reference for', pathname)
            if not hashes[info.hash]:
                deleted += DATABASE.clean(info.hash)
            del files[pathname]
    if deleted:
        print(deleted, 'files removed from database')


def save_database():
    out = dict()
    for key, val in DATABASE.data.items():
        out[key] = val.tojson()
    DATABASE.save(out)


def load_database():
    '''Load the database'''
    DATABASE.load()
    files = DATABASE.data
    if files:
        print("Sucessfully loaded info on", len(files), 'files')
        print("Database was last saved", fmt_time(time.time() - DATABASE.last_save), 'ago.')

    for pathname, info in files.items():
        files[pathname] = Info(load=info)

    return files

def database_upgrade():
    "Fix databases made with version 1.0"

    if DATABASE.version < 1.1:
        files = DATABASE.data
        print("Upgrading old database 1.0 -> 1.1")
        for info in files.values():
            pathname = info.pathname
            relpath = rel_path(pathname)
            info.pathname = relpath
            # print(vars(info))
        DATABASE.version = 1.1


def main():

    files = load_database()         # relative filename to Info
    visited = []                    # File names visited
    start_time = tpc()
    newpars = NewPars(UARGS['min'], UARGS['max'])   # Files to parity


    # Repair single file and quit, if requested
    if UARGS['repair']:
        return repair(files, UARGS['repair'])



    # Walk through file tree looking for files that need to be processed
    print("\nScanning file tree:", UARGS['target'])
    minimum = UARGS['minscan'] or 1
    maximum = UARGS['maxscan'] or 0
    for stat, pathname in walk(UARGS['target'], exclude=DATABASE.basedir, minimum=minimum, maximum=maximum):
        relpath = rel_path(pathname)
        visited.append(relpath)
        mtime = stat.st_mtime
        if relpath in files:
            info = files[relpath]
            info.pathname = relpath
            if mtime > info.mtime or not info.hash:
                newpars.new(info)
        else:
            info = Info(relpath)
            newpars.new(info)
        files[relpath] = info
        # print(relpath, vars(info))
    print("Done. Scanned", len(visited), 'files in', fmt_time(tpc() - start_time))


    # Check for files deleted from database
    if UARGS['clean']:
        if UARGS['minscan'] or UARGS['maxscan']:
            # If a file changed name and wasn't scanned by walk it could get dereferenced.
            print("\n\nCan't clean database in restricted mode.")
            print("Please run pardatabase.py --clean without additional arguments")
        else:
            print("\n\nRunning database cleaner...")
            cleaner(files, visited)
            save_database()
    else:
        newpars.gen_pars(sequential=UARGS['sequential'])


    # Verify files in database
    if UARGS['verify']:
        print('\nChecking .par2 files in database:')
        DATABASE.verify()


    # Look for files with errors
    file_errors = []                # List of files with errors in them
    if UARGS['verify']:
        print('\nVerifying hashes of all files in directory:')
        file_errors = verify(files)


    # Look for files with ioerror hashes
    for filename, info in list(files.items()):
        if info.hash == 'ioerror':
            file_errors.append(filename)
            del files[filename]

    if file_errors:
        print("\n\nWARNING! THE FOLLOWING FILES HAD ERRORS:")
        print('\n'.join(file_errors))

    save_database()
    if file_errors:
        sys.exit(7)
    sys.exit(0)


# Future ideas:
# Save files under 4k as a copy instead of using .par2
# Store files under 10k in sqlite instead of on disk


if __name__ == "__main__":
    UARGS = parse_args()            # User argument
    if not UARGS:
        sys.exit(1)
    DATABASE = HexBase(basedir=UARGS['basedir'])

    if not shutil.which('par2'):
        print("Please install par2 to continue")
        sys.exit(1)

    main()
