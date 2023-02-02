#!/usr/bin/python3

# A deduplicating hash and par2 table to verify and repair a directory even when files are changed.

import os
import sys
import time
import signal
import shutil
import hashlib
from time import perf_counter as tpc

from info import Info
from hexbase import HexBase

from sd.easy_args import easy_parse
from sd.file_progress import FileProgress, tprint
from sd.common import ConvertDataSize, fmt_time, rfs


# Run self with ionice if available
try:
    import psutil
    psutil.Process().ionice(psutil.IOPRIO_CLASS_IDLE)
except ModuleNotFoundError:
    print("Install psutil with: pip3 install psutil")
    print("to automatically reduce the io impact of this program.\n\n")


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
    ['hash', '', str, 'sha512'],
    "Hash function to use",
    ['clean', '', bool],
    "Delete old unused .par2 files from the database.",

    ['min', 'minpar', str],
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


def walk(dirname, exclude, minimum=1, maximum=None):
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



class Database:
    "Database of files, hashes and their par2 files"

    def __init__(self, basedir, target,):

        # Base directory to put the .pardatabase files
        self.basedir = os.path.join(basedir, '.pardatabase')

        self.target = target                # Target directory to scan
        self.hexbase = HexBase(self.basedir)
        self.files = self.load()            # relative filename to Info
        self.visited = []                   # List of files visited by scanner
        self.delay = None                   # Delay after hashing
        self.new_pars = []                  # Files to calculate .par2


    def load(self,):
        '''Load the database'''
        self.hexbase.load()
        files = self.hexbase.data
        if files:
            print("Database was last saved", fmt_time(time.time() - self.hexbase.last_save), 'ago')
            print("Sucessfully loaded info on", len(files), 'files')
        for pathname, info in files.items():
            files[pathname] = Info(load=info, base=self.target)
        return files


    def save(self,):
        out = dict()
        for key, val in self.hexbase.data.items():
            out[key] = val.tojson()
        self.hexbase.save(out)


    def scan(self, minscan=None, maxscan=None,):
        '''
        Walk through file tree looking for files that need to be processed
        minscan         = Minimum file size to scan
        maxscan         = Maximum file size to scan
        '''
        minscan = minscan or 1
        maxscan = maxscan or 0


        start_time = tpc()
        visited = []
        print("\nScanning file tree:", self.target)
        for stat, pathname in walk(self.target, exclude=self.basedir, minimum=minscan, maximum=maxscan):
            relpath = self.rel_path(pathname)
            visited.append(relpath)
            mtime = stat.st_mtime
            if relpath in self.files:
                info = self.files[relpath]
                info.pathname = relpath
                if mtime > info.mtime or not info.hash or info.hash not in self.hexbase.pfiles:
                    self.new_pars.append(info)
            else:
                info = Info(relpath, base=self.target)
                self.new_pars.append(info)
            self.files[relpath] = info
        print("Done. Scanned", len(visited), 'files in', fmt_time(tpc() - start_time))
        self.visited.extend(visited)


    def get_hash(self, path):
        "Hash a file and optional sleep for delay * read_time"
        if not self.delay:
            return self.hexbase.get_hash(path)
        else:
            start = tpc()
            result = self.hexbase.get_hash(path)
            delay = (tpc() - start) * self.delay
            tprint("Sleeping for...", fmt_time(delay))
            time.sleep(delay)
            return result


    def rel_path(self, pathname):
        "Return path of files relative to target directory"
        return os.path.relpath(pathname, self.target)


    def fullpath(self, pathname):
        "Return full path of relative filename."
        return os.path.join(self.target, pathname)


    def cleaner(self,):
        '''Clean database of non existant files, assumes that every file has actually been tried'''

        # Build list of hashes to info (could be multiples)
        hashes = dict()
        for info in self.files.values():
            hashes.setdefault(info.hash, []).append(info)

        # Look for hashes in the database that no longer correspond to .par2 files and delete those par2
        deleted = 0
        for pathname in list(self.files.keys()):
            if pathname not in self.visited and not os.path.exists(self.fullpath(pathname)):
                info = self.files[pathname]
                hashes[info.hash].remove(info)
                print('\nRemoving reference for', pathname)
                if not hashes[info.hash]:
                    deleted += self.hexbase.clean(info.hash)
                del self.files[pathname]
        if deleted:
            print(deleted, 'files removed from database')


    def verify(self,):
        "Verify files in directory"

        # Look for files with errors
        file_errors = []                # List of files with errors in them
        print('\nVerifying hashes of all files referenced in database:')

        fp = FileProgress(len(self.files), sum(info.size for info in self.files.values()))
        missing = 0         # Files with missing hashes
        for relpath, info in self.files.items():
            if not info.hash:
                missing += 1
                continue

            fullpath = info.fullpath
            if not os.path.exists(fullpath):
                continue

            tprint(fp.progress(filename=fullpath)['default'] + ':', relpath)

            if info.hash != self.get_hash(info.fullpath):
                print("\n\nError in file!", relpath)
                file_errors.append(relpath)
        tprint("Done. Hashed", fp.done()['msg'])
        print()

        if missing:
            print('\n')
            print(missing, 'files had no hash in the database')
            print("Run pardatabase without the --verify to add them.")

        print('\nChecking .par2 files in database:')
        self.hexbase.verify()
        return not bool(file_errors)


    def repair(self, name):
        "Attempt to repair a file with the files found in the database"

        if os.path.isabs(name):
            name = self.rel_path(name)

        if not os.path.exists(self.fullpath(name)):
            print("Error, Can't find filename:", name)
            return False

        if name not in self.files:
            print("Error, No record of filename in database.")
            print(self.files)
            return False

        print("\nAttempting repair on", name)
        info = self.files[name]
        dest_files = self.hexbase.get(info.hash, info.cwd)
        for file in dest_files:
            print("Found parity file:", file)
        if not dest_files:
            print("No par2 files found for:", name)
            return False

        if info.repair(dest_files):
            info.hash = self.get_hash(info.fullpath)
            info.update()
            print("File fixed!\n\n")
            return True
        return False



    def gen_pars(self, minpar=None, maxpar=None, sequential=False, singlecharfix=False, par2_options=None):
        '''Rehash files and Generate new .par2 files
        sequential      = Run in sequential mode (generate hash first, then parity)
        minpar          = Minimum file size to parity
        maxpar          = Maximum file size to parity
        singlecharfix   = Rename files before running par2
        par2_options    = Passed onto par2 program
        '''
        minpar = minpar or 1    # Minimum size to parity

        newpars = []            # Files to process that meet reqs
        newhashes = []

        for info in self.new_pars:
            size = info.size
            if size < minpar or (maxpar and size > maxpar):
                if not info.hash:
                    newhashes.append(info)
            else:
                newpars.append(info)

        if newpars and newhashes:
            print("\nBased on the options selected:")
            print(len(newhashes), "files will be hashed without parity and")
            print(len(newpars), "files will be both hashed and have parity files created.")

        if newhashes:
            data2process = sum(info.size for info in newhashes)
            fp = FileProgress(len(newhashes), data2process)
            print("\nCreating only hashes for",
                  len(newhashes), 'files spanning', rfs(data2process))
            for info in newhashes:
                size = info.size
                tprint("File", fp.progress(size)['default'] + ':', info.pathname)
                info.hash = self.get_hash(info.fullpath)
            tprint("\nDone. Processed", fp.done()['msg'])
            print()

        if not newpars:
            return False

        data2process = sum(info.size for info in newpars)
        print("\nCreating parity and hashes for",
              len(newpars), 'files spanning', rfs(data2process))

        last_save = time.time() # Last time database was saved
        fp = FileProgress(len(newpars), data2process)
        results = []
        for count, info in enumerate(newpars):
            # + = multi - = sequential     '+-'[sequential],
            size = info.size
            tprint("File", fp.progress(size)['default'] + ':', info.pathname)

            status, files = self.generate(info, sequential, singlecharfix, par2_options)
            results.append(status)

            for number, name in enumerate(files):
                # total_size += os.path.getsize(name)
                self.hexbase.put(name, info.hash, '.' + str(number) + '.par2')


            if not sequential and results[-5:] == ['PARALLEL_EARLY_QUIT'] * 5:
                print("Too many files with existing .par2... switch to sequential mode.")
                sequential = True


            # Save every hour
            if not count % 10 and time.time() - last_save >= 3600:
                self.save()
                last_save = time.time()

            if len(results) > 5:
                results.pop(0)

        tprint("\nDone. Processed", fp.done()['msg'])
        print()
        return True


    def generate(self, info, sequential=False, singlecharfix=False, par2_options=None):
        '''Generate par2 or find existing, return True on new files
            sequential = Hash the file first, before running par2 (instead of in parallel)
            singlecharfix = Temporarily replace single character file names
            par2_options = Options for par2 command
        '''
        ret = None
        old_name = info.fullpath                        # Original base filename
        new_name = old_name                             # Modified name
        if singlecharfix and len(os.path.basename(old_name)) == 1:
            new_name = old_name + '.pardatabase.tmp.rename'

        def interrupt(*_):
            print("\n\nCaught ctrl-c!")
            rename(new_name, old_name)
            if ret:
                ret.kill()
            print("Saving database, please wait...")
            self.save()
            info.remove_existing()
            print("Done")
            sys.exit(0)


        def rename(old, new, verbose=False):
            "Swap name old for new"
            if old != new:
                os.rename(os.path.join(self.target, old), os.path.join(self.target, new))
                info.pathname = new
                if verbose:
                    print("File name restored:", new)

        # Finish before get_hash in sequential mode or run in parallel
        signal.signal(signal.SIGINT, interrupt)     # Catch Ctrl-C
        rename(old_name, new_name)                  # Fix 1 char filenames (if needed)
        if sequential:
            info.hash = self.get_hash(info.fullpath)
            if info.hash in self.hexbase.pfiles:
                rename(new_name, old_name)
                return False, []
            code = info.run_par2(par2_options, new_name).wait()
        else:
            ret = info.run_par2(par2_options, new_name)
            info.hash = self.get_hash(new_name)
            if info.hash in self.hexbase.pfiles:
                ret.terminate()
                info.remove_existing()
                rename(new_name, old_name)
                return 'PARALLEL_EARLY_QUIT', []
            else:
                code = ret.wait()
        rename(new_name, old_name)                  # Swap name back
        signal.signal(signal.SIGINT, lambda *args: sys.exit(1))

        # File read error or par2 error
        if code:
            print("par2 returned code:", code)
        if len(os.path.basename(old_name)) == 1 and not singlecharfix:
            print("Run the program with --singlecharfix or update par2 to fix these errors\n")
        if not info.hash or code:
            return False, []

        return True, info.find_tmp()


def main():
    uargs = parse_args()            # User arguments
    if not uargs:
        return False

    db = Database(uargs['basedir'], uargs['target'],)
    if uargs['delay']:
        db.delay = uargs['delay']

    # Repair single file and quit, if requested
    if uargs['repair']:
        return db.repair(uargs['repair'])

    # Verify files in database
    if uargs['verify']:
        db.scan(uargs['minscan'], uargs['maxscan'])
        return db.verify()

    # Check for files deleted from database
    if uargs['clean']:
        db.scan(uargs['minscan'], uargs['maxscan'])
        print("\n\nRunning database cleaner...")
        db.cleaner()
        db.save()
        return True


    # Walk through file tree looking for files that need to be processed
    db.scan(uargs['minscan'], uargs['maxscan'])
    # Generate new parity files
    db.gen_pars(minpar=uargs['minpar'], maxpar=uargs['maxpar'],
                sequential=uargs['sequential'],
                singlecharfix=uargs['singlecharfix'],
                par2_options=uargs['options'])

    db.save()
    return True




if __name__ == "__main__":
    if not shutil.which('par2'):
        print("Please install par2 to continue")
        sys.exit(1)
    sys.exit(not main())
