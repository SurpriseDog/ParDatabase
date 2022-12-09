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
    Minimum file size to check
    Example: --min 4k
    ''',
    ['max', '', str],
    '''
    Maximum file size to check
    Example: --max 1G
    ''',
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
    ['repair', '', bool],
    "Verify and repair existing files",
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
    for arg in 'min max'.split():
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




def find_tmp(cwd, basename=''):
    "Find existing par2 tmp files"
    for name in os.listdir(cwd):
        if name.endswith('.par2') and name.startswith(basename):
            yield name


class Info:
    "Info on filepaths within system"

    def __init__(self, pathname=None, load=None):
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
            cmd = "par2 repair".split() + [sorted(dest_files)[0]]

            print(' '.join(cmd))
            ret = subprocess.run(cmd, check=False, cwd=cwd)
            status = not ret.returncode
            if status:
                for file in dest_files:
                    os.remove(file)
            return status
        else:
            return False


    def generate(self, quiet=False, rehash=False, sequential=False):
        "Generate par2 or find existing"

        if self.hash and self.hash in DATABASE.pfiles:
            if not quiet:
                qprint("Found existing par2 for:", self.pathname)
            return 0
        else:


            cwd = os.path.dirname(self.fullpath())
            basename = '.pardatabase_tmp_file'
            ret = None

            def remove_existing():
                "Delete leftover .par2 files"
                for name in find_tmp(cwd):
                    qprint("Removing existing par2 file:", rel_path(os.path.join(cwd, name)))
                    os.remove(os.path.join(cwd, name))

            def check_hash():
                "Check the hash, return True if par2 already exists."
                if rehash or not self.hash:
                    self.hash = get_hash(self.fullpath())
                if self.hash in DATABASE.pfiles:
                    return True
                return False

            def interrupt(*_):
                print("\n\nCaught ctrl-c!")
                if ret:
                    ret.kill()
                print("Saving database, please wait...")
                DATABASE.save()
                remove_existing()
                print("Done")
                sys.exit(0)

            def run_par2():
                "Run par2 command"
                remove_existing()
                cmd = "par2 create -n1 -qq".split()
                options = UARGS['options']
                if options:
                    cmd.extend(('-'+options).split())
                cmd += ['-a', basename + '.par2', '--', os.path.basename(self.fullpath())]
                return subprocess.Popen(cmd, stdout=subprocess.DEVNULL, cwd=cwd)


            signal.signal(signal.SIGINT, interrupt)


            # Finish before get_hash in sequential mode or run in parallel
            if sequential:
                if check_hash():
                    return 0
                code = run_par2().wait()
            else:
                ret = run_par2()
                if check_hash():
                    ret.terminate()
                    remove_existing()
                    return 0
                else:
                    code = ret.wait()
            signal.signal(signal.SIGINT, lambda *args: sys.exit(1))

            # File read error or par2 error
            if code:
                print("par2 returned code:", code)
            if not self.hash or code:
                return 0

            # Move files into database:
            total_size = 0                          # Running count of total .par2 size
            for name in find_tmp(cwd, basename):
                src = os.path.join(cwd, name)
                total_size += os.path.getsize(src)
                DATABASE.put(src, self.hash, name=name.replace(basename, ''))
            return total_size



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



def gen_pars(new_pars, data2process, sequential=True):
    "Rehash files and Generate new .par2 files"
    last_save = time.time() # Last time database was saved
    fp = FileProgress(len(new_pars), data2process)
    for count, info in enumerate(new_pars):

        # Use sequential mode if asked or just for small files
        if sequential:
            seq = True
        elif info.size < 1e6:
            seq = True
        else:
            seq = False

        # + = multi - = sequential
        tprint('+-'[seq], "File", fp.progress(info.size)['default'], ':', info.pathname)
        info.generate(rehash=True, sequential=seq)

        # Save every hour
        if not count % 10 and time.time() - last_save >= 3600:
            DATABASE.save()
            last_save = time.time()

    tprint("Done. Processed", fp.done()['msg'])


def verify(files, repair=False):
    "Verify and repair files in directory"
    file_errors = []

    fp = FileProgress(len(files), sum(info.size for info in files.values()))
    for relpath, info in files.items():
        tprint(fp.progress(filename=info.fullpath())['default'])

        if not info.verify():
            print("\n\nError in file!", relpath)
            file_errors.append(relpath)
            if repair:
                print("Attempting repair...")
                if info.repair():
                    info.rehash()
                    info.update()
                    file_errors.pop(-1)
                    print("File fixed!\n\n")
        else:
            info.update()
    tprint("Done. Hashed", fp.done()['msg'])
    return file_errors


def rel_path(pathname):
    "Return path of files relative to target directory"
    return os.path.relpath(pathname, UARGS['target'])

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

    # Load the database
    DATABASE.load()
    files = DATABASE.data                       # relative filename to Info
    if files:
        print("Sucessfully loaded info on", len(files), 'files')
        print("Database was last saved", fmt_time(time.time() - DATABASE.last_save), 'ago.')

    for pathname, info in files.items():
        files[pathname] = Info(load=info)

    visited = []                    # File names visited
    start_time = tpc()
    new_pars = []                   # Files to calculate .par2
    data2process = 0                # Data left to process into .par2


    # Walk through file tree looking for files that need to be processed
    print("\nScanning file tree:", UARGS['target'])
    minimum = UARGS['min'] or 1
    maximum = UARGS['max'] or 0
    for stat, pathname in walk(UARGS['target'], exclude=DATABASE.basedir, minimum=minimum, maximum=maximum):
        relpath = rel_path(pathname)
        visited.append(relpath)
        mtime = stat.st_mtime
        if relpath in files:
            info = files[relpath]
            info.pathname = relpath
            if mtime > info.mtime or not info.hash:
                new_pars.append(info)
                data2process += info.size
        else:
            info = Info(relpath)
            new_pars.append(info)
            data2process += info.size
        files[relpath] = info
    print("Done. Scanned", len(visited), 'files in', fmt_time(tpc() - start_time))


    # Rehash and generate .par2 files for files found earlier
    if new_pars:
        print("\nCreating parity for", len(new_pars), 'files spanning', rfs(data2process))
        if UARGS['sequential']:
            seq = True
        # Default to seqential mode if not a lot has changed:
        elif len(new_pars) / len(visited) < 0.1:
            seq = True
        else:
            seq = False
        gen_pars(new_pars, data2process, sequential=seq)



    # Check for files deleted from database
    if UARGS['clean']:
        DATABASE.clean()
        for pathname in list(files.keys()):
            if pathname not in visited:
                qprint("Filename not found in folder:", pathname)
                del files[pathname]


    # Verify files in database
    if UARGS['verify']:
        print('\nChecking .par2 files in database:')
        DATABASE.verify()


    # Look for files with errors
    file_errors = []                # List of files with errors in them
    if UARGS['verify'] or UARGS['repair']:
        print('\nVerifying hashes of all files in directory:')
        file_errors = verify(files, repair=UARGS['repair'])


    # Look for files with missing hashes (caused by io errors while reading
    for filename, info in list(files.items()):
        if not info.hash:
            file_errors.append(filename)
    if file_errors:
        print("\n\nWARNING! THE FOLLOWING FILES HAD ERRORS:")
        print('\n'.join(file_errors))

    DATABASE.save()
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
