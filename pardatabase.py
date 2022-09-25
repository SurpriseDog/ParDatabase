#!/usr/bin/python3

# A deduplicating hash and par2 table to verify and repair a directory even when files are changed.

import os
import sys
import time
import shutil
import hashlib
import subprocess
from time import perf_counter as tpc
from hexbase import HexBase
from sd.easy_args import easy_parse
from sd.common import fmt_time, rfs, sig, ConvertDataSize

def tprint(*args, newline=False, **kargs):
    '''
    Terminal print: Erasable text in terminal
    newline = True will leave the text on the line
    '''

    term_width = shutil.get_terminal_size()[0]      # 2.5 microseconds
    text = ' '.join(map(str, args))
    if len(text) > term_width:
        text = text[:term_width-3] + '...'

    # Filling out the end of the line with spaces ensures that if something else prints it will not be mangled
    print('\r' + text + ' ' * (term_width - len(text)), **kargs, end='' if not newline else '\n')


def oprint(*args, **kargs):
    tprint(*args, newline=True, **kargs)


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
    ['min', '', str, ''],
    '''
    Minimum file size to check
    Example: --min 4k
    ''',
    ['max', '', str, ''],
    '''
    Maximum file size to check
    Example: --max 1G
    ''',
    "Clean database of extraneous files",
    ['options', '', str],
    '''Options passed onto par2 program. Add quotes"
    For example "-r5" will set the target recovery percentage at 5%
    More options can be found by typing: man par2''',
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
    if args['min']:
        args['min'] = ConvertDataSize()(args['min'])
    if args['max']:
        args['max'] = ConvertDataSize()(args['max'])

    return args



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
            self.pathname = pathname        # absolute path (can be changed between runs)
            self.hash = None
            self.mtime = None
            self.size = None
            self.update()


    def verify(self,):
        "Verify file is correct or generate new one"
        return bool(self.hash == DATABASE.get_hash(self.pathname))


    def update(self,):
        "Update file size and mtime"
        self.mtime = os.path.getmtime(self.pathname)
        self.size = os.path.getsize(self.pathname)


    def rehash(self,):
        "Rehash the file"
        self.hash = DATABASE.get_hash(self.pathname)


    def repair(self):
        "Attempt to repair a file with the files found in the database"
        if self.hash not in DATABASE.pfiles:
            print("No par2 files found for:", rel_path(self.pathname))
            return False

        cwd = os.path.dirname(self.pathname)
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


    def generate(self, quiet=False, rehash=False):
        "Generate par2 or find existing"

        if self.hash and self.hash in DATABASE.pfiles:
            if not quiet:
                qprint("Found existing par2 for:", rel_path(self.pathname))
            return 0
        else:

            cwd = os.path.dirname(self.pathname)
            basename = '.pardatabase_tmp_file'

            # Delete leftover .par2 files
            for name in find_tmp(cwd):
                oprint("Overwriting existing par2 file:", rel_path(os.path.join(cwd, name)))
                time.sleep(.1)
                os.remove(os.path.join(cwd, name))

            # Run par2 command
            cmd = "par2 create -n1 -qq".split()
            options = UARGS['options']
            if options:
                cmd.extend(('-'+options).split())
            cmd += ['-a', basename + '.par2', '--', os.path.basename(self.pathname)]
            ret = subprocess.Popen(cmd, stdout=subprocess.DEVNULL, cwd=cwd)

            if rehash or not self.hash:
                self.hash = DATABASE.get_hash(self.pathname)
                # File read error
                if not self.hash:
                    return 0
            code = ret.wait()

            if code:
                return 0

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



def gen_par2(new_pars, data2process):
    "Rehash files and Generate new .par2 files"
    start_time = tpc()
    rt_start = start_time   # Realtime time start
    rt_data = 0             # Realtime data start
    rt_rate = 0

    data_seen = 0           # Data processed
    par_total = 0           # Size of parity generated
    data_total = 0          # Data actually processed into .par2

    last_save = time.time() # Last time database was saved

    def bps(num):
        return rfs(num, digits=2) + '/s'

    print("Creating parity for", len(new_pars), 'files spanning', rfs(data2process))
    for count, info in enumerate(new_pars):
        status = 'Processing #' + str(count + 1)
        if data_total > 10e6:
            status += ' with ' + sig(par_total / data_total * 100, 2)+'% parity,'
        if data_seen:
            # rate = data_seen / (time.time() - start_time)
            eta = (data2process - data_seen) / rate
            if rt_rate:
                status += ' at ' + bps(rt_rate)
            status += ' averaging ' + bps(rate) + ' with ' + fmt_time(eta) + ' remaining:'


        data_seen += info.size
        tprint(status, rel_path(info.pathname))

        par_size = info.generate(rehash=True)
        # rate = 200; time.sleep(1); tprint('erased'); time.sleep(1); continue
        if par_size:
            par_total += par_size
            data_total += info.size


        # Calculate real time rate
        if tpc() - rt_start >= 4:
            rt_rate = (data_seen - rt_data) / (tpc() - rt_start)
            rt_start = tpc()
            rt_data = data_seen

            # Save every hour
            if time.time() - last_save >= 3600:
                DATABASE.save()
                last_save = time.time()

        rate = data_seen / (tpc() - start_time)

    tprint("Done. Processed", len(new_pars), 'files in', fmt_time(tpc() - start_time))


def rel_path(pathname):
    "Return path of files relative to target directory"
    return os.path.relpath(pathname, UARGS['target'])


def main():

    # Load the database
    DATABASE.load()
    files = DATABASE.data                       # relative filename to Info
    for pathname, info in files.items():
        files[pathname] = Info(load=info)


    visited = []                    # File names visited
    file_errors = []                # List of files with errors in them
    start_time = tpc()
    new_pars = []                   # Files to calculate .par2
    data2process = 0                # Data left to process into .par2


    # Walk through file tree looking for files that need to be rehashed
    print("Scanning file tree:", UARGS['target'])
    minimum = UARGS['min'] or 1
    maximum = UARGS['max'] or 0
    for stat, pathname in walk(UARGS['target'], exclude=DATABASE.basedir, minimum=minimum, maximum=maximum):
        relpath = rel_path(pathname)
        visited.append(relpath)
        mtime = stat.st_mtime
        if relpath in files:
            info = files[relpath]
            info.pathname = pathname
            if mtime > info.mtime:
                # tprint("File changed:", relpath)
                new_pars.append(info)
                data2process += info.size
            elif UARGS['verify'] or UARGS['repair']:
                tprint("Verifying file:", relpath)
                if not info.verify():
                    print("\n\nError in file!", relpath)
                    file_errors.append(relpath)
                    if UARGS['repair']:
                        print("Attempting repair...")
                        if info.repair():
                            info.rehash()
                            info.update()
                            file_errors.pop(-1)
                            print("File fixed!\n\n")
                else:
                    info.update()

        else:
            # tprint("New file:", relpath)
            info = Info(pathname)
            new_pars.append(info)
            data2process += info.size
        files[relpath] = info

    else:
        tprint()

    # Rehash and generate .par2 files for files found earlier
    if new_pars:
        gen_par2(new_pars, data2process)
    else:
        print("Done. Scanned", len(visited), 'files in', fmt_time(tpc() - start_time))

    # Look for file with errors
    for filename, info in list(files.items()):
        if not info.hash:
            del files[filename]
            file_errors.append(filename)
    if file_errors:
        print("\n\nWARNING! THE FOLLOWING FILES HAD ERRORS:")
        print('\n'.join(file_errors))

    # Check for files deleted from database
    if UARGS['clean']:
        DATABASE.clean()
        for pathname in list(files.keys()):
            if pathname not in visited:
                qprint("Filename not found in folder:", pathname)
                del files[pathname]

    if UARGS['verify']:
        DATABASE.verify()

    DATABASE.save()
    if file_errors:
        return False
    return True


# Future ideas:
# Save files under 4k as a copy instead of using .par2
# Store files under 10k in sqlite instead of on disk
# verify should check pardatabase hashes too

if __name__ == "__main__":
    UARGS = parse_args()            # User argument
    if not UARGS:
        sys.exit(1)
    DATABASE = HexBase(basedir=UARGS['basedir'])

    if not shutil.which('par2'):
        print("Please install par2 to continue")
        sys.exit(1)
    sys.exit(int(not main()))
