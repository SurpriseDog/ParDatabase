#!/usr/bin/python3

# A deduplicating hash and par2 table to verify and repair a directory even when files are changed.

import os
import sys
import json
import lzma
import time
import shutil
import hashlib
import subprocess
from time import perf_counter as tpc

from sd.easy_args import easy_parse
from sd.chronology import fmt_time
from sd.numerology import rfs, sig


BASEDIR = '.par2_database'          # Where to put the database folder
TARGETDIR = '.'                     # What folder to scan
HASHFUNC = hashlib.sha512           # Which hash function to use
HEXES = [(('0' + hex(num)[2:])[-2:]).upper() for num in range(0, 256)]
TERM_WIDTH = shutil.get_terminal_size()[0]


def rel_path(pathname):
    return os.path.relpath(pathname, TARGETDIR)


def tprint(*args, **kargs):
    "Terminal print: Erasable text in terminal"
    if not UARGS['quiet']:
        text = ' '.join(map(str, args))
        if len(text) > TERM_WIDTH:
            text = text[:TERM_WIDTH-3] + '...'
        print('\r' * TERM_WIDTH + text, **kargs, end=' ' * (TERM_WIDTH - len(text)))


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

    args['basedir'] = os.path.realpath(os.path.join(basedir, '.par2_database'))
    args['target'] = os.path.realpath(target)

    return args


def get_hash(path, chunk=1024**2):
    "Get sha512 of filename"
    m = HASHFUNC()

    with open(path, 'rb') as f:
        while True:
            data = f.read(chunk)
            if data:
                m.update(data)
            else:
                # return m.hexdigest()[:2] + base64.urlsafe_b64encode(m.digest()[1:]).decode()
                # on disks savings of 10596 vs 11892 = 11% after lzma compression
                # may be useful for in memory savings in future
                return m.hexdigest()


def hash2file(hexa):
    "Take hash and convert to folder + name"
    # hexa = binascii.hexlify(hexa).decode()
    return hexa[:2].upper(), hexa[2:32+2]



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
        return bool(self.hash == get_hash(self.pathname))


    def update(self,):
        "Update file size and mtime"
        self.mtime = os.path.getmtime(self.pathname)
        self.size = os.path.getsize(self.pathname)


    def rehash(self,):
        "Rehash the file"
        self.hash = get_hash(self.pathname)


    def repair(self, pfiles):
        "Attempt to repair a file with the files found in the database"
        if not self.hash in pfiles:
            print("No par2 files found for:", rel_path(self.pathname))
            return False

        folder, _ = hash2file(self.hash)
        cwd = os.path.dirname(self.pathname)
        dest_files = []
        for name, phash in pfiles[self.hash].items():
            src = os.path.join(BASEDIR, 'par2', folder, name)
            dest = os.path.join(os.path.dirname(self.pathname), name)

            # Verify src integrity
            if not os.path.exists(src):
                print("ERROR! Missing .par2 file")
                return False
            if phash != get_hash(src):
                print("WARNING! .par2 files failed vaildation!")

            # Ensure dest in clear
            if os.path.exists(dest):
                print("Warning! path exists:", dest)
                ans = None
                while not ans:
                    ans = input("Overwrite? Y/N ").lower().strip()[:1]
                    if ans == 'n':
                        return False
                    if ans == 'y':
                        break

            # copy .par2 files over
            shutil.copy(src, dest)
            dest_files.append(dest)

        cmd = "par2 repair".split() + [sorted(pfiles[self.hash].keys())[0]]

        print(' '.join(cmd))
        ret = subprocess.run(cmd, check=False, cwd=cwd)
        status = not ret.returncode
        if status:
            for file in dest_files:
                os.remove(file)

        return status


    def generate(self, pfiles, quiet=False):
        "Generate par2 or find existing"
        if self.hash in pfiles:
            if not quiet:
                qprint("Found existing par2 for:", rel_path(self.pathname))
            return 0
        else:
            folder, oname = hash2file(self.hash)
            cwd = os.path.dirname(self.pathname)

            def find_par():
                "Find existing par2 files"
                for name in os.listdir(cwd):
                    if name.startswith(oname) and name.endswith('.par2'):
                        yield name

            cmd = "par2 create -n1 -qq".split()
            options = UARGS['options']
            if options:
                cmd.extend(('-'+options).split())
            cmd += ['-a', oname, '--', os.path.basename(self.pathname)]


            # Delete leftover .par2 files
            for name in find_par():
                print("Overwriting existing par2 file:", rel_path(os.path.join(cwd, name)))
                time.sleep(.1)
                os.remove(os.path.join(cwd, name))

            ret = subprocess.run(cmd, check=True, cwd=cwd, stdout=subprocess.PIPE)
            stdout = ret.stdout.decode(encoding='utf-8', errors='replace').strip() if ret.stdout else ''
            if stdout:
                print(stdout)

            # Look through generated .par2 files, hash them, move the files and add to pfiles dict
            ofiles = dict()
            total_size = 0
            for name in find_par():
                dest = os.path.realpath(os.path.join(BASEDIR, 'par2', folder, name))
                if os.path.exists(dest):
                    os.remove(dest)
                src = os.path.join(cwd, name)
                total_size += os.path.getsize(src)
                phash = get_hash(src)
                shutil.move(src, dest)
                ofiles[name] = phash
            pfiles[self.hash] = ofiles
            return total_size


def walk(dirname):
    "Walk through directory returning entry and pathname"
    for entry in os.scandir(dirname):
        if entry.is_symlink():
            continue
        pathname = os.path.join(dirname, entry.name)
        if not os.access(pathname, os.R_OK):
            if not pathname.endswith('.par2'):
                print("Could not access", pathname)
            continue
        if entry.is_dir():
            if not pathname == BASEDIR:
                yield from walk(pathname)
        else:
            stat = entry.stat()
            if stat.st_size > 0:
                yield stat, pathname


def clean(pfiles):
    "Clean database of extraneous files"
    par2_dir = os.path.join(BASEDIR, 'par2')

    # List of file hashes that are supposed to be in the folders:
    known_files = []
    for dic in pfiles.values():
        known_files.extend([entry.split('.')[0] for entry in dic.keys()])

    # Clear out any files that are unknown (probably left over from last session)
    for folder in os.listdir(par2_dir):
        if folder in HEXES:
            for name in os.listdir(os.path.join(par2_dir, folder)):
                if name.endswith('.par2'):
                    if not name.split('.')[0] in known_files:
                        path = os.path.join(par2_dir, folder, name)
                        print("Removing extraneous file:", path)
                        os.remove(path)
                        time.sleep(.1)


def gen_par2(new_pars, data2process, pfiles):
    "Rehash files and Generate new .par2 files"
    start_time = tpc()
    rt_start = start_time   # Realtime time start
    rt_data = 0             # Realtime data start
    rt_rate = 0

    data_seen = 0           # Data processed
    par_total = 0           # Size of parity generated
    data_total = 0          # Data actually processed into .par2

    def bps(num):
        return rfs(num, digits=2) + '/s'

    print("Creating parity for", len(new_pars), 'files spanning', rfs(data2process))
    for count, info in enumerate(new_pars):
        status = 'Processing #' + str(count + 1)
        if data_total > 10e6:
            status += ' with ' + sig(par_total / data_total * 100, 2)+'% .par2'
        if data_seen:
            # rate = data_seen / (time.time() - start_time)
            eta = (data2process - data_seen) / rate
            if rt_rate:
                status += ' at ' + bps(rt_rate)
            status += ' averaging ' + bps(rate) + ' with ' + fmt_time(eta) + ' remaining:'


        data_seen += info.size
        tprint(status, rel_path(info.pathname))
        info.rehash()
        info.update()
        par_size = info.generate(pfiles)
        if par_size:
            par_total += par_size
            data_total += info.size


        # Calculate real time rate
        if tpc() - rt_start >= 4:
            rt_rate = (data_seen - rt_data) / (tpc() - rt_start)
            rt_start = tpc()
            rt_data = data_seen

        rate = data_seen / (tpc() - start_time)

    tprint("Done. Processed", len(new_pars), 'files in', fmt_time(tpc() - start_time))




def save_database(par_database, files, pfiles):
    # Save the database in lzma which adds a nice checksum
    if files:       # Avoid deleting database if run on wrong folder
        with lzma.open(par_database, mode='wt', check=lzma.CHECK_CRC64, preset=3) as f:
            for pathname, info in files.items():
                files[pathname] = vars(info)
            meta = dict(mtime=time.time(),      # modification time
                        hash=UARGS['hash'],     # hash choice
                        encoding='hex',         # encode hash as hexadecimal
                        truncate=False,         # truncate hash to this many bits
                        version=1.0,            # Database version
                       )
            json.dump([meta, files, pfiles], f)


def load_database(par_database):
    files = dict()      # relative filename to Info
    pfiles = dict()     # hashes to dict(par file name : hash of par file)
    par2_dir = os.path.join(BASEDIR, 'par2')

    os.makedirs(BASEDIR, exist_ok=True)
    os.makedirs(par2_dir, exist_ok=True)
    # Repository of filenames, hashes, modificaton times and more


    # Make data folders if they don't exist
    folders = []
    for folder in HEXES:
        folders.append(folder)
        os.makedirs(os.path.join(BASEDIR, 'par2', folder), exist_ok=True)

    # Load the database if possible
    if os.path.exists(par_database):
        with lzma.open(par_database, mode='rt') as f:
            meta, files, pfiles = json.load(f)
            for pathname, info in files.items():
                files[pathname] = Info(load=info)
    else:
        return files, pfiles


    if meta['hash'] != UARGS['hash']:
        print("Hash type for database is", meta['hash'], "not", UARGS['hash'])
        print("Delete database to change the hash type")
        sys.exit(1)

    # Rotate the database and delete old versions
    if time.time() - meta['mtime'] > 3600:
        copy_name = os.path.splitext(par_database)[0] + '.' + str(int(meta['mtime'])) + '.xz'
        shutil.copy(par_database, copy_name)
        names = [name for name in os.listdir(BASEDIR) if name.startswith('database.')]
        for name in sorted(names)[3:]:
            os.remove(os.path.join(BASEDIR, name))

    return files, pfiles


def main():
    par_database = os.path.join(BASEDIR, 'database.xz')
    files, pfiles = load_database(par_database)
    if UARGS['clean']:
        clean(pfiles)

    visited = []
    file_errors = []
    last_save = time.time()         # Last time database was saved
    start_time = tpc()

    new_pars = []                   # Files to calculate .par2
    data2process = 0                # Data left to process into .par2


    # Walk through file tree looking for files that need to be rehashed
    print("Scanning file tree:", TARGETDIR)
    for stat, pathname in walk(TARGETDIR):
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
                        if info.repair(pfiles):
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
        # Save every hour
        if time.time() - last_save >= 3600:
            save_database(par_database, files, pfiles)
    else:
        tprint()

    # Rehash and generate .par2 files for files found earlier
    if new_pars:
        gen_par2(new_pars, data2process, pfiles)
    else:
        print("Done. Scanned", len(visited), 'files in', fmt_time(tpc() - start_time))

    if file_errors:
        print("\n\nWARNING! THE FOLLOWING FILES HAD ERRORS:")
        print('\n'.join(file_errors))

    # Check for files deleted from database
    if UARGS['clean']:
        for pathname in list(files.keys()):
            if pathname not in visited:
                qprint("Filename not found in folder:", pathname)
                del files[pathname]


    save_database(par_database, files, pfiles)
    if file_errors:
        sys.exit(1)

# Future ideas:
# Save files under 4k as a copy instead of using .par2
# Store files under 10k in sqlite instead of on disk
# verify should check pardatabase hashes too

if __name__ == "__main__":
    if not shutil.which('par2'):
        print("Please install par2 to continue")
        sys.exit(1)

    UARGS = parse_args()            # User arguments
    if not UARGS:
        sys.exit(1)

    BASEDIR = UARGS['basedir']
    TARGETDIR = UARGS['target']
    HASHFUNC = getattr(hashlib, UARGS['hash'], hashlib.sha512)

    main()
