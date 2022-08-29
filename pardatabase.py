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

from sd.easy_args import easy_parse
from sd.chronology import fmt_time


BASEDIR = '.par2_database'          # Where to put the database folder
TARGETDIR = '.'                     # What folder to scan
HEXES = [(('0' + hex(num)[2:])[-2:]).upper() for num in range(0, 256)]
TERM_WIDTH = shutil.get_terminal_size()[0]


def rel_path(pathname):
    return os.path.relpath(pathname, TARGETDIR)


def tprint(*args, **kargs):
    "Terminal print: Erasable text in terminal"
    if not UARGS['quiet']:
        text = ' '.join(map(str, args))
        if len(text) < TERM_WIDTH:
            print('\r' * TERM_WIDTH + text, **kargs, end=' ' * (TERM_WIDTH - len(text)))
        else:
            print(text, **kargs)


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
    '''Options passed onto par2 program.
    For example -c2 will set the recovery block count at 2.
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
    func = getattr(hashlib, UARGS['hash'], hashlib.sha512)
    m = func()

    with open(path, 'rb') as f:
        while True:
            data = f.read(chunk)
            if data:
                m.update(data)
            else:
                return m.hexdigest()


def hash2file(hexa):
    "Take hash and convert to folder + name"
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
            self.rehash()
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

    def repair(self, par2):
        "Attempt to repair a file with the files found in the database"
        if not self.hash in par2:
            print("No par2 files found for:", rel_path(self.pathname))
            return False

        folder, _ = hash2file(self.hash)
        cwd = os.path.dirname(self.pathname)
        dest_files = []
        for name in par2[self.hash]:
            src = os.path.join(BASEDIR, 'par2', folder, name)
            dest = os.path.join(os.path.dirname(self.pathname), name)
            if os.path.exists(dest):
                print("Warning! path exists:", dest)
                ans = None
                while not ans:
                    ans = input("Overwrite? Y/N ").lower().strip()[:1]
                    if ans == 'n':
                        return False
                    if ans == 'y':
                        break
            shutil.copy(src, dest)
            dest_files.append(dest)
        cmd = "par2 repair".split() + [sorted(par2[self.hash])[0]]
        print(' '.join(cmd))
        ret = subprocess.run(cmd, check=False, cwd=cwd)
        status = not ret.returncode
        if status:
            for file in dest_files:
                os.remove(file)

        return status


    def generate(self, par_table, quiet=False):
        "Generate par2 or find existing"
        if self.hash in par_table:
            if not quiet:
                qprint("Found existing par2 for:", rel_path(self.pathname))
        else:
            folder, oname = hash2file(self.hash)
            cwd = os.path.dirname(self.pathname)

            def find_par():
                "Find existing par2 files"
                for name in os.listdir(cwd):
                    if name.startswith(oname) and name.endswith('.par2'):
                        yield name

            cmd = "par2 create -n1 -qq -a".split() + [oname, '--', os.path.basename(self.pathname)]

            # Delete leftover .par2 files
            for name in find_par():
                print("Overwriting existing par2 file:", rel_path(os.path.join(cwd, name)))
                time.sleep(1)
                os.remove(os.path.join(cwd, name))

            ret = subprocess.run(cmd, check=True, cwd=cwd, stdout=subprocess.PIPE)
            stdout = ret.stdout.decode(encoding='utf-8', errors='replace').strip() if ret.stdout else ''
            if stdout:
                print(stdout)

            ofiles = []
            for name in find_par():
                dest = os.path.realpath(os.path.join(BASEDIR, 'par2', folder, name))
                if os.path.exists(dest):
                    os.remove(dest)
                shutil.move(os.path.join(cwd, name), dest)
                ofiles.append(name)
            par_table[self.hash] = ofiles


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


def clean(par2):
    "Clean database of extraneous files"
    par2_dir = os.path.join(BASEDIR, 'par2')

    # List of file hashes that are supposed to be in the folders:
    known_files = []
    for lis in par2.values():
        known_files.append(lis[0].split('.')[0])

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


def load_database(par_database):
    files = dict()      # relative filename to Info
    par2 = dict()       # list of hashes with par2 available
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
    modified_date = time.time()
    hash_type = None
    if os.path.exists(par_database):
        with lzma.open(par_database, mode='rt') as f:
            modified_date, hash_type, files, par2 = json.load(f)
            for pathname, info in files.items():
                files[pathname] = Info(load=info)

    if hash_type and hash_type != UARGS['hash']:
        print("Hash type for database is", hash_type, "not", UARGS['hash'])
        print("Delete database to change the hash type")
        sys.exit(1)

    # Rotate the database and delete old versions
    if time.time() - modified_date > 3600:
        copy_name = os.path.splitext(par_database)[0] + '.' + str(int(modified_date)) + '.xz'
        shutil.copy(par_database, copy_name)
        names = [name for name in os.listdir(BASEDIR) if name.startswith('database.')]
        for name in sorted(names)[3:]:
            os.remove(os.path.join(BASEDIR, name))

    return files, par2


def save_database(par_database, files, par2):
    # Save the database in lzma which adds a nice checksum
    if files:       # Avoid deleting database if run on wrong folder
        with lzma.open(par_database, mode='wt', check=lzma.CHECK_CRC64, preset=3) as f:
            for pathname, info in files.items():
                files[pathname] = vars(info)
            json.dump([time.time(), UARGS['hash'], files, par2], f)


def main():
    par_database = os.path.join(BASEDIR, 'database.xz')
    files, par2 = load_database(par_database)
    if UARGS['clean']:
        clean(par2)

    visited = []
    file_errors = []
    last_save = time.time()             # Last time database was saved
    start_time = time.time()
    for stat, pathname in walk(TARGETDIR):
        relpath = rel_path(pathname)
        visited.append(relpath)
        mtime = stat.st_mtime
        if relpath in files:
            info = files[relpath]
            info.pathname = pathname
            if mtime > info.mtime:
                print("File changed:", relpath)
                info.rehash()
                info.update()
                info.generate(par2)
            elif UARGS['verify'] or UARGS['repair']:
                tprint("Verifying file:", relpath)
                if not info.verify():
                    print("\n\nError in file!", relpath)
                    file_errors.append(relpath)
                    if UARGS['repair']:
                        print("Attempting repair...")
                        if info.repair(par2):
                            info.rehash()
                            info.update()
                            file_errors.pop(-1)
                            print("File fixed!\n\n")
                else:
                    info.update()

        else:
            tprint("New file:", relpath)
            info = Info(pathname)
            info.generate(par2)
        files[relpath] = info
        # Save every hour
        if time.time() - last_save >= 3600:
            save_database(par_database, files, par2)
    else:
        tprint()

    if file_errors:
        print("\n\nWARNING! THE FOLLOWING FILES HAD ERRORS:")
        print('\n'.join(file_errors))
    else:
        print("Done. Scanned", len(visited), 'files in', fmt_time(time.time() - start_time))

    # Check for files deleted from database
    if UARGS['clean']:
        for pathname in list(files.keys()):
            if pathname not in visited:
                qprint("Filename not found in folder:", pathname)
                del files[pathname]


    save_database(par_database, files, par2)
    if file_errors:
        sys.exit(1)



if __name__ == "__main__":
    UARGS = parse_args()            # User arguments
    BASEDIR = UARGS['basedir']
    TARGETDIR = UARGS['target']
    if UARGS:
        main()
