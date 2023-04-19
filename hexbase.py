#!/usr/bin/python3
import os
import time
import lzma
import json
import shutil
import hashlib

from sd.rotate import rotate
from sd.file_progress import FileProgress, tprint

BAK_NUM = 8     # Number of database backups
TRUNCATE = 64   # Hashes are truncated to 64 hex = 256 bits for space savings in database.xz
                # Not using sha256 because truncated sha512 is better and faster.
VERSION = 1.2   # Database version number
MINHASH = 16    # Minimum size of hash = 64 bits

assert(TRUNCATE) >= MINHASH

def user_answer(text='Y/N ?'):
    "Ask the user yes/no questions"
    ans = None
    while not ans:
        ans = input(text + " ").lower().strip()[:1]
        if ans == 'n':
            return False
        if ans == 'y':
            return True


def hash_cmp(a, b):
    "Compare hashes of unequal length"
    length = min(len(a), len(b))
    assert length >= MINHASH
    if a[:length] != b[:length]:
        return False
    return True


class HexBase:
    "Store files with unique hashed file names in hex folder structure"


    def __init__(self, basedir='.pardatabase'):
        self.basedir = basedir                  # Where to put the database folder

        # Repository of filenames, hashes, modificaton times and more
        self.index = os.path.join(basedir, 'database.xz')
        self.pfiles = dict()                    # hashes to dict(par file name : hash of par file)
        self.data = dict()                      # Anything that can be serialized into json
        self.last_save = 0                      # Last save time
        self.version = VERSION                  # Database version number

        self.hashfunc = hashlib.sha512
        self.hashname = 'sha512'                # Custom user hash
        self.hexes = [(('0' + hex(num)[2:])[-2:]).upper() for num in range(0, 256)]



    def print(self,):
        print('data      ', type(self.data), len(self.data))
        print('index     ', self.index)
        print('basedir   ', self.basedir)
        print('pfiles    ', len(self.pfiles))


    def load(self, hashname=None):
        # Make data folders if they don't exist
        os.makedirs(self.locate(), exist_ok=True)
        folders = []
        for folder in self.hexes:
            folders.append(folder)
            os.makedirs(self.locate(folder), exist_ok=True)

        # Load the database if possible
        baks = rotate(self.index, move=False, limit=BAK_NUM)
        good = None         # Name of file successfully loaded
        for path in baks:
            if os.path.exists(path):
                try:
                    with lzma.open(path, mode='rt') as f:
                        meta, self.data, self.pfiles = json.load(f)
                        if meta['hash']:
                            self.hashname = meta['hash']
                        self.version = meta['version']
                        self.last_save = meta['mtime']
                        good = path
                        break
                except (OSError, ValueError, EOFError):
                    print("Error loading database:", path)
        else:
            # If no good database detected:
            if not good and os.path.exists(baks[0]):
                print("Corrupted database moved to", baks[1])
                rotate(self.index, limit=BAK_NUM)
            if hashname:
                self.hashname = hashname

        if self.hashname and self.hashname != 'sha512':
            print("Using custom hash:", self.hashname)
            self.hashfunc = vars(hashlib)[self.hashname]

        if self.version < 1.2:
            # Added hash truncation
            self.version = 1.2


    def save(self, data=None, mintime=0):
        "Save the database in lzma which adds a nice checksum and rotate"

        if mintime and time.time() - self.last_save < mintime:
            return False

        # Rotate any old backup files
        baks = rotate(self.index, limit=BAK_NUM)

        # Save to file
        with lzma.open(self.index, mode='wt', check=lzma.CHECK_CRC64, preset=2) as f:
            meta = dict(mtime=time.time(),      # modification time
                        hash=self.hashname,     # hash choice
                        encoding='hex',         # encode hash as hexadecimal
                        truncate=TRUNCATE,      # truncate hash to this many bits
                        version=self.version,   # Database version
                       )


            json.dump([meta, data, self.pfiles], f)
            self.last_save = time.time()
            f.flush()
            os.fsync(f)


        # If only copy, make a backup
        if not os.path.exists(baks[1]):
            shutil.copy(baks[0], baks[1])

        return True


    def clean(self, fhash):
        "Look up hash in the database and remove any .par2 files if unused"
        if fhash not in self.pfiles:
            return 0

        deleted = []
        for name in self.pfiles[fhash].keys():
            src = self.locate(name)
            if os.path.exists(src):
                print('Deleting', src)
                os.remove(src)
            else:
                print('Missing .par2 file, cannot delete!', fhash)
            deleted.append(fhash)
        if fhash in deleted:
            del self.pfiles[fhash]
        return len(deleted)



    def locate(self, name=''):
        "Return absolute path of filename in .pardatabase"
        return os.path.join(self.basedir, 'par2', name)


    def put(self, src, fhash, ending):
        "Given a hash move the file from the src location to the appropiate folder and update self.files"
        folder = fhash[:2].upper()
        oname = fhash[2:32+2] + ending
        oname = os.path.join(folder, oname)

        dest = self.locate(oname)
        phash = self.get_hash(src)

        # print('put', src, dest); input()
        if os.path.exists(dest):
            # print("Overwriting existing file:", dest)
            os.remove(dest)
        shutil.move(src, dest)
        if fhash not in self.pfiles:
            self.pfiles[fhash] = dict()
        self.pfiles[fhash][oname] = phash



    def get(self, fhash, cwd):
        "Given a hash, copy files from vault and put them in cwd"

        dest_files = []
        for name, phash in self.pfiles[fhash].items():
            src = self.locate(name)
            dest = os.path.join(cwd, os.path.basename(name))

            # Verify src integrity
            if not os.path.exists(src):
                print("ERROR! Missing .par2 file:", src)
                return False
            if not hash_cmp(phash, self.get_hash(src)):
                print("WARNING! .par2 files failed vaildation!")

            # Ensure dest in clear
            if os.path.exists(dest):
                print("Warning! path exists:", dest)
                if not user_answer("Overwrite? Y/N"):
                    return False
            # print('get', src, '\nto', dest)
            shutil.copy(src, dest)
            dest_files.append(dest)
        return dest_files



    def get_hash(self, path, chunk=4 * 1024 * 1024):
        "Get sha512 of filename"
        m = self.hashfunc()
        with open(path, 'rb') as f:
            while True:
                try:
                    data = f.read(chunk)
                except IOError as err:
                    print('\nIO Error in', path)
                    print(err)
                    return 'ioerror'
                if data:
                    m.update(data)
                else:
                    # return m.hexdigest()[:2] + base64.urlsafe_b64encode(m.digest()[1:]).decode()
                    # on disks savings of 10596 vs 11892 = 11% after lzma compression
                    # may be useful for in memory savings in future
                    return m.hexdigest()[:TRUNCATE]


    def verify(self,):
        "Verify that the .par2 files are available and their hash is still valid"
        verified = 0
        records = self.pfiles.values()

        fp = FileProgress()
        for record in records:
            for filename in record.keys():
                fp.scan_file(self.locate(filename))


        for record in records:
            for filename, phash in record.items():
                src = self.locate(filename)
                tprint("Verifying File", fp.progress(filename=src)['default'])
                if not os.path.exists(src):
                    print('WARNING: Could not find', src)
                elif not hash_cmp(phash, self.get_hash(src)):
                    print('WARNING: incorrect hash', src)
                else:
                    verified += 1
        tprint("Done. Scanned", fp.done()['msg'])
        print()


# Version History
# 1.1 Store relative pathname for files instead now. Requires fix for old pathnames
