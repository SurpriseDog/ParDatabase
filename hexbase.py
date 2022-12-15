#!/usr/bin/python3
import os
import time
import lzma
import json
import shutil
import hashlib

from sd.file_progress import FileProgress, tprint



def user_answer(text='Y/N ?'):
    "Ask the user yes/no questions"
    ans = None
    while not ans:
        ans = input(text + " ").lower().strip()[:1]
        if ans == 'n':
            return False
        if ans == 'y':
            return True


class HexBase:
    "Store files with unique hashed file names in hex folder structure"


    def __init__(self, basedir = '.pardatabase'):
        self.basedir = basedir                  # Where to put the database folder

        # Repository of filenames, hashes, modificaton times and more
        self.index = os.path.join(basedir, 'database.xz')
        self.pfiles = dict()                    # hashes to dict(par file name : hash of par file)
        self.data = dict()                      # Anything that can be serialized into json
        self.last_save = 0                      # Last save time
        self.version = 1.1                      # Database version number

        self.hashfunc = hashlib.sha512
        self.hashname = None                    # Custom user hash
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
        if os.path.exists(self.index):
            with lzma.open(self.index, mode='rt') as f:
                meta, self.data, self.pfiles = json.load(f)
                if meta['hash']:
                    self.hashname = meta['hash']
                self.version = meta['version']
                self.last_save = meta['mtime']

        else:
            if hashname:
                self.hashname = hashname

        if self.hashname:
            print(self.hashname)
            self.hashfunc = vars(hashlib)[self.hashname]


    def save(self, data=None):
        "Save the database in lzma which adds a nice checksum"
        # Rotate the database and delete old versions
        if os.path.exists(self.index):
            existing = []                   # existing database.xz files
            for name in os.listdir(self.basedir):
                if name.startswith('database.') and name.endswith('.xz'):
                    existing.append(name)
            if time.time() - self.last_save > 3600 * 8 or len(existing) < 2:
                copy_name = os.path.splitext(self.index)[0] + '.' + str(int(self.last_save)) + '.xz'
                shutil.copy(self.index, copy_name)
                for name in sorted(existing)[3:]:
                    print("Removing old database file:", name)
                    os.remove(os.path.join(self.basedir, name))

        # Save to file
        with lzma.open(self.index, mode='wt', check=lzma.CHECK_CRC64, preset=2) as f:
            meta = dict(mtime=time.time(),      # modification time
                        hash=self.hashname,     # hash choice
                        encoding='hex',         # encode hash as hexadecimal
                        truncate=False,         # truncate hash to this many bits
                        version=self.version,   # Database version
                       )


            json.dump([meta, data, self.pfiles], f)
            self.last_save = time.time()
            f.flush()
            os.fsync(f)


        # Save backup of backup
        src = os.path.splitext(self.index)
        bak = src[0] + '.bak' + src[1]
        src = self.index
        if os.path.exists(bak):
            os.remove(bak)
        shutil.copy(src, bak)


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
            if phash != self.get_hash(src):
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
                    return m.hexdigest()


    def verify(self,):
        "Verify that the .par2 files are available and their hash is still valid"
        verified = 0
        records = self.pfiles.values()
        fp = FileProgress()
        fp.scan([self.locate(filename) for filename, _ in records])
        for record in records:
            for filename, phash in record.items():
                src = self.locate(filename)
                tprint("Verifying File", fp.progress(filename=src)['default'])
                if not os.path.exists(src):
                    print('WARNING: Could not find', src)
                elif not phash == self.get_hash(src):
                    print('WARNING: incorrect hash', src)
                else:
                    verified += 1
        tprint("Done. Scanned", fp.done()['msg'])
        print()


# Version History
# 1.1 Store relative pathname for files instead now. Requires fix for old pathnames
