#!/usr/bin/python3
import os
import time
import lzma
import json
import shutil
import hashlib


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


    def __init__(self, basedir='.'):
        basedirname = '.par2_database'          # Where to put the database folder
        self.basedir = os.path.join(basedir, basedirname)
        self.tmpname = '.pardatabase_tmp_file'  # Temporary output name

        # Repository of filenames, hashes, modificaton times and more
        self.index = os.path.join(basedir, basedirname, 'database.xz')
        self.pfiles = dict()                    # hashes to dict(par file name : hash of par file)
        self.data = dict()                      # Anything that can be serialized into json
        self.last_save = 0                      # Last save time

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
                self.last_save = meta['mtime']

        else:
            if hashname:
                self.hashname = hashname

        if self.hashname:
            self.hashfunc = vars(hashlib)[self.hashname]


    def save(self):
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
                        version=1.0,            # Database version
                       )


            out = dict()
            for key, val in self.data.items():
                out[key] = vars(val)
            json.dump([meta, out, self.pfiles], f)
            self.last_save = time.time()


    def clean(self,):
        "Clean database of extraneous files"

        # List of file hashes that are supposed to be in the folders:
        known_files = []
        for dic in self.pfiles.values():
            known_files.extend([entry.split('.')[0] for entry in dic.keys()])

        # Clear out any files that are unknown (probably left over from last session)
        for folder in os.listdir(self.locate()):
            if folder in self.hexes:
                for name in os.listdir(self.locate(folder)):
                    name = os.path.join(folder, name)
                    if name.endswith('.par2') and not name.split('.')[0] in known_files:
                        path = self.locate(name)
                        print("Removing extraneous file:", path)
                        if user_answer():
                            os.remove(path)


    def locate(self, name=''):
        "Return absolute path of filename in .par2_database"
        return os.path.join(self.basedir, 'par2', name)


    def put(self, src, fhash, name=''):
        "Given a hash move the file from the src location to the approriate folder and update self.files"
        folder = fhash[:2].upper()
        oname = fhash[2:32+2]
        if name:
            oname += name
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
                    print(err)
                    return None
                if data:
                    m.update(data)
                else:
                    # return m.hexdigest()[:2] + base64.urlsafe_b64encode(m.digest()[1:]).decode()
                    # on disks savings of 10596 vs 11892 = 11% after lzma compression
                    # may be useful for in memory savings in future
                    return m.hexdigest()


    def verify(self,):
        "Verify that the .par2 files are available and their hash is still valid"
        count = 0
        for record in self.pfiles.values():
            # print(record, '\n\n')
            for filename, phash in record.items():
                # print(filename, phash)
                src = self.locate(filename)
                if not os.path.exists(src):
                    print('WARNING: Could not find', src)
                elif not phash == self.get_hash(src):
                    print('WARNING: incorrect hash', src)
                else:
                    count += 1
        print()
        print(count, 'parity files verified')
