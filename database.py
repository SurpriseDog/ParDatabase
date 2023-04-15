#!/usr/bin/python3


import os
import sys
import time
import signal
from time import perf_counter as tpc

from info import Info
from hexbase import HexBase
from sd.common import fmt_time, rfs
from sd.file_progress import FileProgress, tprint



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
        self.delay = None                   # Delay after hashing


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


    def save(self, *args, **kargs):
        out = dict()
        for key, val in self.hexbase.data.items():
            out[key] = val.tojson()
        self.hexbase.save(out, *args, **kargs)


    def scan(self, minscan=None, maxscan=None, minpar=None, maxpar=None):
        '''
        Walk through file tree looking for files that need to be processed
        minscan         = Minimum file size to scan
        maxscan         = Maximum file size to scan
        minpar          = Minimum file size to parity
        maxpar          = Maximum file size to parity
        '''
        minscan = minscan or 1          # Minimum size to scan
        maxscan = maxscan or 0          # Maximum size to scan
        minpar = minpar or 1            # Minimum size to parity
        maxpar = maxpar or float('inf') # Maximum size to parity

        newpars = []                # Files to process that meet reqs
        newhashes = []              # Files that need to be hashed


        start_time = tpc()
        print("\nScanning file tree:", self.target)

        # Scan the file tree only including files between sizes:
        # minscan and maxscan, decide to what to do with each file
        visited = 0
        for stat, pathname in walk(self.target, exclude=self.basedir, minimum=minscan, maximum=maxscan):
            relpath = self.rel_path(pathname)
            visited += 1

            # Get actual mtime and size, not one in database
            mtime = stat.st_mtime
            size = stat.st_size

            # For existing files:
            if relpath in self.files:
                info = self.files[relpath]
                info.pathname = relpath

                # Files that need to be updated:
                if size < minpar or size > maxpar:
                    # If file is too big or small to get a par2 file

                    if not info.hash or mtime != info.mtime:
                        newhashes.append(info)
                else:
                    # Files with the range to get a parity
                    if info.hash not in self.hexbase.pfiles or \
                       not info.hash or mtime != info.mtime:
                        newpars.append(info)

            else:
                # For new files
                info = Info(relpath, base=self.target)
                newpars.append(info)

            # Update info link in database
            self.files[relpath] = info
        print("Done. Scanned", visited, 'files in', fmt_time(tpc() - start_time))
        return newpars, newhashes


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
        '''Clean database of non existant files'''

        # Build list of hashes to info (could be multiples)
        hashes = dict()
        for info in self.files.values():
            hashes.setdefault(info.hash, []).append(info)

        # Look for hashes in the database that no longer correspond to .par2 files and delete those par2
        deleted = 0
        for pathname in list(self.files.keys()):
            if not os.path.exists(self.fullpath(pathname)):
                print('\nRemoving reference for', pathname)
                info = self.files[pathname]
                hashes[info.hash].remove(info)
                if not hashes[info.hash]:
                    deleted += self.hexbase.clean(info.hash)
                del self.files[pathname]
        if deleted:
            print(deleted, 'files removed from database')


    def verify(self,):
        "Verify files in directory"

        # Look for files with errors
        file_errors = []                # List of files with errors in them
        updated = 0                     # Files updated on the disk, but not in database

        print('\nVerifying hashes of all files referenced in database:')

        fp = FileProgress(len(self.files), sum(info.size for info in self.files.values()))
        missing = 0         # Files with missing hashes
        for relpath, info in self.files.items():
            if not info.hash:
                missing += 1
                continue

            fullpath = info.fullpath

            # Files deleted from disk continue to exist in database until cleaner is run
            if not os.path.exists(fullpath):
                continue

            tprint(fp.progress(filename=fullpath)['default'] + ':', relpath)

            if os.path.getmtime(info.fullpath) > info.mtime + 1e-3:
                updated += 1
                print("File updated on disk without being rescanned:", relpath)
                continue

            if info.hash != self.get_hash(info.fullpath):
                print(info, vars(info))
                print("\n\nError in file!", relpath)
                file_errors.append(relpath)

        tprint("Done. Hashed", fp.done()['msg'])
        print()

        if missing:
            print('\n')
            print(missing, 'files had no hash in the database')
            print("Run pardatabase without the --verify to add them.")

        if updated:
            print('\n')
            print(updated, 'files were updated on the disk without being rescanned.')
            print("Run pardatabase without the --verify to add them.")

        if updated > 10 and updated / len(self.files) > 0.2:
            print("Set up pardatabse with a LazyCron job to ensure" + \
                  "that the database is consistently updated.")
            print("https://github.com/SurpriseDog/LazyCron")

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


    def gen_hashes(self, newhashes):
        "Generate new hashes for files."
        data2process = sum(info.size for info in newhashes)
        fp = FileProgress(len(newhashes), data2process)
        print("\nCreating only hashes for",
              len(newhashes), 'files spanning', rfs(data2process))
        for info in newhashes:
            size = info.size
            tprint("File", fp.progress(size)['default'] + ':', info.pathname)
            info.hash = self.get_hash(info.fullpath)
            info.update()
        tprint("\nDone. Processed", fp.done()['msg'])
        print()


    def gen_pars(self, newpars, sequential=False, singlecharfix=False, par2_options=None):
        '''Rehash files and Generate new .par2 files
        sequential      = Run in sequential mode (generate hash first, then parity)
        singlecharfix   = Rename files before running par2
        par2_options    = Passed onto par2 program
        '''

        data2process = sum(info.size for info in newpars)
        print("\nCreating parity and hashes for",
              len(newpars), 'files spanning', rfs(data2process))

        fp = FileProgress(len(newpars), data2process)
        results = []
        for count, info in enumerate(newpars):
            # + = multi - = sequential     '+-'[sequential],
            size = info.size
            tprint("File", fp.progress(size)['default'] + ':', info.pathname)

            status, files = self.generate(info, sequential, singlecharfix, par2_options)
            if status:
                info.update()
            results.append(status)

            for number, name in enumerate(files):
                # total_size += os.path.getsize(name)
                self.hexbase.put(name, info.hash, '.' + str(number) + '.par2')


            if not sequential and results[-5:] == ['PARALLEL_EARLY_QUIT'] * 5:
                print("Too many files with existing .par2... switch to sequential mode.")
                sequential = True


            # Save every hour
            if not count % 10:
                if self.save(mintime=3600):
                    print("Database saved successfuly at", time.strftime('%H:%M'))

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
            info.hash = self.get_hash(new_name)
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
