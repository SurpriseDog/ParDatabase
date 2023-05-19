#!/usr/bin/python3
# Filesystem directory handling
# Testing: ./tree <dirname>

import os
import sys
import mimetypes

from sd.format_number import rfs

TREE_ARGS = [\

    # Verbosity
    ["print_files", '', bool],
    "Print Size, Name for all files",

    ["print_skips", '', bool],
    "Print paths skipped, not based on size or mtime",


    # File constraints
    ["mintime", 'min_t', float],
    "Files modified after unix time",

    ["maxtime", 'max_t', float],
    "Files modified before unix time",

    ["minsize", 'min_size', str],
    "Min file size",

    ["maxsize", 'max_size', str],
    "Max file size",


    # Skips
    ["skip_exts", '', list],
    "File extensions to ignore",

    ["skip_mimes", '', list],
    "Skip mime types like image png",

    ["skip_dirs", '', list],
    '''
    Ignore directory names with these keywords
    Example: --skip_dirs foo bar
    ''',

    ["skip_paths", '', list],
    "Skip relative pathnames",

    ["skip_hidden", '', bool, False],
    "Skip hidden files",

    ["skip_cache", '', bool, False],
    "Skip files with cache in name",

    ["skip_syms", '', bool, True],
    "Skip symbolic links",
    ]


def _parse_args():
    "Parse arguments"

    positionals = [\
    ["root", '', str, '.'],
    "Target Directory to Scan."
    ]


    args = easy_parse(TREE_ARGS,
                      positionals,
                      usage='<Target Directory>, options...',
                      description='Explore a filesystem, keep stats on files seen',
                      )

    args = vars(args)


    # Convert user data sizes
    convert_list = "min_size max_size".split()
    for arg in convert_list:
        val = args[arg]
        if val and type(val) is str:
            args[arg] = ConvertDataSize()(args[arg])

    return args


def dirtime(folder='.', deep=-1, verbose=False):
    '''Get the most updated mtime in directory.
    deep        # Recursion level, -1 = infinite, 0 = Don't recurse
    '''
    mtime = 0
    for entry in os.scandir(folder):
        if verbose:
            print(entry.path)
        if entry.is_symlink():
            continue
        if entry.is_dir() and (deep != 0):
            mtime = max(mtime, dirtime(entry.path, deep-1))
        else:
            stat = entry.stat(follow_symlinks=False)
            mtime = max(mtime, stat.st_mtime)
    return mtime


def folder_size(dirname='.', kargs=None):
    "Pass arguments to Tree, to get the size of a folder or file"
    if os.path.isdir(dirname):
        t = Tree(dirname, kargs)
        t.scan()
        return t.size
    else:
        return os.stat(dirname).st_size


def walker(dirname, verbose=False):
    "walk through directory, yielding filenames"
    # Like tree, only shorter
    for path, _, files in os.walk(dirname):
        for file in files:
            fullpath = os.path.join(path, file)
            yield fullpath
            if verbose:
                print(fullpath)




def walk_folders(dirname, delete_empty=False):
    '''Walk through all folders, while yielding each one
    delete_empty will delete empty folders recursively
    '''
    for path, folders, _ in os.walk(dirname, topdown=not delete_empty):
        for folder in folders:
            fullpath = os.path.join(path, folder)
            if delete_empty:
                if not os.listdir(fullpath):
                    yield fullpath
                    os.rmdir(fullpath)
            else:
                yield fullpath


class Tree:
    '''Explore a filesystem, keep stats on files seen'''

    # Build up a dictionary of default limitations from the defaults in TREE_ARGS
    # Refer to TREE_ARGS for documentation
    inf = float('inf')
    default_args = {
        'print_files': False,
        'print_skips': False,
        'min_t': 0,
        'max_t': inf,
        'min_size': 0,
        'max_size': inf,
        'skip_exts': None,
        'skip_mimes': None,
        'skip_dirs': None,
        'skip_paths': None,
        'skip_hidden': False,
        'skip_cache': False,
        'skip_syms': True,
        }

    def __init__(self, root='.', kargs=None):
        '''
        Pass a dictionary with the file constraints you would like
        See the TREE_ARGS in the tree.py file for more information
        '''

        # Internal stats on files seen
        self.count = 0              # Number of files seen
        self.size = 0               # Size of all files walked
        self.root = root

        # Substitute user given limitations
        uargs = self.default_args.copy()
        if kargs:
            for key, val in kargs.items():
                if key in self.default_args:
                    # print(key, val)
                    if val is not None:
                        uargs[key] = kargs[key]
                else:
                    raise ValueError("Unknown key for Tree:", key)

        # print('kargs', kargs)
        # print('uargs', uargs)
        self.uargs = uargs


    def reset(self,):
        "Reset the count and size of files seen."
        self.count = 0
        self.size = 0


    def scan(self, dirname=None, verbose=False):
        "Scan the file tree and return the count and total size."
        self.reset()
        for _ in self.walk(dirname):
            pass
        if verbose:
            print("\nScanned", self.count, 'files spanning', rfs(self.size))

        return self.count, self.size


    def skip(self, entry, pathname):
        "Given an entry and pathname, should it be skipped?"
        name = entry.name

        def sprint(text):
            if self.uargs['print_skips']:
                print(text.ljust(20), pathname)

        if self.uargs['skip_exts']:
            if os.path.splitext(name)[-1] in self.uargs['skip_exts']:
                return True

        if self.uargs['skip_mimes']:
            mime = mimetypes.guess_type(name)[0]
            if mime and [m for m in self.uargs['skip_mimes'] if m in mime]:
                sprint('Skipping bad mime: ' + str(mime))
                return True

        if self.uargs['skip_hidden']:
            if name[-1] == '~' or name[0] == '.':
                sprint('Skipping hidden:')
                return True

        if self.uargs['skip_cache']:
            if 'cache' in name.lower():
                sprint('Skipping cache:')
                return True

        if self.uargs['skip_syms'] and entry.is_symlink():
            sprint('Skipping symlink:')
            return True

        if self.uargs['skip_paths']:
            for spath in self.uargs['skip_paths']:
                if pathname == spath:
                    sprint('Skipping path:')
                    return True

        # print(entry.name, entry.is_symlink(), self.uargs['skip_syms'])
        if entry.is_dir():
            if self.uargs['skip_dirs']:
                lower = name.lower()
                for expr in self.uargs['skip_dirs']:
                    if expr in lower:
                        sprint('Skipping dir:')
                        return True
            if not os.access(pathname, os.R_OK):
                sprint("Can't access:")
                return True


        return False


    def walk(self, dirname=None, yield_stat=False):
        '''Walk through the file tree yielding filenames if conditions are met.
        yield_stat = yield files, stat information
        '''
        if not dirname:
            dirname = self.root
        for entry in os.scandir(dirname):
            name = entry.name
            pathname = os.path.join(dirname, name)
            if self.skip(entry, pathname):
                continue

            if entry.is_dir():
                yield from self.walk(pathname, yield_stat)
            else:
                # Process only files in bounds
                stat = entry.stat(follow_symlinks=False)
                if self.uargs['min_t'] <= stat.st_mtime <= self.uargs['max_t'] and \
                   self.uargs['min_size'] <= stat.st_size <= self.uargs['max_size']:
                    path = os.path.abspath(pathname)
                    size = stat.st_size
                    if self.uargs['print_files']:
                        print(rfs(size).ljust(11),
                              os.path.relpath(pathname, start=self.root),
                             )
                    self.size += size
                    self.count += 1
                    if yield_stat:
                        yield path, stat
                    else:
                        yield path



################################################################################
# Main






def _main():

    uargs = _parse_args()           # User arguments
    # print('hello', uargs)
    if not uargs:
        return False
    root = uargs.pop('root')

    tree = Tree(root, uargs)
    tree.scan(verbose=True)


    print("\nfolder_size test:", rfs(folder_size(root)))
    print("\ndirtime test:", dirtime(root))
    return True



if __name__ == "__main__":
    from sd.easy_args import easy_parse
    from sd.cds import ConvertDataSize
    sys.exit(not _main())
