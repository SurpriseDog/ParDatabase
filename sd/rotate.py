#!/usr/bin/python3
# Usage: rotate.py <filename> limit prefix

import os
import sys

def rotate(path, limit=3, prefix='.', move=True, verbose=False):
    '''Given a file path, Rotate files in a sequence 1, 2, 3... up to limit,
    and delete last file if and only if the sequence is full with no gaps

    limit = Max number of backup files in sequence (not including original)
    prefix = Add a prefix before each number
    move = Actually move the files instead of just listing them

    Returns sequence of files (whether or not there are any gaps)

    '''
    files = [path]
    path = os.path.splitext(path)
    files += [path[0] + prefix + str(num) + path[1] for num in range(1, limit+1)]
    print(files)
    if move:

        # Go thru file list looking for the first missing file
        gap = 0         # Position of first missing file
        for gap, name in enumerate(files):
            if not os.path.exists(name):
                break
        else:
            dest = files[-1]
            if verbose:
                print("Removing", dest)
            os.remove(dest)

        # Go thru file list backwards, moving each one
        dest = files[gap]
        for src in reversed(files[:gap]):
            if verbose:
                print("Moving", src, dest)
            os.rename(src, dest)
            dest = src

    return files



def _main():
    if len(sys.argv) - 1 >= 1:
        filename = sys.argv[1]
    else:
        print("Must include a filename to rotate.")
        return False

    if len(sys.argv) - 1 >= 2:
        limit = int(sys.argv[2])
    else:
        limit = 3

    if len(sys.argv) - 1 >= 3:
        prefix = sys.argv[3]
    else:
        prefix = '.'

    rotate(filename, limit, prefix, verbose=True)
    return True


if __name__ == "__main__":
    sys.exit(not _main())
