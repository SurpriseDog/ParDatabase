#!/usr/bin/python3
import os
import sys

def rotate(path, limit=3, prefix='.', move=True, verbose=False):
    '''Given a file path, move files through a progression until limit is reached and delete oldest file,.''
    limit = Max number of files before deleting final one
    prefix = add a prefix before each number
    move = Actually move the files instead of just listing them

    Returns list of files in sequence

    '''
    files = [path]
    path = os.path.splitext(path)
    files += [path[0] + prefix + str(num) + path[1] for num in range(1, limit+1)]
    dest = files.pop(-1)
    if move:
        if os.path.exists(dest):
            if verbose:
                print("Removing", dest)
            os.remove(dest)
        for src in reversed(files):
            if os.path.exists(src):
                if verbose:
                    print("Moving", src, dest)
                os.rename(src, dest)
            dest = src
    return files


def main():
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

    rotate(filename, limit, prefix)
    return True


if __name__ == "__main__":
    sys.exit(not main())
