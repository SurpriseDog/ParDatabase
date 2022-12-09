ParDatabase allows you to recursively create .par2 parity files for an entire folder or filesystem. It uses the [par2](https://manpages.org/par2) utility to prevent **bit rot** caused by hardware failure or errors, and uses [SHA-512](https://en.wikipedia.org/wiki/SHA-2) sums to detect malicious file tampering. Unlike other utilities, it will also look at modification times to automatically detect when files have been updated and generate new .par2 files instead of simply declaring the file corrupted.

All of the .par2 files are kept in a single .pardatabase folder at the root of the folder you are scanning, instead of scattered across the filesystem. This means that you are free to rename, delete or move files around without worrying about the .par2 files. Because the .par2 files are referencing the hash of your files, not the filename, they won't need to be regenerated.

Just simply run pardatabase.py again every so often with cron to keep the database updated.

## Usage

Make sure the par2 utility is installed with: `sudo apt install par2`

Run: `pardatabase.py <Target Directory>`

to generate a .pardatabase folder in the target directory with all of the parity files needed. If any files are modified in the future, simply run it again to create new parity files for any modified files.

Get more detailed help with: `pardatabase.py -h` including options to:

  * `--repair`           Verify and repair existing files.
  * `--verify `          Verify existing files by comparing the hash.


## Testing

To fake corrupt a file, try appending to a text file, while keeping the modification date in the past.


  * `echo "new text" >> a.uniquely.named.test.file.txt`
  * `pardatabase.py`
  * `echo "new text" >> a.uniquely.named.test.file.txt`
  * `touch -d 2000-01-1 a.uniquely.named.test.file.txt`
  * `pardatabase.py' --repair`
