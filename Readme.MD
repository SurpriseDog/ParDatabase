ParDatabase allows you to recursively create .par2 parity files for an entire folder or filesystem. It uses the [par2](https://manpages.org/par2) utility to prevent **bit rot** caused by hardware failure or errors, and uses [SHA-512](https://en.wikipedia.org/wiki/SHA-2) sums to detect malicious file tampering. Unlike other utilities, it will also look at modification times to automatically detect when files have been updated and generate new .par2 files instead of simply declaring the file corrupted.

All of the .par2 files are kept in a single .pardatabase folder at the root of the folder you are scanning, instead of scattered across the filesystem. This means that you are free to rename, delete or move files around without worrying about the .par2 files. Because the .par2 files are referencing the hash of your files, not the filename, they won't need to be regenerated.

Just simply run pardatabase.py again every so often with cron to keep the database updated.

## Usage

Make sure the par2 utility is installed with: `sudo apt install par2`

Run: `pardatabase.py <Target Directory>`

to generate a .pardatabase folder in the target directory with all of the parity files needed. If any files are modified in the future, simply run it again to create new parity files for any modified files. Press Ctrl-C at anytime to pause par2 file generation

Get more detailed help with: `pardatabase.py -h`


| | |
| :---   | :--- |
| **Run Modes** |    |
| | |
| `--verify ` | Verify existing files by comparing the hash. |
|| By default, pardatabase will only scan your directory for modified files. To recalculate the hash of all existing files, please run with the `--verify` option.|
| | |
| `--repair <filename>` | Verify and repair existing files. |
| | Running verify will only check the hash of existing files. To repair files at the same time, use the `--repair` option. Don’t worry, it won’t modify existing bad files, only create new ones after attempting to repair them with their parity file.|
| | |
| `--clean` | Delete old unused .par2 files from the database. |
| | par2 files will stay in the database until removed with the `--clean` option. Run `pardatabase.py --clean` occasionally to search for and remove these orphaned par2 files. Because pardatabase references files by their hash, 2 or more named files could be identical and sharing the same .par2 files which means that the cleaner will only delete par2 files after all references to them have disappeared from the file-system. |
| | |


## Debugging

  * Error: `what(): basic_string::at: __n (which is 1) >= this->size() (which is 1)`
    - This is a par2 error caused by [single character file names.](https://github.com/Parchive/par2cmdline/issues/145)
    - The solution is to update your version of par2.
    - If this can’t be done, then you can run the program with the `--singlecharfix` option to temporarily rename these files.

## Testing

To fake corrupt a file, try appending to a text file, while keeping the modification date in the past.


  * `date '+%H:%M:%S'` >> a.uniquely.named.test.file.txt`
  * `pardatabase.py`
  * `date '+%H:%M:%S'` >> a.uniquely.named.test.file.txt`
  * `touch -d 2000-01-1 a.uniquely.named.test.file.txt`
  * `pardatabase.py' --repair`
