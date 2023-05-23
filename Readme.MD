ParDatabase enables you to recursively generate `.par2` parity files for folders or filesystems, preventing **bit rot** resulting from hardware failure or other errors. It utilizes the [par2](https://manpages.org/par2) utility and employs [SHA-512](https://en.wikipedia.org/wiki/SHA-2) checksums to detect any malicious file tampering. Unlike alternative tools, ParDatabase examines modification times to automatically identify modified files, generating new .par2 files instead of declaring them corrupted.


Features:


* Central database with deduplication:
    All .par2 files are centralized within a dedicated .pardatabase folder located at the root of the scanned folder, eliminating the scattering of parity files throughout the filesystem. This setup provides the freedom to rename, delete, or even move files without concern for the corresponding .par2 files. Since the .par2 files reference the [SHA-512](https://en.wikipedia.org/wiki/SHA-2) hash of your files rather than their filenames, regeneration is only required if the referenced file is modified. In cases where duplicate files exist, they will share the same hash and therefore won't require duplicate parity. This means that files remain protected even if they are renamed or relocated within the folder hierarchy.

* Fine control over which files are selected.
     - You have the flexibility to choose which files are scanned and have parity generated. By default, all files are scanned and hashed, but parity generation is limited to files selected based on size, modification time, MIME type and more...
     - See the section on [Parity and Scan arguments] for more information or run `pardatabase.py -h` to see the detailed help menu.

* Pause and Resume capability:
    - You can pause and save the database at any time by pressing ctrl-c. When you resume, it will seamlessly pick up from where you left off during the previous session.


### Usage

Make sure the par2 utility is installed with: `sudo apt install par2`

Run: `pardatabase.py <Target Directory>`

To generate a .pardatabase folder in the target directory, containing all the necessary parity files. If any files are modified in the future, running it again will create new parity files for the modified files. Feel free to press Ctrl-C at any time to pause the par2 file generation.

Get more detailed help with: `pardatabase.py -h`


Note: Make sure to set up a cron job to keep the database updated automatically or new files will not have protection!

Reminder: Don't forget to set up a cron job to automatically keep your database updated!


### Parity and Scan Arguments

You have the flexibility to choose which files are scanned and have parity generated. By default, all files are scanned and hashed, but parity generation is limited to files over 1 MB. There are two reasons for this:

    1. Par2 is inefficient for smaller files and can create parity files larger than the original file itself.

    2. Larger files are more likely to suffer bit rot and sector failures, making it more crucial to protect them with parity.

To customize the selection of files for parity generation, simply run the program with options such as `--minsize` and `--maxsize`. Additionally, you have a wide array of other options at your disposal to choose files based on criteria like file modification time, MIME type, and more. Explore the full range of available options by running the program with `-h`

Use `--dryrun` to test what your arguments do.

Use `--print_files` to see a detailed list of what files are being included.


### Run Modes


| | |
|
| `--verify ` | Verify existing files by comparing the hash. |
|| By default, Pardatabase performs a scan for modified files within your directory. To recalculate the hash of all existing files, simply run the program with the `--verify` option. This verify the hash of all files in the directory. |
| | |
| `--repair <filename>` | Verify and repair existing files. |
| | To repair damaged files, utilize the --repair option. Rest assured, this process won't alter the existing files; it will only create new ones after attempting to repair them using their corresponding parity files. |
| | |
| `--clean` | Delete old unused .par2 files from the database. |
| | Par2 files will persist in the database until removed using the --clean option. Periodically running `pardatabase.py --clean` will search for and eliminate any orphaned par2 files. It's worth noting that since pardatabase references files by their hash, it's possible for two or more named files to be identical and share the same .par2 files. As a result, the cleaner will only delete par2 files once all references to them have been deleted from the file system.
|
| | |

### Debugging

  * Error: `what(): basic_string::at: __n (which is 1) >= this->size() (which is 1)`
    - This is a par2 error caused by [single character file names.](https://github.com/Parchive/par2cmdline/issues/145)
    - The solution is to update your version of par2.
    - If this can’t be done, then you can run the program with the `--singlecharfix` option to temporarily rename these files.

### Testing

To fake corrupt a file, try appending to a text file, while keeping the modification date in the past.


  * `date '+%H:%M:%S'` >> a.uniquely.named.test.file.txt`
  * `pardatabase.py`
  * `date '+%H:%M:%S'` >> a.uniquely.named.test.file.txt`
  * `touch -d 2000-01-1 a.uniquely.named.test.file.txt`
  * `pardatabase.py' --repair`


### Future ideas:

Let me know in the github `issues` section if this or any other idea is interesting to you.

  * Creating parity for large files in sections. - This way if only a section of a multi gigabyte file changes, the parity does not have to be recomputed for the entire thing.

  * Copying small files to a database structure. - Since par2 is inefficient for small files under a megabyte, copying the files to a database could make more sense.

  * Compatability with other operating systems such as Windows or Mac.
