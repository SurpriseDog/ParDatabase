ParDatabase is a simple python script which allows you to recursively create .par2 parity files for an entire folder or filesystem. It uses the [par2](https://manpages.org/par2) utility to prevent **bit rot** caused by hardware failure or errors.

Unlike other utilities, it will look at modification times to automatically detect when files have been updated and generate new .par2 files. Also it doesn't litter .par2 files all over the filessytem, instead keeping them all in a single .par2_database folder.

** Usage **

Install par2 with: `sudo apt install par2`

Run: `pardatabase.py <Target Directory>`

to generate a .par2_database folder in the target directory with all of the parity files needed. If any files are modified in the future, simply run it again to create new parity files for any modified files.

Get more detailed help with: `pardatabase.py -h` including options to:

  * `--repair`           Verify and repair existing files.
  * `--verify `          Verify existing files by comparing the hash.


** Testing **

To fake corrupt a file, try appending to a text file, while keeping the modification date in the past:

  * `echo "new text" >> "$file" && touch -d 2000-01-1 "$file"`
