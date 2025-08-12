#!/bin/bash

target="/tmp/pardatabase_test_folder"
rm -r /tmp/pardatabase_test_folder || echo

set -e

# Change to the directory where the script is located
cd "$(dirname "$0")"


title() {
	echo -e "\n\n\n\n$*"
	echo "================================================================"
}


title "Creating test folder at $target"
mkdir -p "$target"

# Create an empty file
touch "$target/empty_file"

# Create a file between 1-2 MB (e.g., 1.5 MB)
dd if=/dev/urandom of="$target/large" bs=1K count=1198 status=none

# Create a one letter file
dd if=/dev/urandom of="$target/Q" bs=1K count=64 status=none

# Copy file
cp "$target/Q" "$target/Q_copy"

# Create a subfolder with files of varying size (1–100 KB)
subfolder="$target/subfolder"
mkdir -p "$subfolder"


for i in {1..4}; do
	# Random size between 1 and 100 KB
	size_kb=$(( (RANDOM % 100) + 1 ))
	dd if=/dev/urandom of="$subfolder/${i}" bs=1K count=$size_kb status=none
done


title "Creating par2 database"
'../pardatabase.py' $target --singlecharfix


title "Including small files"
'../pardatabase.py' $target --singlecharfix --min 0


title "Verifying database:"
../pardatabase.py  $target --verify


title "Moving file and updating database."
mv $target/subfolder/2 $target/subfolder/2.$RANDOM.$RANDOM
'../pardatabase.py' $target --min 0


title "Deleting file and updating database."
rm $target/subfolder/1
'../pardatabase.py' $target --min 0  --clean


# todo test alt basedir



# invert file
title "Corrupting file:"
./byte_inverter.py CORRUPT_MY_FILE $target/large 2000
../pardatabase.py $target --verify && { echo "Unexpected Success???"; exit 1; } || echo "Got expected error code."

title "Repair file"
../pardatabase.py $target --repair $target/large
../pardatabase.py $target


title "Corrupting par2:"
./byte_inverter.py CORRUPT_MY_FILE `find $target/.pardatabase/par2 -name "*1.par2" -type f | head -n 1`
../pardatabase.py $target --verify


title "Corrupting original copy of the database"
sed '3s/^/X/' -i "$target/.pardatabase/database.csv"
'../pardatabase.py' $target


basedir="/tmp/.pardatabase_alt_folder"
title "Testing alternative basedir location: $basedir"
rm -r /tmp/.pardatabase_alt_folder || echo
mv $target/.pardatabase/ /tmp/.pardatabase_alt_folder
'../pardatabase.py' $target --basedir $basedir --verify


title "Adding file and updating."
echo "newfile" > $target/newfile
'../pardatabase.py' $target --basedir $basedir --min 0
'../pardatabase.py' $target --basedir $basedir --verify


title "Checking that .pardatabase is gone."
'../pardatabase.py' $target --verify && { echo "Unexpected Success???"; exit 1; } || echo "Got expected error code."

title "Restoring .pardatabase to it's original location"
mv /tmp/.pardatabase_alt_folder $target/.pardatabase/

title "All ok!"






# todo delete test folder 
# rm -r /tmp/pardatabase_test_folder
