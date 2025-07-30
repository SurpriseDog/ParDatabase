#!/bin/bash

set -e

# Change to the directory where the script is located
cd "$(dirname "$0")"


title() {
	echo -e "\n\n\n$*"
	echo "================================================================"
}


# Create the test folder and assign it to a variable
target="/tmp/pardatabase_test_folder"
title "Creating test folder at $target"
if [ -d "$target" ]; then
	mv "$target" "${target}.old.$(date +%s)"
fi
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

for i in {1..3}; do
	# Random size between 1 and 100 KB
	size_kb=$(( (RANDOM % 100) + 1 ))
	dd if=/dev/urandom of="$subfolder/${i}" bs=1K count=$size_kb status=none
done





title "Creating par2 database"
'../pardatabase.py' $target --min 0 --singlecharfix



title "Moving file and updating database."
mv $target/subfolder/2 $target/subfolder/2.renamed
'../pardatabase.py' $target --min 0



title "Deleting file and updating database."
rm $target/subfolder/1
'../pardatabase.py' $target --min 0  --clean



title "Database:"
cat $target/.pardatabase/database.xz | unxz

# todo test alt basedir



# invert file
title "Corrupting file:"
./byte_inverter.sh CORRUPT_MY_FILE $target/large

title "Checking for error"
../pardatabase.py --verify $target || echo "Got expected error code"


title "Repair file"
../pardatabase.py $target --repair $target/large



title "Verifying database:"
../pardatabase.py --verify $target

title "All ok!"









# todo delete test folder 
# rm -r /tmp/pardatabase_test_folder
