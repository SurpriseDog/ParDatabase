#!/bin/bash

# Check to make sure the user is serious
if [[ "$1" != "CORRUPT_MY_FILE" ]]; then
	echo -e "This is a debug tool designed to corrupt and uncorrupt files without changing the modification date."
	echo -e "To run it type ./byte_inverter.sh CORRUPT_MY_FILE <filename> "
	echo -e "Then run it again to uncorrupt the file."
	exit 1
fi

# Check for input file
file="$2"
if [[ ! -f "$file" ]]; then
	exit 1
fi


# Get original modification and access times
atime=$(stat -c %X "$file")
mtime=$(stat -c %Y "$file")

# Read the first byte
byte=$(dd if="$file" bs=1 count=1 2>/dev/null | xxd -p)

# Invert the byte (bitwise NOT)
inv_byte=$(printf "%02x" $(( 0xFF ^ 0x$byte )))

# Write the inverted byte back
printf "\\x$inv_byte" | dd of="$file" bs=1 count=1 conv=notrunc 2>/dev/null

echo "First byte of $file has been inverted."

# Restore timestamps
touch -a -d @"$atime" "$file"
touch -m -d @"$mtime" "$file"

