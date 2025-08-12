#!/usr/bin/env python3

import os
import sys


def print_usage():
	print("This is a debug tool designed to corrupt and uncorrupt files without changing the modification date.")
	print("To run it type: ./byte_inverter.py CORRUPT_MY_FILE <filename>")
	print("Then run it again to uncorrupt the file.")


def main():
	if len(sys.argv) < 3 or sys.argv[1] != "CORRUPT_MY_FILE":
		print_usage()
		sys.exit(1)

	file = sys.argv[2]

	if not os.path.isfile(file):
		print(f"Error: '{file}' is not a valid file.")
		sys.exit(1)

	# Optional third argument: number of bytes to invert
	num_bytes = 2
	if len(sys.argv) >= 4:
		num_bytes = abs(int(sys.argv[3]))


	# Get access and modification times
	stat_info = os.stat(file)
	atime = stat_info.st_atime
	mtime = stat_info.st_mtime

	with open(file, 'r+b') as f:
		# Read the first num_bytes bytes
		original = f.read(num_bytes)
		if len(original) < num_bytes:
			print(f"Warning: File has only {len(original)} bytes. Inverting available bytes.")
			num_bytes = len(original)

		# Invert each byte
		inverted = bytes([~b & 0xFF for b in original])

		# Seek back and write them
		f.seek(0)
		f.write(inverted)

	print(f"First {num_bytes} bytes of '{file}' have been inverted.")

	# Restore timestamps
	os.utime(file, (atime, mtime))

if __name__ == "__main__":
	main()
