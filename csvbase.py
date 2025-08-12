#!/usr/bin/python3

import os
import sys
import csv
import time
import json
import shutil
import hashlib


from hash import TRUNCATE
from sd.rotate import rotate
from sd.format_number import fmt_time

BAK_NUM = 8	    # Number of database backups

VERSION = 2.0   
# Database version number
# 2.0 Switched to csv for increased robustness and speed


class Csvbase:
	"Create a csv database that rotates and has a checksum."


	def __init__(self, basedir, name, headers=None):
		self.basedir = basedir	
		self.name = name	
		self.version = VERSION
		self.last_save = time.time()	# Last save time
		self.meta = dict()				# First line contains dict
		
		if headers:
			self.headers = headers
		else:
			self.headers = []
	
	
	def load(self,):
		'''Load the database'''
		# Load the database if possible
		baks = rotate(os.path.join(self.basedir, self.name), move=False, limit=BAK_NUM)
		good = None		 # Name of file successfully loaded
		rows = []
		for path in baks:
			if os.path.exists(path):
				print("Loading:", path)
				rows = self.load_csv(path)
				if rows != False:
					good = path
					break
		else:
			# If no good database detected:
			if not good and os.path.exists(baks[0]):
				print("Corrupted database moved to", baks[1])
				rotate(baks[0], limit=BAK_NUM)
			return []
		
		if rows:
			print("\tDatabase was last saved", fmt_time(time.time() - self.last_save), 'ago')
		return rows
			


	def load_csv(self, path):
		"Load the database from CSV format"
		rows = []

		m = hashlib.sha512()
		with open(path, mode='r', newline='', encoding='utf-8') as f:
			reader = csv.reader(f)
			meta_line = next(reader, None)
			if not meta_line:
				return

			meta = json.loads(meta_line[0])
			self.version = meta.get('version', 1.0)
			self.last_save = meta.get('mtime', 0)

			self.headers = next(reader, [])
			for row in reader:
				if row[0].startswith('#CHECKSUM'):
					if '#CHECKSUM:' + m.hexdigest()[:TRUNCATE] == row[0]:
						return rows
					else:
						print("Checksum mismatch!")
						return False
						
						
				else:
					m.update(','.join(row).encode())
			
				rows.append(row)
		return False
			


	def save(self, rows):

		# Rotate any old backup files
		baks = rotate(os.path.join(self.basedir, self.name), limit=BAK_NUM)

		meta = dict(
			mtime=time.time(),
			encoding='hex',
			version=self.version,
		)
		self.last_save = meta['mtime']
		m = hashlib.sha512()

		# Save main CSV
		out = baks[0]
		if not os.path.exists(baks[1]):
			print("Creating database:", out)
		
		with open(out, mode='w', newline='', encoding='utf-8') as f:
			writer = csv.writer(f)
			writer.writerow([json.dumps(meta)])
			writer.writerow(self.headers)
			for row in rows:
				writer.writerow(row)
				m.update(','.join(row).encode())			
			writer.writerow([f'#CHECKSUM:{m.hexdigest()[:TRUNCATE]}'])
				
			f.flush()
			os.fsync(f.fileno())
		print("Wrote:", out)
		
			
		
		# If only copy, make a backup
		if not os.path.exists(baks[1]):
			shutil.copy(baks[0], baks[1])

		return True



def tester():
	print("\nTesting load:")
	base = sys.argv[1]
	name = sys.argv[2]
	c = Csvbase(base, name)
	rows = c.load()
	
	print("\nRows:")
	for row in rows:
		print(row)
	
		
if __name__ == "__main__":
	tester()
