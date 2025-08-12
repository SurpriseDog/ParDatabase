#!/usr/bin/python3
import os
import shutil

from csvbase import Csvbase
from hash import get_hash, hash_cmp
from sd.file_progress import FileProgress, tprint


def user_answer(text='Y/N ?'):
	"Ask the user yes/no questions"
	ans = None
	while not ans:
		ans = input(text + " ").lower().strip()[:1]
		if ans == 'n':
			return False
		if ans == 'y':
			return True
	return False


class HexBase:
	"Store files with unique hashed file names in hex folder structure"


	def __init__(self, basedir):
		self.basedir = os.path.join(basedir, 'par2')		# Where to put the database folder
		self.pfiles = dict()				  				# hashes to dict(par file name : hash of par file)
		self.csv = Csvbase(self.basedir, 'hexbase.csv', headers="fhash filename phash".split())
		self.load()
		# print('debug hexbase', self.pfiles);
		

	def print(self,):
		print('basedir   ', self.basedir)
		print('pfiles	', len(self.pfiles))


	def clean(self, fhash):
		"Look up hash in the database and remove any .par2 files if unused"
		if fhash not in self.pfiles:
			return 0

		deleted = []
		for name in self.pfiles[fhash].keys():
			src = self.locate(name)
			if os.path.exists(src):
				print('Deleting', src)
				assert src.endswith('.par2')
				os.remove(src)
			else:
				print('Missing .par2 file, cannot delete!', fhash)
			deleted.append(fhash)
		if fhash in deleted:
			del self.pfiles[fhash]
		return len(deleted)



	def locate(self, name=''):
		"Return absolute path of filename in .pardatabase"
		assert 'par2' in self.basedir
		return os.path.join(self.basedir, name)


	def put(self, src, fhash, ending):
		"Given a hash move the file from the src location to the appropiate folder"
		# print("debug put", src, fhash)
		folder = fhash[:2].upper()
		oname = fhash[2:32+2] + ending
		oname = os.path.join(folder, oname)

		dest = self.locate(oname)
		phash = get_hash(src)


		if os.path.exists(dest):
			# print("Overwriting existing file:", dest)
			assert dest.endswith('.par2')
			os.remove(dest)
		shutil.move(src, dest)
		if fhash not in self.pfiles:
			self.pfiles[fhash] = dict()
		self.pfiles[fhash][oname] = phash
		
	
	def save(self,):
		rows = []
		for fhash, files in self.pfiles.items():
			for filename, phash in files.items():
				rows.append([fhash, filename, phash])
		return self.csv.save(rows)
		

	def load(self):
		self.pfiles = dict()
		rows = self.csv.load()
		for fhash, filename, phash in rows:
			if fhash not in self.pfiles:
				self.pfiles[fhash] = {}
			self.pfiles[fhash][filename] = phash
					
		# Make hex folders
		if not os.path.exists(os.path.join(self.basedir, '00')):
			for folder in [(('0' + hex(num)[2:])[-2:]).upper() for num in range(0, 256)]:
				os.makedirs(os.path.join(self.basedir, folder), exist_ok=True)


	def delete(self, fhash):
		"Given fhash, delete file from database"
		for name, _ in self.pfiles[fhash].items():
			path = self.locate(name)
			print('Deleting', path)
			assert path.endswith('.par2')
			os.remove(path)
		del self.pfiles[fhash]
		


	def get(self, fhash, cwd):
		"Given a hash, copy files from vault and put them in cwd"

		dest_files = []
		for name, phash in self.pfiles[fhash].items():
			src = self.locate(name)
			dest = os.path.join(cwd, os.path.basename(name))

			# Verify src integrity
			if not os.path.exists(src):
				print("ERROR! Missing .par2 file:", src)
				return False
			if not hash_cmp(phash, get_hash(src)):
				print("WARNING! .par2 files failed vaildation!")

			# Ensure dest in clear
			if os.path.exists(dest):
				print("Warning! path exists:", dest)
				if not user_answer("Overwrite? Y/N"):
					return False
			# print('get', src, '\nto', dest)
			shutil.copy(src, dest)
			dest_files.append(dest)
		return dest_files


	def verify(self,):
		"Verify that the .par2 files are available and their hash is still valid"
		# print('\ndebug hexbase verify data', self.data)
		# print('\ndebug hexbase verify pfiles', self.pfiles)
		 
		
		verified = 0
		deleted = set()
		
		fp = FileProgress()
		for fhash, record in self.pfiles.items():
			for filename in record.keys():
				filename = self.locate(filename)
				if os.path.exists(filename):
					fp.scan_file(filename)
				else:
					print("Missing file:", filename)
					deleted.add(fhash)

		for fhash in deleted:
			print("Removing record for:", fhash)
			del self.pfiles[fhash]
		

		bad_hashes = set()
		for fhash, record in self.pfiles.items():
			for filename, phash in record.items():
				src = self.locate(filename)
				tprint("Verifying File", fp.progress(filename=src)['default'])
				if not os.path.exists(src):
					print('WARNING: Could not find', src)
				elif not hash_cmp(phash, get_hash(src)):
					print('WARNING: incorrect hash', src)
					bad_hashes.add(fhash)
				else:
					verified += 1
		tprint("Done. Hashed", fp.done()['msg'])
		print()
		
		if deleted:
			self.save()
			
		return bad_hashes
