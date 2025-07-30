#!/usr/bin/python3

import os
import subprocess

TMPNAME = '.pardatabase_tmp_file'	   # Temporary file used when generating .par2

class Info:
	"Info on filepaths within system"

	def __init__(self, pathname=None, load=None, base='.'):

		if load:
			# Load from json dict
			for key, val in load.items():
				setattr(self, key, val)
		else:
			self.pathname = pathname		# Relative path (can be changed between runs)
			self.hash = None
			self.mtime = None
			self.size = None

		self.fullpath = os.path.join(base, self.pathname)
		self.cwd = os.path.dirname(self.fullpath)

		if not load:
			self.update()


	def tojson(self,):
		"Return neccesary variables as compact json dict."
		return {key:val for key, val in vars(self).items() if key in ['pathname', 'hash', 'mtime', 'size']}


	def update(self,):
		"Update file size and mtime"
		self.mtime = os.path.getmtime(self.fullpath)
		self.size = os.path.getsize(self.fullpath)


	def find_tmp(self,):
		"Find existing par2 tmp files"
		for name in os.listdir(self.cwd):
			if name.endswith('.par2') and name.startswith(TMPNAME):
				yield os.path.join(self.cwd, name)


	def remove_existing(self,):
		"Delete leftover .par2 files"
		for name in self.find_tmp():
			print("Removing existing par2 file:", name)
			os.remove(name)


	def repair(self, dest_files):
		"Attempt to repair a file with the files found in the database"
		cmd = "par2 repair".split() + [sorted(dest_files)[0]] + [self.fullpath]
		print('\n' + ' '.join(cmd))
		ret = subprocess.run(cmd, check=False, cwd=self.cwd)
		status = not ret.returncode
		if status:
			for file in dest_files:
				os.remove(file)
		return status


	def run_par2(self, par2_options, name, verbose=False):
		'''Run par2 command
		par2_options are passed to par2
		name is the alterate name in case the file needs to be renamed
		'''
		self.remove_existing()
		cmd = "par2 create -n1 -qq".split()
		if par2_options:
			cmd.extend(('-' + par2_options).split())
		cmd += ['-a', TMPNAME + '.par2', '--', os.path.basename(name)]
		if verbose:
			print(' '.join(cmd))
		return subprocess.Popen(cmd, stdout=subprocess.DEVNULL, cwd=self.cwd)
