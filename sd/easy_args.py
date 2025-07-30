#!/usr/bin/python3
# Learn more about how this works by visiting: https://github.com/SurpriseDog/EasyArgs

import os
import re
import sys
from argparse import ArgumentParser, SUPPRESS

from sd.common import list_get
from sd.columns import auto_cols
from sd.common import undent

################################################################################
# Easy helper functions:

def easy_parse(options, positionals=None, hidden=None, **kargs):
	'''
	Simpler way to pass arguments to ArgMaster class.
	All kargs are passed to ArgMaster. See the actual implementation for details.
	'''
	am = ArgMaster(**kargs)
	am.update(options, title="Optional Arguments")

	if positionals:
		am.update(positionals, title="Positional Arguments", positionals=True)

	if hidden:
		am.update(hidden, "Used for testing purposes:", hidden=True)

	return am.parse()


def show_args(uargs):
	"Show the arguments returned"
	if not uargs:
		return
	out = sorted([[key, repr(val)] for key, val in (vars(uargs)).items()])
	out.insert(0, ('Variable:', 'Value:'))
	out.insert(1, ('=========', '======'))
	auto_cols(out)
	print()


def dict_args(lines, **kargs):
	"Given ArgMaster lines, make a dictionary of alias to arg"
	return {arg['alias']:arg for arg in ArgMaster().parse_lines(lines, **kargs)}


# #############################################################################3
class ArgMaster():
	'''
	A wrapper class for ArgumentParser with easy to use arguments.
	sortme will sort all arguments by name (except positionals)
	other arguments are passed onto the ArgumentParser constructor


	Format for the lines:
		Pass an array with lines in the format:

			('alias', 'variable_name', type, default),
			"help string",

		You only need to include the fields required, but you can't skip over any.
			('alias', '',)		# okay
			('alias', type,)	  # not okay

		Substitute the word list with a number like "2" to get that number of args required.
			('list-args, '', 2)

		Positional arguments are optional by default, but you can specify a number to make them required.
		To use them, make sure to pass: positionals=True to parse_lines

			('pos-arg', '', 1)

	See what your arguments are producing by passing verbose=True or running easy_args.show_args(args)

	'''
	_default_info = dict(\
						alias=None,	 # --variable name
						varname=None,   # Variable Name
						default=None,   # Default value
						typ=None,	   # Variable Type
						action=None,	# Special for bool variables
						msg='',		 # Message help for each line
						nargs=None,	 # nargs for parser
						)

	def __init__(self, sortme=True, allow_abbrev=True, usage=None, description=None, newline='\n',
				 verbose=False, quit_on_error=True, **kargs):

		# Parsing
		self.verbose = verbose				  # Print what each argument does
		self.dashes = ('-', '--')			   # - or -- before argument
		self.quit_on_error = quit_on_error	  # Quit on error

		# Help Formatting:
		self.sortme = sortme			# Sort all non positionals args
		self.usage = usage			  # Usage message
		self.description = description
		self.newline = newline		  # Newlines in print_help

		# Internal
		self.parser = ArgumentParser(allow_abbrev=allow_abbrev, add_help=False, usage=SUPPRESS, **kargs)
		self.groups = []				# List of all groups

		# Allow optionals before positionals:
		self.intermixed = hasattr(self.parser, 'parse_intermixed_args')


	def parser_add(self, info, parser, positionals=False):
		"Update argument to parser:"

		if positionals:
			parser.add_argument(info['varname'], default=info['default'], nargs=info['nargs'], help=info['msg'])
		else:
			# info['alias'] = '--' + info[alias']
			aliases = [d + info['alias'] for d in self.dashes]
			if info['typ'] == bool:
				parser.add_argument(*aliases, dest=info['varname'], default=info['default'],
									action=info['action'], help=info['msg'])
			else:
				parser.add_argument(*aliases, dest=info['varname'], default=info['default'],
									type=info['typ'], nargs=info['nargs'], help=info['msg'], metavar='')


	def process_args(self, info, args, positionals=False):
		'''Extract parser information from each args array'''

		# Read the values from the tuple:
		info['alias'] = args[0].lstrip('-')

		# Variable Name
		info['varname'] = list_get(args, 1)
		if not info['varname']:
			info['varname'] = info['alias']

		# Type
		info['typ'] = list_get(args, 2, str)

		# Default value
		if info['typ'] == list or type(info['typ']) == int:
			info['default'] = list_get(args, 3, [])
		else:
			info['default'] = list_get(args, 3, None)

		# Argument Type and number required
		if info['typ'] == list:
			info['nargs'] = '*'
			info['typ'] = str
		elif isinstance(info['typ'], int):
			if positionals and info['typ'] == 1:
				info['nargs'] = None
			else:
				info['nargs'] = info['typ']
			info['typ'] = str
		else:
			info['nargs'] = '?'

		# Special handing for booleans
		if info['typ'] == bool:
			if info['default']:
				info['action'] = 'store_false'
			else:
				info['action'] = 'store_true'
				info['default'] = False


	def parse_lines(self, lines, autoformat=True, positionals=False, hidden=False):
		'''
		Scan lines and convert to info dict
		Details for which are found in the class documentation.

		positonals = Is this a list of positonal arguments
		hidden =	 Is this a list of hidden arguments that don't show up in help
		autoformat = Capitalize sentences and add a period

		'''

		# Make sure the loop ends on a help string
		if not isinstance(lines[-1], str):
			lines.append("")

		out = []			#  List of info dicts
		info = self._default_info.copy()

		for index, args in enumerate(lines):
			# Add help if available
			if isinstance(args, str):
				info['msg'] = undent(args.strip())
				if autoformat:
					if info['msg'] and not info['msg'].endswith('.'):
						last = info['msg'].split()[-1]
						if last[-1].isalnum() and not last.startswith('-'):
							info['msg'] = info['msg'] + '.'
				if info['default']:
					info['msg'] += "  Default: " + str(info['default'])

			# Hide the help text:
			if hidden:
				info['msg'] = SUPPRESS

			# If on a new tuple line, add_argument
			if info['alias'] or info['varname']:
				out.append(info)
				info = self._default_info.copy()

			# Continue if not on a new tuple line
			if isinstance(args, str):
				continue

			self.process_args(info, args, positionals)

			# Last line
			if index == len(lines) - 1:
				out.append(info)


		if self.verbose:
			for info in out:
				print('alias  :', repr(info['alias']))
				print('dest   :', repr(info['varname']))
				print('default:', repr(info['default']))
				print('type   :', repr(info['typ']))
				print('nargs  :', repr(info['nargs']), '\n\n')

		return out

	def parse(self, args=None, am_help=True, **kargs):
		"Parse the args and return them"
		if not args:
			args = sys.argv[1:]

		if not am_help:
			self.parser.add_help = True
			return self.parser.parse_args(args)

		# Match help
		for arg in args:
			if re.match('--*h$|--*help$', arg):
				self.print_help(**kargs)
				if self.quit_on_error:
					sys.exit(0)
				return None

		# Try to parse the arguments and show help if failed
		try:
			if self.intermixed:
				return self.parser.parse_intermixed_args(args)
			else:
				return self.parser.parse_args(args)
		except SystemExit:
			self.print_help(**kargs)
			if self.quit_on_error:
				sys.exit(0)


	def print_help(self, show_type=True, wrap=-4, tab='  '):
		'''Print a custom help message using only ArgMaster args
		show_type = append the variable type expected after each optional argument.
		--arg <int> <int> will expect 2 integers after the arg
		wrap = word wrap instead of using full terminal. 0 = Terminal width
		sort = sort alphabetically. Positional variables are never sorted.
		To sort individual groups, add a special key: group.sortme = True

		Warning: If your variable is not in a group, it will not be shown!'''

		if self.description:
			print('\n' + self.description)

		if self.usage:
			name = os.path.basename(sys.argv[0])
			print('\n' + "Usage:", name, self.usage)
		final = []
		width = 0					   # Max width of the variables column
		for group in self.groups:
			out = []
			for args in group['args']:
				msg = args['msg']
				if msg == SUPPRESS:
					continue
				alias = max(self.dashes) + args['alias']
				if show_type:
					if args['typ'] and args['typ'] != bool:
						if args['typ'] == list:
							typ = '...'
						else:
							typ = '<' + str(args['typ']).replace('class ', '')[2:-2] + '>'
						alias += ' ' + typ
				if len(alias) > width:
					width = len(alias)
				out.append([alias, msg])

			if group['sortme'] is not None:
				sortme = group['sortme']
			else:
				sortme = self.sortme
			if sortme:
				# Sort the group while mainting the order of positional arguments at the top
				positionals = [out.pop(line) for line in range(len(out) - 1, -1, -1) if out[line][0].startswith('<')]
				out.sort()
				out = list(reversed(positionals)) + out
			final.append(out)

		for index, out in enumerate(final):
			group = self.groups[index]
			if out:
				print(self.newline, end='')
				for line, _ in enumerate(out):
					out[line][0] = tab + out[line][0].ljust(width)
				if 'title' in group:
					print(group['title'])   # .rstrip(':') + ':')
				if 'description' in group:
					auto_cols([[tab + group['description']]], wrap=wrap)
				auto_cols(out, wrap=wrap)
		print()


	def update(self, lines=None, argdicts=None, title=None, sortme=None, positionals=False, hidden=False):
		"Pass list to parse_lines() and append result to parser"

		group = self.parser.add_argument_group(title)
		if lines:
			argdicts = self.parse_lines(lines, hidden=hidden, positionals=positionals)

		for info in argdicts:
			self.parser_add(info, parser=group, positionals=positionals)
		self.groups.append(dict(args=argdicts, title=title, sortme=sortme))


def help_parser(parser, show_type=True, sortme=True, wrap=100, tab='  '):
	'''Standalone version of help that only needs an argparse parser

		Print a custom help message from the ArgumentParser class
		show_type = append the variable type expected after each optional argument.
		--arg <int> <int> will expect 2 integers after the arg
		wrap = word wrap instead of using full terminal. 0 = disable
		sort = sort alphabetically. Positional variables are never sorted.
		To sort individual groups, add a special key: group['sortme'] = True

		Warning: If your variable is not in a group, it will not be shown!
	'''
	def alias(action):
		"Return string representing the variable for the leftmost column"
		if action.option_strings:
			text = ', '.join(action.option_strings)
		else:
			text = action.dest
		if show_type:
			if not action.option_strings:
				text = '<' + text + '>'
			if action.type and action.type != bool:
				if action.type == list:
					typ = '...'
				else:
					typ = '<' + str(action.type).replace('class ', '')[2:-2] + '>'

				if isinstance(action.nargs, int):
					count = action.nargs
				else:
					count = 1
				text += ' ' + ' '.join([typ] * count)
		return text

	final = []
	width = 0
	for group in parser._action_groups:		 # pylint: disable=W0212
		out = []
		for action in group._group_actions:	 # pylint: disable=W0212
			if action.help and action.help == SUPPRESS:
				continue
			msg = list(action.help)
			msg[0] = msg[0].title()
			variable = alias(action)
			out.append([variable, ''.join(msg)])
			if len(variable) > width:
				width = len(variable)
		if sortme or group.__dict__.get('sortme', False):
			# Sort the group while mainting the order of positional arguments at the top
			positionals = [out.pop(line) for line in range(len(out) - 1, -1, -1) if out[line][0].startswith('<')]
			out.sort()
			out = list(reversed(positionals)) + out
		final.append(out)

	for group in parser._action_groups:		 # pylint: disable=W0212
		out = final.pop(0)
		if out:
			for line, _ in enumerate(out):
				out[line][0] = tab + out[line][0].ljust(width)

			print()
			if group.title:
				print(group.title + ':')
			if group.description:
				auto_cols([[tab + group.description]])
			auto_cols(out, wrap=wrap)





################################################################################
# Testing:



def _tester():
	positionals = [\
	["target", '', str, '.'],
	"Target Directory to generate par2",
	]

	args = [\
	['basedir', '', str],
	"Base directory to put the par2 database, (Defaults to the target directory)",
	['clean', '', bool],
	"Delete old unused .par2 files from the database.",
	['dryrun', '', bool],
	"Show what action would be done without doing it.",
	['nice', '', int, 8],
	"Run program with a nice level, 0=disable",
	['singlecharfix', '', bool],
	"Temporarily rename files to fix a bug in par2.",
	"Clean database of extraneous files",
	['options', '', str],
	'''Options passed onto par2 program, use quotes:
	For example "-r5" will set the target recovery percentage at 5%
	More options can be found by typing: man par2''',
	['sequential', '', bool],
	"Hash the file before running par2 (instead of running in parallel)",
	['delay', '', float],
	"Wait for (delay * read_time) after every read to keep drive running cooler.",
	['verify', '', bool],
	"Verify existing files by comparing the hash",
	['repair', '', str],
	"Repair an existing file",
	]

	args = easy_parse(args,
					  positionals,
					  usage='<Target Directory>, options...',
					  description='Create a database of .par2 files for a directory')


	show_args(args)

if __name__ == "__main__":
	_tester()
