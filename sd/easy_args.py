#!/usr/bin/python3
# Learn more about how this works by visiting: https://github.com/SurpriseDog/EasyArgs

import os
import re
import sys
from argparse import ArgumentParser, SUPPRESS

from sd.common import list_get
from sd.columns import auto_cols
from sd.common import undent


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
    auto_cols(sorted([[key, repr(val)] for key, val in (vars(uargs)).items()]))
    print()


class ArgMaster():
    '''
    A wrapper class for ArgumentParser with easy to use arguments. See update_parser() for details.
    sortme will sort all arguments by name (except positionals)
    other arguments are passed onto the ArgumentParser constructor
    '''

    def __init__(self, sortme=True, allow_abbrev=True, usage=None, description=None, newline='\n',
                 verbose=False, exit=True, **kargs):

        # Parsing
        self.verbose = verbose          # Print what each argument does
        self.dashes = ('-', '--')       # - or -- before argument
        self.exit = exit                # Quit on error

        # Help Formatting:
        self.sortme = sortme            # Sort all non positionals args
        self.usage = usage              # Usage message
        self.description = description
        self.newline = newline          # Newlines in print_help
        self.autoformat = True          # Capitalize sentences and add a period

        # Internal
        self.parser = ArgumentParser(allow_abbrev=allow_abbrev, add_help=False, usage=SUPPRESS, **kargs)
        self.groups = []                # List of all groups

        # Allow optionals before positionals:
        self.intermixed = hasattr(self.parser, 'parse_intermixed_args')

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
                if self.exit:
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
            if self.exit:
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
        width = 0                       # Max width of the variables column
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


    def update(self, args, title=None, sortme=None, **kargs):
        "Pass list to update_parser() and append result to parser"
        group = self.parser.add_argument_group(title)
        args = self.update_parser(args, group, **kargs)
        self.groups.append(dict(args=args, title=title, sortme=sortme))


    def update_parser(self, lines, parser=None, hidden=False, positionals=False):
        '''
        A more intuitive method for adding arguments
        parser can be empty to return a new parser or a parser argument group
        hidden = Suppress arguments from showing up in help
        positionals = Make group positional arguments
        verbose = Show verbosely how each line in the array is added to argparse


        Format:
            Pass an array with lines in the format:

                ('alias', 'variable_name', type, default),
                "help string",

            You only need to include the fields required, but you can't skip over any.
                ('alias', '',)        # okay
                ('alias', type,)      # not okay

            Substitute the word list with a number like "2" to get that number of args required.
                ('list-args, '', 2)

            Positional arguments are optional by default, but you can specify a number to make them required.
            To use them, make sure to pass: positionals=True to update_parser

                ('pos-arg', '', 1)

        See what your arguments are producing by passing verbose=True or running easy_args.show_args(args)
        '''

        # Make sure the loop ends on a help string
        if not isinstance(lines[-1], str):
            lines.append("")

        alias = None        # --variable name
        varname = None      # Variable Name
        default = None      # Default value
        out = []

        def update():
            nonlocal alias
            "# Update argument to parser:"
            if parser:
                if positionals:
                    parser.add_argument(varname, default=default, nargs=nargs, help=msg)
                else:
                    # alias = '--' + alias
                    aliases = [d + alias for d in self.dashes]
                    if typ == bool:
                        parser.add_argument(*aliases, dest=varname, default=default, action=action, help=msg)
                    else:
                        parser.add_argument(*aliases, dest=varname, default=default, type=typ,
                                            nargs=nargs, help=msg, metavar='')
                out.append(dict(alias=alias, dest=varname, typ=typ, default=default, msg=msg))
                if self.verbose:
                    print('alias  :', repr(alias))
                    print('dest   :', repr(varname))
                    print('default:', repr(default))
                    print('type   :', repr(typ))
                    print('nargs  :', repr(nargs), '\n\n')

        for index, args in enumerate(lines):

            # Add help if available
            if isinstance(args, str):
                msg = undent(args.strip())
                if self.autoformat:
                    if msg and not msg.endswith('.'):
                        last = msg.split()[-1]
                        if last[-1].isalnum() and not last.startswith('-'):
                            msg = msg + '.'
                if default:
                    msg += "  Default: " + str(default)

            if hidden:
                # Hide the help text:
                msg = SUPPRESS

            # If on a new tuple line, add_argument
            if alias or varname:
                update()
                alias = None
                varname = None
                msg = ""

            # Continue if not on a new tuple line
            if isinstance(args, str):
                continue

            # Read the values from the tuple:
            alias = args[0].lstrip('-')

            # Variable Name
            varname = list_get(args, 1)
            if not varname:
                varname = alias

            # Type
            typ = list_get(args, 2, str)


            # Default value
            if typ == list or type(typ) == int:
                default = list_get(args, 3, [])
            else:
                default = list_get(args, 3, None)



            # Argument Type and number required
            if typ == list:
                nargs = '*'
                typ = str
            elif isinstance(typ, int):
                if positionals and typ == 1:
                    nargs = None
                else:
                    nargs = typ
                typ = str
            else:
                nargs = '?'

            # Special handing for booleans
            if typ == bool:
                if default:
                    action = 'store_false'
                else:
                    action = 'store_true'
                    default = False
            if index == len(lines) - 1:
                update()

        return out


def help_parser(parser, show_type=True, sortme=True, wrap=100, tab='  '):
    "Standalone version of help that only needs an argparse parser"
    '''Print a custom help message from the ArgumentParser class
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
    for group in parser._action_groups:         # pylint: disable=W0212
        out = []
        for action in group._group_actions:     # pylint: disable=W0212
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

    for group in parser._action_groups:         # pylint: disable=W0212
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
