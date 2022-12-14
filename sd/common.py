#!/usr/bin/python3
# An autogenerated selection of SurpriseDog's common functions relevant to this project.
# To see how this file was created visit: https://github.com/SurpriseDog/Star-Wrangler

import math
import datetime
from shutil import get_terminal_size


def undent(text, tab=''):
    "Remove whitespace at the beginning of lines of text"
    return '\n'.join([tab + line.lstrip() for line in text.splitlines()])


def _fit_in_width(col_width, max_width):
    "Adjust array of column widths to fit inside a maximum"
    extra = sum(col_width) - max_width          # Amount columns exceed the terminal width

    def fill_remainder():
        "After operation to reduce column sizes, use up any remaining space"
        remain = max_width - sum(col_width)
        for x, _ in enumerate(col_width):
            if remain:
                col_width[x] += 1
                remain -= 1

    # Reduce column widths to fit in terminal
    if extra > 0:
        if max(col_width) > 0.5 * sum(col_width):
            # If there's one large column, reduce it
            index = col_width.index(max(col_width))
            col_width[index] -= extra
            if col_width[index] < max_width // len(col_width):
                # However if that's not enough reduce all columns equally
                col_width = [max_width // len(col_width)] * len(col_width)
                fill_remainder()
        else:
            # Otherwise reduce all columns proportionally
            col_width = [int(width * (max_width / (max_width + extra))) for width in col_width]
            fill_remainder()
        # print(col_width, '=', sum(col_width))
    return col_width


def crop_columns(array, crop):
    "Given a 2d array, crop any cell which exceeds the crop value and append ..."
    out = []
    for row in array:
        line = []
        for index, item in enumerate(row):
            cut = crop.get(index, 0)
            length = len(item)
            if length > cut > 3:
                line.append(item[:cut-3]+'...')
            elif cut > 0:
                line.append(item[:cut])
            else:
                line.append(item)
        out.append(line)
    return out


def expand_newlines(line):
    "Take a list with newlines in it and split into 2d array while maintaining column position"
    out = [[''] * len(line)]
    for x, section in enumerate(line):
        if '\n' in section:
            for y, elem in enumerate(section.split('\n')):
                if y >= len(out):
                    out.append([''] * len(line))
                out[y][x] = elem
        else:
            out[0][x] = section
    return out


def _just2func(just):
    "Given a justification of left, right, center : convert to function"
    j = just.lower()[0]
    if j == 'l':
        return str.ljust
    elif j == 'r':
        return str.rjust
    elif j == 'c':
        return str.center
    else:
        raise ValueError("Cannot understand justification:", just)


def indenter(*args, header='', level=0, tab=4, wrap=-4, even=False):
    '''
    Break up text into tabbed lines.
    Wrap at max characters:
    0 = Don't wrap
    negative = wrap to terminal width minus wrap
    '''
    if wrap < 0:
        wrap = TERM_WIDTH + wrap

    if type(tab) == int:
        tab = ' ' * tab
    header = str(header) + tab * level
    words = (' '.join(map(str, args))).split(' ')

    lc = float('inf')       # line count
    for cut in range(wrap, -1, -1):
        out = []
        line = ''
        count = 0
        for word in words:
            if count:
                new = line + ' ' + word
            else:
                new = header + word
            count += 1
            if cut and len(new.replace('\t', ' ' * 4)) > cut:
                out.append(line)
                line = header + word
            else:
                line = new
        if line:
            out.append(line)
        if not even:
            return out
        if len(out) > lc:
            return prev
        prev = out.copy()
        lc = len(out)
    return out


def print_columns(args, col_width=20, columns=None, just='left', space=0, wrap=True):
    '''Print columns of col_width size.
    columns = manual list of column widths
    just = justification: left, right or center'''

    if not columns:
        columns = [col_width] * len(args)

    output = ""
    extra = []
    for count, section in enumerate(args):
        width = columns[count]
        section = str(section)

        if wrap:
            lines = None
            if len(section) > width - space:
                lines = indenter(section, wrap=width - space)
                if len(lines) >= 2 and len(lines[-1]) <= space:
                    lines[-2] += lines[-1]
                    lines.pop(-1)
            if '\n' in section:
                lines = section.split('\n')
            if lines:
                section = lines[0]
                for lineno, line in enumerate(lines[1:]):
                    if lineno + 1 > len(extra):
                        extra.append([''] * len(args))
                    extra[lineno][count] = line

        output += _just2func(just)(section, width)
    print(output)

    for line in extra:
        print_columns(line, col_width, columns, just, space, wrap=False)


def map_nested(func, array):
    "Apply a function to a nested array and return it"
    out = []
    for item in array:
        if type(item) not in (tuple, list):
            out.append(func(item))
        else:
            out.append(map_nested(func, item))
    return out


def auto_columns(array, space=4, manual=None, printme=True, wrap=0, crop=None, just='left'):
    '''Automatically adjust column size
    Takes in a 2d array and prints it neatly
    space = spaces between columns
    manual = dictionary of column adjustments made to space variable
    crop = dict of max length for each column, 0 = unlimited
        example: {-1:2} sets the space variable to 2 for the last column
    wrap = wrap at this many columns. 0 = terminal width
    printme = False : return array instead of printing it
    '''
    if not manual:
        manual = dict()

    # Convert generators and map objects:
    array = map_nested(str, array)

    # Find any \n and bump it to the next line of array
    for index, line in reversed(list(enumerate(array))):
        if '\n' in ''.join(line):
            array.pop(index)
            for l2 in reversed(expand_newlines(line)):
                array.insert(index, l2)

    if crop:
        array = crop_columns(array, crop)


    # Fixed so array can have inconsistently sized rows
    col_width = {}
    for row in array:
        row = list(map(str, row))
        for col, _ in enumerate(row):
            length = len(row[col])
            if col not in col_width or length > col_width[col]:
                col_width[col] = length

    col_width = [col_width[key] for key in sorted(col_width.keys())]
    spaces = [space] * len(col_width)
    if spaces:
        spaces[-1] = 0

    # Make any manual adjustments
    for col, val in manual.items():
        spaces[col] = val

    col_width = [sum(x) for x in zip(col_width, spaces)]

    # Adjust for line wrap and fit in terminal
    max_width = TERM_WIDTH - 1 # Terminal size
    if wrap < 0:
        wrap = max_width + wrap
    if wrap:
        max_width = min(max_width, wrap)
    col_width = _fit_in_width(col_width, max_width)

    '''
    # Turn on for visual representation of columns:
    print(col_width)
    print(''.join([str(count) * x  for count, x in enumerate(col_width)]))
    for line in array:
        print(line)
    '''

    if printme:
        for row in array:
            print_columns(row, columns=col_width, space=0, just=just)
        return None
    else:
        out = []
        op = _just2func(just)
        for row in array:
            line = []
            for index, item in enumerate(row):
                line.append(op(item, col_width[index]))
            out.append(line)
        return out


def list_get(lis, index, default=''):
    '''Fetch a value from a list if it exists, otherwise return default
    Now accepts negative indexes'''

    length = len(lis)
    if -length <= index < length:
        return lis[index]
    else:
        return default


def is_num(num):
    "Is the string a number?"
    if str(num).strip().replace('.', '', 1).replace('e', '', 1).isdigit():
        return True
    return False


class ConvertDataSize():
    '''
    Convert data size. Given a user input size like
    "80%+10G -1M" = 80% of the blocksize + 10 gigabytes - 1 Megabyte
    '''

    def __init__(self, blocksize=1e9, binary_prefix=1000, rounding=0):
        self.blocksize = blocksize                  # Device blocksize for multiplying by a percentage
        self.binary_prefix = binary_prefix          # 1000 or 1024 byte kilobytes
        self.rounding = rounding                    # Round to sector sizes

    def _process(self, arg):
        arg = arg.strip().upper().replace('B', '')
        if not arg:
            return 0

        start = arg[0]
        end = arg[-1]

        if start == '-':
            return self.blocksize - self._process(arg[1:])

        if '+' in arg:
            return sum(map(self._process, arg.split('+')))

        if '-' in arg:
            args = arg.split('-')
            val = self._process(args.pop(0))
            for a in args:
                val -= self._process(a)
                return val

        if end in 'KMGTPEZY':
            return self._process(arg[:-1]) * self.binary_prefix ** (' KMGTPEZY'.index(end))

        if end == '%':
            if arg.count('%') > 1:
                raise ValueError("Extra % in arg:", arg)        # pylint: disable=W0715
            arg = float(arg[:-1])
            if not 0 <= arg <= 100:
                print("Percentages must be between 0 and 100, not", str(arg) + '%')
                return None
            else:
                return int(arg / 100 * self.blocksize)

        if is_num(arg):
            return float(arg)
        else:
            print("Could not understand arg:", arg)
            return None

    def __call__(self, arg):
        "Pass string to convert"
        val = self._process(arg)
        if val is None:
            return None
        val = int(val)
        if self.rounding:
            return val // self.rounding * self.rounding
        else:
            return val


def rfs(num, mult=1000, digits=3, order=' KMGTPEZYB', suffix='B', space=' '):
    '''A "readable" file size
    mult is the value of a kilobyte in the filesystem. (1000 or 1024)
    order is the name of each level
    suffix is a trailing character (B for Bytes)
    space is the space between '3.14 M' for 3.14 Megabytes
    '''
    if abs(num) < mult:
        return sig(num) + space + suffix

    # https://cmte.ieee.org/futuredirections/2020/12/01/what-about-brontobytes/
    bb = mult**9
    if bb <= num < 2 * bb:
        print("Fun Fact: The DNA of all the cells of 100 Brontosauruses " + \
              "combined contains around a BrontoByte of data storage")
    if num >= bb:
        # Comment this out when BrontoBytes become mainstream
        order = list(order)
        order[9] = 'BrontoBytes'
        suffix = ''

    # Faster than using math.log:
    for x in range(len(order) - 1, -1, -1):
        magnitude = mult**x
        if abs(num) >= magnitude:
            return sig(num / magnitude, digits) + space + (order[x] + suffix).rstrip()
    return str(num) + suffix        # Never called, but needed for pylint


def bisect_small(lis, num):
    '''Given a sorted list, returns the index of the biggest number <= than num
    Unlike bisect will never return an index which doesn't exist'''
    end = len(lis) - 1
    for x in range(end + 1):
        if lis[x] >= num:
            return max(x - 1, 0)
    else:
        return end


def sig(num, digits=3):
    "Return number formatted for significant digits"
    num = float(num)
    if num == 0:
        return '0'
    negative = '-' if num < 0 else ''
    num = abs(num)
    power = math.log(num, 10)
    if num < 1:
        num = int(10**(-int(power) + digits) * num)
        return negative + '0.' + '0' * -int(power) + str(int(num)).rstrip('0')
    elif power < digits - 1:
        return negative + ('{0:.' + str(digits) + 'g}').format(num)
    else:
        return negative + str(int(num))


def fmt_clock(num, smallest=None):
    '''
    Format in 9:12 format
    smallest    = smallest units for non pretty printing
    '''
    # Normal "2:40" style format
    num = int(num)
    s = str(datetime.timedelta(seconds=num))
    if num < 3600:
        s = s[2:]  # .lstrip('0')

    # Strip smaller units
    if smallest == 'minutes' or (not smallest and num >= 3600):
        return s[:-3]
    elif smallest == 'hours':
        return s[:-6] + ' hours'
    else:
        return s


def fmt_time(num, digits=2, pretty=True, smallest=None, fields=None, zeroes='skip', **kargs):
    '''Return a neatly formated time string.
    sig         = the number of significant digits.
    fields      = Instead of siginificant digits, specify the number of date fields to produce.
    fields overrides digits
    zeroes      = Show fields with zeroes or skip to the next field
    todo make fields the default?
    '''
    if num < 0:
        num *= -1
        return '-' + fmt_time(**locals())
    if not pretty:
        return fmt_clock(num, smallest)

    if fields:
        digits = 0
        fr = fields     # fields remaining
    elif 'sig' in kargs:
        fr = 0
        digits = kargs['sig']
        print("\nWarning! sig is deprecated. Use <digits> instead.\n")

    # Return number and unit text
    if num < 5.391e-44:
        return "0 seconds"
    out = []
    # For calculations involving leap years, use the datetime library:
    limits = (5.391e-44, 1e-24, 1e-21, 1e-18, 1e-15, 1e-12, 1e-09, 1e-06, 0.001, 1, 60,
              3600, 3600 * 24, 3600 * 24 * 7, 3600 * 24 * 30.4167, 3600 * 24 * 365.2422)
    names = (
        'Planck time',
        'yoctosecond',
        'zeptosecond',
        'attosecond',
        'femtosecond',
        'picosecond',
        'nanosecond',
        'microsecond',
        'millisecond',
        'second',
        'minute',
        'hour',
        'day',
        'week',
        'month',
        'year')

    index = bisect_small(limits, num) + 1
    while index > 0:
        index -= 1
        unit = limits[index]        #
        u_num = num / unit          # unit number for current name
        name = names[index]         # Unit name like weeks

        if name == 'week' and u_num < 2:
            # Replace weeks with days when less than 2 weeks
            digits -= 1
            continue

        # In fields modes, just keep outputting fields until fr is exhausted
        if fields:
            fr -= 1
            u_num = int(u_num)
            if u_num == 0 and zeroes == 'skip':
                continue
            out += [str(u_num) + ' ' + name + ('s' if u_num != 1 else '')]
            num -= u_num * unit
            if fr == 0:
                break
            continue
        # In digits mode, output fields containing significant digits until seconds are reached, then stop
        elif digits <= 0:
            break


        # Avoids the "3 minutes, 2 nanoseconds" nonsense.
        if u_num < 1 and zeroes == 'skip':
            if name in ('second', 'minute', 'hour', 'week', 'month'):
                digits -= 2
            else:
                digits -= 3
            continue


        if num >= 60:     # Minutes or higher
            u_num = int(u_num)
            out += [str(u_num) + ' ' + name + ('s' if u_num != 1 else '')]
            digits -= len(str(u_num))
            num -= u_num * unit
        else:
            # If time is less than a minute, just output last field and quit
            d = digits if digits >= 1 else 1
            out += [sig(u_num, d) + ' ' + name + ('s' if u_num != 1 else '')]
            break

    return ', '.join(out)


TERM_WIDTH = max(get_terminal_size().columns, 20)
auto_cols = auto_columns    # pylint: disable=C0103

'''
&&&&%%%%%&@@@@&&&%%%%##%%%#%%&@@&&&&%%%%%%/%&&%%%%%%%%%%%&&&%%%%%&&&@@@@&%%%%%%%
%%%%%%%%&@&(((((#%%&%%%%%%%%%&@@&&&&&&%%%&&&&&%%%%%%%%%%%&&&&%&%#((((/#@@%%%%%%%
&&%%%%%%&@(*,,,,,,,/%&%%%%%%%&@@&&&&&%%&&&&%%&&%%%%%%%%%%&&&%#*,,,,,,*/&@&%%%%%%
%%%%%%%&@&/*,,,*,*,,*/%&%%%%%&@@&&&&&&%%&&&&&&&%%%%%%&%%%&&%*,,,,,,,,**#@&&%%%%%
&&&&&%%&@#(**********,*(#&%%%&@&&&&%%%%%%%%%&&&%%%%%%&%&&#*****,*******#@&&%%%%%
&&&%%%&&#/***/*****/*,**,*%&%&@@&&&&&&&&&&&&&&&%%%%%%&&#*,,,*/******/***(%&%%%%%
&&&%%%&%/*****///////**,,,,*/%%&&@@@@@@@@@@@@@@@@&&%#*,,,*,*(///////*****#%&%%%%
@@&%%#&#/,,,*/(//((((//**,,*/#&@@@@@&&&&&&&&&&@@@@@%(/*,,**/(/(((/(//*,,*(&&%%%%
&&&%##&#*,,,*////((((/*///(&@&@@&&&#%((//(/###%&@&@@@@#//**//(#(///***,.,/&&%%%%
%%%%%#%#*,,,**////(///((#&&&%@&%%(/*,,......,,/(#%&&&@@@%((/(/#(///**,,,,(&%%%%%
&&%%%#%%/,..***//(#(#%%&@@@&@%(*.,,..       ...,.,/#@&@@@&&%#(((///**,..,#%%%%%%
%&%%%%%#*,****/(##&@@@&@@@@&%*,....           ....,,(&@@@@@@&@&%((//****,(%%%%%%
%&%%%%%#/,**/#&@@@&@@@@@@@&(*,......    .     ..,..,.(&@@@@@@@&@@@&%#**,*(%%%%%%
&&%%%%#&#(#&@@@&@@@@@@@@%((#@@%&&((,,,,,..,,(**(%@@&@%##(&@@@@@@@@&&@@%#(%%%%%%%
&&&%%%%%&&&&&&@@@@@@%###%@(,%&/@@&(%(/*,..,*/%##&&,%@(*&@#((%&@@@@@@&&@&%%%%&&%%
&&%%%%%%&&&@@@@@@@@#((*#@%,#%%&@#%(/**//,****/(#%%%&&%*(@@*/#(&@@@@@@@&&%%%%%%%%
&&&%%%%%&@@@@&%#/,,,,*,(/%&@@&((%(*,*,,*,**,,*,*#%(#@@&%((**,,,,*#(%&@@&&%%%%%%%
&&&%%%%%@@@@%*/*,...,*,,/*#(//#****,***********,**/#/##(/*,*,...,*/*/&@@&%%&%%%%
&&%%%%%%&@@@(//,....,,*/****/,,/**************/***/,,//**/**,....,*//&@@&%%&%%%%
&&&%%%%%&@@%(/*,. ...,****/*/(//*%&@@&%%%%%%&&&&//*/(*/**/**......,/*#&@&%&&&&%%
&&%%%%%%&@@%(**,,....,/**/((/,#&&&&&%#((((((%&&&@&%/*/(/**/*,. ..,,*/((#@&&&&&&%
%&%%%%%%&&#(/**,..,,,***/((,./%&%&&&@&(/#((#@@&&&%&%,,/((*,/*,,..,,,///(%&%&&&&&
&&%%%%%%&#,**,.,..,,*(//(/,,.,&&&@#&@@##%(#&@&%%@&&#.,,/(((//*,,..,,**,*&%&&%&&&
&&%##%%%#/**,,,,..,*/((((*...,,#&##%(#%%&%%###%(%&/,.. **((((/,...,,,,**(%%#%%%&
&&%####(**,,,.,,.,,/(/(//*,,..../%&(##%&&&%%(#%&#, .. .**//(/(*,,..,.,,**/((#%%%
&&&%#///*,........,/(((//**,.   ,,(#%%%%%%&#%##**.   ,,*//((((*,........,*//(%%%
%%%%(/**...       .,/(((///*., .,*(#(%%%%%%%%##/*,..,,*///((/*.      .....**/(%%
%%%%#(,..          .,/((/(//****,/(((###%#%(#///**,,**/((/((*,          .,.,(%%%
&&%%%#/*...          ,*/(/(/((%%&#&#(/%./.*%(#%#%#&&(((/(/*,.          ..,**(&%%
&&%%%%(*.....          ..*((/**(#&&&&&&&%%%&%&&&%(/,*/((*..           .,..*(&&%%
&&%%%%&#*.      .        */(#/*,,*/((%#%%%%%((**,.*/(#(/,       .       ,(%&%%&%
%%%%%%&%#//**,..           .**(((*,...,,**,,..*,/((/*,.          ...,,//(#%%%%%%
%%%&&&%(/*,**,..,,.,..       .,,**//**,*,,,*,////*,,.        .,.,...,,,**//#%&%%
%%%&&%#/*,*,.    ...      ..         ...  ,.. .       .       ...   ..,,*/(#%&%%
&&&&&%(((*.*... . .*,.   .           .*%%#(,.          .    .*,. ..,.,,**/(%#&%%

Generated by https://github.com/SurpriseDog/Star-Wrangler
a Python tool for picking only the required code from source files
written by SurpriseDog at: https://github.com/SurpriseDog
2022-09-26
'''
