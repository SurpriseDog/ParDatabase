#!/usr/bin/python3
# Manipulate columns of text
from shutil import get_terminal_size

def term_width():
    return max(get_terminal_size().columns, 20)


def indenter(*args, header='', level=0, tab=4, wrap=-4, even=False):
    '''
    Break up text into tabbed lines.
    Wrap at max characters:
    0 = Don't wrap
    negative = wrap to terminal width minus wrap
    '''
    if wrap < 0:
        wrap = term_width() + wrap

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


def tab_printer(*args, **kargs):
    for line in indenter(*args, **kargs):
        print(line)

################################################################################


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

print_cols = print_columns  # pylint: disable=C0103


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
    max_width = term_width() - 1 # Terminal size
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

auto_cols = auto_columns    # pylint: disable=C0103


def _tester():
    text = "Sed ut perspiciatis unde omnis iste natus error sit voluptatem accusantium doloremque laudantium, totam rem aperiam, eaque ipsa quae ab illo inventore veritatis et quasi architecto beatae vitae dicta sunt explicabo. Nemo enim ipsam voluptatem quia voluptas sit aspernatur aut odit aut fugit, sed quia consequuntur magni dolores eos qui ratione voluptatem sequi nesciunt. Neque porro quisquam est, qui dolorem ipsum quia dolor sit amet, consectetur, adipisci velit, sed quia non numquam eius modi tempora incidunt ut labore et dolore magnam aliquam quaerat voluptatem. Ut enim ad minima veniam, quis nostrum exercitationem ullam corporis suscipit laboriosam, nisi ut aliquid ex ea commodi consequatur? Quis autem vel eum iure reprehenderit qui in ea voluptate velit esse quam nihil molestiae consequatur, vel illum qui dolorem eum fugiat quo voluptas nulla pariatur? At vero eos et accusamus et iusto odio dignissimos ducimus qui blanditiis praesentium voluptatum deleniti atque corrupti quos dolores et quas molestias excepturi sint occaecati cupiditate non provident, similique sunt in culpa qui officia deserunt mollitia animi, id est laborum et dolorum fuga. Et harum quidem rerum facilis est et expedita distinctio. Nam libero tempore, cum soluta nobis est eligendi optio cumque nihil impedit quo minus id quod maxime placeat facere " # pylint: disable=C0301

    print("\nindenter:")
    for line in indenter(text, wrap=term_width() // 2):
        print(line)

    out = []
    alpha = '0123456789ABCDEFGHIJKLMNOPQRSTUVWXYZ'
    for length in range(8, 22):
        line = [alpha[:length]]*3
        out.append(line)

    print("\nauto_cols")
    auto_cols(out)

    print("\nCrop column 1")
    auto_cols(out, crop={1:8})

    print("\nReturn array with printme=False")
    for line in auto_cols(out, printme=False):
        print(line)


if __name__ == "__main__":
    _tester()
