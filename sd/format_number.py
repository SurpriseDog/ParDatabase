#!/usr/bin/python3
# Format numbers for easy reading

import datetime
from sd.common import bisect_small

INF = float("inf")


def percent(num, digits=0):
    if not digits:
        return str(int(num * 100)) + '%'
    else:
        return sig(num * 100, digits) + '%'


def rfs(num, mult=1000, digits=3, order=' KMGTPEZYB', suffix='B', space=' ', fixed=None):
    '''A "readable" file size
    mult is the value of a kilobyte in the filesystem. (1000 or 1024)
    order is the name of each level
    fixed is the number of digits to force display ex: '5.000 MB'
    suffix is a trailing character (B for Bytes)
    space is the space between '3.14 M' for 3.14 Megabytes
    '''
    if abs(num) < mult:
        return sig(num) + space + suffix

    # Let's Learn about BrontoBytes
    # Comment this out when BrontoBytes become mainstream
    # https://cmte.ieee.org/futuredirections/2020/12/01/what-about-brontobytes/
    if num >= 10e+26:
        bb = mult**9
        if num >= bb:
            order = list(order)
            order[9] = 'BrontoBytes'
            suffix = ''
            if num < 1.9 * bb:
                print("Fun Fact: The DNA of all the cells of 100 Brontosauruses " + \
                      "combined contains around a BrontoByte of data storage")

    # Faster than using math.log:
    for x in range(len(order) - 1, -1, -1):
        magnitude = mult**x
        if abs(num) >= magnitude:
            if fixed:
                num = ('{0:.' + str(fixed) + 'f}').format(num / magnitude)
            else:
                num = sig(num / magnitude, digits)
            return num + space + (order[x] + suffix).rstrip()
    return str(num) + suffix        # Never called, but needed for pylint


def mrfs(*args):
    "rfs for memory sizes"
    return rfs(*args, mult=1024, order=[' ', 'Ki', 'Mi', 'Gi', 'Ti', 'Pi', 'Ei', 'Zi', 'Yi', 'Bi'])


def rns(num):
    "readble number size"
    if num < 1e16:
        return rfs(num, order=' KMBT', suffix='', space='')
    else:
        return num


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


'''
# Quick version (doesn't handle numbers below 1e-3):
def sig(num, digits=3):
    return ("{0:." + str(digits) + "g}").format(num) if abs(num) < 10**digits else str(int(num))
'''

'''
# Math.log method doesn't work because of rounding errors.
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
'''


def sig(num, digits=3, trailing=False):
    # post to https://stackoverflow.com/questions/658763/how-to-suppress-scientific-notation-when-printing-float-values
    '''Return number formatted for significant digits
    trailing = True will enable trailing zeroes
    '''
    num = float(num)
    if num == 0:
        if trailing:
            return '0.' + '0' * (digits - 1)
        return '0'
    if num == INF:
        return 'inf'

    # Return as integer if it meets the digits req
    out = str(int(num))
    if len(out.strip('-')) >= digits:
        return out

    # Use the g method if possible, but fails for small numbers
    out = ('{0:.' + str(digits) + 'g}').format(num)
    if 'e' not in out:
        return out.rstrip('0.') if not trailing else out

    # Otherwise try to fromat as float using the correct precision
    # Text processing is the only way to get this to work correctly without
    # rounding errors of the other methods like math.log
    power = int(out.split('e-')[-1])
    out = ('{0:.' + str(power + digits - 1) + 'f}').format(num)
    return out.rstrip('0.') if not trailing else out


def _test_sig():
    for power in range(-8, 8):
        num = 1 * 10 **power
        print(str(num).ljust(12), sig(num, trailing=True).ljust(15), sig(-num))

    for exp in range(-22, 66):
        t = 1.7**(exp/2)
        print()
        print(sig(t, 5), fmt_time(t))
        print(sig(t, 5), fmt_time(t, fields=2, zeroes='skip'))
        print(sig(t, 5), fmt_time(t, fields=0, zeroes='skip'))



if __name__ == "__main__":
    _test_sig()
