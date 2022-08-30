#!/usr/bin/python3
# Functions for formatting time and numbers

################################################################################

import math
import random
import secrets


INF = float("inf")
NAN = float("nan")

def rint(num):
    return str(int(round(num)))


def sint(num):
    return str(int(num))


def sround(num, digits=3):
    "Round a number (with trailing 0s) for printing"
    return ('{0:.' + str(digits) + 'f}').format(num)

def srnd(num, acc=2):
    return str(round(num), acc)

def randint(start, stop):
    "Better than random.randint"
    size = int(stop + 1 - start)
    return secrets.randbelow(size) + int(start)

def secret_shuffle(lis):
    "Shuffle with secrets, (twice!)"
    length = len(lis)
    for _x in range(length*2):
        lis.insert(secrets.randbelow(length+1), lis.pop())



def randexp(low, high=None):
    '''
    Returns a random number within an exponential range between 2**low and 2**high minus 1 to include zero.
    Useful for function testing
    '''
    if not high:
        high = low
        low = 0
    exp = randint(low, high-1)
    return random.uniform(2**exp, 2**(exp+1)) - 1


def is_num(num):
    "Is the string a number?"
    if str(num).strip().replace('.', '', 1).replace('e', '', 1).isdigit():
        return True
    return False


'''
# Quick version (doesn't handle numbers below 1e-3):
def sig(num, digits=3):
    return ("{0:." + str(digits) + "g}").format(num) if abs(num) < 10**digits else str(int(num))
'''


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



def percent(num, digits=0):
    if not digits:
        return str(int(num * 100)) + '%'
    else:
        return sig(num * 100, digits) + '%'


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


def mrfs(*args):
    "rfs for memory sizes"
    return rfs(*args, mult=1024, order=[' ', 'Ki', 'Mi', 'Gi', 'Ti', 'Pi', 'Ei', 'Zi', 'Yi', 'Bi'])


def rns(num):
    "readble number size"
    if num < 1e16:
        return rfs(num, order=' KMBT', suffix='', space='')
    else:
        return num


################################################################################
# Modulus Madness

def roundint(num, mod):
    "Round an integer to the nearest modulus"
    ret = num // mod * mod
    remainder = num % mod
    if remainder < mod // 2:
        return ret
    else:
        return ret + mod


def chunk_up(num, chunk=64):
    "Return a multiple of chunk >= num"
    return ((num - 1) // chunk + 1) * chunk


def round_up(num, mod=1):
    "Round a number up to the next modulus"
    if not num % mod:
        return num
    return num // mod * mod + mod


def round_down(num, mod=1):
    "Round a number down to the next modulus"
    if not num % mod:
        return num
    return num // mod * mod



################################################################################
#

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
