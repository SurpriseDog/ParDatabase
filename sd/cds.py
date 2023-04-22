#!/usr/bin/python3
#

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

def _tester():
    try:
        while True:
            val = input("\n\nType a data size:")
            ret = ConvertDataSize()(val)
            print("Converted:", ret,)

    except ValueError:
        pass

if __name__ == "__main__":
    _tester()
