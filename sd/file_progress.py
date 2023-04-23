#!/usr/bin/python3

import os
import math
import time
import random
import shutil
import unicodedata
from sd.format_number import rfs, fmt_time

def tprint(*args, ending='...', **kargs):
    "Terminal print: Erasable text in terminal"
    length = shutil.get_terminal_size()[0]      # 2.5 microseconds
    text = ' '.join(map(str, args))
    # Sum up the width of each character in the text
    # Special thanks to this answer https://stackoverflow.com/q/48598304/11343425
    # Measured at 20 microseconds on slow hardware excluding print statement
    widths = []             # Widths of each character in the text
    total = 0               # Total width seen thus fur
    for c in text:
        if 32 <= ord(c) <= 126:
            wide = 1
        elif unicodedata.category(c)[0] in ('M', 'C'):
            wide = 0
        elif unicodedata.east_asian_width(c) in ('N', 'Na', 'H', 'A'):
            wide = 1
        else:
            wide = 2
        widths.append(wide)
        total += wide

        # If total exceeds length, delete chars at end one by one until the ending ... can be fit in
        if total > length:
            length -= len(ending)
            while total > length and widths:
                total -= widths.pop(-1)
            text = text[:len(widths)] + ending
            break

    # Filling out the end of the line with spaces ensures that if something else prints it will not be mangled
    print('\r' + text + ' ' * (length - total), **kargs, end='')

    # Test wth the toxic megacolons:  tprint('：'*222)


def bps(num):
    return rfs(num) + '/s'


class FileProgress:
    "Track user time and data rate while processing files"


    def __init__(self, total=0, data_total=0):
        "Use self.scan, if number of files and total size is unknown"

        self.count = 0
        self.total = total              # Number of files
        self.start = time.perf_counter()

        self.rate = 0                   # Average data rate
        self.eta = 0                    # Average eta based on history
        self.history = []               # array with expected eta

        self.data_seen = 0              # Data Processed
        self.data_total = data_total    # Total expected data size

        self.rt_start = 0               # Realtime time start
        self.rt_data = 0                # Realtime data start
        self.rt_rate = 0                # Realtime data rate (over at least 4 seconds of data)
        self.rt_interval = 4


    def done(self,):
        "Return a message about how long the scan took"
        elapsed = time.perf_counter() - self.start
        return dict(count=self.count,
                    elapsed=elapsed,
                    msg=str(self.count) + " files in " + fmt_time(elapsed),
                    )


    def scan(self, files, ):
        "Scan a list of files, to get total file size."
        total = 0
        for file in files:
            total += os.path.getsize(file)
        self.data_total = total
        self.total = len(files)


    def scan_file(self, file,):
        "Scan a single file"
        self.data_total += os.path.getsize(file)
        self.total += 1


    def progress(self, size=0, filename=None):
        '''Mark a single file as processed and return status text'''

        now = time.perf_counter()
        if filename and not size:
            size = os.path.getsize(filename)

        # First file
        if not self.count:
            self.start = now
            self.rt_start = now
        self.count += 1


        # Update history eta
        if size and self.data_seen:
            last = self.history[0][0] if self.history else 0
            if now - last > 1:
                self.rate = self.data_seen / (now - self.start)
                eta = now + (self.data_total - self.data_seen) / self.rate
                self.history.append((now, eta))
                self.eta = eta
            while len(self.history) > 10 and now - last > 10:
                self.history.pop(0)
            self.eta = self.calc_eta()


        # Calculate real time rate if enough time has passed
        if now - self.rt_start >= self.rt_interval:
            self.rt_rate = (self.data_seen - self.rt_data) / (now - self.rt_start)
            self.rt_start = now
            self.rt_data = self.data_seen

        # Update data seen with the new data
        self.data_seen += size

        return self.status()



    def calc_eta(self, verbose=False):
        '''Calculate estimated time remaing based on
        exponential moving average of self.history
        '''
        now = time.perf_counter()

        if len(self.history) > 2:
            total = 0
            weights = 0
            ref = self.eta - now        # Last eta generated
            if verbose:
                print("age      weight   left     diff")
            for t, eta in reversed(self.history):
                age = now - t
                left = eta - now            # Convert to seconds left

                # Toss out etas that vary more than 50% from last
                if abs(left - ref) / ref > 0.5:
                    continue

                # Weighted average with more recent etas counting more
                weight = 10 / math.log(age + 2, 2)      # 220 ns
                total += left * weight
                weights += weight
                if verbose:
                    out = (round(age, 2), \
                           round(weight, 2), \
                           round(left, 2), \
                           round(abs(left - ref) / ref, 2), \
                           )


                    print("{:<8} {:<8} {:<8} {:<8}".format(*out))

            if verbose:
                print('Average:  ' + ' '*7, round(total / weights, 2))
                print('Reference:' + ' '*7, round(ref, 2), '\n')

            # print('Calculated in:', fmt_time(time.perf_counter() - now))
            # Usually <20 microseconds

            return now + total / weights
        else:
            return self.eta



    def status(self, start_delay=1):
        '''Returns a dictionary with useful info including:
            default = A summary line with most useful info
            realtime = Data rate over the last few seconds
            average = Average data rate
            Remaining = Time remaining

            Args:
            start_delay = Wait a bit before displaying text to ensure information
        '''
        now = time.perf_counter()
        txt = {"processing" : str(self.count) + ' of ' + str(self.total)}
        default = "#" + txt['processing']

        if self.eta and now - self.start > start_delay:
            if self.rt_rate:
                txt['realtime'] = bps(self.rt_rate)
                default += ' at ' + txt['realtime']

            txt['average'] = bps(self.rate)
            remain = self.eta - now
            if remain >= 10:
                txt['remaining'] = fmt_time(remain)
            else:
                txt['remaining'] = str(round(remain, 1)) + ' seconds'
            default += ' averaging ' + txt['average'] + \
                       ' with ' + txt['remaining'] + ' remaining'
                       # +  str(len(self.history))
        txt['default'] = default
        return txt


def _tester():
    # simulate files
    files = [random.randrange(0, 1e6) for x in range(64)]
    fp = FileProgress(total=len(files), data_total=sum(files))
    print("Size:".ljust(8), "Default Text:")
    for file in files:
        tprint(format(file, ",").ljust(8), fp.progress(size=file)['default'])

        # Simulate unpredictable file processing
        delay = (file / 200000) * random.uniform(0.8, 1.2)

        # Poll every second for a continuous countdown
        remain = delay
        while True:
            if remain < 1:
                time.sleep(remain)
                break
            else:
                time.sleep(1)
                remain -= 1
                tprint(format(file, ",").ljust(8), fp.status()['default'])





if __name__ == "__main__":
    _tester()
