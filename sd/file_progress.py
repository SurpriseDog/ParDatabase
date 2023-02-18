#!/usr/bin/python3

import os
import time
import shutil
import unicodedata
from sd.common import fmt_time, rfs

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



class FileProgress:
    "Track user time and data rate while processing files"


    def __init__(self, total=0, data_total=0):
        "Use self.scan, if number of files and total size is unknown"

        self.count = 0
        self.total = total              # Number of files
        self.start = time.perf_counter()
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
        "Mark a single file as processed and return status text, use default or look at code for more."
        txt = dict(processing='',           # Processing File #
                   realtime='',             # Realtime rate
                   average='',              # Average rate
                   remaining='',            # Time remaining
                   default='',)             # A sentence with a conglomeration of the above

        now = time.perf_counter()
        if filename:
            size = os.path.getsize(filename)

        # First file
        if not self.count:
            self.start = now
            self.rt_start = now
        self.count += 1


        def bps(num):
            return rfs(num, digits=2) + '/s'


        txt['processing'] = str(self.count) + ' of ' + str(self.total)
        default = "#" + txt['processing']

        # Calculate real time rate
        if now - self.rt_start >= self.rt_interval:
            self.rt_rate = (self.data_seen - self.rt_data) / (now - self.rt_start)
            self.rt_start = now
            self.rt_data = self.data_seen

        if self.data_seen:
            rate = self.data_seen / (now - self.start)
            if self.rt_rate:
                txt['realtime'] = bps(self.rt_rate)
                default += ' at ' + txt['realtime']

            txt['average'] = bps(rate)
            txt['remaining'] = fmt_time((self.data_total - self.data_seen) / rate)
            default += ' averaging ' + txt['average'] + ' with ' + txt['remaining'] + ' remaining'

        txt['default'] = default
        self.data_seen += size
        return txt
