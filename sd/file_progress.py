#!/usr/bin/python3

import os
import time
import shutil
from sd.common import fmt_time, rfs



def tprint(*args, **kargs):
    '''
    Terminal print: Erasable text in terminal
    newline = True will leave the text on the line
    '''

    term_width = shutil.get_terminal_size()[0]      # 2.5 microseconds
    text = ' '.join(map(str, args))
    if len(text) > term_width:
        text = text[:term_width-3] + '...'

    # Filling out the end of the line with spaces ensures that if something else prints it will not be mangled
    print('\r' + text + ' ' * (term_width - len(text)), **kargs, end='')


class FileProgress:
    "Track user time and data rate while processing files"


    def __init__(self, total=0, data_total=0):
        self.count = 0
        self.total = total
        self.start = 0
        self.data_seen = 0              # Data Processed
        self.data_total = data_total    # Total expected data size

        self.rt_start = 0               # Realtime time start
        self.rt_data = 0                # Realtime data start
        self.rt_rate = 0                # Realtime data rate (over at least 4 seconds of data)
        self.rt_interval = 4


    def done(self,):
        "Return a message about how long the scan took"
        elapsed = time.perf_counter() - self.start
        return dict(count = self.count,
                    elapsed = elapsed,
                    msg = str(self.count) + " files in " + fmt_time(elapsed),
                    )


    def scan(self, files, ):
        "Scan a list of files"
        total = 0
        for file in files:
            total += os.path.getsize(file)


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
