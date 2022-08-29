#!/usr/bin/python3

import re
import sys
import time
import datetime
from collections import Counter
from datetime import datetime as dada

from sd.common import warn
from sd.common import sig
from sd.common import bisect_small, search_list

def int_time():
    return int(time.time())


def strptime(text, fmt):
    '''Alias for strptime'''
    return datetime.datetime.strptime(text, fmt).timestamp()


def psleep(seconds):
    "Sleep and tell us how long for"
    print("Sleeping for", fmt_time(seconds, digits=2) + '...', file=sys.stderr)
    time.sleep(seconds)


def msleep(seconds, accuracy=1/60):
    '''Sleep for a time period and return amount of missing time during sleep
    For example, if computer was in suspend mode.
    Average error is about 100ms per 1000 seconds = .01%
    '''
    if seconds <= 0:
        return 0
    start = time.time()
    time.sleep(seconds)
    elapsed = time.time() - start
    if elapsed / seconds > 1 + accuracy:
        return elapsed - seconds
    else:
        return 0


def pmsleep(seconds):
    "Combination of psleep and msleep"
    print("Sleeping for", fmt_time(seconds, digits=2) + '...', file=sys.stderr)
    return msleep(seconds)


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


        # Avoids the "3 minutes, 2 nanoseconds" nonsense.
        if u_num < 1 and zeroes == 'skip':
            if name in ('second', 'minute', 'hour', 'week', 'month'):
                digits -= 2
            else:
                digits -= 3
            continue

        # In digits mode, output fields containing significant digits until seconds are reached, then stop
        if num >= 60:     # Minutes or higher
            u_num = int(u_num)
            out += [str(u_num) + ' ' + name + ('s' if u_num != 1 else '')]
            digits -= len(str(u_num))
            num -= u_num * unit
            if digits <= 0:
                break
        else:
            # If time is less than a minute, just output last field and quit
            d = digits if digits >= 1 else 1
            out += [sig(u_num, d) + ' ' + name + ('s' if u_num != 1 else '')]
            break

    return ', '.join(out)


'''
for exp in range(-22,66):
    t = 1.7**(exp/2)
    print()
    print(sig(t,5), fmt_time(t, fields=2, zeroes='skip'))
    print(sig(t,5), fmt_time(t, fields=0, zeroes='skip'))
'''


def seconds_since_midnight(seconds=None):
    if seconds:
        tim = time.localtime(seconds)
    else:
        tim = time.localtime()
    return tim.tm_hour * 3600 + tim.tm_min * 60 + tim.tm_sec + time.time() % 1


def diff_days(*args):
    '''Return days between two timestamps
    or between now and timestamp
    Ex: diff_days(time.time(), time.time()+86400)
    Ex: diff_days(timestamp)'''
    if len(args) == 2:
        start = args[0]
        end = args[1]
    else:
        end = args[0]
        start = time.time()
    diff = (dada.fromtimestamp(end) - dada.fromtimestamp(start))
    return diff.days + diff.seconds / 86400  # + diff.microseconds/86400e6


def add_date(date, years=0, months=0, days=0):
    "Add a number of years, months, days to date object"
    if days:
        date += datetime.timedelta(days=days)
    new_y, new_m = date.year, date.month
    new_y += (new_m + months - 1) // 12 + years
    new_m = (new_m + months - 1) % 12 + 1
    return date.replace(year=new_y, month=new_m)

'''
today = datetime.datetime(*datetime.datetime.now().timetuple()[:3])
for months in range(22):
    print(months, add_date(today, months=months))
'''


def local_time(timestamp=None, user_format=None, lstrip=True):
    '''Given a unix timestamp, show the local time in a nice format:
    By default will not show date, unless more than a day into future.Format info here:
    https://docs.python.org/3.5/library/time.html#time.strftime '''
    if not timestamp:
        timestamp = time.time()

    if user_format:
        fmt = user_format
    else:
        fmt = '%I:%M %p'
        if timestamp and time.localtime()[:3] != time.localtime(timestamp)[:3]:
            if time.localtime()[:2] != time.localtime(timestamp)[:2]:
                # New month
                fmt = '%Y-%m-%d'
            else:
                if diff_days(timestamp) < 7:
                    # New day of week
                    fmt = '%a %I:%M %p'
                else:
                    # New day in same month
                    fmt = '%m-%d %I:%M %p'

    if lstrip:
        return time.strftime(fmt, time.localtime(timestamp)).lstrip('0')
    else:
        return time.strftime(fmt, time.localtime(timestamp))

'''
    print(local_time(time.time() + 1e2))
    print(local_time(time.time() + 1e4))
    print(local_time(time.time() + 1e8))
'''

def match_conversion(text, conversions):
    "match text against a list of conversions"
    if text in conversions:
        return conversions[text]
    matches = search_list(text, conversions.keys())
    if len(matches) == 1:
        return conversions[matches[0]]
    elif len(matches) > 1:
        if len({conversions[m] for m in matches}) == 1:
            return conversions[matches[0]]
        else:
            warn("Multiple Matches found for:", text)
            for match in matches:
                print("\t", match.title())
            raise ValueError
    return None


def udate(text):
    '''
    Convert a user formatted date into a number of days and length of cycle
    S = Sunday, Sa = saturday
    3-7 = days of month, January 9, January 9th, Jan 2nd (throw away 2 digits after number)
    Per convention with datetime, weeks start on monday
    '''
    text = str(text).strip().lower()
    digits = sum([char.isdigit() for char in text])

    # Extract count (if available)
    # Example: Every 2nd Tuesday
    count = 1
    if digits < len(text):
        match = re.match('^[0-9][0-9]*', text)
        if match and len(match.group()) > 0:
            count = int(match.group())
            if len(text) <= 4:
                text = re.sub('^[0-9]*[^ ]{0,2}', '', text)
            if ' ' in text:
                text = re.sub('^[0-9]*[^ ]{0,2} ', '', text)


    # Count the remaining digits
    text = text.strip()
    digits = sum([char.isdigit() for char in text])
    # print('count', count, 'digits', digits, 'text', text)

    if not text:
        return count, 'month'

    # Match Tu T = Tuesday
    if digits == 0 and text:
        days = dict(
            m=0,
            monday=0,
            munday=0,
            t=1,
            tu=1,
            tuesday=1,
            tuseday=1,
            twosday=1,
            toosday=1,
            w=2,
            wednesday=2,
            wensday=2,
            r=3,
            h=3,
            th=3,
            thursday=3,
            thorsday=3,
            f=4,
            friday=4,
            fryday=4,
            s=5,
            saturday=5,
            u=6,
            sunday=6,
        )
        if len(text) >= 2:
            text = text.rstrip('s')
        new = match_conversion(text, days)
        if new is not None:
            return (count - 1) * 7 + new, 'week'

    # March 3
    if digits < len(text):
        day = 1
        month = time.strptime(text[:3], '%b').tm_mon
        text = re.sub('^[^0-9]*', '', text).strip()
        if text:
            day = int(re.sub('[^0-9]*', '', text))
        today = dada(*dada.now().timetuple()[:3])
        return today.replace(month=month, day=day), 'year'


    # Just digits
    return int(text), 'month'

'''
# Testing:
import traceback
dates = "3rd", 3, 'Friday', '3rd friday', '3 friday', 'march 3', 'march3rd'
#'3 March', '3March'
for text in dates:
    print(text)
    print(*udate(str(text)), '\n')


while True:
    text = input()
    try:
        print(udate(text))
    except Exception as err:
        print(traceback.format_exc())
'''


def convert_ut_range(unum, **kargs):
    "User time ranges like 3-5pm to machine readable"
    unum = unum.lower()
    if 'to' in unum:
        unum = unum.split('to')
    else:
        unum = unum.split('-')
    unum = list(map(str.strip, unum))

    count = Counter([item[-2:] for item in unum])
    pm = count['pm']
    am = count['am']
    if (pm or am) and not all([pm, am]):
        unit = 'pm' if pm else 'am'
        value = None        # Value of last time encountered
        for x, _ in enumerate(unum):
            if unum[x]:
                if unum[x].endswith(unit):
                    value = convert_user_time(unum[x].strip('pm').strip('am'))
                else:
                    if value is not None and convert_user_time(unum[x]) < value:
                        continue
                    unum[x] = unum[x] + unit
    return [convert_user_time(item, **kargs) for item in unum]

# Test: lmap(fmt_time, *convert_ut_range('3-5pm'))


def _gen_conversions():
    "Generate user time conversions"
    day = 3600 * 24
    year = 365.2422 * day

    conversions = dict(
        seconds=1,
        minutes=60,
        hours=3600,
        days=day,
        weeks=7 * day,
        months=30.4167 * day,
        years=year,
        decades=10 * year,
        centuries=100 * year,
        century=100 * year,
        millenia=1000 * year,
        millenium=1000 * year,

        # Esoteric:
        fortnight=14 * day,
        quarter=30.4167 * day * 3,
        jubilees=50 * year,
        biennium=2 * year,
        gigasecond=1e9,
        aeons=1e9 * year, eons=1e9 * year,
        jiffy=1 / 60, jiffies=1 / 60,
        shakes=1e-8,
        svedbergs=1e-13,
        decasecond=10,
        hectosecond=100,

        # Nonstandard years
        tropicalyears=365.24219 * day,
        gregorianyears=year,
        siderealyears=365.242190 * day,

        # <1 second
        plancktimes=5.391e-44, plancks=5.391e-44,
        yoctoseconds=1e-24, ys=1e-24,
        zeptoseconds=1e-21, zs=1e-21,
        attoseconds=1e-18,
        femtoseconds=1e-15, fs=1e-15,
        picoseconds=1e-12, ps=1e-12,
        nanoseconds=1e-09, ns=1e-9,
        microseconds=1e-06, us=1e-6,
        milliseconds=1e-3, ms=1e-3)
    conversions['as'] = 1e-18
    return conversions


def convert_user_time(unum, default='seconds'):
    '''Convert a user input time like 3.14 days to seconds
    Valid: 3h, 3 hours, 3 a.m., 3pm, 3:14 am, 3:14pm'''
    unum = str(unum).strip().lower()
    if ',' in unum:
        return sum(map(convert_user_time, unum.split(',')))
    if not unum:
        return 0

    # Build conversion table
    self = convert_user_time
    if not hasattr(self, 'conversions'):
        self.conversions = _gen_conversions()
    conversions = self.conversions

    # Primary units
    primary = "seconds minutes hours days months years".split()

    # 12 am/pm fix
    if re.match('12[^1234567890].*am', unum) or unum == '12am':
        unum = unum.replace('12', '0')
    if re.match('12[^1234567890].*pm', unum) or unum == '12pm':
        unum = unum.replace('12', '0')

    # Text processing:
    text = unum.lstrip('0123456789. \t:')
    num = unum.replace(text, '').strip()

    if ':' in num:
        # Convert a num like 3:14:60 into fractions of 60
        seconds = 0
        for x, t in enumerate(num.split(':')):
            seconds += float(t) / 60**x
        num = seconds

    num = float(num)
    if text:
        text = text.replace('.', '').strip().replace(' ', '')
        if text == 'am':
            return num * 3600
        elif text == 'pm':
            return num * 3600 + 12 * 3600
        else:
            # Match the text with the first unit found in units so that 3m matches minutes, not months
            unit = search_list(text, primary, get='first')
            if not unit:
                # Otherwise search for less commonly used units in entire list
                unit = match_conversion(text, conversions)
            else:
                unit = conversions[unit]
            return num * unit
    else:
        return num * conversions[default]


def timer(target=60, polling=1):
    "Countdown from a target time in the terminal"
    start = time.time()
    cur = 0

    while True:
        remaining = convert_user_time(target) - int(time.time() - start)
        if remaining != cur:
            cur = remaining
            if remaining >= 0:
                print('\r'*64, fmt_clock(remaining), sep='', end='', flush=True)
            if remaining <= 0:
                print()
                break
        time.sleep(polling)
    print("Finished timer at", local_time())


def stopwatch(target=0, polling=1):
    "Countup in the terminal, Optional: quit at target"
    start = time.time()
    cur = 0
    while True:
        elapsed = int(time.time() - start)
        if elapsed != cur:
            cur = elapsed
            print('\r'*64, fmt_clock(elapsed), sep='', end='', flush=True)
            if target and elapsed >= target:
                return
        time.sleep(polling)
