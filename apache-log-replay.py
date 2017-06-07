#!/usr/bin/env python3
"""Replay requests from an HTTP access log file.

- Takes time between requests into account, with option to speed up the replay.
- Allows one to send all requests to a selected server (proxy).


Debug information:
[     1000 / 99999999 ] Q: 100 T: 342 Next request in 3 seconds   

- Request number / Total
- Queue Size
- Current used thread count
- Time til next request

Main output:
[     1000 / 99999999 ] [ 3 hours / 2 hours ] 23% faster (1 hour) Failed: 234

- Request number / Total
- Target duration / Actual duration = Deficit
- Failed count
"""
from __future__ import print_function
import sys
import time
# import urllib.request
# import urllib.error
import requests as foo
from datetime import datetime
from datetime import timedelta
from optparse import OptionParser
from email.utils import parsedate_tz
import re
import math
import queue
from termcolor import colored

# Constants that specify access log format (indices
# specify position after splitting on spaces)
TIME_INDEX = 3
PATH_INDEX = 5
TIME_FORMAT = "%d/%b/%Y:%H:%M:%S %z"

import queue
import threading
import time
import random

q = queue.Queue()
print_queue = queue.Queue()
threads = []
failed_count = 0
total_number = 0

def stdout(string):
    print_queue.put((sys.stdout, colored(string, 'green')))

def stderr(string):
    print_queue.put((sys.stderr, colored(string, 'yellow')))

def _attemptRequest(url):
    try:
        response = foo.get(url)
        req_result = response.elapsed.total_seconds()
        req_code = response.status_code
    except Exception as e:
        req_result = "FAILED"
        req_code = None

    return (req_result, req_code)

total_target = timedelta(microseconds=1)
total_actual = timedelta(microseconds=1)
def handle_response(index, request_time, duration, response, expected_code, response_code):
    global total_actual
    global failed_count
    global total_target
    total_target += duration
    if (expected_code != response_code):
        total_actual += duration
        failed_count += 1
    else:
        try:
            total_actual += timedelta(seconds=response)
        except TypeError:
            # When a failed request comes in, treat it as no change vs target duration
            total_actual += duration
            failed_count += 1

    print_main_output(index, total_number, expected_code, response_code, response)

def worker():
    while True:
        #if q.empty():
        #    stderr("<<< Caught up, clearing thread")
            #break
        #    pass
        (index, url, request_time, duration, code) = q.get(block=True)
        q.task_done()
        #stderr(str.format("[%d] Time: %s, Duration: %s, URL: %s" % (index, url, request_time, duration)))

        (response, response_code) = _attemptRequest(url)
        #time.sleep(1 * random.randrange(0, 10))

        #response = random.randrange(1,100)
        
        handle_response(index, request_time, duration, response, code, response_code)



def printer():
    while True:
        (file, line) = print_queue.get(block=True)
        if line is None:
            break
        print(line, file=file)
        sys.stdout.flush()
        sys.stderr.flush()

        print_queue.task_done()


def main(filename, proxy, speedup=1):
    global total_number
    """Setup and start replaying."""
    requests = _parse_logfile(filename)
    total_number = len(requests)
    # Sort list by time
    requests.sort(key=lambda request: request[0])
    print_thread = threading.Thread(target=printer)
    # print_thread.daemon = True
    print_thread.start()
    # Create one thread to start with
    _create_worker_thread()
    _replay(requests, speedup, proxy)

    # block until all tasks are done
    q.join()

    # Close threads....
    print_queue.put((None, None))
    print_thread.join()

def insert_into_queue(tuple):
    if q.qsize() > 0:
        _create_worker_thread()

    q.put(tuple, block=True)


def _create_worker_thread():
    # Clean up threads
    global threads
    threads = [t for t in threads if t.isAlive()]
    t = threading.Thread(target=worker)
    # t.daemon = True
    t.start()
    threads.append(t)

def rpad(number):
    return "{:<10}".format(number)

def lpad(number):
    return "{:>10}".format(number)

def print_debug_output(index, total, next_request_delta, duration, code):
    print(colored("[%s/%s] Q: %d T: %d Next request in %s seconds, expected duration %s seconds, code %s" % (
        lpad(index), rpad(total), q.qsize(), len(threads), next_request_delta, duration, code
    ), 'yellow'), file=sys.stderr)


def hms_string(sec_elapsed):
    h = int(sec_elapsed / (60 * 60))
    m = int((sec_elapsed % (60 * 60)) / 60)
    s = sec_elapsed % 60.
    return "{}:{:>02}:{:>05.2f}".format(h, m, s)
# End hms_string


def print_main_output(index, total, expected_code, response_code, response):
    fail_reason = None
    if expected_code != response_code:
        fail_reason = "Code %s != %s" % (response_code, expected_code)
    else:
        try:
            int(response)
        except TypeError:
            # When a failed request comes in, treat it as no change vs target duration
            fail_reason = "Request failed"

    if total_target > total_actual:
        ahead_behind = "ahead"
        faster_slower = "faster"
        c = total_target.total_seconds() / total_actual.total_seconds()
        lag = total_target - total_actual
    else:
        ahead_behind = "behind"
        faster_slower = "slower"
        c = total_actual.total_seconds() / total_target.total_seconds()
        lag = total_actual - total_target
    print(colored("[%s/%s] (%s/%s) Total %s%% %s (%s %s) Failed: %d %s" % (
        lpad(index), rpad(total), total_target, total_actual, round(c * 100, 2),
        faster_slower, lag, ahead_behind, failed_count, fail_reason
    ), 'green'))

def _replay(requests, speedup, host):
    """Replay the requests passed as argument"""
    total_delta = requests[-1][0] - requests[0][0]
    print("%d requests to go (time: %s)" % (len(requests), total_delta), file=sys.stderr)
    last_time = requests[0][0]
    index = 0
    for request_time, path, duration, code in requests:
        time_delta = (request_time - last_time) // speedup
        print_debug_output(index, total_number, time_delta.total_seconds(), timedelta(microseconds=duration), code)
        time.sleep(time_delta.total_seconds())

        last_time = request_time
        url = "http://" + host + path

        insert_into_queue((index, url, request_time, timedelta(microseconds=duration), code))
        index += 1


def _parse_logfile(filename):
    """Parse the logfile and return a list with tuples of the form
    (<request time>, <requested url>, <request duration>, <http code>)
    """
    logfile = open(filename, "r")
    requests = []
    line_regexp = re.compile("(?<!,) (?!\+\d{4}\])")
    for line in logfile:
        parts = line_regexp.split(line)
        time_text = parts[TIME_INDEX][1:][:-1]
        try:
            request_time = datetime.strptime(time_text, TIME_FORMAT)
        except Exception:
            print(line)
            sys.exit(1)
        path = parts[PATH_INDEX]
        duration = int(parts[-1][:-1])
        code = parts[7]
        requests.append((request_time, path, duration, code))
        # print (request_time, path, duration)
        # print(parts, duration)
        # sys.exit(1)
    if not requests:
        print("Seems like I don't know how to parse this file!" + time_text)
    return requests


if __name__ == "__main__":
    """Parse command line options."""
    usage = "usage: %prog [options] -h hostname logfile"
    parser = OptionParser(usage)
    parser.add_option('-p', '--proxy',
                      help='send requests to hostname',
                      dest='proxy')
    parser.add_option('-s', '--speedup',
                      help='make time run faster by factor SPEEDUP',
                      dest='speedup',
                      type='int',
                      default=1)
    (options, args) = parser.parse_args()
    if len(args) == 1:
        main(args[0], options.proxy, options.speedup)
    else:
        parser.error("incorrect number of arguments")
