#!/usr/bin/env python3
"""Replay requests from an HTTP access log file.

- Takes time between requests into account, with option to speed up the replay.
- Allows one to send all requests to a selected server (proxy).
"""
from __future__ import print_function
import sys
import time
#import urllib.request
#import urllib.error
import requests as foo
from datetime import datetime
from optparse import OptionParser
from email.utils import parsedate_tz
import re
import math


# Constants that specify access log format (indices
# specify position after splitting on spaces)
TIME_INDEX = 3
PATH_INDEX = 5
TIME_FORMAT = "%d/%b/%Y:%H:%M:%S %z"

def main(filename, proxy, speedup=1):
    """Setup and start replaying."""    
    requests = _parse_logfile(filename)
    _replay(requests, speedup, proxy)

def _replay(requests, speedup, host):
    """Replay the requests passed as argument"""
    total_delta = requests[-1][0] - requests[0][0]
    print ("%d requests to go (time: %s)" % (len(requests), total_delta))
    last_time = requests[0][0]
    for request_time, path, duration in requests:
        time_delta = (request_time - last_time) // speedup
        if time_delta:
            if time_delta and time_delta.seconds > 10:
                print("(next request in %d seconds)" % time_delta.seconds)
            time.sleep(time_delta.seconds)
        last_time = request_time
        url = "http://" + host + path
        try:
            response = foo.get(url)
            req_result = math.ceil(response.elapsed.total_seconds() * 1000.0)
        except Exception as e:
            req_result = "FAILED"
        print ("[%s] REQUEST: %s -- %s" % (request_time.strftime("%H:%M:%S"), url, req_result), file=sys.stderr)
        print("[%s] Target: [%s] Actual: [%s]" % (request_time.strftime("%H:%M:%S"), duration, req_result))
        sys.stdout.flush()
        sys.stderr.flush()

def _parse_logfile(filename):
    """Parse the logfile and return a list with tuples of the form
    (<request time>, <requested url>, <request duration>)
    """
    logfile = open(filename, "r")
    requests = []
    for line in logfile:
        parts = re.compile("(?<!,) (?!\+\d{4}\])").split(line)
        time_text = parts[TIME_INDEX][1:][:-1]
        try:
            request_time = datetime.strptime(time_text, TIME_FORMAT)
        except Exception:
            print(line)
            sys.exit(1)
        path = parts[PATH_INDEX]
        duration = parts[-1][:-1]
        requests.append((request_time, path, duration))
        #print (request_time, path, duration)
        #print(parts, duration)
        #sys.exit(1)
    if not requests:
        print ("Seems like I don't know how to parse this file!" + time_text)
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
