#!/usr/bin/env python3

import numpy as np
import sys
import re
import collections
from hashlib import shake_128

# RegExp for extracting the user and query information from the stream
usr_pattern = re.compile('sndr[0-9]{4}')
qry_pattern = re.compile('qry[0-9]{4}')

bin_size = 8 # number of bits representing which bin 
bin_count = (1 << bin_size) - 1 # max number that can be held in bin_size bits
counts = np.zeroes(bin_count) # current estimate for each bin

def hash_64(username):
    hash_ = shake_128(username.encode())
    return int(hash_.hexdigest(64), base=16)

def bin_of(hash):
    bin_mask = (1 << bin_size) - 1 # get a bin string of bin_size '1's
    return hash & bin_mask # retrieve the first bit of size 'bin_size'

def leading_zeros(hash):
    bin_ = bin(hash)
    bin_string = str(bin_)[2:]
    count = 0
    for bit in bin_string:
        if bit == 0:
            count += 1
        else: 
            return count
        
    return count

def current_estimate():
    return 0.79 * bin_count * sum(1/(2 ** est) for est in counts)

# Read in the stream
line = sys.stdin.readline()
usr = re.search(usr_pattern, line)

usr_hash = hash_64(usr)
usr_bin = bin_of(usr_hash)
zero_count = leading_zeros(usr_hash >> bin_size)
counts[usr_bin] = max(zero_count, counts[usr_bin])
print(current_estimate())
