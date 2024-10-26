#!/usr/bin/env python3

import sys
import re
import collections

# key: user+query. 
# value: 1 if query was made before in the day, 
#        0 if query is brand new for the day 
queries = {}

# key: uesr
# value: count of queries that have been made before in the day
duplicate_counts = collections.Counter()

# key: uesr
# value: running average of duplicate queries across all days
averages = {}

# RegExp for extracting the user and query information from the stream
usr_pattern = re.compile('sndr[0-9]{4}')
qry_pattern = re.compile('qry[0-9]{4}')

max_queries = 60
max_days = 3
max_users = 6
day_counter = 0

for i in range(max_queries * max_days):
  # if this is a new day, reset counts. compute new average
  if (i + 1) % max_queries == 0:
    day_counter += 1

    for user_, dups in duplicate_counts.items():
      averages[user_] = averages.setdefault(user_, 0) + ((dups - averages.setdefault(user_, 0)) / day_counter)
        
    queries.clear()
    c.clear()

  # read stream and update running tallies
  line = sys.stdin.readline()
  
  usr = re.search(usr_pattern, line)
  qry = re.search(qry_pattern, line) 

  duplicate = queries.setdefault(usr + qry, 0)

  if duplicate:
    duplicate_counts.update([usr])
  else 
    queries.update([(usr + qry, 1)])
  
