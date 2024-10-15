#!/usr/bin/env python

import collections
import sys

def main(argv):
  code_count = collections.Counter()
  
  for line in sys.stdin:
    line_ = line.strip()
    response, count = line_.split('\t')
  
    try:
      count = int(count)
      code_count.update({response: count})
    except:
      continue # ignore this line silently
      
  eval_codes(code_count)

def eval_codes(code_count):
  for res in code_count:
    print('%s\t%s' % (res,code_count[res]))

if __name__ == "__main__":
  main(sys.argv)
