#!/usr/bin/env python

import collections
import sys

def main(argv):
  code_count = collections.Counter()
  
  for line in sys.stdin:
    line_ = line.strip()
    code, count = line_.split('\t')
  
    try:
      count = int(count)
      if count >= 100 and count < 200:
        response = 'informational'
        
      elif count >= 200 and count < 300:
        response = 'successful'
        
      elif count >= 300 and count < 400:
        response = 'redirection'
        
      elif count >= 400 and count < 500:
        response = 'client error'
        
      elif count >= 500 and count < 600:
        response = 'server error'

      else:
        response = 'BADLINE'
        
      code_count.update({response: count})
    except:
      continue # ignore this line silently
      
  eval_codes(code_count)

def eval_codes(code_count):
  for response in code_count:
    print('%s\t%s' % (response,code_count[response]))

if __name__ == "__main__":
  main(sys.argv)
