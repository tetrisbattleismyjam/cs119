#!/usr/bin/env python

import collections
import sys

def main(argv):
  code_count = collections.Counter()
  
  for line in sys.stdin:
    line_ = line.strip()
    code, count = line_.split('\t')
  
    try:
      code = int(code)
      if code >= 100 and code < 200:
        response = 'informational'
        
      elif code >= 200 and code < 300:
        response = 'successful'
        
      elif code >= 300 and code < 400:
        response = 'redirection'
        
      elif code >= 400 and code < 500:
        response = 'client error'
        
      elif code >= 500 and code < 600:
        response = 'server error'

      else:
        response = 'BADLINE'
        
      code_count.update({response: count})
    except:
      continue # ignore this line silently
      
  eval_codes(code_count)

def eval_codes(code_count):
  for res in code_count:
    print('%s\t%s' % (res,code_count[res]))

if __name__ == "__main__":
  main(sys.argv)
