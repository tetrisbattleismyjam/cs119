#!/usr/bin/env python
import sys

def main(argv):
    line = sys.stdin.readline()
    line = sys.stdin.readline()
    code_index = 8
    
    try:
        while line:
          code = int(line.split()[code_index])

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
              
        print (response + "\t" + "1")
        line = sys.stdin.readline()

    except EOFError as error:
        return None
    
if __name__ == "__main__":
    main(sys.argv)
