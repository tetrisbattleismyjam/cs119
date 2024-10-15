#!/usr/bin/env python
import sys

def main(argv):
    line = sys.stdin.readline()
    line = sys.stdin.readline()
    code_index = 8
    
    try:
        while line:
            code = line.split()[code_index]
            code = int(code)

            if code >= 100 and code < 200:
                response = 'informational'
            elif code >= 200 and code < 300:
                response = 'successful'
            elif code >= 300 and code < 400:
                response = 'redirection'
            elif code >=400 and code < 500:
                response = 'client error'
            elif code >=500 and code < 600:
                response = 'server error'
            else:
                raise ValueError("bad response code")
              
            print (response + "\t" + "1")
            line = sys.stdin.readline()

    except EOFError as error:
        return None
    except ValueError:
        continue
    
if __name__ == "__main__":
    main(sys.argv)
