#!/usr/bin/env python
import sys

def main(argv):
    line = sys.stdin.readline()
    line = sys.stdin.readline()
    code_index = 8
    
    try:
        while line:
            code = line.split()[code_index]
            print (code + "\t" + "1")
            line = sys.stdin.readline()

    except EOFError as error:
        return None
    
if __name__ == "__main__":
    main(sys.argv)
