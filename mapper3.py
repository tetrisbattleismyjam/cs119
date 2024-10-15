#!/usr/bin/env python
import sys

def main(argv):
    line = sys.stdin.readline()
    line = sys.stdin.readline()
    key_index = 0
    code_index = 8

    try:
        while line:
            line_ = line.split()
            key = line_[key_index]
            code = int(line_[code_index])
            if code >= 400 and code < 500:
                value = 1
            else:
                value = 0
            print (key + "\t" + str(value))
            line = sys.stdin.readline()

    except EOFError as error:
        return None
    
if __name__ == "__main__":
    main(sys.argv)
