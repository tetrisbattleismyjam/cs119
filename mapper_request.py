#!/usr/bin/env python
import sys

def main(argv):
    line = sys.stdin.readline()
    line = sys.stdin.readline() # dump empty first line
    request_index = 5

    try:
        while line:
            request = line.split()[request_index].lstrip("\"")
            print ("LongValueSum:" + request.lower() + "\t" + "1")
            line = sys.stdin.readline()

    except EOFError as error:
        return None
    
if __name__ == "__main__":
    main(sys.argv)
