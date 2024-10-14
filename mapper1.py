import sys

def main(argv):
    line = sys.stdin.readline()
    line = sys.stdin.readline()
    request_index = 5

    try:
        while line:
            request = line.split()[request_index][1:]
            print ("LongValueSum:" + request + "\t" + "1")
            line = sys.stdin.readline()

    except EOFError as error:
        return None
    
if __name__ == "__main__":
    main(sys.argv)
