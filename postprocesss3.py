#!/usr/bin/env python
import sys
import collections

def main(argv):
  filepath = argv[1]
  file_ = open(filepath)
  line = file_.readline()
  counts = collections.Counter()
  
  while line:
    key, count = line.split('\t')
    count = int(count)
    counts.update({str(key), count})
    line = file_.readline()

  print(counts.most_common(5))
if __name__ == "__main__":
  main(sys.argv)
