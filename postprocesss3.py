#!/usr/bin/env python
import sys
import collections

def main(argv):
  filepath = argv[1]
  file_ = open(filepath)
  line = file_.readline()
  counts = []
  
  while line:
    key, count = line.split('\t')
    count = int(count)
    counts.append((key, count))
    line = file_.readline()

  sorted_counts = sorted(counts, key=lambda a: a[1], reverse=True)
  print(sorted_counts[:5])
if __name__ == "__main__":
  main(sys.argv)
