#!/usr/bin/env python
import sys

def main(argv):
  filepath = argv[1]
  file_ = open(filepath)
  line = file_.readline()
  total_count = 0
  word_counts = {}
  
  while line:
    word, count = line.split('\t')
    total_count += count
    word_counts[word] = count
    line = file_.readline()

  for key in word_counts:
    print(key, word_counts[key]/total_count)
          
if __name__ == "__main__":
  main(sys.argv)
