#!/usr/bin/env python

import collections
import sys

def main(argv):
  word_count = collections.Counter()
  
  for line in sys.stdin:
    line_ = line.strip()
    word, count = line_.split('\t')
  
    try:
      count = int(count)
      word_count.update({word: count})
    except:
      continue # ignore this line silently
      
  calculatePercentages(word_count)

def calculatePercentages(word_count):
  total_count = word_count.total()

  for word in word_count:
    ratio = word_count[word] / total_count
    print(word + '\t' + str(ratio))

if __name__ == "__main__":
  main(sys.argv)
