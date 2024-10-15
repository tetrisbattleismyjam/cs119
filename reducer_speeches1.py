import collections
import sys

def main(argv):
  line = sys.stdin.readline()
  counter = collections.Counter()
  while line:
    prez, valence, word_count = line.split('\t')
    average = int(valence) / int(word_count)
    counter.update({prez, average})

  for prez in counter:
    print(prez + '\t' + counter[prez])
    
if __name__ == "__main__":
  main(sys.argv)f
