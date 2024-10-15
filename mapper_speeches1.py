#!/usr/bin/env python
import os
import sys
import requests
import string
import re
import collections

stopwords_list = requests.get("https://gist.githubusercontent.com/rg089/35e00abf8941d72d419224cfd5b5925d/raw/12d899b70156fd0041fa9778d657330b024b959c/stopwords.txt").content
stopwords = list(set(stopwords_list.decode().splitlines()))

def main(argv):
    filename = os.environ['map_input_file']
    prez = filename.split('_')[0]
    line = sys.stdin.readline()
    while line:
        phrase = clean_text(line.split())
        print(prez + '\t' + phrase)
        
# Used in clean_text to remove stopwords.
def remove_stopwords(words):
    list_ = re.sub(r"[^a-zA-Z0-9]", " ", words.lower()).split()
    return [itm for itm in list_ if itm not in stopwords]

# Forces lowercase, removes punctuations, and new line characters.
# Removes stopwords
def clean_text(text):
    text = text.lower()
    text = re.sub('\[.*?\]', '', text)
    text = re.sub('[%s]' % re.escape(string.punctuation), ' ', text)
    text = re.sub('[\d\n]', ' ', text)
    return ' '.join(remove_stopwords(text))

if __name__ == "__main__":
    main(sys.argv)
