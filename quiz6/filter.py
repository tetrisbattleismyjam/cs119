#!/usr/bin/env python3
import sys, time
import base64
import pyspark

# from BitVector import BitVector as bv
from pyspark.conf import SparkConf
from pyspark.context import SparkContext
from pyspark.sql import SparkSession
from pyspark.sql.functions import unbase64
from pyspark.sql.functions import explode
from pyspark.sql.functions import split

array_size = 1952
def hash1(word):
  word_sum = sum(map(lambda a: ord(a), word))
  return word_sum % array_size

def hash2(word):
  word_sum = 1 + sum(map(lambda a: ord(a) + 23, word)) * 101
  return word_sum % array_size

def hash3(word):
  word_sum = sum(map(lambda a: ord(a) * 89, word))
  return word_sum % array_size

def get_indices(word):
  return [hash1(word), hash2(word), hash3(word)]

def filter_str(path):
    f = open(path)
    line = f.readline()
    return base64.b64decode(line).decode()
    
if __name__ == "__main__":
    if len(sys.argv) != 4:
        print("Usage: structured_network_wordcount.py <hostname> <port> <bloom filter path>", file=sys.stderr)
        sys.exit(-1)

    print ('Argv', sys.argv)
    
    host = sys.argv[1]
    port = int(sys.argv[2])
    bloom_path = sys.argv[3]

    print("bloom HDFS path: ", bloom_path)
    # get the bloom filter encoded as base64. Decode
    # create DataFrame for the input lines coming in to the given host and port
    spark = SparkSession.builder.appName("CensorshipBoard9000").getOrCreate()
    spark.sparkContext.addFile(bloom_path)
    spark.sparkContext.setLogLevel('WARN')
    print(filter_tr(bloom_path))

    # 
    # lines = spark.readStream.format("socket").option("host", hostt).option("port", port).load()

    # Transform into columns sentence, bloom count.
