#!/usr/bin/env python3
import sys, time
import base64
import pyspark
import mmh3

# from BitVector import BitVector as bv
from pyspark import SparkFiles
from pyspark.conf import SparkConf
from pyspark.context import SparkContext
from pyspark.sql import SparkSession
from pyspark.sql.functions import unbase64, decode, udf, col, explode, split
from pyspark.sql.types import IntegerType

array_size = 1264
def hash1(word):
  return mmh3.hash(word)

def hash2(word):
  return hash(word)

def get_indices(word):
  return [hash1(word) % array_size, 
          hash2(word) % array_size]

def eval_sentence(sentence, filter):
  total = 0
  
  for word in sentence.split(' '):
    indices = get_indices(word)
    total += sum([int(filter[i]) for i in indices])

  return total
      
if __name__ == "__main__":
    if len(sys.argv) != 5:
        print("Usage: structured_network_wordcount.py <hostname> <port> <bloom filter path> <file name>", file=sys.stderr)
        sys.exit(-1)

    print ('Argv', sys.argv)
    
    host = sys.argv[1]
    port = int(sys.argv[2])
    bloom_path = sys.argv[3]
    file_name = sys.argv[4]

    # set up the spark session
    spark = SparkSession.builder.appName("CensorshipBoard9000").master("local[*]").getOrCreate()
    spark.sparkContext.addFile(bloom_path)
    spark.sparkContext.setLogLevel('WARN')
    
    # get the bloom filter encoded as base64. Decode and reprsent as an RDD
    df = spark.read.text(bloom_path)
    rdd_ = df.select(decode(unbase64('value'),'UTF-8').alias('value')).rdd
    bloom_filter = rdd_.map(lambda a: a['value']).flatMap(lambda a: [char for char in a]).collect()
    bloomUDF = udf(lambda a: eval_sentence(a, bloom_filter), IntegerType())
  
    # create DataFrame for the input lines coming in to the given host and port. Code copied from spark example
    lines = spark\
      .readStream\
      .format('socket')\
      .option('host', host)\
      .option('port', port)\
      .load()

    # create dataframe evaluating sentence against bloom filter
    # lines_eval = lines.select(col('value').alias('sentence'))
    lines_eval = lines.select(col('value').alias('sentence'), bloomUDF(col('value')).alias('eval'))

    # filter out the sentences with curse words
    filtered = lines_eval.select(col('sentence'), col('eval')).filter(col('eval') < 1)

                                                                              
    # Transform into columns sentence, bloom count. Output only newly edited rows. output is set to 
    # 'append' by default so only updates to the filtered dataframe are shown
    query = filtered\
        .writeStream\
        .format('console')\
        .option('truncate', False)\
        .start()

    query.awaitTermination()
