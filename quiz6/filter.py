import sys, time

import pyspark
# from BitVector import BitVector as bv
from pyspark.conf import SparkConf
from pyspark.context import SparkContext
from pyspark.sql import SparkSession

from pyspark.sql.functions import explode
from pyspark.sql.functions import split

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
    print(spark.sparkContext.listFiles)
    # lines = spark.readStream.format("socket").option("host", hostt).option("port", port).load()

    # Transform into columns sentence, bloom count.
