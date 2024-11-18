#!/usr/bin/env python3
import sys
import pyspark

from pyspark import SparkFiles
from pyspark.conf import SparkConf
from pyspark.context import SparkContext
from pyspark.sql import SparkSession
import pyspark.sql.functions as sql_f

if __name__ == "__main__":
    if len(sys.argv) != 3:
        print("Usage: stock-price-listener.py <hostname> <port>")

    host = sys.argv[1]
    port = int(sys.argv[2])

    # set up spark session
    spark = SparkSession.builder.appName("MoneyMaker3000") \
                                .master("local[*]") \
                                .getOrCreate() 

    spark.sparkContext.setLogLevel('WARN')

    print(host, port)
    # create streaming dataframe for incoming stock data.
    lines = spark \
            .readStream \
            .format('socket')\
            .option('host', host)\
            .option('port', port)\
            .load()
    
    lines_split = lines.select(sql_f.element_at(sql_f.split(lines.value, '[\t]'), 1).alias('date')\
                               ,sql_f.element_at(sql_f.split(lines.value, '[\t]'), 2).alias('AAPL')\
                               ,sql_f.element_at(sql_f.split(lines.value, '[\t]'), 3).alias('MSFT'))
                               
    query = lines_split\
            .writeStream\
            .format("console")\
            .option('truncate', False)\
            .start()
    
    query.awaitTermination()
