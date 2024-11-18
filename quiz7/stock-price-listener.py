#!/usr/bin/env python3
import sys
import pyspark
import datetime

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
    
    lines_split = lines.select(sql_f.element_at(sql_f.substring(sql_f.split(lines.value, '[\t]'), 1), 1, 10).alias('date')\
                               ,sql_f.element_at(sql_f.split(lines.value, '[\t]'), 2).alias('AAPL')\
                               ,sql_f.element_at(sql_f.split(lines.value, '[\t]'), 3).alias('MSFT'))

    aapl_stream = lines_split.select(sql_f.col('date'), sql_f.col('AAPL').alias('price'))
    msft_stream = lines_split.select(sql_f.col('date'), sql_f.col('MSFT').alias('price'))

    aapl_10 = aapl_stream.withColumn('max_date', sql_f.col('date'))\
                            .filter(sql_f.col('date') > sql_f.date_sub(sql_f.col('max_date'), 10))\
                            .groupBy(sql_f.col('date'))\
    
    query = aapl_10\
            .writeStream\
            .format("console")\
            .outputMode('complete')\
            .option('truncate', False)\
            .start()
    
    query.awaitTermination()
