#!/usr/bin/env python3
import sys
import pyspark
import datetime
import time

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
    
    lines_split = lines.select(sql_f.substring(sql_f.element_at(sql_f.split(lines.value, '[\t]'), 1), 1, 10).alias('date')\
                               ,sql_f.element_at(sql_f.split(lines.value, '[\t]'), 3).alias('AAPL')\
                               ,sql_f.element_at(sql_f.split(lines.value, '[\t]'), 2).alias('MSFT'))

    # aaplPrice and msftPrice
    aapl_stream = lines_split.select(sql_f.col('date'), sql_f.col('AAPL').alias('price'))
    msft_stream = lines_split.select(sql_f.col('date'), sql_f.col('MSFT').alias('price'))

    aapl_10 = aapl_stream.withColumn('max_date', sql_f.col('date'))\
                            .filter(sql_f.col('date') > sql_f.date_sub(sql_f.col('max_date'), 10))\
                            .agg({'price': 'avg', 'date': 'max'})
    
    aapl_40 = aapl_stream.withColumn('max_date', sql_f.col('date'))\
                            .filter(sql_f.col('date') > sql_f.date_sub(sql_f.col('max_date'), 40))\
                            .agg({'price': 'avg', 'date': 'max'})
    
    msft_10 = msft_stream.withColumn('max_date', sql_f.col('date'))\
                            .filter(sql_f.col('date') > sql_f.date_sub(sql_f.col('max_date'), 10))
    
    msft_40 = msft_stream.withColumn('max_date', sql_f.col('date'))\
                            .filter(sql_f.col('date') > sql_f.date_sub(sql_f.col('max_date'), 40))

    aapl_10.writeStream\
        .queryName('aapl_10')\
        .outputMode("complete")\
        .format("memory") \
        .start()

    aapl_40.writeStream\
        .queryName('aapl_40')\
        .outputMode("complete")\
        .format("memory") \
        .start()
    
    time.sleep(5)
    while True:
        rows_10 = spark.sql('select * from aapl_10').collect()
        rows_40 = spark.sql('select * from aapl_40').collect()
        
        avg_date = rows_10[0]['max(date)']
        a_avg_10 = rows_10[0]['avg(price)']
        a_avg_40 = rows_40[0]['avg(price)']

        print(rows_40)
        print(avg_date, a_avg_10, a_avg_40)
        time.sleep(3)
