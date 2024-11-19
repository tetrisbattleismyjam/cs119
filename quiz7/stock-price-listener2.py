#!/usr/bin/env python3
import sys
import pyspark
import datetime
import time

from pyspark.sql.window import Window
from pyspark import SparkFiles
from pyspark.conf import SparkConf
from pyspark.context import SparkContext
from pyspark.sql import SparkSession
import pyspark.sql.functions as sql_f

msft_avg_10 = 0
msft_avg_40 = 0
msft_bull = False

aapl_avg_10 = 0
aapl_avg_40 = 0
aapl_bull = False

def process_batch(df_batch, batch_id):
    
    if df_batch.count() > 0:
        df_batch.persist()
        global msft_avg_10
        global msft_avg_40
        global aapl_avg_10
        global aapl_avg_40
        
        msft_avg_10 = df_batch.withColumn('max_date', sql_f.col('date'))\
                        .filter(sql_f.col('date') > sql_f.date_sub(sql_f.col('max_date'),10))\
                        .filter(sql_f.col('symbol') == 'MSFT')\
                        .agg({'price': 'avg'}).collect()[0]['avg(price)']

        msft_avg_40 = df_batch.withColumn('max_date', sql_f.col('date'))\
                        .filter(sql_f.col('date') > sql_f.date_sub(sql_f.col('max_date'),40))\
                        .filter(sql_f.col('symbol') == 'MSFT')\
                        .agg({'price': 'avg'}).collect()[0]['avg(price)']

        aapl_avg_10 = df_batch.withColumn('max_date', sql_f.col('date'))\
                        .filter(sql_f.col('date') > sql_f.date_sub(sql_f.col('max_date'),10))\
                        .filter(sql_f.col('symbol') == 'AAPL')\
                        .agg({'price': 'avg'}).collect()[0]['avg(price)']
        
        aapl_avg_40 = df_batch.withColumn('max_date', sql_f.col('date'))\
                        .filter(sql_f.col('date') > sql_f.date_sub(sql_f.col('max_date'),40))\
                        .filter(sql_f.col('symbol') == 'AAPL')\
                        .agg({'price': 'avg'}).collect()[0]['avg(price)']
        print(msft_avg_10, msft_avg_40, aapl_avg_10, aapl_avg_40)
        
        df_batch.unpersist()
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
    # create streaming dataframe for incoming stock data. data coming is as (datetime, msft price, aapl price)
    msftPrices = spark \
            .readStream \
            .format('socket')\
            .option('host', host)\
            .option('port', port)\
            .load()\
            .select(sql_f.substring(sql_f.element_at(sql_f.split('value', '[\t]'), 1), 1, 10).alias('date')\
                               ,sql_f.element_at(sql_f.split('value', '[\t]'), 2).alias('price'))

    aaplPrices = spark\
              .readStream \
              .format('socket')\
              .option('host', host)\
              .option('port', port)\
              .load()\
              .select(sql_f.substring(sql_f.element_at(sql_f.split('value', '[\t]'), 1), 1, 10).alias('date')\
                                 ,sql_f.element_at(sql_f.split('value', '[\t]'), 3).alias('price'))

    q = msftPrices.writeStream\
              .outputMode('Append')\
              .format('console')\
              .start()
   
    q2 = aaplPrices.writeStream\
            .outputMode('Append')\
            .format('console')\
            .start()

    q.awaitTermination()

