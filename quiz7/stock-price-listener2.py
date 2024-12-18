#!/usr/bin/env python3
import sys
import pyspark
import datetime
import time

from pyspark.sql import Window
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
            .select(sql_f.to_timestamp(sql_f.element_at(sql_f.split('value', '[\t]'), 1)).alias('date')\
                               ,sql_f.element_at(sql_f.split('value', '[\t]'), 2).cast('float').alias('price'))

    aaplPrices = spark\
                  .readStream \
                  .format('socket')\
                  .option('host', host)\
                  .option('port', port)\
                  .load()\
                  .select(sql_f.to_timestamp(sql_f.element_at(sql_f.split('value', '[\t]'), 1)).alias('date')\
                                     ,sql_f.element_at(sql_f.split('value', '[\t]'), 3).cast('float').alias('price'))
    
    aapl10Day = aaplPrices.withWatermark('date', '15 minutes').groupBy(sql_f.window('date', '10 days', '2 days')).agg({'price': 'avg', 'date': 'max'}).sort('window')
    aapl40Day = aaplPrices.withWatermark('date', '15 minutes').groupBy(sql_f.window('date', '10 days', '2 days')).agg({'price': 'avg', 'date': 'max'}).sort('window')
    msft10Day = msftPrices.withWatermark('date', '15 minutes').groupBy(sql_f.window('date', '10 days', '2 days')).agg({'price': 'avg', 'date': 'max'}).sort('window')
    msft40Day = msftPrices.withWatermark('date', '15 minutes').groupBy(sql_f.window('date', '10 days', '2 days')).agg({'price': 'avg', 'date': 'max'}).sort('window')
    
    q = aapl10Day.writeStream\
                .queryName('aapl10Day')\
                .outputMode('Complete')\
                .option('truncate', False)\
                .format('memory')\
                .start()
    
    aapl40Day.writeStream\
                .queryName('aapl40Day')\
                .outputMode('Complete')\
                .option('truncate', False)\
                .format('memory')\
                .start()

    msft10Day.writeStream\
                .queryName('msft10Day')\
                .outputMode('Complete')\
                .option('truncate', False)\
                .format('memory')\
                .start()

    msft40Day.writeStream\
                .queryName('msft40Day')\
                .outputMode('Complete')\
                .option('truncate', False)\
                .format('memory')\
                .start()
    
    aapl10dayAvg = 0
    aapl40dayAvg = 0
    msft10dayAvg = 0
    msft40dayAvg = 0
    
    msftBull = False
    aaplBull = False

    recommendation = []
    while q.isActive:
            time.sleep(10)
            print("currently have ", len(recommendation), " recommendations")
            print("aapl (10 Day Average, 40 Day Average) ", aapl10dayAvg, aapl40dayAvg)
            print("msft (10 Day Average, 40 Day Average) ", msft10dayAvg, msft40dayAvg)
        
            rows = spark.sql('select * from aapl10day').tail(1)
            if len(rows) > 0:
                aapl10dayAvg = rows[0]['avg(price)']
                aaplDate = rows[0]['max(date)']

            rows = spark.sql('select * from aapl40day').tail(1)
            if len(rows) > 0:
                aapl40dayAvg = rows[0]['avg(price)']

            rows = spark.sql('select * from msft10day').tail(1)
            if len(rows) > 0:
                msft10dayAvg = rows[0]['avg(price)']
                msftDate = rows[0]['max(date)']
                
            rows = spark.sql('select * from msft40day').tail(1)
            if len(rows) > 0:
                msft40dayAvg = rows[0]['avg(price)']

            if msft10dayAvg and msft40dayAvg and msft10dayAvg > msft40dayAvg and not msftBull:
                recommendations.append((msftDate, 'buy', 'MSFT'))
                msftBull = True

            if msft10dayAvg and msft40dayAvg and msft10dayAvg < msft40dayAvg and msftBull:
                recommendations.append((msftDate, 'sell', 'MSFT'))
                msftBull = False
                
            if aapl10dayAvg and aapl40dayAvg and aapl10dayAvg > aapl40dayAvg and not aaplBull:
                recommendations.append((aaplDate, 'buy', 'AAPL'))
                aaplBull = True

            if aapl10dayAvg and aapl40dayAvg and aapl10dayAvg < aapl40dayAvg and aaplBull:
                recommendations.append((aaplDate, 'sell', 'AAPL'))
                aaplBull = False
    
            if len(recommendation) > 10:
                print(recommendation)
            
    q.awaitTermination()

