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

# def get_date_avg(df):
    
#   print(df)
    # print(spark.sql('select * from windowed_average').collect())
    #row = spark.sql('select * from df_q').collect()[0]
    #avg = row['avg(price)']
    #date = row['max(date)']

    # q.stop()
    #return (date, avg)
    # return (1, 1)

                                                                                                                                                                                          
    
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
    lines = spark \
            .readStream \
            .format('socket')\
            .option('host', host)\
            .option('port', port)\
            .load()

    lines_split = lines.select(sql_f.substring(sql_f.element_at(sql_f.split(lines.value, '[\t]'), 1), 1, 10).cast('timestamp').alias('date')\
                               ,sql_f.element_at(sql_f.split(lines.value, '[\t]'), 2).cast('float').alias('price')\
                               ,sql_f.element_at(sql_f.split(lines.value, '[\t]'), 3).alias('symbol'))
    
    # lines_split = lines.select(sql_f.substring(sql_f.element_at(sql_f.split(lines.value, '[\t]'), 1), 1, 10).alias('date')\
    #                           ,sql_f.element_at(sql_f.split(lines.value, '[\t]'), 3).alias('AAPL')\
    #                           ,sql_f.element_at(sql_f.split(lines.value, '[\t]'), 2).alias('MSFT'))

    day_average = lines_split.select('date', 'symbol', 'price').groupby(['date', 'symbol']).avg('price').sort('date')
    
    query = day_average.writeStream\
                .queryName('day_avg')\
                .outputMode('Complete')\
                .format('memory')\
                .start()

    query.awaitTermination()

    while True:
        spark.sql('SELECT * FROM day_avg WHERE symbol = "MSFT"').show()
    # aaplPrice and msftPrice
    # aapl_stream = lines_split.select(sql_f.col('date'), sql_f.col('AAPL').alias('price'))
    # msft_stream = lines_split.select(sql_f.col('date'), sql_f.col('MSFT').alias('price'))

    # aapl_10 = aapl_stream.withColumn('max_date', sql_f.col('date'))\
                            # .filter(sql_f.col('date') > sql_f.date_sub(sql_f.col('max_date'), 10))\
                            # .agg({'price': 'avg', 'date': 'max'})\
                            # .withColumnRenamed('avg(price)', 'aapl_10')
    
    # aapl_40 = aapl_stream.withColumn('max_date', sql_f.col('date'))\
                            # .filter(sql_f.col('date') > sql_f.date_sub(sql_f.col('max_date'), 40))\
                            # .agg({'price': 'avg', 'date': 'max'})\
                            # .withColumnRenamed('avg(price)', 'aapl_40')
    
    # msft_10 = msft_stream.withColumn('max_date', sql_f.col('date'))\
                            # .filter(sql_f.col('date') > sql_f.date_sub(sql_f.col('max_date'), 10))\
                            # .agg({'price': 'avg', 'date': 'max'})\
                            # .withColumnRenamed('avg(price)', 'msft_10')
    
    # msft_40 = msft_stream.withColumn('max_date', sql_f.col('date'))\
                            # .filter(sql_f.col('date') > sql_f.date_sub(sql_f.col('max_date'), 40))\
                            # .agg({'price': 'avg', 'date': 'max'})\
                            # .withColumnRenamed('avg(price)', 'msft_40')


    
    # time.sleep(10)
    # while True:
    #    aapl_10_date, aapl_10_avg = get_date_avg(aapl_10)
    #    aapl_40_date, aapl_40_avg = get_date_avg(aapl_40)
    #    msft_10_date, msft_10_avg = get_date_avg(msft_10)
    #    msft_40_date, msft_40_avg = get_date_avg(msft_40)
        
        # print(aapl_10_date, aapl_10_avg, aapl_40_avg)
        
    #    time.sleep(3)
