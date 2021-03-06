import configparser
from datetime import datetime
import os
from pyspark.sql import SparkSession
from pyspark.sql.functions import udf, col
from pyspark.sql.functions import year, month, dayofmonth, hour, weekofyear, date_format
from sql_queries import  songplay_sql,user_sql,song_sql,artist_sql,time_sql
import logging

config = configparser.ConfigParser()
config.read('dl.cfg')

os.environ['AWS_ACCESS_KEY_ID']=config['AWS']['AWS_ACCESS_KEY_ID']
os.environ['AWS_SECRET_ACCESS_KEY']=config['AWS']['AWS_SECRET_ACCESS_KEY']


def create_spark_session():
    spark = SparkSession \
        .builder \
        .config("spark.jars.packages", "org.apache.hadoop:hadoop-aws:2.7.0") \
        .getOrCreate()
    return spark

def process_song_data(spark, input_data, output_data):
    # get filepath to song data file
    df = spark.read.json(input_data+'/song-data/*/*/*/*/*.json')
    df.createOrReplaceTempView('song')

    # extract columns to create songs table
    song_table = spark.sql(song_sql)
    
    # write songs table to parquet files partitioned by year and artist
    song_table.write.partitionBy('year','artist_id').parquet(output_data+'/song/')

    # extract columns to create artists table
    artists_table = spark.sql(artist_sql)
    
    # write artists table to parquet files
    artists_table.write.parquet(output_data+'/artist/')


def process_log_data(spark, input_data, output_data):
    log_data = spark.read.json(input_data+'/log-data/*')
    log_data.createOrReplaceTempView('log')
    
    # extract columns for users table    
    users_table = spark.sql(user_sql)
    
    # write users table to parquet files
    users_table.write.mode("overwrite").parquet(output_data+'/user/')
    
    # extract columns to create time table
    time_table = spark.sql(time_sql)
    
    # write time table to parquet files partitioned by year and month
    time_table.write.partitionBy('year','month').parquet(output_data+'/time/')

    # read in song data to use for songplays table
    song_df = spark.read.json(input_data+'/song-data/*/*/*/*/*.json')
    song_df.createOrReplaceTempView('song')

    # extract columns from joined song and log datasets to create songplays table 
    songplay_table = spark.sql(songplay_sql)

    # write songplays table to parquet files partitioned by year and month
    songplay_table.write.partitionBy("year","month").parquet(output_data+'/songplay/')

def main():
    spark = create_spark_session()
    input_data = "./data"
    output_data = "./data/analytics"
    
    process_song_data(spark, input_data, output_data)    
    process_log_data(spark, input_data, output_data)


if __name__ == "__main__":
    main()
