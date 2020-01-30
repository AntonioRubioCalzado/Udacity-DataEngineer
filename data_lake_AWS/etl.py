import configparser
from datetime import datetime
import os
from pyspark.sql import SparkSession
from pyspark.sql.functions import udf, col
from pyspark.sql.functions import date_format, to_timestamp
from pyspark.sql.functions import monotonically_increasing_id
from pyspark.sql.types import StructType, StructField, StringType, IntegerType, DoubleType, TimestampType

# Let's read the Access_key_id and Secret_key_id of the IAM user created with S3FullAccess role policy.
config = configparser.ConfigParser()
config.read('dl.cfg')

os.environ['AWS_ACCESS_KEY_ID']=config.get('AWS', 'KEY')
os.environ['AWS_SECRET_ACCESS_KEY']=config.get('AWS', 'SECRET')


def create_spark_session():
    """
    Function that sets the SparkSession with the dependencies necessary to run hadoop and spark on AWS.
    """
    spark = SparkSession \
        .builder \
        .config("spark.jars.packages", "org.apache.hadoop:hadoop-aws:2.7.0") \
        .getOrCreate()
    return spark


def process_song_data(spark, input_data, output_data):
    """
    Function that extract the song data from an S3 bucket, process it to create song and artist tables
    and write them into another S3 bucket in parquet format.
    
    Params:
        spark: SparkSession.
        input_data: Path of the S3 bucket where songs data is located.
        output_data: Path of the S3 bucket where output data are written.
    """
    
    # get filepath to song data file
    
    song_data = f"{input_data}song-data/*/*/*/*.json"
    #For example: song_data = f"{input_data}song-data/A/A/T/TRAATJK128F9339179.json"
    
    # Let's define the schema of the input data before is read.
    song_schema = StructType([
        StructField('artist_id', StringType(), True),
        StructField('artist_latitude', StringType(), True),
        StructField('artist_location', StringType(), True),
        StructField('artist_longitude', StringType(), True),
        StructField('artist_name', StringType(), True),
        StructField('duration', DoubleType(), True),
        StructField('num_songs', IntegerType(), True),
        StructField('song_id', StringType(), True),
        StructField('title', StringType(), True),
        StructField('year', IntegerType(), True)
    ])
    
    # read song data file
    df = spark.read.json(song_data, schema=song_schema)


    # extract columns to create songs table
    
    songs_table =  df.select(col('title'), col('artist_id'), col('artist_name'), col('year'), col('duration')) \
                     .dropDuplicates() \
                     .withColumn('song_id', monotonically_increasing_id())
    
    # write songs table to parquet files partitioned by year and artist
    songs_table.write.mode("overwrite").partitionBy('year', 'artist_id').parquet(output_data + 'songs/')
    
    # extract columns to create artists table
    artists_table = df.select(col('artist_id'), 
                              col('artist_name'), 
                              col('artist_location'),
                              col('artist_latitude'), 
                              col('artist_longitude'))

    # write artists table to parquet files
    artists_table.write.mode("overwrite").parquet(output_data + 'artists/')


def process_log_data(spark, input_data, output_data):
    """
    Function that extract the log data from an S3 bucket, process it to create users and time tables
    and write them into another S3 bucket in parquet format. Furthermore, it joins log and song data 
    to create songplays table.
    
    Params:
        spark: SparkSession.
        input_data: Path of the S3 bucket where logs data is located.
        output_data: Path of the S3 bucket where output data are written.
    """
    # get filepath to log data file
    log_data = f"{input_data}log-data/*/*/*.json"
    #log_data = f"{input_data}log-data/2018/11/2018-11-01-events.json"
    
    # read log data file
    df = spark.read.json(log_data)

    # filter by actions for song plays
    df = df.where(col('page') == 'NextSong')

    # extract columns for users table  

    users_table = df.selectExpr([
        "userId as user_id",
        "firstName as first_name",
        "lastName as last_name",
        "gender",
        "level"
    ]).dropDuplicates()
    
    # write users table to parquet files
    users_table.write.parquet(output_data + 'users/')
    
    # create timestamp column from original timestamp column        
    df = df.withColumn("start_time", date_format(to_timestamp(col('ts')/1000), "dd-MM-yyyy HH:mm:ss")) \
           .withColumn("hour", date_format(to_timestamp(col('ts')/1000), "HH")) \
           .withColumn("day", date_format(to_timestamp(col('ts')/1000), "dd")) \
           .withColumn("week", date_format(to_timestamp(col('ts')/1000), "w")) \
           .withColumn("month", date_format(to_timestamp(col('ts')/1000), "MM")) \
           .withColumn("year", date_format(to_timestamp(col('ts')/1000), "yyyy")) \
           .withColumn("weekday", date_format(to_timestamp(col('ts')/1000), "E"))

    # extract columns to create time table 
    time_table = df.select("start_time",
                           "hour",
                           "day",
                           "week",
                           "month",
                           "year",
                           "weekday") \
                   .dropDuplicates() 
    
    # write time table to parquet files partitioned by year and month
    time_table.write.mode("overwrite").partitionBy('year','month').parquet(output_data + 'time/')

    # read in song data to use for songplays table
    song_df = spark.read.parquet(output_data + 'songs/')\
                   .drop("year")\
                   .drop("month")
    
    # Join the log and the songs info
    join_conditions = [df.song == song_df.title, df.artist == song_df.artist_name]
    log_songs = df.join(song_df, 
                        join_conditions)
        
    # extract columns from joined song and log datasets to create songplays table 
    # songplay_id, start_time, user_id, level, song_id, artist_id, session_id, location, user_agent
        
    songplays_table = log_songs.withColumn('songplay_id', monotonically_increasing_id())\
                               .select(col('songplay_id'),
                                       col('year'),
                                       col('month'),
                                       col('start_time'),
                                       col('userId').alias('user_id'),
                                       col('level'),
                                       col('song_id'),
                                       col('artist_id'),
                                       col('sessionId').alias('session_id'),
                                       col('location'),
                                       col('userAgent').alias('user_agent'))

    ## write songplays table to parquet files partitioned by year and month
    songplays_table.write.mode("overwrite").partitionBy('year','month').parquet(output_data + 'songplays/')


def main():
    """
    Function that create the SparkSession and call the two previous functions.
    """
    spark = create_spark_session()
    input_data = "s3a://udacity-dend/"
    output_data = "s3a://arc-udacity-dataengineer-datalake-output/"
    
    process_song_data(spark, input_data, output_data)    
    process_log_data(spark, input_data, output_data)


if __name__ == "__main__":
    main()