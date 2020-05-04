import configparser
from datetime import datetime
import os
import boto3
from pyspark.sql import SparkSession
from pyspark.sql.functions import udf, col
from pyspark.sql.functions import year, month, dayofmonth, hour, weekofyear, date_format


config = configparser.ConfigParser()
config.read('dl.cfg')

os.environ['AWS_ACCESS_KEY_ID']=config['CONFIG']['AWS_ACCESS_KEY_ID']
os.environ['AWS_SECRET_ACCESS_KEY']=config['CONFIG']['AWS_SECRET_ACCESS_KEY']

def create_spark_session():
    spark = SparkSession \
        .builder \
        .config("spark.jars.packages", "org.apache.hadoop:hadoop-aws:2.7.0") \
        .getOrCreate()
    return spark

def process_song_data(spark, input_data, output_data):
    
    # get filepath to song data file
    song_data = input_data + 'song_data/*/*/*/*.json'
    
    print('---- Processing songs_table ----')
    # read song data file
    df = spark.read.json(song_data)

    # register a view
    df.createOrReplaceTempView("song_data_view")
    
    # extract columns to create songs table
    songs_table = spark.sql("""
                            SELECT 
                                s.song_id as song_id, 
                                s.title as title,
                                s.artist_id as artist_id,
                                s.year as year,
                                s.duration as duration
                            FROM song_data_view s
                            """)
    
    # write songs table to parquet files partitioned by year and artist
    songs_table.write.mode('overwrite').partitionBy("year", "artist_id").parquet(output_data + 'songs_table/')
    print('---- Processing songs_table END ----')

    print('---- Processing artists_table ----')
    # extract columns to create artists table
    artists_table = spark.sql("""
                            SELECT 
                                s.artist_id as artist_id, 
                                s.artist_name as name,
                                s.artist_location as location,
                                s.artist_latitude as lattitude,
                                s.artist_longitude as longitude
                            FROM song_data_view s
                            """) 
    
    # write artists table to parquet files
    artists_table.write.mode('overwrite').parquet(output_data + 'artists_table/')
    print('---- Processing artists_table END ----')


def process_log_data(spark, input_data, output_data):
    # get filepath to log data file
    log_data = input_data + 'log_data/*.json'

    print('---- Processing users_table ----')
    # read log data file
    df = spark.read.json(log_data)
    
    # filter by actions for song plays
    df = df.filter(df.page == 'NextSong')

    # register a view
    df.createOrReplaceTempView("log_data_view")
    
    # extract columns for users table    
    users_table = spark.sql("""
                            SELECT 
                                l.userId as user_id, 
                                l.firstName as first_name, 
                                l.lastName as last_name, 
                                l.gender as gender, 
                                l.level as level
                            FROM log_data_view l
                            """)
    
    # write users table to parquet files
    users_table.write.mode('overwrite').parquet(output_data + 'users_table/')
    print('---- Processing users_table END ----')
    
    print('---- Processing time_table ----')
    # extract columns to create time table
    time_table = spark.sql("""
                            SELECT 
                                A.ts as start_time,
                                hour(A.ts) as hour,
                                dayofmonth(A.ts) as day,
                                weekofyear(A.ts) as week,
                                month(A.ts) as month,
                                year(A.ts) as year,
                                dayofweek(A.ts) as weekday
                            FROM
                            (SELECT 
                                to_timestamp(l.ts/1000) as ts
                            FROM log_data_view l
                            WHERE l.ts IS NOT NULL) A
                        """)
    
    # write time table to parquet files partitioned by year and month
    time_table.write.mode('overwrite').partitionBy("year", "month").parquet(output_data + 'time_table/')
    print('---- Processing time_table END ----')

    print('---- Processing songplays_table ----')
    # read in song data to use for songplays table
    song_df = spark.read.parquet(output_data+'songs_table/')
    
    song_df.createOrReplaceTempView("songs_data_view")

    # extract columns from joined song and log datasets to create songplays table 
    songplays_table = spark.sql("""
                                SELECT 
                                l.ts AS start_time,
                                l.userId AS user_id,
                                l.level AS level,
                                s.song_id AS song_id,
                                s.artist_id AS artist,
                                l.sessionId AS session_id,
                                l.location AS location,
                                l.userAgent AS user_agent,
                                year(to_timestamp(l.ts/1000)) as year_partition,
                                month(to_timestamp(l.ts/1000)) as month_partition
                            FROM log_data_view l
                            left outer join songs_data_view s
                            on l.song = s.title 
                                """)

    # write songplays table to parquet files partitioned by year and month
    songplays_table.write.mode('overwrite').partitionBy("year_partition", "month_partition").parquet(output_data + 'songplays_table/')
    print('---- Processing songplays_table END ----')

def main():
    spark = create_spark_session()
    input_data = "s3a://udacitydatalakeinput/"
    output_data = "s3a://udacitydatalakeoutput/"
    
    process_song_data(spark, input_data, output_data)    
    process_log_data(spark, input_data, output_data)


if __name__ == "__main__":
    main()
