import configparser
from datetime import datetime
import os
from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql import types as T
from pyspark.sql.functions import udf, col
from pyspark.sql.functions import year, month, dayofmonth, hour, weekofyear, date_format


config = configparser.ConfigParser()
config.read('dl.cfg')

os.environ['AWS_ACCESS_KEY_ID']=config['AWS_ACCESS_KEY_ID']
os.environ['AWS_SECRET_ACCESS_KEY']=config['AWS_SECRET_ACCESS_KEY']


def create_spark_session():
    """
    Creates the Spark Session
    :return:
    """
    spark = SparkSession \
        .builder \
        .config("spark.jars.packages", "org.apache.hadoop:hadoop-aws:2.7.0") \
        .getOrCreate()
    return spark


def process_song_data(spark, input_data, output_data):
    """
    Process all the songs data from the json files specified from the input_data
    :param spark:
    :param input_data:
    :param output_data:
    :return:
    """
    # get filepath to song data file
    song_data = input_data + "song_data/*/*/*"
    
    # read song data file
    df = spark.read.json(song_data)

    # extract columns to create songs table
    songs_table = (
        df.select(
            'song_id', 'title', 'artist_id',
            'year', 'duration'
        ).distinct()
    )
    
    # write songs table to parquet files partitioned by year and artist
    songs_table.write.parquet(output_data + "songs.parquet", mode="overwrite")

    # extract columns to create artists table
    artists_table = (
        df.select(
            'artist_id',
            col('artist_name').alias('name'),
            col('artist_location').alias('location'),
            col('artist_latitude').alias('latitude'),
            col('artist_longitude').alias('longitude'),
        ).distinct()
    )
    
    # write artists table to parquet files
    artists_table.write.parquet(output_data + "artists.parquet", mode="overwrite")


def process_log_data(spark, input_data, output_data):
    """
    Process all event logs of the Sparkify app usage, specifically the 'NextSong' event.
    :param spark:
    :param input_data:
    :param output_data:
    :return:
    """
    # get filepath to log data file
    log_data = input_data + "log_data/*/*"

    # read log data file
    df = spark.read.json(log_data)

    # filter by actions for song plays
    df = df.where(df.page == 'NextSong')

    # extract columns for users table    
    users_table = (
        df.select(
            col('userId').alias('user_id'),
            col('firstName').alias('first_name'),
            col('lastName').alias('last_name'),
            col('gender').alias('gender'),
            col('level').alias('level')
        ).distinct()
    )
    
    # write users table to parquet files
    users_table.write.parquet(output_data + "users.parquet", mode="overwrite")

    # create timestamp column from original timestamp column
    df = df.withColumn(
        "ts_timestamp",
        F.to_timestamp(F.from_unixtime((col("ts") / 1000) , 'yyyy-MM-dd HH:mm:ss.SSS')).cast("Timestamp")
    )

    def get_weekday(date):
        import datetime
        import calendar
        date = date.strftime("%m-%d-%Y")  # , %H:%M:%S
        month, day, year = (int(x) for x in date.split('-'))
        weekday = datetime.date(year, month, day)
        return calendar.day_name[weekday.weekday()]

    udf_week_day = udf(get_weekday, T.StringType())
    
    # extract columns to create time table
    time_table = (
        df.withColumn("hour", hour(col("ts_timestamp")))
          .withColumn("day", dayofmonth(col("ts_timestamp")))
          .withColumn("week", weekofyear(col("ts_timestamp")))
          .withColumn("month", month(col("ts_timestamp")))
          .withColumn("year", year(col("ts_timestamp")))
          .withColumn("weekday", udf_week_day(col("ts_timestamp")))
          .select(
            col("ts_timestamp").alias("start_time"),
            col("hour"),
            col("day"),
            col("week"),
            col("month"),
            col("year"),
            col("weekday")
          )
    )
    
    # write time table to parquet files partitioned by year and month
    time_table.write.parquet(output_data + "time.parquet", mode="overwrite")

    # read in song data to use for songplays table
    song_df = spark.read.parquet(output_data + "songs.parquet")

    # extract columns from joined song and log datasets to create songplays table
    songplays_table = (
        df.withColumn("songplay_id", F.monotonically_increasing_id())
          .join(song_df, song_df.title == df.song)
          .select(
            "songplay_id",
            col("ts_timestamp").alias("start_time"),
            col("userId").alias("user_id"),
            "level",
            "song_id",
            "artist_id",
            col("sessionId").alias("session_id"),
            "location",
            col("userAgent").alias("user_agent")
          )
    )

    # write songplays table to parquet files partitioned by year and month
    songplays_table.write.parquet(output_data + "songplays.parquet", mode="overwrite")


def main():
    spark = create_spark_session()
    input_data = "s3a://udacity-dend/"
    output_data = "s3a://social-wiki-datalake/"
    
    process_song_data(spark, input_data, output_data)    
    process_log_data(spark, input_data, output_data)


if __name__ == "__main__":
    main()
