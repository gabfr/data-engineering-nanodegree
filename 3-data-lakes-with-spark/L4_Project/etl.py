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
    spark = SparkSession \
        .builder \
        .config("spark.jars.packages", "org.apache.hadoop:hadoop-aws:2.7.0") \
        .getOrCreate()
    return spark


def process_song_data(spark, input_data, output_data):
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
    # get filepath to log data file
    log_data = input_data + "log_data/*/*"

    # read log data file
    df = (
        spark.read.schema(T.StructType([
            T.StructField("artist", T.StringType(), True),
            T.StructField("auth", T.StringType(), True),
            T.StructField("firstName", T.StringType(), True),
            T.StructField("gender", T.StringType(), True),
            T.StructField("itemInSession", T.IntegerType(), True),
            T.StructField("lastName", T.StringType(), True),
            T.StructField("length", T.DoubleType(), True),
            T.StructField("level", T.StringType(), True),
            T.StructField("location", T.StringType(), True),
            T.StructField("method", T.StringType(), True),
            T.StructField("page", T.StringType(), True),
            T.StructField("registration", T.DoubleType(), True),
            T.StructField("sessionId", T.IntegerType(), True),
            T.StructField("song", T.StringType(), True),
            T.StructField("status", T.IntegerType(), True),
            T.StructField("ts", T.LongType(), True),
            T.StructField("userAgent", T.StringType(), True),
            T.StructField("userId", T.StringType(), True),
        ])).json(log_data)
    )

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
        ).groupBy('userId')
    )
    
    # write users table to parquet files
    users_table.write.parquet(output_data + "users.parquet", mode="overwrite")

    # create timestamp column from original timestamp column
    # get_timestamp = udf()
    df = df.withColumn(
        "ts_timestamp",
        F.to_timestamp(F.from_unixtime((col("ts") / 1000) , 'yyyy-MM-dd HH:mm:ss.SSS')).cast("Timestamp")
    )
    
    # create datetime column from original timestamp column
    # get_datetime = udf()
    df = df.withColumn(
        "ts_datetime",
        F.to_timestamp(F.from_unixtime((col("ts") / 1000), 'yyyy-MM-dd HH:mm:ss.SSS')).cast("DateTime")
    )

    def get_weekday(date):
        import datetime
        import calendar
        date = date.split(' ')[0]
        month, day, year = (int(x) for x in date.split('-'))
        weekday = datetime.date(year, month, day)
        return calendar.day_name[weekday.weekday()]

    udf_week_day = udf(get_weekday, T.StringType())
    
    # extract columns to create time table
    time_table = (
        df.withColumn("hour", hour(col("ts_timestamp")))
          .withColumn("day", day(col("ts_timestamp")))
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
    song_df = 

    # extract columns from joined song and log datasets to create songplays table 
    songplays_table = 

    # write songplays table to parquet files partitioned by year and month
    songplays_table


def main():
    spark = create_spark_session()
    input_data = "s3a://udacity-dend/"
    output_data = ""
    
    process_song_data(spark, input_data, output_data)    
    process_log_data(spark, input_data, output_data)


if __name__ == "__main__":
    main()
