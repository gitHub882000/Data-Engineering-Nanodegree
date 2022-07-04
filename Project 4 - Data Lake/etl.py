import configparser
from io import StringIO
import os
from pyspark.sql import SparkSession
from pyspark.sql.window import Window
from pyspark.sql.functions import lit, col, row_number, last, monotonically_increasing_id
from pyspark.sql.functions import year, month, dayofmonth, hour, weekofyear, date_format
from pyspark.sql.types import TimestampType


def create_spark_session():
    """
    Gets or creates a SparkSession object.

    Returns:
        spark (pyspark.sql.SparkSession): SparkSession object that is used to process
            data.
    """
    spark = SparkSession \
        .builder \
        .config("spark.jars.packages", "org.apache.hadoop:hadoop-aws:2.7.0") \
        .getOrCreate()
    return spark


def parse_config(spark, configpath):
    """
    Parses configuration data from a config file stored on S3 and saves these values as
    environment variables.

    Args:
        spark (pyspark.sql.SparkSession): SparkSession object that is used to process
            data.
        configpath (str): S3 path to the config file.
    """
    # Read config file from S3
    cfg_rdd = spark.sparkContext.textFile(configpath).collect()
    cfg_content = StringIO('\n'.join(cfg_rdd))

    # Parse config
    config = configparser.ConfigParser()
    config.read_file(cfg_content)

    # Save config as environment variables
    os.environ['AWS_ACCESS_KEY_ID'] = config['IAM']['AWS_ACCESS_KEY_ID']
    os.environ['AWS_SECRET_ACCESS_KEY'] = config['IAM']['AWS_SECRET_ACCESS_KEY']
    os.environ['AWS_SESSION_TOKEN'] = config['IAM']['AWS_SESSION_TOKEN']

    os.environ['INPUT_S3_URL'] = config['S3']['INPUT_S3_URL']
    os.environ['OUTPUT_S3_URL'] = config['S3']['OUTPUT_S3_URL']


def process_song_data(spark, input_data, output_data):
    """
    Processes the song data stored in the S3 bucket ``input_data``.
    The song data are extracted, transformed and loaded to two ``DataFrame``s:
    ``artists`` and ``songs``. The ``DataFrame``s are then saved in the S3 bucket
    ``output_data``.
    
    Args:
        spark (pyspark.sql.SparkSession): SparkSession object that is used to process
            data.
        input_data (str): Path of the S3 bucket where the source song data are stored.
        output_data (str): Path of the S3 bucket where the target song data are saved.
    """
    # get filepath to song data file
    song_data = input_data + 'song_data/*/*/*/*.json'
    
    # read song data file
    df = spark.read.json(song_data)
    df.createOrReplaceTempView('song_data')

    # extract columns to create songs table
    songs_table = df.select(col('song_id'), col('title'), col('artist_id'),
                            col('year'), col('duration'))
    songs_window = Window.partitionBy('song_id').orderBy(lit('A'))
    songs_table = songs_table.withColumn('rn', row_number().over(songs_window)) \
                             .filter(col('rn') == 1).drop('rn')
    
    # write songs table to parquet files partitioned by year and artist
    songs_table.write.mode('overwrite').partitionBy('year', 'artist_id').parquet(output_data + 'songs')

    # extract columns to create artists table
    artists_table = df.select(col('artist_id'), col('artist_name').alias('name'), 
                              col('artist_location').alias('location'),
                              col('artist_latitude').alias('latitude'),
                              col('artist_longitude').alias('longitude'))
    artists_window = Window.partitionBy('artist_id').orderBy(lit('A'))
    artists_table = artists_table.withColumn('rn', row_number().over(artists_window)) \
                                 .filter(col('rn') == 1).drop('rn')
    
    # write artists table to parquet files
    artists_table.write.mode('overwrite').parquet(output_data + 'artists')


def process_log_data(spark, input_data, output_data):
    """
    Processes the log data stored in the S3 bucket ``input_data``.
    The log data are extracted, transformed and loaded to three ``DataFrame``s:
    ``users``, ``time`` and ``songplays``. The ``DataFrame``s are then saved in the S3 bucket
    ``output_data``.
    
    Args:
        spark (pyspark.sql.SparkSession): SparkSession object that is used to process
            data.
        input_data (str): Path of the S3 bucket where the source log data are stored.
        output_data (str): Path of the S3 bucket where the target log data are saved.
    """
    # get filepath to log data file
    log_data = input_data + 'log_data/*/*/*.json'

    # read log data file
    df = spark.read.json(log_data)
    
    # filter by actions for song plays
    df = df.filter(col('page') == 'NextSong')

    # extract columns for users table    
    users_table = df.select(col('userId').alias('user_id'), col('firstName').alias('first_name'),
                            col('lastName').alias('last_name'), col('gender'), col('level'))
    users_table = users_table.groupby('user_id') \
        .agg(*[last(col(col_name)).alias(col_name) for col_name in [x for x in users_table.columns if x != 'user_id']])
    
    # write users table to parquet files
    users_table.write.mode('overwrite').parquet(output_data + 'users')
    
    # extract columns to create time table
    df = df.withColumn("start_time", (col("ts") / 1000).cast(TimestampType())) \
           .withColumn("month", month(col('start_time'))) \
           .withColumn("year", year(col('start_time')))

    time_table = df.withColumn("hour", hour(col('start_time'))) \
                   .withColumn("day", dayofmonth(col('start_time'))) \
                   .withColumn("week", weekofyear(col('start_time'))) \
                   .withColumn("weekday", date_format(col('start_time'), 'E')) \
                   .distinct()
    
    # write time table to parquet files partitioned by year and month
    time_table.write.mode('overwrite').partitionBy('year', 'month').parquet(output_data + 'time')

    # read in song data to use for songplays table
    song_df = spark.sql("""
        SELECT song_id, artist_id, title,
               artist_name, duration
        FROM song_data
    """)

    # extract columns from joined song and log datasets to create songplays table 
    songplays_table = df.join(song_df, (df['song'] == song_df['title'])
                                       & (df['artist'] == song_df['artist_name'])
                                       & (df['length'] == song_df['duration'])) \
                        .distinct().select(col('start_time'), col('userId').alias('user_id'),
                                           col('level'), col('song_id'), col('artist_id'),
                                           col('sessionId').alias('session_id'),
                                           col('location'), col('userAgent').alias('user_agent'),
                                           col('month'), col('year')) \
                        .withColumn('songplay_id', monotonically_increasing_id())

    # write songplays table to parquet files partitioned by year and month
    songplays_table.write.mode('overwrite').partitionBy('year', 'month').parquet(output_data + 'songplays')


def main():
    """
    Loads both song and log data from source S3 bucket,
    processes and saves them to our target S3 bucket.
    """
    spark = create_spark_session()

    parse_config(spark, 's3a://dtl-scripts/dtl.cfg')
    input_data = os.environ['INPUT_S3_URL']
    output_data = os.environ['OUTPUT_S3_URL']
    
    process_song_data(spark, input_data, output_data)
    process_log_data(spark, input_data, output_data)
    spark.stop()


if __name__ == "__main__":
    main()
