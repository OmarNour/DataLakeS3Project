import configparser
from datetime import datetime
import os
from pyspark.sql import SparkSession
from pyspark.sql.functions import udf, col
from pyspark.sql.functions import year, month, dayofmonth, dayofweek, hour, weekofyear, date_format
from pyspark.sql.types import StructType as R, StructField as Fld, DecimalType as Dsml, DoubleType as Dbl, StringType as Str, IntegerType as Int, DateType as Date, TimestampType as DateTime

config = configparser.ConfigParser()
config.read('dl.cfg')

os.environ['AWS_ACCESS_KEY_ID'] = config['AWS']['AWS_ACCESS_KEY_ID']
os.environ['AWS_SECRET_ACCESS_KEY'] = config['AWS']['AWS_SECRET_ACCESS_KEY']


def create_spark_session():
    """
    a function to return a spark session
    :return: spark session
    """
    spark = SparkSession \
        .builder \
        .config("spark.jars.packages", "org.apache.hadoop:hadoop-aws:2.7.0") \
        .getOrCreate()
    return spark


def process_song_data(song_data_df, output_data):
    """
    To process json files for song data to be loaded as parquet files
    :param song_data_df: song data as spark dataframe
    :param output_data: the out path
    :return: None
    """

    df = song_data_df

    songs_table = df.select(['song_id', 'title', 'artist_id', 'year', 'duration']).where(col('song_id').isNotNull())

    songs_output_data = output_data + "songs/"
    songs_table.write.parquet(songs_output_data, mode='overwrite', partitionBy=['year','artist_id'])

    artists_table = df.select(['artist_id', 'name', 'location', 'lattitude', 'longitude']).where(col('artist_id').isNotNull())

    artists_output_data = output_data + "artists/"
    artists_table.write.parquet(artists_output_data, mode='overwrite')


def process_log_data(log_data_df, output_data):
    """
    To process json files for log data to be loaded as parquet files
    :param log_data_df: log data as spark dataframe
    :param output_data: the out path
    :return: None
    """

    log_df = log_data_df
    log_df = log_df.where(col('page') == "NextSong")

    users_table = log_df.select(['userId', 'firstName', 'lastName', 'gender', 'level']).where(col('userId').isNotNull())

    users_output_data = output_data + "users/"
    users_table.write.parquet(users_output_data, mode='overwrite')

    time_table = log_df.select(["timestamp",
                            hour('timestamp').alias('hour'),
                            dayofmonth('timestamp').alias('day'),
                            dayofweek('timestamp').alias('week'),
                            month('timestamp').alias('month'),
                            year('timestamp').alias('year'),
                            weekofyear('timestamp').alias('weekday')
                            ]).dropDuplicates()

    time_output_data = output_data + "time/"
    time_table.write.parquet(time_output_data, mode='overwrite', partitionBy=['year', 'month'])


def populate_songplays_table(spark, log_data_df, song_data_df, output_data):
    """
    To populate songplays fact table
    :param spark: spark session
    :param log_data_df: log data as spark dataframe
    :param song_data_df: song data as spark dataframe
    :param output_data: the out path
    :return: None
    """
    log_data_df.createOrReplaceTempView("log_data_t")
    song_data_df.createOrReplaceTempView("song_data_t")

    songplays_table = spark.sql("""select row_number() over (order by timestamp) as songplay_id, 
                                                    timestamp as start_time,
                                                    userId as user_id,
                                                    level,
                                                    s.song_id,
                                                    s.artist_id,
                                                    sessionId session_id,
                                                    l.location,
                                                    userAgent user_agent 
                                    from song_data_t s
                                    join log_data_t l 
                                        on s.artist_name = l.artist 
                                        and s.title = l.song 
                                        and s.duration = l.length  
                                    where page = 'NextSong'                                    
                                """).dropDuplicates()

    songplays_table.withColumn("year", year('timestamp').alias('year'))
    songplays_table.withColumn("month", month('timestamp').alias('month'))

    songplays_output_data = output_data + "songplays/"
    songplays_table.write.parquet(songplays_output_data, mode='overwrite', partitionBy=['year', 'month'])


def main():
    """
    starting point, workflow:
    1- get log data into spark dataframe
    2- get song data into spark dataframe
    3- call process_song_data
    4- call process_log_data
    5- call populate_songplays_table
    :return: None
    """
    spark = create_spark_session()
    input_data = "s3a://udacity-dend/"
    output_data = "s3a://sparkify-datalake/"

    # song_data = input_data + "/song_data/A/A/A/TRAAABD128F429CF47.json"
    # song_data = input_data + "/song_data/A/A/A/*.json"
    song_data = input_data + "/song_data/*/*/*/*.json"
    log_data = input_data + "/log_data/*/*/*.json"

    songs_schema = R([
                    Fld("artist_id", Str()),
                    Fld("artist_latitude", Dbl()),
                    Fld("artist_location", Str()),
                    Fld("artist_longitude", Dbl()),
                    Fld("artist_name", Str()),
                    Fld("duration", Dbl()),
                    Fld("num_songs", Int()),
                    Fld("song_id", Str()),
                    Fld("title", Str()),
                    Fld("year", Int()),
                    Fld("name", Int()),
                    Fld("location", Str()),
                    Fld("lattitude", Dsml()),
                    Fld("longitude", Dsml()),
                    ])

    song_data_df = spark.read.json(song_data, songs_schema)
    log_data_df = spark.read.json(log_data)

    get_timestamp = udf(lambda ts: datetime.fromtimestamp(ts / 1e3), DateTime())
    get_datetime = udf(lambda ts: datetime.fromtimestamp(int(str(ts)[:-3])), DateTime())

    log_data_df = log_data_df.withColumn("timestamp", get_timestamp("ts"))
    log_data_df = log_data_df.withColumn("datetime", get_datetime("ts"))

    process_song_data(song_data_df, output_data)
    process_log_data(log_data_df, output_data)
    populate_songplays_table(spark, log_data_df, song_data_df, output_data)


if __name__ == "__main__":
    main()