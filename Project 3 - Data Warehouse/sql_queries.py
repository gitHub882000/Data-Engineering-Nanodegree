import configparser


# CONFIG
config = configparser.ConfigParser()
config.read('dwh.cfg')

IAM_ROLE_ARN = config.get('IAM_ROLE', 'IAM_ROLE_ARN')
LOG_DATA = config.get('S3', 'LOG_DATA')
LOG_JSONPATH = config.get('S3', 'LOG_JSONPATH')
SONG_DATA = config.get('S3', 'SONG_DATA')


# DROP SCHEMAS WITH CASCADE FEATURES
staging_schema_drop = "DROP SCHEMA IF EXISTS staging CASCADE;"
datamart_schema_drop = "DROP SCHEMA IF EXISTS datamart CASCADE;"


# CREATE TABLES AND SCHEMAS
staging_schema_create = ("""
CREATE SCHEMA IF NOT EXISTS staging;
""")

staging_events_table_create = ("""
CREATE TABLE IF NOT EXISTS staging_events (
    artist VARCHAR,
    auth VARCHAR,
    first_name VARCHAR,
    gender CHAR(1),
    item_in_session INT,
    last_name VARCHAR,
    length FLOAT,
    level VARCHAR,
    location TEXT,
    method VARCHAR,
    page VARCHAR,
    registration NUMERIC,
    session_id INT,
    song VARCHAR,
    status VARCHAR,
    ts BIGINT,
    user_agent TEXT,
    user_id INT
);
""")

staging_songs_table_create = ("""
CREATE TABLE IF NOT EXISTS staging_songs (
    num_songs INT,
    artist_id VARCHAR,
    artist_latitude FLOAT,
    artist_longitude FLOAT,
    artist_location TEXT,
    artist_name VARCHAR,
    song_id VARCHAR,
    title VARCHAR,
    duration FLOAT,
    year INT
);
""")

datamart_schema_create = ("""
CREATE SCHEMA IF NOT EXISTS datamart;
""")

songplay_table_create = ("""
CREATE TABLE IF NOT EXISTS songplays (
    songplay_id INT IDENTITY(1, 1) PRIMARY KEY,
    start_time TIMESTAMP,
    user_id INT DISTKEY,
    level VARCHAR,
    song_id VARCHAR,
    artist_id VARCHAR,
    session_id INT,
    location TEXT,
    user_agent TEXT
);
""")

user_table_create = ("""
CREATE TABLE IF NOT EXISTS users (
    user_id INT DISTKEY PRIMARY KEY,
    first_name VARCHAR,
    last_name VARCHAR,
    gender CHAR(1),
    level VARCHAR
);
""")

song_table_create = ("""
CREATE TABLE IF NOT EXISTS songs (
    song_id VARCHAR PRIMARY KEY,
    title VARCHAR,
    artist_id VARCHAR,
    year INT,
    duration FLOAT
) DISTSTYLE ALL;
""")

artist_table_create = ("""
CREATE TABLE IF NOT EXISTS artists (
    artist_id VARCHAR PRIMARY KEY,
    name VARCHAR,
    location TEXT,
    latitude FLOAT,
    longitude FLOAT
) DISTSTYLE ALL;
""")

time_table_create = ("""
CREATE TABLE IF NOT EXISTS time (
    start_time TIMESTAMP PRIMARY KEY,
    hour INT,
    day INT,
    week INT,
    month INT,
    year INT,
    weekday VARCHAR
) DISTSTYLE ALL;
""")


# STAGING TABLES
staging_events_copy = ("""
COPY staging_events
FROM {}
IAM_ROLE '{}'
JSON {};
""").format(LOG_DATA, IAM_ROLE_ARN, LOG_JSONPATH)

staging_songs_copy = ("""
COPY staging_songs
FROM {}
IAM_ROLE '{}'
JSON 'auto';
""").format(SONG_DATA, IAM_ROLE_ARN)


# FINAL TABLES
songplay_table_insert = ("""
INSERT INTO songplays(start_time, user_id, level, song_id, artist_id, session_id, location, user_agent)
SELECT DISTINCT
                TIMESTAMP WITHOUT TIME ZONE 'epoch' + (se.ts / 1000) * INTERVAL '1 second',
                se.user_id, se.level, ss.song_id, ss.artist_id, se.session_id, se.location, se.user_agent
FROM staging.staging_events AS se JOIN staging.staging_songs AS ss
ON se.artist = ss.artist_name AND se.song = ss.title AND se.length = ss.duration
WHERE se.page = 'NextSong';
""")

user_table_insert = ("""
INSERT INTO users(user_id, first_name, last_name, gender, level)
SELECT DISTINCT se.user_id, se.first_name, se.last_name, se.gender, FIRST_VALUE(se.level IGNORE NULLS)
OVER (
      PARTITION BY se.user_id ORDER BY se.ts DESC
      ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING
     )
FROM staging.staging_events as se
WHERE se.page = 'NextSong' AND se.user_id IS NOT NULL;
 """)

song_table_insert = ("""
INSERT INTO songs(song_id, title, artist_id, year, duration)
SELECT DISTINCT ss.song_id, ss.title, ss.artist_id, ss.year, ss.duration
FROM staging.staging_songs AS ss
WHERE ss.song_id IS NOT NULL;
""")

artist_table_insert = ("""
INSERT INTO artists(artist_id, name, location, latitude, longitude)
SELECT DISTINCT ss.artist_id, ss.artist_name, ss.artist_location, ss.artist_latitude, ss.artist_longitude
FROM staging.staging_songs AS ss
WHERE ss.artist_id IS NOT NULL;
""")

time_table_insert = ("""
INSERT INTO time(start_time, hour, day, week, month, year, weekday)
SELECT DISTINCT
                sp.start_time,
                EXTRACT(hour FROM sp.start_time),
                EXTRACT(day FROM sp.start_time),
                EXTRACT(week FROM sp.start_time),
                EXTRACT(month FROM sp.start_time),
                EXTRACT(year FROM sp.start_time),
                EXTRACT(weekday FROM sp.start_time)
FROM songplays as sp;
""")


# SEARCH PATHS
staging_schema_search_path = "SET search_path TO staging;"
datamart_schema_search_path = "SET search_path TO datamart;"


# QUERY LISTS
# SCHEMAS
create_schema_queries = [staging_schema_create, datamart_schema_create]

drop_schema_queries = [staging_schema_drop, datamart_schema_drop]


# TABLES
create_table_queries = [staging_schema_search_path, staging_events_table_create, staging_songs_table_create, datamart_schema_search_path, songplay_table_create, user_table_create, song_table_create, artist_table_create, time_table_create]


# DATA INSERTIONS
copy_table_queries = [staging_schema_search_path, staging_events_copy, staging_songs_copy]

insert_table_queries = [datamart_schema_search_path, songplay_table_insert, user_table_insert, song_table_insert, artist_table_insert, time_table_insert]
