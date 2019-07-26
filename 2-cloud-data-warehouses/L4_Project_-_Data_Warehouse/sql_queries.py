import configparser


# CONFIG
config = configparser.ConfigParser()
config.read('dwh.cfg')

S3_LOG_DATA = config.get('S3', 'LOG_DATA')
S3_LOG_JSONPATH = config.get('S3', 'LOG_JSONPATH')
S3_SONG_DATA = config.get('S3', 'SONG_DATA')
DWH_IAM_ROLE_ARN = config.get("IAM_ROLE", "ARN")

# DROP TABLES

staging_events_table_drop = "DROP TABLE IF EXISTS staging_events;"
staging_songs_table_drop = "DROP TABLE IF EXISTS staging_songs;"
songplay_table_drop = "DROP TABLE IF EXISTS songplays;"
user_table_drop = "DROP TABLE IF EXISTS users;"
song_table_drop = "DROP TABLE IF EXISTS songs;"
artist_table_drop = "DROP TABLE IF EXISTS artists;"
time_table_drop = "DROP TABLE IF EXISTS time;"

# CREATE TABLES

staging_events_table_create= ("""
    CREATE TABLE staging_events (
        artist VARCHAR(255),
        auth VARCHAR(20),
        firstName VARCHAR(255),
        gender CHAR(1),
        itemInSession INTEGER,
        lastName VARCHAR(255),
        length DECIMAL(12, 5),
        level VARCHAR(10),
        location VARCHAR(255),
        method VARCHAR(20),
        page VARCHAR(255),
        registration VARCHAR(255),
        sessionId INTEGER,
        song VARCHAR(255),
        status INTEGER,
        ts TIMESTAMP,
        userAgent VARCHAR(255),
        userId INTEGER
    );
""")

staging_songs_table_create = ("""
    CREATE TABLE staging_songs (
        num_songs INTEGER,
        artist_id VARCHAR(20),
        artist_latitude DECIMAL(5, 5),
        artist_longitude DECIMAL(5, 5),
        artist_location VARCHAR(255),
        artist_name VARCHAR(255),
        song_id VARCHAR(20),
        title VARCHAR(255),
        duration DECIMAL(15, 5),
        year INTEGER
    );
""")

songplay_table_create = ("""
    CREATE TABLE IF NOT EXISTS songplays (
        songplay_id INTEGER IDENTITY(0,1) SORTKEY,
        start_time INTEGER NOT NULL,
        user_id INTEGER NOT NULL REFERENCES users (user_id),
        level VARCHAR(10),
        song_id VARCHAR(20) REFERENCES songs (song_id),
        artist_id VARCHAR(20) REFERENCES artists (artist_id),
        session_id INTEGER NOT NULL,
        location VARCHAR(255),
        user_agent VARCHAR(255)
    )
""")

user_table_create = ("""
    CREATE TABLE IF NOT EXISTS users (
        user_id INTEGER PRIMARY KEY,
        first_name VARCHAR(50) NOT NULL,
        last_name VARCHAR(50) NOT NULL,
        gender CHAR(1),
        level VARCHAR(10) NOT NULL
    )
""")

song_table_create = ("""
    CREATE TABLE IF NOT EXISTS songs (
        song_id VARCHAR(20) PRIMARY KEY,
        title VARCHAR(255) NOT NULL SORTKEY,
        artist_id VARCHAR NOT NULL DISTKEY REFERENCES artists (artist_id),
        year INTEGER NOT NULL,
        duration DECIMAL (15, 5) NOT NULL
    )
""")

artist_table_create = ("""
    CREATE TABLE IF NOT EXISTS artists (
        artist_id VARCHAR(20) PRIMARY KEY,
        name VARCHAR(100) NOT NULL SORTKEY,
        location VARCHAR(255),
        latitude DECIMAL(10,5),
        longitude DECIMAL(10,5)
    )
""")

time_table_create = ("""
    CREATE TABLE IF NOT EXISTS time (
        start_time TIMESTAMP NOT NULL PRIMARY KEY SORTKEY,
        hour NUMERIC NOT NULL,
        day NUMERIC NOT NULL,
        week NUMERIC NOT NULL,
        month NUMERIC NOT NULL,
        year NUMERIC NOT NULL,
        weekday NUMERIC NOT NULL
    )
""")

# STAGING TABLES

staging_events_copy = ("""

    copy staging_events 
    from {}
    iam_role '{}'
    json {}

""").format(S3_LOG_DATA, DWH_IAM_ROLE_ARN, S3_LOG_JSONPATH)

print(staging_events_copy)

staging_songs_copy = ("""

    copy staging_songs 
    from {}
    iam_role '{}'
    json auto

""").format(S3_SONG_DATA, DWH_IAM_ROLE_ARN)

print(staging_songs_copy)
# FINAL TABLES

songplay_table_insert = ("""
""")

user_table_insert = ("""
""")

song_table_insert = ("""
""")

artist_table_insert = ("""
""")

time_table_insert = ("""
""")

# QUERY LISTS

create_table_queries = [
    staging_events_table_create,
    staging_songs_table_create,
    time_table_create,
    user_table_create,
    artist_table_create,
    song_table_create,
    songplay_table_create
]
drop_table_queries = [staging_events_table_drop, staging_songs_table_drop, songplay_table_drop, user_table_drop, song_table_drop, artist_table_drop, time_table_drop]
copy_table_queries = [staging_events_copy, staging_songs_copy]
insert_table_queries = [songplay_table_insert, user_table_insert, song_table_insert, artist_table_insert, time_table_insert]
