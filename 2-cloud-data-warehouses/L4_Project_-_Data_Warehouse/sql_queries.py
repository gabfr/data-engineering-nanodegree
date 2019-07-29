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
        artist VARCHAR(500),
        auth VARCHAR(20),
        firstName VARCHAR(500),
        gender CHAR(1),
        itemInSession INTEGER,
        lastName VARCHAR(500),
        length DECIMAL(12, 5),
        level VARCHAR(10),
        location VARCHAR(500),
        method VARCHAR(20),
        page VARCHAR(500),
        registration FLOAT,
        sessionId INTEGER,
        song VARCHAR(500),
        status INTEGER,
        ts BIGINT,
        userAgent VARCHAR(500),
        userId INTEGER
    );
""")

staging_songs_table_create = ("""
    CREATE TABLE staging_songs (
        num_songs INTEGER,
        artist_id VARCHAR(20),
        artist_latitude DECIMAL(12, 5),
        artist_longitude DECIMAL(12, 5),
        artist_location VARCHAR(500),
        artist_name VARCHAR(500),
        song_id VARCHAR(20),
        title VARCHAR(500),
        duration DECIMAL(15, 5),
        year INTEGER
    );
""")

songplay_table_create = ("""
    CREATE TABLE IF NOT EXISTS songplays (
        songplay_id INTEGER IDENTITY(0,1) SORTKEY,
        start_time BIGINT NOT NULL,
        user_id INTEGER NOT NULL REFERENCES users (user_id),
        level VARCHAR(10),
        song_id VARCHAR(20) REFERENCES songs (song_id),
        artist_id VARCHAR(20) REFERENCES artists (artist_id),
        session_id INTEGER NOT NULL,
        location VARCHAR(500),
        user_agent VARCHAR(500)
    )
""")

user_table_create = ("""
    CREATE TABLE IF NOT EXISTS users (
        user_id INTEGER PRIMARY KEY,
        first_name VARCHAR(500) NOT NULL,
        last_name VARCHAR(500) NOT NULL,
        gender CHAR(1),
        level VARCHAR(10) NOT NULL
    )
""")

song_table_create = ("""
    CREATE TABLE IF NOT EXISTS songs (
        song_id VARCHAR(20) PRIMARY KEY,
        title VARCHAR(500) NOT NULL SORTKEY,
        artist_id VARCHAR NOT NULL DISTKEY REFERENCES artists (artist_id),
        year INTEGER NOT NULL,
        duration DECIMAL (15, 5) NOT NULL
    )
""")

artist_table_create = ("""
    CREATE TABLE IF NOT EXISTS artists (
        artist_id VARCHAR(20) PRIMARY KEY,
        name VARCHAR(500) NOT NULL SORTKEY,
        location VARCHAR(500),
        latitude DECIMAL(12,6),
        longitude DECIMAL(12,6)
    )
""")

time_table_create = ("""
    CREATE TABLE IF NOT EXISTS time (
        start_time BIGINT NOT NULL PRIMARY KEY SORTKEY,
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
    region 'us-west-2'
    iam_role '{}'
    compupdate off 
    format as json {}
    timeformat as 'epochmillisecs'

""").format(S3_LOG_DATA, DWH_IAM_ROLE_ARN, S3_LOG_JSONPATH)

print(staging_events_copy)

staging_songs_copy = ("""

    copy staging_songs 
    from {}
    region 'us-west-2'
    iam_role '{}'
    compupdate off 
    format as json 'auto'

""").format(S3_SONG_DATA, DWH_IAM_ROLE_ARN)

print(staging_songs_copy)
# FINAL TABLES

songplay_table_insert = ("""
    INSERT INTO songplays (start_time, user_id, level, song_id, artist_id, session_id, location, user_agent)
    SELECT
        (TO_CHAR(ts :: DATE, 'yyyyMMDDHH24')::integer) AS start_time,
        userId as user_id,
        level,
        (SELECT song_id FROM songs WHERE title = staging_events.song AND artist_id = (SELECT artist_id FROM artists WHERE name = staging_events.artist)) AS song_id,
        (SELECT artist_id FROM artists WHERE name = staging_events.artist) AS artist_id,
        sessionId AS session_id,
        location,
        userAgent AS user_agent
    FROM
        staging_events
""")

user_table_insert = ("""
    INSERT INTO users (user_id, first_name, last_name, gender, level)
    SELECT
        userId AS user_id,
        firstName AS first_name,
        lastName AS last_name,
        gender,
        level
    FROM
        staging_events
    GROUP BY
        userId, firstName, lastName, gender
""")

song_table_insert = ("""
    INSERT INTO songs (song_id, title, artist_id, year, duration)
    SELECT         
        song_id,
        title,
        artist_id,
        year,
        duration
    FROM
        staging_songs
    GROUP BY song_id, title, artist_id, year, duration;
""")

artist_table_insert = ("""
    INSERT INTO artists (artist_id, name, location, latitude, longitude)
    SELECT         
        artist_id,
        artist_name AS name,
        artist_location AS location,
        artist_latitude AS latitude,
        artist_longitude AS longitude
    FROM
        staging_songs
    GROUP BY artist_id, artist_name, artist_location, artist_latitude, artist_longitude;
""")

time_table_insert = ("""
    INSERT INTO time (start_time, hour, day, week, month, year, weekday)
    SELECT
        DISTINCT(TO_CHAR(ts :: DATE, 'yyyyMMDDHH24')::integer) AS start_time,
        EXTRACT(hour FROM ts)                              AS hour,
        EXTRACT(day FROM ts)                              AS day,
        EXTRACT(week FROM ts)                              AS week,
        EXTRACT(month FROM ts)                              AS month,
        EXTRACT(year FROM ts)                              AS year,
        (CASE WHEN EXTRACT(ISODOW FROM ts) IN (6, 7) THEN false ELSE true END) AS weekday
    FROM
        staging_events
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
insert_table_queries = [artist_table_insert, song_table_insert, time_table_insert, user_table_insert, songplay_table_insert]
