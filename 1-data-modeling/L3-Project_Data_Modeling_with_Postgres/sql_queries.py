# DROP TABLES

songplay_table_drop = "DROP TABLE IF EXISTS songplays"
user_table_drop = "DROP TABLE IF EXISTS users"
song_table_drop = "DROP TABLE IF EXISTS songs"
artist_table_drop = "DROP TABLE IF EXISTS artists"
time_table_drop = "DROP TABLE IF EXISTS time"

# CREATE TABLES

songplay_table_create = ("""
    CREATE TABLE IF NOT EXISTS songplays (
        songplay_id INTEGER ,
        start_time TIMESTAMP NOT NULL,
        user_id INTEGER NOT NULL REFERENCES users (user_id),
        level NUMERIC (15, 5),
        song_id VARCHAR NOT NULL REFERENCES songs (song_id),
        artist_id VARCHAR NOT NULL REFERENCES artists (artist_id),
        session_id INTEGER NOT NULL,
        location VARCHAR,
        user_agent VARCHAR
    )
""")

user_table_create = ("""
    CREATE TABLE IF NOT EXISTS users (
        user_id INTEGER PRIMARY KEY,
        first_name VARCHAR NOT NULL,
        last_name VARCHAR NOT NULL,
        gender CHAR(1),
        level VARCHAR NOT NULL
    )
""")

song_table_create = ("""
    CREATE TABLE IF NOT EXISTS songs (
        song_id VARCHAR PRIMARY KEY,
        title VARCHAR NOT NULL,
        artist_id VARCHAR NOT NULL REFERENCES artists (artist_id),
        year INTEGER NOT NULL,
        duration NUMERIC (15, 5) NOT NULL
    )
""")

artist_table_create = ("""
    CREATE TABLE IF NOT EXISTS artists (
        artist_id VARCHAR PRIMARY KEY,
        name VARCHAR NOT NULL,
        location VARCHAR,
        latitude NUMERIC,
        longitude NUMERIC
    )
""")

time_table_create = ("""
    CREATE TABLE IF NOT EXISTS time (
        start_time TIMESTAMP NOT NULL PRIMARY KEY,
        hour NUMERIC NOT NULL,
        day NUMERIC NOT NULL,
        week NUMERIC NOT NULL,
        month NUMERIC NOT NULL,
        year NUMERIC NOT NULL,
        weekday NUMERIC NOT NULL
    )
""")

# INSERT RECORDS

songplay_table_insert = ("""
    INSERT INTO songplays (
        songplay_id,
        start_time,
        user_id,
        level,
        song_id,
        artist_id,
        session_id,
        location,
        user_agent 
    )
    VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s)
""")

user_table_insert = ("""
    
    INSERT INTO users (
        user_id,
        first_name,
        last_name,
        gender,
        level
    )
    VALUES (%s, %s, %s, %s, %s)
    ON CONFLICT (user_id)
    DO UPDATE
        SET first_name = EXCLUDED.first_name,
            last_name  = EXCLUDED.last_name,
            gender     = EXCLUDED.gender,
            level      = EXCLUDED.level
""")

song_table_insert = ("""
    INSERT INTO songs (
        song_id,
        title,
        artist_id,
        year,
        duration
    )
    VALUES (%s, %s, %s, %s, %s)
    ON CONFLICT (song_id)
    DO UPDATE
        SET title     = EXCLUDED.title,
            artist_id = EXCLUDED.artist_id,
            year      = EXCLUDED.year,
            duration  = EXCLUDED.duration
""")

artist_table_insert = ("""
    INSERT INTO artists (
        artist_id,
        name,
        location,
        latitude,
        longitude
    )
    VALUES (%s, %s, %s, %s, %s)
    ON CONFLICT (artist_id)
    DO UPDATE
        SET artist_id = EXCLUDED.artist_id,
            name      = EXCLUDED.name,
            location  = EXCLUDED.location,
            latitude  = EXCLUDED.latitude,
            longitude = EXCLUDED.longitude
""")


time_table_insert = ("""
    INSERT INTO time (
        start_time,
        hour,
        day,
        week,
        month,
        year,
        weekday
    )
    VALUES (%s, %s, %s, %s, %s, %s, %s)
    ON CONFLICT (start_time)
    DO UPDATE
        SET hour    = EXCLUDED.hour,
            day     = EXCLUDED.day,
            week    = EXCLUDED.week,
            month   = EXCLUDED.month,
            year    = EXCLUDED.year,
            weekday = EXCLUDED.weekday
""")

# FIND SONGS

song_select = ("""
""")

# QUERY LISTS

create_table_queries = [time_table_create, user_table_create, artist_table_create, song_table_create, songplay_table_create]
drop_table_queries = [songplay_table_drop, user_table_drop, song_table_drop, artist_table_drop, time_table_drop]