# # -*- coding: utf-8 -*-
# """
# Created on Fri Apr  9 22:32:50 2021

# @author: ycheng
# """
# refer to Flor91
# #DROP TABLES
# songplay_table_drop = "DROP TABLE IF EXISTS songplays"
# user_table_drop = "DROP TABLE IF EXISTS users"
# song_table_drop = "DROP TABLE IF EXISTS songs"
# artist_table_drop = "DROP TABLE IF EXISTS artists"
# time_table_drop = "DROP TABLE IF EXISTS time"

# #CREATE TABLES
# # start_time date  REFERENCES time(start_time) means: 
#     #start_time in this table refer to the start_time in table time
# songplay_table_create = ("""
#     CREATE TABLE IF NOT EXISTS songplays
#     (songplay_id int PRIMARY KEY,
#      start_time date  REFERENCES time(start_time), 
#      user_id int NOT NULL REFERENCES users(user_id),
#      level text,
#      song_id text REFERENCES songs(song_id),
#      artist_id text REFERENCES artists(artist_id),
#      session_id int,
#      location text,
#      user_agent text)
# """)
                         
# user_table_create = ("""
#     CREATE TABLE IF NOT EXISTS users
#     (user_id int PRIMARY KEY,
#      first_name text NOT NULL,
#      last_name text NOT NULL,
#      gender text,
#      level text)
# """)
                     
# song_table_create = ("""
#     CREATE TABLE IF NOT EXISTS songs
#     (song_id text PRIMARY KEY,
#      title text NOT NULL,
#      artist_id text NOT NULL REFERENCES artists(artist_id),
#      year int,
#      duration float NOT NULL)
# """)

# artist_table_create = ("""
#     CREATE TABLE IF NOT EXISTS artists
#     (artist_id text PRIMARY KEY,
#      name text NOT NULL,
#      location text,
#      latitude float,
#      longitude float)
# """)

# time_table_create = ("""
#     CREATE TABLE IF NOT EXISTS time
#     (start_time date PRIMARY KEY,
#      hour int,
#      day int,
#      week int,
#      month int,
#      year int,
#      weekday text)
# """)

# #INSERT TABLE
# songplay_table_insert = ("""
#     INSERT INTO songplays
#     (songplay_id, start_time, user_id, level, song_id, artist_id, session_id, location, user_agent)
#     VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s)
#     ON CONFLICT (songplay_id) DO NOTHING;
# """)

# user_table_insert = ("""
#     INSERT INTO users
#     (user_id, first_name, last_name, gender, level)
#     VALUES (%s, %s, %s, %s, %s)
#     ON CONFLICT (user_id) DO NOTHING;
# """)

# song_table_insert = ("""
#     INSERT INTO songs
#     (song_id, title, artist_id, year, duration)
#     VALUES (%s, %s, %s, %s, %s)
#     ON CONFLICT (song_id) DO NOTHING;
# """)

# artist_table_insert = ("""
#     INSERT INTO artists
#     (artist_id, name, location, lattitude, longitude)
#     VALUES (%s, %s, %s, %s, %s)
#     ON CONFLICT (artist_id) DO NOTHING;
# """)


# time_table_insert = ("""
#     INSERT INTO time
#     (start_time, hour, day, week, month, year, weekday)
#     VALUES (%s, %s, %s, %s, %s, %s, %s)
#     ON CONFLICT (start_time) DO NOTHING;
# """)

# #FIND SONGS
# song_select = ("""
#     SELECT song_id, artists.artist_id
#     FROM songs JOIN artists ON songs.artist_id = artists.artist_id
#     WHERE songs.title = %s
#     AND artists.name = %s
#     AND songs.duration = %s
# """)

# #QUERY LISTS
# create_table_queries = [user_table_create, artist_table_create, song_table_create, time_table_create, songplay_table_create]
# drop_table_queries = [user_table_drop, artist_table_drop, song_table_drop, time_table_drop, songplay_table_drop]

################################################################################################################################
#refer to gabfr
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
        level VARCHAR,
        song_id VARCHAR REFERENCES songs (song_id),
        artist_id VARCHAR REFERENCES artists (artist_id),
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
        SET level      = EXCLUDED.level
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
    DO NOTHING
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
    DO NOTHING
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
    DO NOTHING
""")

# FIND SONGS

song_select = ("""
    SELECT 
        songs.song_id AS song_id,
        songs.artist_id AS artist_id
    FROM
        songs
        JOIN artists ON (songs.artist_id = artists.artist_id)
    WHERE
        songs.title = %s AND 
        artists.name = %s AND 
        songs.duration = %s
""")

# QUERY LISTS

create_table_queries = [time_table_create, user_table_create, artist_table_create, song_table_create, songplay_table_create]
drop_table_queries = [songplay_table_drop, user_table_drop, song_table_drop, artist_table_drop, time_table_drop]