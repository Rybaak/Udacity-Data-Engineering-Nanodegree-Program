import configparser


# CONFIG
config = configparser.ConfigParser()
config.read('dwh.cfg')

IAM_ROLE = config['IAM_ROLE']['ARN']
LOG_DATA = config['S3']['LOG_DATA']
SONG_DATA = config['S3']['SONG_DATA']
LOG_JSONPATH = config['S3']['LOG_JSONPATH']

# DROP TABLES

staging_events_table_drop = "DROP TABLE IF EXISTS staging_events;"
staging_songs_table_drop = "DROP TABLE IF EXISTS staging_songs;"
songplay_table_drop = "DROP TABLE IF EXISTS songplay;"
user_table_drop = "DROP TABLE IF EXISTS users;"
song_table_drop = "DROP TABLE IF EXISTS songs;"
artist_table_drop = "DROP TABLE IF EXISTS artist;"
time_table_drop = "DROP TABLE IF EXISTS time;"

# CREATE TABLES

staging_events_table_create= ("""
 CREATE TABLE staging_events (
            artist TEXT,
            auth TEXT,
            firstName TEXT,
            gender TEXT,
            itemInSession INTEGER,
            lastName TEXT,
            length NUMERIC,
            level TEXT,
            location TEXT,
            method TEXT,
            page TEXT,
            registration NUMERIC,
            sessionId INTEGER,
            song TEXT,
            status INTEGER,
            ts TIMESTAMP,
            userAgent TEXT,
            userId INTEGER);
""")

staging_songs_table_create = ("""
CREATE  TABLE IF NOT EXISTS staging_songs(
            num_songs INTEGER,
            artist_id TEXT,
            artist_latitude NUMERIC,
            artist_longitude NUMERIC,
            artist_location TEXT,
            artist_name TEXT,
            song_id TEXT,
            title TEXT,
            duration NUMERIC,
            year INTEGER)
""")

songplay_table_create = ("""
CREATE TABLE IF NOT EXISTS songplay(
            start_time timestamp NOT NULL PRIMARY KEY,
            user_id TEXT NOT NULL,
            level TEXT NOT NULL,
            song_id TEXT NOT NULL,
            artist_id TEXT NOT NULL,
            session_id TEXT,
            location TEXT,
            user_agent TEXT)
""")

user_table_create = ("""
CREATE TABLE IF NOT EXISTS users (
            user_id INTEGER PRIMARY KEY,
            first_name TEXT,
            last_name TEXT,
            gender TEXT,
            level TEXT)
""")

song_table_create = ("""
CREATE TABLE IF NOT EXISTS songs (
            song_id TEXT PRIMARY KEY,
            title TEXT,
            artist_id TEXT NOT NULL,
            year INTEGER,
            duration INTEGER)
""")

artist_table_create = ("""
CREATE TABLE IF NOT EXISTS artist (
            artist_id TEXT PRIMARY KEY,
            name TEXT,
            location TEXT,
            lattitude INTEGER,
            longitude INTEGER)
""")

time_table_create = ("""
CREATE TABLE IF NOT EXISTS time (
            start_time timestamp PRIMARY KEY,
            hour INTEGER,
            day INTEGER,
            week INTEGER,
            month INTEGER,
            year INTEGER,
            weekday TEXT)
""")

# STAGING TABLES

staging_events_copy = (f"""
            copy staging_events 
            from {LOG_DATA}
            iam_role {IAM_ROLE}
            json {LOG_JSONPATH}
            timeformat as 'epochmillisecs';
""")

staging_songs_copy = (f"""
            copy staging_songs 
            from {SONG_DATA}
            iam_role {IAM_ROLE}
            json 'auto';
""")

# FINAL TABLES

songplay_table_insert = ("""
INSERT INTO songplay
        (start_time, user_id, level, song_id, artist_id, session_id, location, user_agent)
SELECT 
        se.ts AS start_time,
        se.userId AS user_id,
        se.level AS level,
        ss.song_id AS song_id,
        ss.artist_id AS artist_id,
        se.sessionId AS session_id,
        se.location AS location,
        se.userAgent AS user_agent
    FROM staging_events se 
    join staging_songs ss
    on se.song = ss.title 
    WHERE se.page = 'NextSong'
""")

user_table_insert = ("""
INSERT INTO users 
        (user_id, first_name, last_name, gender, level)
SELECT DISTINCT
        se.userId AS user_id,
        se.firstName AS first_name,
        se.lastName AS last_name,
        se.gender,
        se.level
    FROM staging_events se
    WHERE page = 'NextSong'
""")

song_table_insert = ("""
INSERT INTO songs 
        (song_id, title, artist_id, year, duration)
SELECT 
        ss.song_id AS song_id,
        ss.title AS title,
        ss.artist_id AS artist_id,
        ss.year AS year,
        ss.duration AS duration
    FROM staging_songs ss
""")

artist_table_insert = ("""
INSERT INTO artist 
        (artist_id, name, location, lattitude, longitude)
SELECT 
    ss.artist_id AS artist_id,
    ss.artist_name AS name,
    ss.artist_location AS location,
    ss.artist_latitude AS lattitude,
    ss.artist_longitude AS longitude
    FROM staging_songs ss
""")

time_table_insert = ("""
INSERT INTO time 
        (start_time, hour, day, week, month, year, weekday)
SELECT 
    se.ts AS start_time,
    EXTRACT(hour FROM se.ts) AS hour,
    EXTRACT(day FROM se.ts) AS day,
    EXTRACT(week FROM se.ts) AS week,
    EXTRACT(month FROM se.ts) AS month,
    EXTRACT(year FROM se.ts) AS year,
    EXTRACT(weekday FROM se.ts) AS weekday
    FROM staging_events se
""")

# QUERY LISTS

create_table_queries = [staging_events_table_create, staging_songs_table_create, songplay_table_create, user_table_create, song_table_create, artist_table_create, time_table_create]
drop_table_queries = [staging_events_table_drop, staging_songs_table_drop, songplay_table_drop, user_table_drop, song_table_drop, artist_table_drop, time_table_drop]
copy_table_queries = [staging_events_copy, staging_songs_copy]
insert_table_queries = [songplay_table_insert, user_table_insert, song_table_insert, artist_table_insert, time_table_insert]
