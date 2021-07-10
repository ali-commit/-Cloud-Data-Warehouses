import configparser


# CONFIG
config = configparser.ConfigParser()
config.read('dwh.cfg')
LOG_DATA = config.get("S3","LOG_DATA")
LOG_JSONPATH = config.get("S3", "LOG_JSONPATH")
SONG_DATA = config.get("S3", "SONG_DATA")
ROLE_ARN = config.get("IAM_ROLE","ARN")
# DROP TABLES

staging_events_table_drop = "DROP TABLE IF NOT EXISTS staging_events"
staging_songs_table_drop = "DROP TABLE IF NOT EXISTS staging_songs"
songplay_table_drop = "DROP TABLE IF NOT EXISTS songplay"
user_table_drop = "DROP TABLE IF NOT user"
song_table_drop = "DROP TABLE IF NOT EXISTS song"
artist_table_drop = "DROP TABLE IF NOT EXISTS artist"
time_table_drop = "DROP TABLE IF NOT EXISTS TIME"

# CREATE TABLES

staging_events_table_create= ("""
CREATE TABLE staging_events(
    artist VARCHAR(max) sortkey distkey,
    auth VARCHAR(max),
    firstName VARCHAR(250),
    gender VARCHAR(250),
    iteminSession INT,
    lastName VARCHAR(max),
    length NUMERIC,
    level VARCHAR(max),
    location VARCHAR(max),
    method VARCHAR(max),
    page VARCHAR(300),
    registration NUMERIC,
    sessionId VARCHAR(max),
    song VARCHAR(max),
    status INT,
    ts BIGINT,
    userAgent VARCHAR(max),
    userId VARCHAR(max)
);
""")

staging_songs_table_create = ("""
CREATE TABLE staging_songs(
    num_songs INT4 sortkey distkey,
    artist_id VARCHAR(max),
    artist_latitude NUMERIC,
    artist_longitude NUMERIC,
    artist_location VARCHAR(max),
    artist_name VARCHAR(max),
    song_id VARCHAR(max),
    title VARCHAR(max),
    duration NUMERIC,
    year INT4
);
""")


songplay_table_create =(""" CREATE TABLE IF NOT EXISTS songplay( songplay_id TEXT PRIMARY KEY  distkey, start_time TIME, user_id TEXT, level TEXT, song_id TEXT , artist_id TEXT, session_id TEXT , location TEXT , user_agent TEXT ) """)

user_table_create = ("""CREATE TABLE IF NOT EXISTS user( user_id INTEGER PRIMARY key  distkey, first_name text , last_name text , gender text , level text )""")

song_table_create = ("""CREATE TABLE IF NOT EXISTS song( song_id text PRIMARY key distkey , title text , artist_id text, YEAR INTEGER , duration text )""")

artist_table_create = ("""CREATE TABLE IF NOT EXISTS artist ( artist_id text PRIMARY key distkey, name text, location text , lattitude text, longitude text ) """)

time_table_create = ("""CREATE TABLE IF NOT EXISTS TIME ( start_time text PRIMARY key distkey, HOUR text , DAY text , week text , MONTH text, YEAR INTEGER , weekday text ) """)


staging_events_copy = ("""copy staging_events 
    from {}
    credentials 'aws_iam_role={}' 
    json {}
""").format(LOG_DATA, ROLE_ARN, LOG_JSONPATH)

staging_songs_copy = ("""
copy staging_songs 
    from {} 
    credentials 'aws_iam_role={}' 
    json 'auto';
""").format(SONG_DATA, ROLE_ARN)

###

# FINAL TABLES

songplay_table_insert = (""" 
INSERT INTO
   songplay( songplay_id, start_time , user_id, level, song_id , artist_id , session_id , location , user_agent ) 
VALUES
   (
       % s, % s, % s, % s, % s, % s, % s, % s
   )
   """)

user_table_insert = (""" 
INSERT INTO
    user( user_id, first_name , last_name , gender , level )
VALUES
    (
         %s, %s, %s, %s,% s
    )
    """)

song_table_insert = ("""
INSERT INTO
    song( song_id , title , artist_id , YEAR , duration ) 
VALUES
    (
        % s, % s, % s, % s, % s
    )
    """)
artist_table_insert = (""" 
INSERT INTO
    artist ( artist_id , name , location , lattitude , longitude ) 
VALUES
    (
        % s, % s, % s, % s, % s
    )
    """)

time_table_insert = (""" 
INSERT INTO
    TIME ( start_time , HOUR , DAY , week , MONTH , YEAR , weekday ) 
VALUES
    (
        % s, % s, % s, % s, % s, % s, % s 
    )
    """)

# QUERY LISTS

create_table_queries = [staging_events_table_create, staging_songs_table_create, songplay_table_create, user_table_create, song_table_create, artist_table_create, time_table_create]
drop_table_queries = [staging_events_table_drop, staging_songs_table_drop, songplay_table_drop, user_table_drop, song_table_drop, artist_table_drop, time_table_drop]
copy_table_queries = [staging_events_copy, staging_songs_copy]
insert_table_queries = [songplay_table_insert, user_table_insert, song_table_insert, artist_table_insert, time_table_insert]