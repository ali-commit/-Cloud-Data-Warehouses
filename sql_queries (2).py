import configparser


# CONFIG
config = configparser.ConfigParser()
config.read('dwh.cfg')
LOG_DATA = config.get("S3","LOG_DATA")
LOG_JSONPATH = config.get("S3", "LOG_JSONPATH")
SONG_DATA = config.get("S3", "SONG_DATA")
ROLE_ARN = config.get("IAM_ROLE","ARN")
# DROP TABLES

staging_events_table_drop = "DROP TABLE IF  EXISTS staging_events"
staging_songs_table_drop = "DROP TABLE IF  EXISTS staging_songs"
songplay_table_drop = "DROP TABLE IF  EXISTS songplay"
user_table_drop = "DROP TABLE IF EXISTS user"
song_table_drop = "DROP TABLE IF EXISTS song"
artist_table_drop = "DROP TABLE IF  EXISTS artist"
time_table_drop = "DROP TABLE IF  EXISTS TIME"

# CREATE TABLES
# here is dump table staging_events
staging_events_table_create= ("""
CREATE TABLE staging_events(
    artist VARCHAR(max) ,
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
# here is tump table staging_songs
staging_songs_table_create = ("""
CREATE TABLE staging_songs(
    num_songs INT4 ,
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


songplay_table_create =(""" CREATE TABLE IF NOT EXISTS songplay( songplay_id TEXT PRIMARY KEY  distkey, start_time TIME, user_id TEXT, level TEXT, song_id TEXT , artist_id TEXT, session_id TEXT , location TEXT , user_agent TEXT )
sortkey(songplay_id, start_time); """)


user_table_create = ("""CREATE TABLE IF NOT EXISTS user( user_id INTEGER PRIMARY key not null  distkey , first_name text , last_name text , gender text , level text )sortkey(user_id, level)""")
song_table_create = ("""CREATE TABLE IF NOT EXISTS song( song_id text PRIMARY key not null distkey , title text , artist_id text, YEAR INTEGER , duration text )sortkey(song_id, artist_id)""")

artist_table_create = ("""CREATE TABLE IF NOT EXISTS artist ( artist_id text PRIMARY key not null distkey , name text, location text , lattitude text, longitude text )sortkey(artist_id, name) """)

time_table_create = ("""CREATE TABLE IF NOT EXISTS TIME ( start_time text PRIMARY key not null  distkey , HOUR text , DAY text , week text , MONTH text, YEAR INTEGER , weekday text )sortkey(start_time, weekday) """)


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
    INSERT INTO songplays (start_time, user_id, level, song_id, artist_id, session_id, location, user_agent)
    SELECT events.start_time, events.user_id, events.level, songs.song_id, songs.artist_id, events.session_id,    events.location, events.useragent
    FROM (SELECT TIMESTAMP 'epoch' + ts/1000 * interval '1 second' AS start_time, *
          FROM staging_events
          WHERE page='NextSong') events
    LEFT JOIN staging_songs songs
    ON events.song = songs.title
    AND events.artist = songs.artist_name
    AND events.length = songs.duration
""")

user_table_insert = (""" 
INSERT INTO
    user( user_id, first_name , last_name , gender , level )
    SELECT DISTINCT userId, firstName , lastName , gender , level
    from staging_events
    where page='NextSong'
""")

song_table_insert = ("""
INSERT INTO
    song( song_id , title , artist_id , YEAR , duration ) 
    SELECT DISTINCT ( song_id , title , artist_id , year , duration)
    from staging_songs
    

    """)
artist_table_insert = (""" 
INSERT INTO
    artist ( artist_id , name , location , lattitude , longitude ) 
    SELECT DISTINCT artist_id , artist_name , artist_location , artist_latitude ,   artist_longitude 
    from staging_songs
                 
    """)

time_table_insert = (""" 
INSERT INTO
    TIME ( start_time , HOUR , DAY , week , MONTH , YEAR , weekday ) 
    SELECT DISTINCT ts,EXTRACT(HOUR ts),EXTRACT(DAY ts),EXTRACT(week ts),EXTRACT(MONTH ts),EXTRACT(YEAR ts),
    EXTRACT(dow ts)
    from staging_events
    where page='NextSong'
    """)
#SELECT EXTRACT(YEAR FROM start_time);

# QUERY LISTS

create_table_queries = [staging_events_table_create, staging_songs_table_create, songplay_table_create, user_table_create, song_table_create, artist_table_create, time_table_create]
drop_table_queries = [staging_events_table_drop, staging_songs_table_drop, songplay_table_drop, user_table_drop, song_table_drop, artist_table_drop, time_table_drop]
copy_table_queries = [staging_events_copy, staging_songs_copy]
insert_table_queries = [songplay_table_insert, user_table_insert, song_table_insert, artist_table_insert, time_table_insert]
