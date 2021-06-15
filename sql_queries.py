import configparser


# CONFIG
config = configparser.ConfigParser()
config.read('dwh.cfg')

# DROP TABLES

staging_events_table_drop = "DROP table IF EXISTS staging_events"
staging_songs_table_drop = "DROP table IF EXISTS staging_songs"
songplay_table_drop = "DROP table IF EXISTS songplays"
user_table_drop = "DROP table IF EXISTS users"
song_table_drop = "DROP table IF EXISTS songs"
artist_table_drop = "DROP table IF EXISTS artists"
time_table_drop = "DROP table IF EXISTS time"

# CREATE TABLES

staging_events_table_create = ("CREATE TABLE IF NOT EXISTS staging_events"
                               "("
                               "artist varchar , "
                               "auth varchar, "
                               "first_name varchar, "
                               "gender char(1), "
                               "item_in_session smallint, "
                               "last_name varchar, "
                               "length real, "
                               "level char(4),"
                               "location varchar,"
                               "method char(3),"
                               "page varchar,"
                               "registration double precision,"
                               "session_id integer,"
                               "song varchar,"
                               "status integer,"
                               "ts bigint,"
                               "user_agent varchar,"
                               "userid smallint"
                               ");"
                               )

staging_songs_table_create = ("CREATE TABLE IF NOT EXISTS staging_songs"
                              "("
                              "num_songs smallint, "
                              "artist_id char(18), "
                              "artist_latitude float, "
                              "artist_longitude float, "
                              "artist_location varchar, "
                              "artist_name varchar, "
                              "song_id char(18), "
                              "title varchar, "
                              "duration real, "
                              "year integer"
                              ");"
                              )

songplay_table_create = ("CREATE TABLE IF NOT EXISTS songplays"
                         "("
                         "songplay_id identity(0,1) PRIMARY KEY, "
                         "start_time bigint NOT NULL, "
                         "user_id smallint NOT NULL, "
                         "level char(4), "
                         "song_id char(18), "
                         "artist_id char(18), "
                         "session_id integer, "
                         "location varchar, "
                         "user_agent varchar"
                         ")"
                         "DISTSTYLE KEY"
                         "DISTKEY (start_time)"
                         "SORTKEY (start_time);"
                         )

user_table_create = ("CREATE TABLE IF NOT EXISTS users"
                     "("
                     "user_id smallint PRIMARY KEY, "
                     "first_name varchar, "
                     "last_name varchar, "
                     "gender char(1), "
                     "level char(4)"
                     ")"
                     "SORTKEY (song_id);"
                     )

song_table_create = ("CREATE TABLE IF NOT EXISTS songs"
                     "("
                     "song_id char(18) PRIMARY KEY, "
                     "title varchar, "
                     "artist_id char(18) NOT NULL, "
                     "year integer, "
                     "duration real"
                     ")"
                     "SORTKEY (song_id);"
                     )

artist_table_create = ("CREATE TABLE IF NOT EXISTS artists"
                       "("
                       "artist_id char(18) PRIMARY KEY, "
                       "name varchar, "
                       "location varchar, "
                       "latitude float, "
                       "longitude float"
                       ")"
                       "SORTKEY (artist_id);"
                       )

time_table_create = ("CREATE TABLE IF NOT EXISTS time"
                     "("
                     "start_time time PRIMARY KEY, "
                     "hour smallint, "
                     "day smallint, "
                     "week smallint, "
                     "month smallint, "
                     "year smallint, "
                     "weekday smallint"
                     ")"
                     "DISTSTYLE KEY"
                     "DISTKEY (start_time)"
                     "SORTKEY (start_time);"
                     )

# STAGING TABLES

staging_events_copy = ("""
COPY staging_events
FROM {}
iam_role {}
FORMAT AS json {};
""").format(config['S3']['LOG_DATA'], config['IAM_ROLE']['ARN'], config['S3']['LOG_JSONPATH'])

staging_songs_copy = ("""
COPY staging_songs
FROM {}
iam_role {}
FORMAT AS json 'auto';
""").format(config['S3']['SONG_DATA'], config['IAM_ROLE']['ARN'])

# FINAL TABLES

songplay_table_insert = ("""
INSERT INTO songplays (start_time, user_id, level, song_id, artist_id, session_id, location, user_agent)
SELECT DISTINCT
    TIMESTAMP 'epoch' + (se.ts / 1000) * INTERVAL '1 second' as start_time,
    e.userId,
    e.level,
    s.song_id,
    s.artist_id,
    e.sessionId,
    e.location,
    e.userAgent
FROM staging_songs s
INNER JOIN staging_events e
ON (s.title = e.song AND e.artist = s.artist_name)
AND e.page = 'NextSong';
""")


user_table_insert = ("""
INSERT INTO users (user_id, first_name, last_name, gender, level)
SELECT DISTINCT 
    userId, 
    firstName, 
    lastName, 
    gender, 
    level
FROM staging_events
WHERE userId IS NOT NULL
AND page = 'NextSong';
""")

song_table_insert = ("""
INSERT INTO songs (song_id, title, artist_id, year, duration)
SELECT DISTINCT 
    song_id, 
    title, 
    artist_id, 
    year, 
    duration
FROM staging_songs
WHERE song_id IS NOT NULL;
""")

artist_table_insert = ("""
INSERT INTO artists (artist_id, name, location, latitude, longitude)
SELECT DISTINCT 
    artist_id, 
    artist_name, 
    artist_location, 
    artist_latitude, 
    artist_longitude
FROM staging_songs;
""")


time_table_insert = ("""
INSERT INTO time (start_time, hour, day, week, month, year, weekday)
SELECT DISTINCT
       TIMESTAMP 'epoch' + (ts/1000) * INTERVAL '1 second' as start_time,
       EXTRACT(HOUR FROM start_time) AS hour,
       EXTRACT(DAY FROM start_time) AS day,
       EXTRACT(WEEKS FROM start_time) AS week,
       EXTRACT(MONTH FROM start_time) AS month,
       EXTRACT(YEAR FROM start_time) AS year,
       EXTRACT(DAY FROM start_time) AS weekday
FROM staging_events;
""")

# QUERY LISTS

create_table_queries = [staging_events_table_create, staging_songs_table_create, songplay_table_create, user_table_create, song_table_create, artist_table_create, time_table_create]
drop_table_queries = [staging_events_table_drop, staging_songs_table_drop, songplay_table_drop, user_table_drop, song_table_drop, artist_table_drop, time_table_drop]
copy_table_queries = [staging_events_copy, staging_songs_copy]
insert_table_queries = [songplay_table_insert, user_table_insert, song_table_insert, artist_table_insert, time_table_insert]
