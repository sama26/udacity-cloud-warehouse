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
                               "firstName varchar, "
                               "gender varchar, "
                               "itemInSession integer, "
                               "lastName varchar, "
                               "length real, "
                               "level varchar,"
                               "location varchar,"
                               "method varchar,"
                               "page varchar,"
                               "registration real,"
                               "sessionId integer,"
                               "song varchar,"
                               "status integer,"
                               "ts bigint,"
                               "userAgent varchar,"
                               "userId integer"
                               ");"
                               )

staging_songs_table_create = ("CREATE TABLE IF NOT EXISTS staging_songs"
                              "("
                              "num_songs integer, "
                              "artist_id varchar, "
                              "artist_latitude real, "
                              "artist_longitude real, "
                              "artist_location varchar, "
                              "artist_name varchar , "
                              "song_id varchar, "
                              "title varchar , "
                              "duration real, "
                              "year real"
                              ");"
                              )

songplay_table_create = ("CREATE TABLE IF NOT EXISTS songplays"
                         "("
                         "songplay_id integer identity(1,1) PRIMARY KEY, "
                         "start_time timestamp, "
                         "user_id integer, "
                         "level varchar, "
                         "song_id varchar, "
                         "artist_id varchar, "
                         "session_id integer, "
                         "location varchar, "
                         "user_agent varchar"
                         ")"
                         "SORTKEY(start_time);"
                         )

user_table_create = ("CREATE TABLE IF NOT EXISTS users"
                     "("
                     "user_id integer PRIMARY KEY, "
                     "first_name varchar, "
                     "last_name varchar, "
                     "gender char(1), "
                     "level varchar"
                     ")"
                     "SORTKEY(user_id);"
                     )

song_table_create = ("CREATE TABLE IF NOT EXISTS songs"
                     "("
                     "song_id varchar PRIMARY KEY, "
                     "title varchar, "
                     "artist_id varchar, "
                     "year integer, "
                     "duration real"
                     ")"
                     "SORTKEY(song_id);"
                     )

artist_table_create = ("CREATE TABLE IF NOT EXISTS artists"
                       "("
                       "artist_id varchar PRIMARY KEY, "
                       "name varchar, "
                       "location varchar, "
                       "latitude real, "
                       "longitude real"
                       ")"
                       "SORTKEY(artist_id);"
                       )

time_table_create = ("CREATE TABLE IF NOT EXISTS time"
                     "("
                     "start_time timestamp PRIMARY KEY, "
                     "hour integer, "
                     "day integer, "
                     "week integer, "
                     "month integer, "
                     "year integer, "
                     "weekday VARCHAR"
                     ")"
                     "SORTKEY(start_time);"
                     )

# STAGING TABLES

staging_events_copy = ("""
COPY staging_events
FROM {}
iam_role {}
TRUNCATECOLUMNS
FORMAT AS json {};
""").format(config['S3']['LOG_DATA'], config['IAM_ROLE']['ARN'], config['S3']['LOG_JSONPATH'])

staging_songs_copy = ("""
COPY staging_songs
FROM {}
iam_role {}
TRUNCATECOLUMNS
FORMAT AS json 'auto';
""").format(config['S3']['SONG_DATA'], config['IAM_ROLE']['ARN'])

# FINAL TABLES

songplay_table_insert = ("""
INSERT INTO songplays (START_TIME, USER_ID, LEVEL, SONG_ID, ARTIST_ID, SESSION_ID, LOCATION, USER_AGENT)
SELECT 
       TIMESTAMP 'epoch' + (e.ts / 1000) * INTERVAL '1 second' as start_time,
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
WHERE e.page = 'NextSong';
""")


user_table_insert = ("""
INSERT INTO users (user_id, first_name, last_name, gender, level)
SELECT
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
SELECT
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
SELECT
       TIMESTAMP 'epoch' + (ts/1000) * INTERVAL '1 second' as start_time,
       EXTRACT(HOUR FROM start_time) AS hour,
       EXTRACT(DAY FROM start_time) AS day,
       EXTRACT(WEEKS FROM start_time) AS week,
       EXTRACT(MONTH FROM start_time) AS month,
       EXTRACT(YEAR FROM start_time) AS year,
       TO_CHAR(start_time, 'Day') AS weekday
FROM staging_events;
""")


# QUERY LISTS

create_table_queries = [staging_events_table_create, staging_songs_table_create, songplay_table_create, user_table_create, song_table_create, artist_table_create, time_table_create]
drop_table_queries = [staging_events_table_drop, staging_songs_table_drop, songplay_table_drop, user_table_drop, song_table_drop, artist_table_drop, time_table_drop]
copy_table_queries = [staging_events_copy, staging_songs_copy]
insert_table_queries = [songplay_table_insert, user_table_insert, song_table_insert, artist_table_insert, time_table_insert]
