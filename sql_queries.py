import configparser

# CONFIG
config = configparser.ConfigParser()
config.read('dwh.cfg')

# DROP SCHEMA

staging_schema_drop = "DROP SCHEMA IF EXISTS STAGING CASCADE"
main_schema_drop = "DROP SCHEMA IF EXISTS MAIN CASCADE"

# DROP TABLES

staging_events_table_drop = "DROP TABLE IF EXISTS STAGING.EVENTS"
staging_songs_table_drop = "DROP TABLE IF EXISTS STAGING.SONGS"

songplay_table_drop = "DROP TABLE IF EXISTS MAIN.SONGPLAYS"
user_table_drop = "DROP TABLE IF EXISTS MAIN.USERS"
song_table_drop = "DROP TABLE IF EXISTS MAIN.SONGS"
artist_table_drop = "DROP TABLE IF EXISTS MAIN.ARTISTS"
time_table_drop = "DROP TABLE IF EXISTS MAIN.TIME"

# CREATE SCHEMA

staging_schema_create = ("""CREATE SCHEMA IF NOT EXISTS STAGING""")
main_schema_create = ("""CREATE SCHEMA IF NOT EXISTS MAIN""")

# CREATE TABLES

staging_events_table_create= ("""CREATE TABLE STAGING.EVENTS(
  artist VARCHAR(255)
, auth VARCHAR(255)
, firstName VARCHAR(255)
, gender CHAR
, itemInSession BIGINT
, lastName VARCHAR(255)
, length DOUBLE PRECISION
, level VARCHAR(255)
, location VARCHAR(255)
, method VARCHAR(255)
, page VARCHAR(255)
, registration DOUBLE PRECISION
, sessionId BIGINT
, song VARCHAR(255)
, status BIGINT
, ts BIGINT
, userAgent VARCHAR(255)
, userId BIGINT
);
""")

staging_songs_table_create = ("""CREATE TABLE IF NOT EXISTS STAGING.SONGS (
  artist_id VARCHAR(255)
, artist_latitude FLOAT
, artist_longitude FLOAT
, artist_location VARCHAR(255)
, artist_name VARCHAR(255)
, num_songs BIGINT
, song_id VARCHAR(255)
, title VARCHAR(255)
, duration FLOAT
, year INT4
);
""")

songplay_table_create = (""" CREATE TABLE MAIN.SONGPLAYS (
  songplay_id BIGINT IDENTITY(0,1) PRIMARY KEY
, start_time TIMESTAMP NOT NULL
, user_id VARCHAR(255) NOT NULL
, level VARCHAR(255) 
, song_id VARCHAR(255) NOT NULL
, artist_id VARCHAR(255) NOT NULL
, session_id VARCHAR(255)
, location VARCHAR(255)
, user_agent VARCHAR(255)
, FOREIGN KEY (start_time) REFERENCES MAIN.TIME (start_time)
, FOREIGN KEY (user_id) REFERENCES MAIN.USERS (user_id)
, FOREIGN KEY (song_id) REFERENCES MAIN.SONGS (song_id)
, FOREIGN KEY (artist_id) REFERENCES MAIN.ARTISTS (artist_id)
);
""")

user_table_create = (""" CREATE TABLE MAIN.USERS (
  user_id VARCHAR(255) NOT NULL PRIMARY KEY
, first_name VARCHAR(255)
, last_name VARCHAR(255)
, gender VARCHAR(50)
, level VARCHAR(50)
);
""")

song_table_create = (""" CREATE TABLE MAIN.SONGS (
  song_id VARCHAR(255) NOT NULL PRIMARY KEY
, title VARCHAR(255)
, artist_id VARCHAR(255) NOT NULL 
, year INT
, duration INT
);
""")

artist_table_create = (""" CREATE TABLE MAIN.ARTISTS (
  artist_id VARCHAR(255) NOT NULL PRIMARY KEY
, name VARCHAR(255)
, location VARCHAR(255)
, latitude FLOAT(8)
, longitude FLOAT(8)
);
""")

time_table_create = ("""CREATE TABLE MAIN.TIME (
  start_time TIMESTAMP NOT NULL PRIMARY KEY
, hour INT NOT NULL
, day INT NOT NULL
, year_week INT NOT NULL
, month INT NOT NULL
, year INT NOT NULL
, week_day INT NOT NULL
);
""")

# STAGING TABLES

staging_events_copy = ("""COPY STAGING.EVENTS FROM {}
credentials {}
compupdate off 
region 'us-west-2'
json {};
""").format(config.get('S3', 'LOG_DATA'), config.get('IAM_ROLE', 'ARN'), config.get('S3', 'LOG_JSONPATH'))

staging_songs_copy = (""" COPY STAGING.SONGS FROM {}
credentials {}
compupdate off 
region 'us-west-2'
json 'auto';
""").format(config.get('S3', 'SONG_DATA'), config.get('IAM_ROLE', 'ARN'))

# FINAL TABLES

songplay_table_insert = (""" INSERT INTO MAIN.SONGPLAYS
(ARTIST_ID, LEVEL, LOCATION, SESSION_ID, SONG_ID, START_TIME, USER_AGENT, USER_ID)
SELECT DISTINCT
  S.ARTIST_ID
, E.LEVEL
, S.ARTIST_LOCATION
, E.SESSIONID, S.SONG_ID
, TIMESTAMP 'epoch' + E.ts/1000 * interval '1 second' AS START_TIME
, E.USERAGENT
, E.USERID
FROM STAGING.EVENTS E
JOIN STAGING.SONGS S
ON E.song = S.title
AND E.artist = S.artist_name
WHERE E.PAGE = 'NextSong'
""")

user_table_insert = (""" INSERT INTO MAIN.USERS
(FIRST_NAME, GENDER, LAST_NAME, LEVEL, USER_ID)
SELECT DISTINCT FIRSTNAME, GENDER, LASTNAME, LEVEL, USERID
FROM STAGING.EVENTS 
WHERE PAGE = 'NextSong'
""")

song_table_insert = (""" INSERT INTO MAIN.SONGS
(  ARTIST_ID, DURATION, SONG_ID, TITLE, YEAR)
SELECT DISTINCT ARTIST_ID, DURATION, SONG_ID, TITLE, YEAR FROM STAGING.SONGS
""")

artist_table_insert = (""" INSERT INTO MAIN.ARTISTS
(ARTIST_ID, NAME, LOCATION, LATITUDE, LONGITUDE)
SELECT DISTINCT ARTIST_ID, ARTIST_NAME, ARTIST_LOCATION, ARTIST_LATITUDE::DOUBLE PRECISION, ARTIST_LONGITUDE::DOUBLE PRECISION FROM STAGING.SONGS
""")

time_table_insert = (""" INSERT INTO MAIN.TIME
(START_TIME, HOUR, DAY, YEAR_WEEK, MONTH, YEAR, WEEK_DAY)
SELECT DISTINCT
  START_TIME AS START_TIME 
, date_part('HOUR', START_TIME)::INT AS HOUR
, date_part('DAY', START_TIME)::INT AS DAY
, date_part('WEEK', START_TIME)::INT AS YEAR_WEEK
, date_part('MONTH', START_TIME)::INT AS MONTH
, date_part('YEAR', START_TIME)::INT AS YEAR
, date_part('WEEKDAY', START_TIME)::INT AS WEEK_DAY
FROM MAIN.SONGPLAYS;
""")

# QUERY LISTS

create_schema_queries = [staging_schema_create, main_schema_create]
create_table_queries = [staging_events_table_create, staging_songs_table_create, user_table_create, song_table_create, artist_table_create, time_table_create, songplay_table_create]
drop_table_queries = [staging_events_table_drop, staging_songs_table_drop, songplay_table_drop, user_table_drop, song_table_drop, artist_table_drop, time_table_drop]
drop_schema_queries = [staging_schema_drop, main_schema_drop]
copy_table_queries = [staging_events_copy, staging_songs_copy]
insert_table_queries = [songplay_table_insert, user_table_insert, song_table_insert, artist_table_insert, time_table_insert]
