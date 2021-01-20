import configparser


# CONFIG
config = configparser.ConfigParser()
config.read('dwh.cfg')

IAM_ROLE = config['IAM_ROLE']['ARN']
LOG_DATA = config['S3']['LOG_DATA']
LOG_JSONPATH = config['S3']['LOG_JSONPATH']
SONG_DATA = config['S3']['SONG_DATA']

# DROP TABLES

staging_events_table_drop = "DROP TABLE IF EXISTS staging_events;"
staging_songs_table_drop = "DROP TABLE IF EXISTS staging_songs"
songplay_table_drop = "DROP TABLE IF EXISTS song_play;"
user_table_drop = "DROP TABLE IF EXISTS users;"
song_table_drop = "DROP TABLE IF EXISTS songs;"
artist_table_drop = "DROP TABLE IF EXISTS artists;"
time_table_drop = "DROP TABLE IF EXISTS time;"

# CREATE TABLES

staging_songs_table_create= ("""
CREATE TABLE staging_songs
(
  num_songs          INTEGER    NULL,
  artist_id          VARCHAR    NULL,
  artist_latitude    NUMERIC    NULL,
  artist_longitude   NUMERIC    NULL,
  artist_location    VARCHAR    NULL,
  artist_name        VARCHAR    NULL,
  song_id            VARCHAR    NULL,
  title              VARCHAR    NULL,
  duration           NUMERIC    NULL, 
  year               INTEGER    NULL
);
""")

staging_events_table_create = ("""
CREATE TABLE staging_events
(
  event_id       BIGINT IDENTITY(0, 1) NULL,
  artist         VARCHAR               NULL,
  auth           VARCHAR               NULL,
  firstName      VARCHAR               NULL,
  gender         VARCHAR               NULL,
  itemInSession  INTEGER               NULL,
  lastName       VARCHAR               NULL,
  length         DECIMAL               NULL,
  level          VARCHAR               NULL,
  location       VARCHAR               NULL, 
  method         VARCHAR               NULL,
  page           VARCHAR               NULL,
  registration   BIGINT                NULL,
  sessionId      INTEGER               NULL,
  song           VARCHAR               NULL,
  status         INTEGER               NULL,
  ts             BIGINT                NULL,
  userAgent      VARCHAR               NULL,
  userId         INTEGER               NULL
);
""")

songplay_table_create = ("""
CREATE TABLE song_play
(
  songplay_id    BIGINT IDENTITY(0,1) NOT NULL,
  start_time     TIMESTAMP        NOT NULL,
  user_id        INTEGER          NOT NULL,
  level          VARCHAR          NULL,
  song_id        VARCHAR          NOT NULL,
  artist_id      VARCHAR          NOT NULL,
  session_id     INTEGER          NOT NULL,
  location       VARCHAR          NULL,
  user_agent     VARCHAR          NOT NULL,
  PRIMARY KEY(songplay_id)
)
DISTKEY(start_time)
COMPOUND SORTKEY(location, level);
""")

user_table_create = ("""
CREATE TABLE users
(
  user_id       INTEGER    NOT NULL, 
  first_name    VARCHAR    NOT NULL, 
  last_name     VARCHAR    NULL, 
  gender        VARCHAR    NULL, 
  level         VARCHAR    NOT NULL,
  PRIMARY KEY(user_id)
)
DISTKEY(user_id)
COMPOUND SORTKEY(level, gender);
""")

song_table_create = ("""
CREATE TABLE songs
(
  song_id      VARCHAR    NOT NULL, 
  title        VARCHAR    NOT NULL, 
  artist_id    VARCHAR    NOT NULL, 
  year         INTEGER    NOT NULL, 
  duration     FLOAT    NOT NULL,
  PRIMARY KEY(song_id)
)
DISTKEY(song_id)
COMPOUND SORTKEY(year, duration);
""")

artist_table_create = ("""
CREATE TABLE artists
(
  artist_id    VARCHAR    NOT NULL, 
  name         VARCHAR    NOT NULL, 
  location     VARCHAR    NULL, 
  latitude    FLOAT    NULL, 
  longitude    FLOAT    NULL,
  PRIMARY KEY(artist_id)
)
DISTKEY(artist_id)
COMPOUND SORTKEY(location, name);
""")

time_table_create = ("""
CREATE TABLE time
(
  start_time    TIMESTAMP    NOT NULL, 
  hour          DECIMAL      NOT NULL, 
  day           INTEGER      NOT NULL, 
  week          INTEGER      NOT NULL, 
  month         INTEGER      NOT NULL, 
  year          INTEGER      NOT NULL, 
  weekday       INTEGER      NOT NULL
);
""")

# STAGING TABLES

staging_events_copy = ("""
    COPY staging_events 
    FROM {}
    credentials 'aws_iam_role={}'
    format as json {}
    STATUPDATE ON
    region 'us-west-2';
""").format(LOG_DATA, IAM_ROLE, LOG_JSONPATH)

staging_songs_copy = ("""
    COPY staging_songs 
    FROM {}
    credentials 'aws_iam_role={}'
    json 'auto'
    region 'us-west-2';
""").format(SONG_DATA, IAM_ROLE)

# FINAL TABLES

songplay_table_insert = ("""
INSERT INTO song_play (start_time, user_id, level, song_id, artist_id, session_id, location, user_agent)
    SELECT DISTINCT TIMESTAMP 'epoch' + E.ts/1000 * INTERVAL '1 second' as start_time,
           E.userId,
           E.level,
           S.song_id,
           S.artist_id,
           E.sessionId,
           E.location,
           E.userAgent
    FROM staging_events E
    JOIN staging_songs S ON (E.artist = S.artist_name AND E.song = S.title)
    WHERE E.page = 'NextSong';
""")

user_table_insert = ("""
INSERT INTO users (user_id, first_name, last_name, gender, level)
    SELECT DISTINCT userId,
                    firstName,
                    lastName,
                    gender,
                    level
    FROM staging_events
    WHERE page = 'NextSong';
""")

song_table_insert = ("""
INSERT INTO songs (song_id, title, artist_id, year, duration)
    SELECT DISTINCT song_id,
                    title,
                    artist_id,
                    year,
                    duration
    FROM staging_songs;
""")

artist_table_insert = ("""
INSERT INTO artists (artist_id, name, location, latitude, longitude)
    SELECT DISTINCT artist_id, 
                    artist_name,
                    artist_location,
                    artist_latitude,
                    artist_longitude
    FROM staging_songs;
""")

time_table_insert = ("""
INSERT INTO time (start_time, hour, day, week, month, year, weekday)
    SELECT DISTINCT t.start_time,
                    EXTRACT(hour FROM t.start_time),
                    EXTRACT(day FROM t.start_time),
                    EXTRACT(week FROM t.start_time),
                    EXTRACT(month FROM t.start_time),
                    EXTRACT(year FROM t.start_time),
                    EXTRACT(weekday FROM t.start_time)
    FROM song_play t;
""")

# QUERY LISTS

create_table_queries = [staging_events_table_create, staging_songs_table_create, songplay_table_create, user_table_create, song_table_create, artist_table_create, time_table_create]
drop_table_queries = [staging_events_table_drop, staging_songs_table_drop, songplay_table_drop, user_table_drop, song_table_drop, artist_table_drop, time_table_drop]
copy_table_queries = [staging_events_copy, staging_songs_copy]
insert_table_queries = [songplay_table_insert, user_table_insert, song_table_insert, artist_table_insert, time_table_insert]
