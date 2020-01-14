import configparser
import boto3


# CONFIG
config = configparser.ConfigParser()
config.read('dwh.cfg')

LOG_JSON_PATH      = config.get("S3","LOG_JSONPATH")
LOG_DATA           = config.get("S3","LOG_DATA")
SONG_DATA          = config.get("S3","SONG_data")
DWH_IAM_ROLE_NAME  = config.get("DWH","DWH_IAM_ROLE_NAME")
KEY                = config.get('AWS','KEY')
SECRET             = config.get('AWS','SECRET')

iam = boto3.client('iam', aws_access_key_id=KEY, aws_secret_access_key=SECRET)
ROLE_ARN = iam.get_role(RoleName=DWH_IAM_ROLE_NAME)['Role']['Arn']


# DROP TABLES

staging_events_table_drop = "DROP TABLE IF EXISTS staging_events;"
staging_songs_table_drop = "DROP TABLE IF EXISTS staging_songs;"
songplay_table_drop = "DROP TABLE IF EXISTS songplay;"
user_table_drop = "DROP TABLE IF EXISTS users;"
song_table_drop = "DROP TABLE IF EXISTS songs;"
artist_table_drop = "DROP TABLE IF EXISTS artists;"
time_table_drop = "DROP TABLE IF EXISTS time;"

# CREATE TABLES

staging_events_table_create= ("""
CREATE TABLE staging_events(
    artist        VARCHAR,
    auth          VARCHAR,
    firstName     VARCHAR,
    gender        VARCHAR,
    iteminSession INTEGER,
    lastName      VARCHAR,
    length        FLOAT      SORTKEY,
    level         VARCHAR,
    location      VARCHAR,
    method        VARCHAR,
    page          VARCHAR,
    registration  FLOAT,
    sessionId     INTEGER,
    song          VARCHAR,
    status        INTEGER,
    ts            TIMESTAMP,
    userAgent     VARCHAR,
    userId        INTEGER
);
""")

staging_songs_table_create = ("""
CREATE TABLE staging_songs(
    num_songs        INTEGER, 
    artist_id        VARCHAR,
    artist_latitude  FLOAT,
    artist_longitude FLOAT, 
    artist_location  VARCHAR, 
    artist_name      VARCHAR,
    song_id          VARCHAR,
    title            VARCHAR,
    duration         FLOAT     SORTKEY, 
    year             INT
);
""")

songplay_table_create = ("""
CREATE TABLE songplay(
    songplay_id  INTEGER   IDENTITY(0,1) PRIMARY KEY, 
    start_time   DATETIME  NOT NULL DISTKEY SORTKEY, 
    user_id      VARCHAR   NOT NULL, 
    level        VARCHAR   NOT NULL,  
    song_id      VARCHAR   NOT NULL, 
    artist_id    VARCHAR   NOT NULL, 
    session_id   VARCHAR   NOT NULL, 
    location     VARCHAR, 
    user_agent   VARCHAR
);
""")

user_table_create = ("""
CREATE TABLE users(
    user_id       VARCHAR NOT NULL SORTKEY PRIMARY KEY,
    first_name    VARCHAR NOT NULL, 
    last_name     VARCHAR NOT NULL, 
    gender        VARCHAR, 
    level         VARCHAR
);
""")

song_table_create = ("""
CREATE TABLE songs(
    song_id     VARCHAR  NOT NULL SORTKEY PRIMARY KEY, 
    title       VARCHAR  NOT NULL, 
    artist_id   VARCHAR  NOT NULL, 
    year        INTEGER  NOT NULL,
    duration    FLOAT
);
""")

artist_table_create = ("""
CREATE TABLE artists(
    artist_id  VARCHAR  NOT NULL SORTKEY PRIMARY KEY, 
    name       VARCHAR  NOT NULL, 
    location   VARCHAR, 
    latitude  FLOAT,
    longitude  FLOAT
)
diststyle all;
""")

time_table_create = ("""
CREATE TABLE time(
    start_time TIMESTAMP   NOT NULL DISTKEY SORTKEY PRIMARY KEY, 
    hour       INTEGER     NOT NULL, 
    day        SMALLINT    NOT NULL, 
    week       SMALLINT    NOT NULL, 
    month      SMALLINT    NOT NULL, 
    year       INTEGER     NOT NULL, 
    weekday    VARCHAR     NOT NULL
);
""")

# STAGING TABLES

staging_events_copy = (f"""
    copy staging_events
    from {LOG_DATA}
    CREDENTIALS 'aws_iam_role={ROLE_ARN}'
    REGION 'us-west-2'
    FORMAT AS JSON {LOG_JSON_PATH}
    TIMEFORMAT AS 'epochmillisecs';
""")

staging_songs_copy = (f"""
    copy staging_songs
    from {SONG_DATA}
    CREDENTIALS 'aws_iam_role={ROLE_ARN}'
    REGION 'us-west-2'
    FORMAT AS JSON 'auto';
""")

# FINAL TABLES

songplay_table_insert = ("""
INSERT INTO songplay(start_time, user_id, level, song_id, artist_id, session_id, location, user_agent)
SELECT DISTINCT(ev.ts) as start_time,
       ev.userId as user_id,
       ev.level as level,
       so.song_id as song_id,
       so.artist_id as artist_id,
       ev.sessionId as session_id,
       so.artist_location as location,
       ev.userAgent as user_agent
FROM staging_events ev
JOIN staging_songs so ON (ev.song = so.title AND ev.artist = so.artist_name)
WHERE ev.page = 'NextSong';
""")

user_table_insert = ("""
INSERT INTO users(user_id, first_name, last_name, gender, level)
SELECT DISTINCT(userId) as user_id,
       firstName as first_name,
       lastName as last_name,
       gender as gender,
       level as level
FROM staging_events
WHERE user_id IS NOT NULL
AND page = 'NextSong';
""")

song_table_insert = ("""
INSERT INTO songs(song_id, title, artist_id, year, duration)
SELECT DISTINCT(song_id) as song_id,
       title as title,
       artist_id as artist_id,
       year as year,
       duration as duration
FROM staging_songs
WHERE song_id IS NOT NULL;
""")

artist_table_insert = ("""
INSERT INTO artists(artist_id, name, location, latitude, longitude)
SELECT DISTINCT(artist_id) as artist_id,
       artist_name as name,
       artist_location as location,
       artist_latitude as latitude,
       artist_longitude as longitude
FROM staging_songs
WHERE artist_id IS NOT NULL;
""")

time_table_insert = ("""
INSERT INTO time(start_time, hour, day, week, month, year, weekday)
SELECT DISTINCT(start_time) as start_time,
       EXTRACT(hour FROM start_time) as hour,
       EXTRACT(day FROM start_time) as day,
       EXTRACT(week FROM start_time) as week,
       EXTRACT(month FROM start_time) as month,
       EXTRACT(year FROM start_time) as year,
       EXTRACT(weekday FROM start_time) as weekday
FROM songplay
WHERE start_time IS NOT NULL;
""")

# QUERY LISTS

create_table_queries = [staging_events_table_create, staging_songs_table_create, songplay_table_create, user_table_create, song_table_create, artist_table_create, time_table_create]
drop_table_queries = [staging_events_table_drop, staging_songs_table_drop, songplay_table_drop, user_table_drop, song_table_drop, artist_table_drop, time_table_drop]
copy_table_queries = [staging_events_copy, staging_songs_copy]
insert_table_queries = [songplay_table_insert, user_table_insert, song_table_insert, artist_table_insert, time_table_insert]