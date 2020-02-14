import configparser


# CONFIG
config = configparser.ConfigParser()
config.read('dwh.cfg')

LOG_DATA = config['S3']['LOG_DATA']
LOG_JSONPATH = config['S3']['LOG_JSONPATH']
SONG_DATA = config['S3']['SONG_DATA']

ARN = config['IAM_ROLE']['ARN']


# DROP TABLES

staging_events_table_drop = "Drop table IF EXISTS staging_events;"
staging_songs_table_drop = "Drop table IF EXISTS staging_songs;"
songplay_table_drop = "Drop table IF EXISTS songplay;"
user_table_drop = "Drop table IF EXISTS users;"
song_table_drop = "Drop table IF EXISTS song"
artist_table_drop = "Drop table IF EXISTS artist"
time_table_drop = "Drop table IF EXISTS time"

# CREATE TABLES

staging_events_table_create= ("""
CREATE TABLE if not exists staging_events(
    event_id INT IDENTITY(0,1),
    artist varchar(255),
    auth varchar(255),
    first_name varchar(255),
    gender varchar(255),
    item_in_session integer,
    last_name varchar(255),
    length DOUBLE PRECISION,
    level varchar(25) NOT NULL,
    location varchar(255),
    method varchar(255),
    page varchar(255),
    registration bigint,
    session_id bigint NOT NULL,
    song varchar(255),
    status integer,
    ts bigint,
    user_agent varchar(255),
    user_id varchar(255) NOT NULL,
    PRIMARY KEY (event_id));
""")

staging_songs_table_create = ("""
CREATE TABLE if not exists staging_songs(
    song_event_id INT IDENTITY(0,1),
    num_songs int,
    artist_id varchar(255), 
    artist_latitude DOUBLE PRECISION,
    artist_longitude DOUBLE PRECISION,
    artist_location varchar(255),
    artist_name varchar(255),
    song_id varchar(255),
    title varchar(255),
    duration DOUBLE PRECISION,
    year integer,
    PRIMARY KEY (song_event_id));
""")

songplay_table_create = ("""
CREATE TABLE if not exists songplays (
    songplay_id INT IDENTITY(0,1),
    start_time bigint,
    user_id varchar(255) NOT NULL,
    level varchar(255) NOT NULL,
    song_id varchar(255) sortkey,
    artist_id varchar(255),
    session_id bigint NOT NULL distkey,
    location varchar(255),
    user_agent varchar(255));
""")

user_table_create = ("""
CREATE TABLE if not exists users (
    user_id varchar(255) sortkey,
    first_name varchar(255),
    last_name varchar(255),
    gender varchar(255),
    level varchar(255)) diststyle all;
""")

song_table_create = ("""
CREATE TABLE if not exists songs (
    song_id varchar(255) sortkey,
    title varchar(255),
    artist_id varchar(255),
    year integer,
    duration DOUBLE PRECISION) diststyle all;
""")

artist_table_create = ("""
CREATE TABLE if not exists artists (
    artist_id varchar(25) sortkey,
    name varchar(255),
    location varchar(255),
    latitude DOUBLE PRECISION,
    longitude DOUBLE PRECISION) diststyle all;
""")

time_table_create = ("""
CREATE TABLE if not exists time (
    start_time bigint sortkey,
    hour integer,
    day integer,
    week integer,
    month integer,
    year integer,
    weekday integer) diststyle all;
""")

# STAGING TABLES

staging_events_copy = ("""COPY staging_events FROM 's3://udacity-dend/log_data'
    credentials 'aws_iam_role={}'
    json 's3://udacity-dend/log_json_path.json'
    STATUPDATE ON region 'us-west-2';""".format(ARN)
).format()


staging_songs_copy = ("""COPY staging_songs FROM 's3://udacity-dend/song_data'
    credentials 'aws_iam_role={}'
    json 'auto' truncatecolumns
    STATUPDATE ON region 'us-west-2';""".format(ARN)
).format()

# FINAL TABLES

songplay_table_insert = ("""INSERT INTO songplays (start_time, user_id, level, song_id, artist_id, session_id, location, user_agent) 
select staging_events.ts,
    staging_events.user_id,
    staging_events.level,
    staging_songs.song_id,
    staging_songs.artist_id,
    staging_events.session_id,
    staging_events.location,
    staging_events.user_agent  
FROM staging_events 
JOIN staging_songs 
    ON (staging_events.artist=staging_songs.artist_name)
    AND (staging_events.length=staging_songs.duration)
    AND (staging_events.song=staging_songs.title)
    WHERE staging_events.page = 'NextSong'

""")

user_table_insert = ("""INSERT INTO users (user_id, first_name, last_name, gender, level) 
select  user_id, first_name, last_name, gender, level FROM staging_events
 
""")

song_table_insert = ("""INSERT INTO songs (song_id, title, artist_id, year, duration)
select song_id, title, artist_id, year, duration FROM staging_songs
 
""")

artist_table_insert = ("""INSERT INTO artists (artist_id, name, location, latitude, longitude) 
select artist_id, artist_name, artist_location, artist_latitude, artist_longitude FROM staging_songs
 
""")

time_table_insert = ("""INSERT INTO time (start_time, hour, day, week, month, year, weekday) 
select ts, DATE_PART(hour, timestamp 'epoch' + ts/1000 * interval '1 second') AS hour, DATE_PART(day, timestamp 'epoch' + ts/1000 * interval '1 second') AS day, DATE_PART(week, timestamp 'epoch' + ts/1000 * interval '1 second') AS week, DATE_PART(month, timestamp 'epoch' + ts/1000 * interval '1 second') AS month,  DATE_PART(year, timestamp 'epoch' + ts/1000 * interval '1 second') AS year, DATE_PART(dayofweek, timestamp 'epoch' + ts/1000 * interval '1 second') AS weekday FROM staging_events

""")

# QUERY LISTS

create_table_queries = [staging_events_table_create, staging_songs_table_create, songplay_table_create, user_table_create, song_table_create, artist_table_create, time_table_create]
drop_table_queries = [staging_events_table_drop, staging_songs_table_drop, songplay_table_drop, user_table_drop, song_table_drop, artist_table_drop, time_table_drop]
copy_table_queries = [staging_events_copy, staging_songs_copy]
insert_table_queries = [songplay_table_insert, user_table_insert, song_table_insert, artist_table_insert, time_table_insert]
