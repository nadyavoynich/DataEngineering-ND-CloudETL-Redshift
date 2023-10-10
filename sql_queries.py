import configparser


# CONFIG
config = configparser.ConfigParser()
config.read('dwh.cfg')

# DROP TABLES

staging_events_table_drop = "DROP TABLE IF EXISTS staging_events;"
staging_songs_table_drop = "DROP TABLE IF EXISTS staging_songs;"
songplay_table_drop = "DROP TABLE IF EXISTS songplays;"
user_table_drop = "DROP TABLE IF EXISTS users;"
song_table_drop = "DROP TABLE IF EXISTS songs;"
artist_table_drop = "DROP TABLE IF EXISTS artists;"
time_table_drop = "DROP TABLE IF EXISTS time;"

# CREATE TABLES

staging_events_table_create = ("""
CREATE TABLE IF NOT EXISTS staging_events (
    artist          varchar,
    auth            varchar,
    firstName       varchar,
    gender          varchar,
    iteminSession   smallint,
    lastName        varchar,
    length          float4,
    level           varchar,
    location        varchar,
    method          varchar,
    page            varchar,
    registration    float8,
    sessionId       integer,
    song            varchar,
    status          smallint,
    ts              bigint,
    userAgent       varchar,
    userId          integer
);
""")

staging_songs_table_create = ("""
CREATE TABLE IF NOT EXISTS staging_songs (
    num_songs           integer,
    artist_id           varchar,
    artist_latitude     float4,
    artist_longitude    float4,
    artist_location     varchar,
    artist_name         varchar,
    song_id             varchar,
    title               varchar,
    duration            float4,
    year                smallint
);
""")

songplay_table_create = ("""
CREATE TABLE IF NOT EXISTS songplays (
    songplay_id     bigint IDENTITY(0, 1) PRIMARY KEY,
    start_time      timestamp NOT NULL,          
    user_id         integer,         
    level           varchar,
    song_id         varchar,
    artist_id       varchar,
    session_id      integer,
    location        varchar,
    user_agent      varchar
);
""")

user_table_create = ("""
CREATE TABLE IF NOT EXISTS users (
    user_id         integer PRIMARY KEY,
    first_name      varchar,
    last_name       varchar,
    gender          varchar,
    level           varchar,
);
""")

song_table_create = ("""
CREATE TABLE IF NOT EXISTS songs (
    song_id         varchar PRIMARY KEY,
    title           varchar,
    artist_id       varchar,
    year            smallint,
    duration        float4,
);
""")

artist_table_create = ("""
CREATE TABLE IF NOT EXISTS artists (
    artist_id       varchar PRIMARY KEY,
    name            varchar,
    location        varchar,
    latitude        float4,
    longitude       float4
);
""")

time_table_create = ("""
CREATE TABLE IF NOT EXISTS time (
    start_time      timestamp PRIMARY KEY,
    hour            smallint NOT NULL,            
    day             smallint NOT NULL,
    week            smallint NOT NULL,
    month           smallint NOT NULL,
    year            smallint NOT NULL,
    weekday         smallint NOT NULL
);
""")

# STAGING TABLES

staging_events_copy = ("""
    COPY staging_events
    FROM {}
    IAM_ROLE {}
    JSON {}
    REGION {};
""").format(config['S3']['LOG_DATA'],
            config['IAM_ROLE']['ARN'],
            config['S3']['LOG_JSONPATH'],
            config['CLUSTER']['REGION']
            )

staging_songs_copy = ("""
    COPY staging_songs
    FROM {}
    IAM_ROLE {}
    FORMAT AS JSON 'auto'
    REGION {};
""").format(config['S3']['LOG_DATA'],
            config['IAM_ROLE']['ARN'],
            config['CLUSTER']['REGION']
            )

# FINAL TABLES

songplay_table_insert = ("""
INSERT INTO songplays (start_time, user_id, level, song_id, artist_id, session_id, location, user_agent)
SELECT  
FROM 
JOIN ON ()
""")

user_table_insert = ("""
INSERT INTO users (user_id, first_name, last_name, gender, level)
SELECT  
FROM 
JOIN ON ()
""")

song_table_insert = ("""
INSERT INTO songs (song_id, title, artist_id, year, duration)
SELECT  
FROM 
JOIN ON ()
""")

artist_table_insert = ("""
INSERT INTO artists (artist_id, name, location, latitude, longitude)
SELECT  
FROM 
JOIN ON ()
""")

time_table_insert = ("""
INSERT INTO time (start_time, hour, day, week, month, year, weekday)
SELECT  
FROM
JOIN ON ()
""")

# QUERY LISTS

create_table_queries = [staging_events_table_create, staging_songs_table_create, songplay_table_create, user_table_create, song_table_create, artist_table_create, time_table_create]
drop_table_queries = [staging_events_table_drop, staging_songs_table_drop, songplay_table_drop, user_table_drop, song_table_drop, artist_table_drop, time_table_drop]
copy_table_queries = [staging_events_copy, staging_songs_copy]
insert_table_queries = [songplay_table_insert, user_table_insert, song_table_insert, artist_table_insert, time_table_insert]
