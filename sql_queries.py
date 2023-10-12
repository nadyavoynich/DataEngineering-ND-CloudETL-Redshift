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
        first_name      varchar,
        gender          varchar,
        item_in_session smallint,
        last_name       varchar,
        length          float4,
        level           varchar,
        location        varchar,
        method          varchar,
        page            varchar,
        registration    float8,
        session_id      integer,
        song            varchar,
        status          smallint,
        ts              bigint,
        user_agent      varchar,
        user_id         integer
    ) diststyle auto;
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
    ) diststyle auto;
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
    ) diststyle auto;
""")

user_table_create = ("""
    CREATE TABLE IF NOT EXISTS users (
        user_id         integer PRIMARY KEY,
        first_name      varchar,
        last_name       varchar,
        gender          varchar,
        level           varchar
    ) diststyle auto;
""")

song_table_create = ("""
    CREATE TABLE IF NOT EXISTS songs (
        song_id         varchar PRIMARY KEY,
        title           varchar,
        artist_id       varchar,
        year            smallint,
        duration        float4
    ) diststyle auto;
""")

artist_table_create = ("""
    CREATE TABLE IF NOT EXISTS artists (
        artist_id       varchar PRIMARY KEY,
        name            varchar,
        location        varchar,
        latitude        float4,
        longitude       float4
        ) diststyle auto;
""")

time_table_create = ("""
    CREATE TABLE IF NOT EXISTS time (
        start_time      bigint      PRIMARY KEY,
        hour            smallint    NOT NULL,            
        day             smallint    NOT NULL,
        week            smallint    NOT NULL,
        month           smallint    NOT NULL,
        year            smallint    NOT NULL,
        weekday         smallint    NOT NULL
    ) diststyle auto;
""")

# STAGING TABLES

staging_events_copy = f"""
    COPY staging_events
    FROM {config['S3']['LOG_DATA']}
    CREDENTIALS 'aws_iam_role={config['IAM_ROLE']['ARN']}'
    REGION {config['CLUSTER']['REGION']}
    JSON {config['S3']['LOG_JSONPATH']}
"""

#

staging_songs_copy = f"""
    COPY staging_songs
    FROM {config['S3']['SONG_DATA']}
    CREDENTIALS 'aws_iam_role={config['IAM_ROLE']['ARN']}'
    REGION {config['CLUSTER']['REGION']}
    JSON 'auto'
"""

# FINAL TABLES

songplay_table_insert = ("""
    INSERT INTO songplays (start_time, user_id, level, song_id, artist_id, session_id, location, user_agent)
    SELECT se.ts,
        se.user_id,
        se.level,
        ss.song_id,
        ss.artist_id,
        se.session_id,
        se.location,
        se.user_agent      
    FROM staging_events se
    JOIN staging_songs ss ON se.song = ss.title AND se.artist = ss.artist_name
    WHERE se.page = 'NextSong';
""")

user_table_insert = ("""
    INSERT INTO users SELECT DISTINCT (user_id)
        user_id,
        first_name,
        last_name,
        gender,
        level
    FROM staging_events;
""")

song_table_insert = ("""
    INSERT INTO songs SELECT DISTINCT (song_id)
        song_id,
        title,
        artist_id,
        year,
        duration
    FROM staging_songs;
""")

artist_table_insert = ("""
    INSERT INTO artists SELECT DISTINCT (artist_id)
        artist_id,
        artist_name,
        artist_location,
        artist_latitude,
        artist_longitude,
    FROM staging_songs;
""")

time_table_insert = ("""
    INSERT INTO time (
        SELECT DISTINCT ts AS start_time,
            EXTRACT (hour FROM timestamp 'epoch' + ts/1000 * interval '1 second') AS hour,
            EXTRACT (day FROM timestamp 'epoch' + ts/1000 * interval '1 second') AS day,
            EXTRACT (week FROM timestamp 'epoch' + ts/1000 * interval '1 second') AS week,
            EXTRACT (month FROM timestamp 'epoch' + ts/1000 * interval '1 second') AS month,
            EXTRACT (year FROM timestamp 'epoch' + ts/1000 * interval '1 second') AS year,
            EXTRACT (weekday FROM timestamp 'epoch' + ts/1000 * interval '1 second') AS weekday
    FROM staging_events
    );
""")

# QUERY LISTS

create_table_queries = [staging_events_table_create, staging_songs_table_create, songplay_table_create, user_table_create, song_table_create, artist_table_create, time_table_create]
drop_table_queries = [staging_events_table_drop, staging_songs_table_drop, songplay_table_drop, user_table_drop, song_table_drop, artist_table_drop, time_table_drop]
copy_table_queries = [staging_events_copy, staging_songs_copy]
insert_table_queries = [songplay_table_insert, user_table_insert, song_table_insert, artist_table_insert, time_table_insert]
