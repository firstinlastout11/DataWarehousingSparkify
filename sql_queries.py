import configparser


# CONFIG
config = configparser.ConfigParser()
config.read('dwh.cfg')

# DROP TABLES

staging_events_table_drop = "DROP TABLE IF EXISTS staging_events"
staging_songs_table_drop = "DROP TABLE IF EXISTS staging_songs"
songplay_table_drop = "DROP TABLE IF EXISTS songplays"
user_table_drop = "DROP TABLE IF EXISTS users"
song_table_drop = "DROP TABLE IF EXISTS songs"
artist_table_drop = "DROP TABLE IF EXISTS artists"
time_table_drop = "DROP TABLE IF EXISTS time"

# CREATE TABLES

staging_events_table_create= ("""

CREATE TABLE staging_events (event_id BIGINT IDENTITY(0,1),
                                    artist VARCHAR,
                                    auth VARCHAR ,
                                    firstName VARCHAR,
                                    gender CHAR(1),
                                    itemInSession VARCHAR,
                                    lastName VARCHAR(15),
                                    length VARCHAR,
                                    level VARCHAR(4) ,
                                    location VARCHAR,
                                    method VARCHAR,
                                    page VARCHAR,
                                    registration VARCHAR,
                                    sessionId INTEGER,
                                    song VARCHAR,
                                    status INTEGER,
                                    ts BIGINT,
                                    userAgent VARCHAR,
                                    userId INTEGER);

""") 

staging_songs_table_create = ("""

CREATE TABLE staging_songs(num_songs INTEGER,
                            artist_id VARCHAR  sortkey,
                            artist_latitude VARCHAR,
                            artist_longitude VARCHAR,
                            artist_location VARCHAR,
                            artist_name VARCHAR,
                            song_id VARCHAR ,
                            title VARCHAR(500) ,
                            duration DECIMAL,
                            year INTEGER);
""")


songplay_table_create = ("""
CREATE TABLE songplays (songplay_id BIGINT IDENTITY(0,1) sortkey,
                            start_time TIMESTAMP ,
                            user_id INTEGER,
                            level VARCHAR(4),
                            song_id VARCHAR,
                            artist_id VARCHAR,
                            session_id INTEGER,
                            location VARCHAR,
                            user_agent VARCHAR);
""")

user_table_create = ("""
CREATE TABLE users (user_id INTEGER  sortkey,
                    first_name VARCHAR PRIMARY KEY,
                    last_name VARCHAR ,
                    gender CHAR(1),
                    level VARCHAR(4) );
""")

song_table_create = ("""
CREATE TABLE songs (song_id VARCHAR  sortkey PRIMARY KEY,
                    title VARCHAR(500) ,
                    artist_id VARCHAR ,
                    year SMALLINT,
                    duration DECIMAL);
""")

artist_table_create = ("""
CREATE TABLE artists (artist_id VARCHAR sortkey PRIMARY KEY,
                    name VARCHAR,
                    location VARCHAR,
                    latitude VARCHAR,
                    longitude VARCHAR);
""")

time_table_create = ("""
CREATE TABLE time (start_time TIMESTAMP PRIMARY KEY sortkey,
                    hour SMALLINT,
                    day SMALLINT,
                    week SMALLINT,
                    month SMALLINT,
                    year SMALLINT,
                    weekday SMALLINT);

""")

# STAGING TABLES

staging_events_copy = ("""
    copy staging_events from {}
        iam_role {}
        format as  json {};
""").format(config.get("S3","LOG_DATA"), config.get("IAM_ROLE","ARN"),config.get("S3","LOG_JSONPATH"))

staging_songs_copy = ("""
    copy staging_songs from {}
        iam_role {}
        format as json 'auto';
""").format(config.get("S3","SONG_DATA"), config.get("IAM_ROLE","ARN"))

# FINAL TABLES

songplay_table_insert = ("""
INSERT INTO songplays (start_time, user_id, level, song_id, artist_id, session_id, location, user_agent)
    SELECT DISTINCT TIMESTAMP 'epoch' + ts/1000 * INTERVAL '1 second', userId, level, song_id, artist_id, sessionId, location, userAgent
    FROM staging_events se
    JOIN staging_songs ss ON (se.song = ss.title AND se.artist = ss.artist_name)
    WHERE se.page = 'NextSong';
    
""")

user_table_insert = ("""

INSERT INTO users
    SELECT DISTINCT userId, firstName, lastName, gender, level
    FROM staging_events
    WHERE page = 'NextSong';

""")

song_table_insert = ("""
    INSERT INTO songs
        SELECT DISTINCT song_id, title, artist_id, year, duration
        FROM staging_songs;
""")

artist_table_insert = ("""
    INSERT INTO artists
        SELECT DISTINCT artist_id, artist_name, artist_location, artist_latitude, artist_longitude
        FROM staging_songs;
""")

time_table_insert = ("""
    INSERT INTO time
        SELECT DISTINCT TIMESTAMP 'epoch' + ts/1000 * INTERVAL '1 second' AS start_time,
        EXTRACT(hour FROM start_time) as hour,
        EXTRACT(day FROM start_time) as day,
        EXTRACT(week FROM start_time) as week,
        EXTRACT(month FROM start_time) as month,
        EXTRACT(year FROM start_time) as year,
        EXTRACT(dayofweek FROM start_time) as weekday
        FROM staging_events
        WHERE page = 'NextSong';
            

""")

# QUERY LISTS

create_table_queries = [staging_events_table_create, staging_songs_table_create, songplay_table_create, user_table_create, song_table_create, artist_table_create, time_table_create]
drop_table_queries = [staging_events_table_drop, staging_songs_table_drop, songplay_table_drop, user_table_drop, song_table_drop, artist_table_drop, time_table_drop]
copy_table_queries = [staging_events_copy, staging_songs_copy]
insert_table_queries = [songplay_table_insert, user_table_insert, song_table_insert, artist_table_insert, time_table_insert]
