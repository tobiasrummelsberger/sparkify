# DROP TABLES

songplay_table_drop = "DROP TABLE IF EXISTS songplays"
user_table_drop = "DROP TABLE IF EXISTS users"
song_table_drop = "DROP TABLE IF EXISTS song"
artist_table_drop = "DROP TABLE IF EXISTS artists"
time_table_drop = "DROP TABLE IF EXISTS time"

# CREATE TABLES

songplay_table_create = ("""
CREATE TABLE songplays (
    songplay_id SERIAL PRIMARY KEY, 
    start_time TIMESTAMP, 
    user_id VARCHAR, 
    level VARCHAR, 
    song_id VARCHAR, 
    artist_id VARCHAR, 
    session_id VARCHAR, 
    location VARCHAR, 
    user_agent VARCHAR
)
""")

user_table_create = ("""
CREATE TABLE users (
    user_id VARCHAR PRIMARY KEY, 
    first_name VARCHAR, 
    last_name VARCHAR, 
    gender VARCHAR, 
    level VARCHAR
)
""")

song_table_create = ("""
CREATE TABLE songs (
    song_id VARCHAR PRIMARY KEY, 
    title VARCHAR, 
    artist_id VARCHAR, 
    year INT, 
    duration NUMERIC
)
""")

artist_table_create = ("""
CREATE TABLE artists (
    artist_id VARCHAR PRIMARY KEY, 
    name VARCHAR, 
    location VARCHAR, 
    latitude NUMERIC, 
    longitude NUMERIC
)
""")

time_table_create = ("""
CREATE TABLE time (
    start_time TIMESTAMP PRIMARY KEY, 
    hour INT, 
    day INT, 
    week INT, 
    month INT, 
    year INT, 
    weekday INT
)
""")

# INSERT RECORDS

songplay_table_insert = ("""
INSERT INTO songplays (start_time, user_id, level, song_id, artist_id, session_id, location, user_agent)
VALUES (%s, %s, %s, %s, %s, %s, %s, %s)
ON CONFLICT DO NOTHING;
""")

create_tmp_songplay_table = ("""
CREATE TEMP TABLE tmp_songplay (ts TIMESTAMP, userId VARCHAR, level VARCHAR, sessionId VARCHAR, location VARCHAR, 
userAgent VARCHAR, song 
VARCHAR, artist VARCHAR, length NUMERIC) ON COMMIT DROP;
""")

songplay_table_bulk_insert = ("""
INSERT INTO songplays (start_time, user_id, level, song_id, artist_id, session_id, location, user_agent) 
(SELECT tmp_songplay.ts, tmp_songplay.userId, tmp_songplay.level, songs.song_id, artists.artist_id, tmp_songplay.sessionId, tmp_songplay.location, tmp_songplay.userAgent FROM tmp_songplay 
LEFT JOIN songs ON tmp_songplay.song = songs.title AND tmp_songplay.length = songs.duration
LEFT JOIN artists ON tmp_songplay.artist = artists.name)
ON CONFLICT DO NOTHING;
DROP TABLE IF EXISTS tmp_songplay;
""")

user_table_insert = ("""
INSERT INTO users (user_id, first_name, last_name, gender, level) VALUES (%s, %s, %s, %s, %s)
ON CONFLICT DO NOTHING;
""")

create_tmp_users_table = ("""
CREATE TEMP TABLE tmp_users (LIKE users) ON COMMIT DROP;
""")

users_table_bulk_insert = ("""
INSERT INTO users SELECT DISTINCT ON (user_id) * FROM tmp_users ON CONFLICT DO NOTHING;
DROP TABLE IF EXISTS tmp_users;
""")

song_table_insert = ("""
INSERT INTO songs (song_id, title, artist_id, year, duration) VALUES (%s, %s, %s, %s, %s)
ON CONFLICT DO NOTHING;
""")

artist_table_insert = ("""
INSERT INTO artists (artist_id, name, location, latitude, longitude) VALUES (%s, %s, %s, %s, %s)
ON CONFLICT DO NOTHING;
""")

time_table_insert = ("""
INSERT INTO time (start_time, hour, day, week, month, year, weekday) VALUES (%s, %s, %s, %s, %s, %s, %s)
ON CONFLICT DO NOTHING;
""")

create_tmp_time_table = ("""
CREATE TEMP TABLE tmp_time (LIKE time) ON COMMIT DROP;
""")

time_table_bulk_insert = ("""
INSERT INTO time SELECT DISTINCT ON (start_time) * FROM tmp_time ON CONFLICT DO NOTHING;
DROP TABLE IF EXISTS tmp_time;
""")


# FIND SONGS

song_select = ("""
SELECT songs.song_id, songs.artist_id 
FROM songs 
LEFT JOIN artists ON songs.artist_id = artists.artist_id 
WHERE songs.title = %s AND artists.name = %s AND songs.duration = %s ;
""")

# QUERY LISTS

create_table_queries = [songplay_table_create, user_table_create, song_table_create, artist_table_create, time_table_create]
drop_table_queries = [songplay_table_drop, user_table_drop, song_table_drop, artist_table_drop, time_table_drop]