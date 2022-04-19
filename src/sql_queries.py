songplays_drop = "DROP TABLE IF EXISTS songplays"
users_drop = "DROP TABLE IF EXISTS users"
artists_drop = "DROP TABLE IF EXISTS artists"
songs_drop = "DROP TABLE IF EXISTS songs"
time_drop = "DROP TABLE IF EXISTS time"


songplays_create =  \
    """ 
    CREATE TABLE IF NOT EXISTS songplays (
        songplay_id SERIAL, start_time TIMESTAMP, 
        user_id INT, level varchar, 
        song_id varchar, artist_id varchar,
        session_id int, location text, 
        user_agent text,
        PRIMARY KEY (songplay_id),
        FOREIGN KEY (start_time) REFERENCES time(start_time),
        FOREIGN KEY (song_id) REFERENCES songs(song_id),
        FOREIGN KEY (user_id) REFERENCES users(user_id),
        FOREIGN KEY (artist_id) REFERENCES artists(artist_id)
    );
    """

users_create = \
    """
     CREATE TABLE IF NOT EXISTS users(
         user_id int, first_name varchar, 
         last_name varchar, gender varchar, 
         level varchar, 
         PRIMARY KEY(user_id)
     );
    """

songs_create = \
    """
     CREATE TABLE IF NOT EXISTS songs(
         song_id varchar, title text, 
         artist_id varchar, year int, 
         duration int, 
         PRIMARY KEY (song_id),
         FOREIGN KEY (artist_id) REFERENCES artists(artist_id)
     );
    """

artists_create = \
    """
     CREATE TABLE IF NOT EXISTS artists(
         artist_id text, name varchar, 
         location text, latitude FLOAT, 
         longitude FLOAT,
         PRIMARY KEY(artist_id)
     );
    """

time_create = \
    """
     CREATE TABLE IF NOT EXISTS time(
         start_time TIMESTAMP, hour int, 
         day int, week int, 
         month int, year int, 
         weekday varchar, 
         PRIMARY KEY(start_time)
     );
    """


songplays_insert = \
    """
    INSERT INTO 
    songplays(start_time, user_id, level, song_id, artist_id, session_id, location, user_agent)
    VALUES (%s, %s, %s, %s, %s, %s, %s, %s);
    """

users_insert = \
    """
    INSERT INTO
    users(user_id, first_name, last_name, gender, level)
    VALUES(%s, %s, %s, %s, %s)
    ON CONFLICT (user_id)
    DO UPDATE
    SET level = EXCLUDED.level;
    """

songs_insert = \
    """
    INSERT INTO
    songs(song_id, title, artist_id, year, duration)
    VALUES(%s, %s, %s, %s, %s)
    ON CONFLICT (song_id)
    DO NOTHING
    """

artists_insert = \
    """
    INSERT INTO 
    artists(artist_id, name, location, latitude, longitude)
    VALUES(%s, %s, %s, %s, %s)
    ON CONFLICT (artist_id)
    DO NOTHING
    """

time_insert = \
    """
    INSERT INTO 
    time(start_time, hour, day, week, month, year, weekday)
    VALUES(%s, %s, %s, %s, %s, %s, %s)
    ON CONFLICT (start_time)
    DO NOTHING
    """

select = \
    """
    SELECT songs.song_id, artists.artist_id
    FROM songs 
    JOIN artists ON songs.artist_id = artists.artist_id
    WHERE songs.title=%s AND artists.name=%s AND songs.duration=%s
    """

create_statements = [time_create, artists_create, songs_create, users_create, songplays_create]
drop_statements = [songplays_drop, time_drop, songs_drop, artists_drop, users_drop]