import os
import glob
import psycopg2
import pandas as pd
from sql_queries import *
from concurrent.futures import ThreadPoolExecutor
import time

conn = psycopg2.connect(host="localhost", database="sparkifydb", user="root", password="root")
cur = conn.cursor()


#
def get_files(filepath):
    all_files = []
    for root, dirs, files in os.walk(filepath):
        files = glob.glob(os.path.join(root, '*.json'))
        for f in files:
            all_files.append(os.path.abspath(f))
    return all_files


def process_songdata(file, ):
    df = pd.read_json(file, lines=True)

    # insert artist data
    artist_data = df[["artist_id", "artist_name", "artist_location", "artist_latitude", "artist_longitude"]].values[
        0].tolist()

    try:
        cur.execute(artists_insert, tuple(artist_data))
        conn.commit()
        print("inserted data into artist table")
    except psycopg2.Error as e:
        print("error inserting to artist table")
        print(e)

    # Insert song data
    song_data = df[["song_id", "title", "artist_id", "year", "duration"]].values[0].tolist()

    try:
        cur.execute(songs_insert, song_data)
        conn.commit()
        print("inserted data into song table")
    except psycopg2.Error as e:
        print("error inserting to song tables")
        print(e)


def process_logfile(file):
    # read files
    df2 = pd.read_json(file, lines=True)

    # filter files for nextsong
    df2 = df2[df2["page"] == "NextSong"]

    #  convert to date time
    t = pd.to_datetime(df2["ts"])
    df2["ts"] = t

    time_data = [t, t.dt.hour, t.dt.day, t.dt.isocalendar().week, t.dt.month, t.dt.year, t.dt.weekday]
    column_labels = ["start_date", "hour", "day", "week", "month", "year", "weekday"]

    # create time dataframe
    time_df = pd.DataFrame(dict(zip(column_labels, time_data)))
    time_rows = time_df.values.tolist()

    # insert into time table
    for row in time_rows:

        try:
            cur.execute(time_insert, row)
            conn.commit()
            print("inserted data into time table")
        except psycopg2.Error as e:
            print("error inserting to time tables")
            print(e)

    # create dataframe for users table
    user_df = df2[["userId", "firstName", "lastName", "gender", "level"]].values.tolist()

    # insert into users table
    for row in user_df:

        try:
            cur.execute(users_insert, row)
            conn.commit()
            print("inserted data into user table")
        except psycopg2.Error as e:
            print("error inserting to user tables")
            print(e)

    # insert into songplay table
    for index, row in df2.iterrows():

        cur.execute(select, [row.song, row.artist, row.length])
        results = cur.fetchone()
        if results:
            songid, artistid = results
        else:
            songid, artistid = None, None

        songplay_data = (row.ts, row.userId, row.level, songid, artistid, row.sessionId, row.location, row.userAgent)

        try:
            cur.execute(songplays_insert, songplay_data)
            conn.commit()
            print("inserted data into songplay table")
        except psycopg2.Error as e:
            print("error inserting to songplay tables")
            print(e)


def main():
    song_files = get_files("../data/song_data")
    log_files = get_files("../data/log_data")

    # insert all songdata
    with ThreadPoolExecutor(max_workers=5) as executor:
        executor.map(process_songdata, song_files)

    # insert all logfiles

    with ThreadPoolExecutor(max_workers=5) as executor:
        executor.map(process_logfile, log_files)


if __name__ == "__main__":
    main()
