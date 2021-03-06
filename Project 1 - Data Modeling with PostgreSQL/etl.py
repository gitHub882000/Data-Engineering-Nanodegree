import os
import glob
import psycopg2
import pandas as pd
from sql_queries import *


def process_song_file(cur, filepath):
    """
    This function can be called to process the song data contained in ``filepath``.
    The song data are extracted, transformed and loaded to 2 dimension tables
    ``artists`` and ``songs``.
    
    Args:
        cur (psycopg2.connection.cursor): Cursor object for executing SQL queries.
        filepath (str): Path of the song data file.
    """
    # open song file
    df = pd.read_json(filepath, lines=True)
    
    # insert artist record
    artist_data = df[['artist_id', 'artist_name', 'artist_location', 'artist_latitude',
                      'artist_longitude']].values[0].tolist()
    cur.execute(artist_table_insert, artist_data)
    
    # insert song record
    song_data = df[['song_id', 'title', 'artist_id', 'year', 'duration']].values[0].tolist()
    cur.execute(song_table_insert, song_data)


def process_log_file(cur, filepath):
    """
    This function can be called to process the log data contained in ``filepath``.
    The log data are extracted, transformed and loaded to 3 dimension tables
    ``time``, ``users`` and ``songplays``.
    
    Args:
        cur (psycopg2.connection.cursor): Cursor object for executing SQL queries.
        filepath (str): Path of the log data file.
    """
    # open log file
    df = pd.read_json(filepath, lines=True)

    # filter by NextSong action
    df = df[df['page'] == 'NextSong']

    # convert timestamp column to datetime
    t = pd.to_datetime(df['ts'], unit='ms')
    
    # insert time data records
    dt = t.dt
    time_data = (t, dt.hour, dt.day, dt.week, dt.month, dt.year, dt.weekday)
    column_labels = ('start_time', 'hour', 'day', 'week', 'month', 'year', 'weekday')
    time_df = pd.DataFrame(dict(zip(column_labels, time_data)))

    for i, row in time_df.iterrows():
        cur.execute(time_table_insert, list(row))

    # load user table
    user_df = df[['userId', 'firstName', 'lastName', 'gender', 'level']]

    # insert user records
    for i, row in user_df.iterrows():
        cur.execute(user_table_insert, row)

    # insert songplay records
    for index, row in df.iterrows():
        
        # get songid and artistid from song and artist tables
        cur.execute(song_select, (row.song, row.artist, row.length))
        results = cur.fetchone()
        
        if results:
            songid, artistid = results
        else:
            songid, artistid = None, None

        # insert songplay record
        songplay_data = (t[index], row.userId, row.level, songid,
                         artistid, row.sessionId, row.location, row.userAgent)
        cur.execute(songplay_table_insert, songplay_data)


def process_data(cur, conn, filepath, func):
    """
    This function can be called to apply the function ``func(cur, datafile)`` to
    every ``datafile``, where each ``datafile`` resides in the data folder ``filepath``.
    
    Args:
        cur (psycopg2.connection.cursor): Cursor object for executing SQL queries.
        conn (psycopg2.connection): Connection object to commit after each
            ``func(cur, datafile)`` call.
        filepath (str): Path of the log data file.
        func (func): Function that will be called for each data file.
    """
    # get all files matching extension from directory
    all_files = []
    for root, dirs, files in os.walk(filepath):
        files = glob.glob(os.path.join(root,'*.json'))
        for f in files :
            all_files.append(os.path.abspath(f))

    # get total number of files found
    num_files = len(all_files)
    print('{} files found in {}'.format(num_files, filepath))

    # iterate over files and process
    for i, datafile in enumerate(all_files, 1):
        func(cur, datafile)
        conn.commit()
        print('{}/{} files processed.'.format(i, num_files))


def main():
    """
    This function can be called to process and insert both song and log data to our database.
    """
    conn = psycopg2.connect("host=127.0.0.1 dbname=sparkifydb user=student password=student")
    cur = conn.cursor()

    process_data(cur, conn, filepath='data/song_data', func=process_song_file)
    process_data(cur, conn, filepath='data/log_data', func=process_log_file)

    conn.close()


if __name__ == "__main__":
    main()