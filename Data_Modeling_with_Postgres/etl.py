import glob
import os

import pandas as pd
import psycopg2
from sql_queries import *


def process_song_file(cur, filepath):
    """
    song_data & artist_data
    1) Read song_file files
    2) Select song_id/title/artist_id/year/duration column
    3) Remove possible duplicates
    4) Iterate over DF and insert records to PG - song_data file contains only 1 record, so in this particular case I dont have to iterator over 1 record (in production is more likely that fila contains thousand of records), but        this ETL is ready to iterate over whole file.
    """
    # open song file
    df = pd.read_json(filepath, lines=True)
    
    
    song_count = 0
    artist_count = 0

    # insert song record
    song_data = df[['song_id', 'title', 'artist_id', 'year', 'duration']]
    song_data = song_data.drop_duplicates(keep='first')
    for i, row in song_data.iterrows():
        cur.execute(song_table_insert, list(row))
        song_count += 1

    # insert artist record
    artist_data = df[['artist_id', 'artist_name', 'artist_location', 'artist_latitude', 'artist_longitude']]
    artist_data = artist_data.drop_duplicates(keep='first')
    for i, row in artist_data.iterrows():
        cur.execute(artist_table_insert, list(row))
        artist_count += 1
    print(f'Process {filepath} file. song_count = {song_count}, artist_count = {artist_count}')

def process_log_file(cur, filepath):
    """
    time_data
    1) Read log_file files and convert to DF
    2) Select records with page == 'NextSong'
    3) Convert column timestamp to human readable datetime
    4) Divide datetime into column start_time/hour/day/week/month/year/weekday
    5) Zip to dict and put into DF
    6) Iterate over DF and insert records to PG
    """
    # open log file
    df = pd.read_json(filepath, lines=True)
    time_count = 0
    user_count = 0
    songplay = 0 
    

    # filter by NextSong action
    df = df[df['page']=='NextSong']
    
    # convert timestamp column to datetime
    t = pd.to_datetime(df['ts'], unit='ms')

    # insert time data records
    t_hour = t.dt.hour
    t_day  = t.dt.day
    t_week = t.dt.week
    t_month = t.dt.month
    t_year = t.dt.year
    t_weekday = t.dt.weekday
    time_data = (t, t_hour, t_day, t_week, t_month, t_year, t_weekday)
    column_labels = ('start_time', 'hour', 'day', 'week', 'month', 'year', 'weekday')
    time_df = pd.DataFrame(dict(zip(column_labels, time_data)))

    for i, row in time_df.iterrows():
        cur.execute(time_table_insert, list(row))
        time_count += 1

    """
    user
    1) Read DF
    2) Select userId/firstName/lastName/gender/level column
    3) Remove possible duplicates
    4) Iterate over DF and insert records to PG
    """
    # load user table
    user_df = df[['userId', 'firstName', 'lastName', 'gender', 'level']]
    user_df = user_df.drop_duplicates(keep='first')
    for i, row in user_df.iterrows():
        cur.execute(user_table_insert, list(row))
        user_count += 1
        
        
    """
    songplay
    1) iter over df records
    2) Find by passing into select WHERE attributes (row.song, row.artist, row.length) and find corresponding songid and artistid and put them into variables
    3) Put data into songplay_data  
    4) Insert into PG
    """
    # insert songplay records
    for index, row in df.iterrows():
    
        # get songid and artistid from song and artist tables
        cur.execute(song_select, (row.song, row.artist, row.length))
        results = cur.fetchone()

        if results:
            songid, artistid = results
        else:
            songid, artistid = None, None
            
        t_time = pd.to_datetime(row.ts, unit='ms')

        # insert songplay record
        songplay_data = (t_time, row.userId, row.level, songid, artistid, row.sessionId, row.location, row.userAgent)
        cur.execute(songplay_table_insert, songplay_data)
        songplay += 1

    print(f'Process {filepath} file. time_count = {time_count}, user_count = {user_count}, songplay = {songplay}')
    
def process_data(cur, conn, filepath, func):
    # get all files matching extension from directory
    all_files = []
    for root, dirs, files in os.walk(filepath):
        files = glob.glob(os.path.join(root, '*.json'))
        for f in files:
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
    conn = psycopg2.connect("host=127.0.0.1 dbname=sparkifydb user=student password=student")
    cur = conn.cursor()

    process_data(cur, conn, filepath='data/song_data', func=process_song_file)
    process_data(cur, conn, filepath='data/log_data', func=process_log_file)

    conn.close()


if __name__ == "__main__":
    main()
