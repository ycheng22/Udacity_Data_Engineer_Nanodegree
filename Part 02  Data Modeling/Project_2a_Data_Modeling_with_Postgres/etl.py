import os
import glob
import psycopg2
import pandas as pd
import numpy as np
from sql_queries import *

def insert_from_dataframe(cur, df, insert_query):
    for i, row in df.iterrows():
        cur.execute(insert_query, list(row))
        
def get_files(filepath):
    all_files = []
    for root, dirs, files in os.walk(filepath):
        files = glob.glob(os.path.join(root,'*.json'))
        for f in files :
            all_files.append(os.path.abspath(f))
    
    return all_files

def process_song_file(cur, filepath):
    
    #read one song file
    df = pd.read_json(filepath, lines=True)
    
    #insert artist record
    artist_data = df[['artist_id', 'artist_name', 'artist_location', 'artist_latitude', 'artist_longitude']]
    artist_data = artist_data.drop_duplicates()
    artist_data = artist_data.replace({np.nan: None})
    insert_from_dataframe(cur, artist_data, artist_table_insert)
    
    #insert song record
    song_data = df[['song_id', 'title', 'artist_id', 'year', 'duration']]
    song_data = song_data.drop_duplicates()
    song_data = song_data.replace({np.nan: None})
    insert_from_dataframe(cur, song_data, song_table_insert)
    
def process_log_file(cur, filepath):
    
    #read one log file
    df = pd.read_json(filepath, lines=True)
    
    #filter by NextSong
    df = df[df['page']=='NextSong']
    
    #time data
    tf = pd.DataFrame({'start_time': pd.to_datetime(df['ts'], unit='ms')})
    tf['hour'] = tf['start_time'].dt.hour
    tf['day'] = tf['start_time'].dt.day
    # tf['week'] = tf['start_time'].dt.week #deprecated
    tf['week'] = tf['start_time'].dt.isocalendar().week
    tf['month'] = tf['start_time'].dt.month
    tf['year'] = tf['start_time'].dt.year
    tf['weekday'] = tf['start_time'].dt.weekday
    tf = tf.drop_duplicates()

    #insert time to time table
    insert_from_dataframe(cur, tf, time_table_insert)
    
    user_df = df[['userId', 'firstName', 'lastName', 'gender', 'level']]
    user_df = user_df.drop_duplicates()
    user_df = user_df.replace({np.nan: None})
    user_df.columns = ['user_id', 'first_name', 'last_name', 'gender', 'level']
    
    #insert user to user table
    insert_from_dataframe(cur, user_df, user_table_insert)
    
    #insert songplay_record
    for index, row in df.iterrows():
        cur.execute(song_select, (row.song, row.artist, row.length))
        results = cur.fetchone()
        
        if results:
            songid, artistid = results
        else:
            songid, artistid = None, None
        songplay_data = (index, pd.to_datetime(row.ts, unit='ms'), int(row.userId), row.level, songid, artistid, row.sessionId, row.location, row.userAgent)
        cur.execute(songplay_table_insert, songplay_data)
        
def process_data(cur, conn, filepath, func):
    
    all_files = get_files(filepath)
    num_files = len(all_files)
    print(f'{num_files} files found in path: {filepath}')
    
    #iterate over files and insert into table
    for i, datafile in enumerate(all_files, 1): #start count index is 1
        func(cur, datafile)
        conn.commit()
        print(f'{i}/{num_files} files processed')
        
def main():
    
    conn = psycopg2.connect("host=127.0.0.1 dbname=sparkifydb user=postgres password=2020")
    cur = conn.cursor()
    process_data(cur, conn, filepath='data/song_data/', func=process_song_file)
    process_data(cur, conn, filepath='data/log_data/', func=process_log_file)
    
    conn.close()
    
if __name__ == '__main__':
    main()
        
        