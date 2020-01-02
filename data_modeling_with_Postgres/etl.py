import os
import glob
import psycopg2
import pandas as pd
from sql_queries import *


def process_song_file(cur, filepath):
    
    """ 
        This function read a json song file, creates the songs and artists tables and insert 
        them into their respective Postgres tables. 
  
        Parameters: 
            cur: Cursor pointing to a Postgres table throught psycopg2 connection. 
            filepath (string): Path where the song file is located.
          
    """
    # Read the json song data and transform it into a Pandas dataframe.
    df = pd.read_json(filepath, lines=True)

    # Select the fields of the song table and insert them into its Postgres corresponding table.
    song_data = df[['song_id', 'title', 'artist_id', 'year', 'duration']].values[0]
    cur.execute(song_table_insert, song_data)
    
    # Select the fields of the artist table and insert them into its Postgres corresponding table.
    artist_data = df[['artist_id', 'artist_name', 'artist_location', \
                      'artist_latitude', 'artist_longitude']].values[0]
    cur.execute(artist_table_insert, artist_data)


def process_log_file(cur, filepath):
    """ 
        This function read a json log file, it filters and transforms its data to create time, 
        users and songplays tables and insert this data into different Postgres tables. 
  
        Parameters: 
            cur: Cursor pointing to a Postgres table throught psycopg2 connection. 
            filepath (string): Path where the log file is located.
          
    """
    # Read the json log data and transform it into a Pandas dataframe.
    df = pd.read_json(filepath, lines=True)
    
    # Filter the previous dataframe to those records associated with song plays.
    df = df[df['page'] == "NextSong"]
    
    # Select the 'ts' column ant transform it properly into a datetime.
    t = pd.to_datetime(df['ts'], unit='ms')
    
    # With the previous transformation, we can select different granularity of this datetime for each value.
    # A dataframe with this data is created.
    time_data = pd.concat([t, t.dt.hour, t.dt.day, t.dt.week, t.dt.month, t.dt.year, t.dt.weekday], axis=1)
    column_labels = ['start_time', 'hour', 'day', 'week', 'month', 'year', 'weekday']
    time_df = pd.DataFrame(data=time_data.values, columns=column_labels)

    # Insertion of each row of the previous df into its Postgres time table.
    for i, row in time_df.iterrows():
        cur.execute(time_table_insert, list(row))

    # From the original df dataframe, we select some fields and insert row by row into its Posgres users table.  
    user_df = df[['userId', 'firstName', 'lastName', 'gender', 'level']]
    for i, row in user_df.iterrows():
        cur.execute(user_table_insert, row)

    # For each row in the original df dataframe.
    for index, row in df.iterrows():
        
        # Execute the join query of song and artist. If songid and artistid don't match they are assigned nulls.
        cur.execute(song_select, (row.song, row.artist, row.length))
        results = cur.fetchone()
        
        if results:
            songid, artistid = results
        else:
            songid, artistid = None, None
        
        # Selection of some fields of the row, transforming ts into datetime and insertion into its Postgres table.
        songplay_data = (index, pd.to_datetime(row.ts, unit='ms'), row.userId, row.level, songid, artistid, \
                         row.sessionId, row.location, row.userAgent)
        
        cur.execute(songplay_table_insert, songplay_data)


def process_data(cur, conn, filepath, func):
    """ 
        This function iterates over every files located both on log and song data path. After that, it 
        applies process_log_file or process_song_file depending on the file.
        Parameters: 
            cur: Cursor pointing to a Postgres table throught psycopg2 connection. 
            conn: Connection to the database.
            filepath (string): Path where the log/song file is located.
            func: process_log_file or process_song_file function.        
    """
    # Create a list of all the absolute paths of each file under the filepath.
    all_files = []
    for root, dirs, files in os.walk(filepath):
        files = glob.glob(os.path.join(root,'*.json'))
        for f in files :
            all_files.append(os.path.abspath(f))

    num_files = len(all_files)
    print('{} files found in {}'.format(num_files, filepath))

    # For each datafile, we apply the corresponding func (Might be process_log_file or 
    # process_song_file).
    for i, datafile in enumerate(all_files, 1):
        func(cur, datafile)
        conn.commit()
        print('{}/{} files processed.'.format(i, num_files))


def main():
    """
        Function where connection, cursor are created and process both log and song files.
    """
    # Connection to the sparkifydb database.
    conn = psycopg2.connect("host=127.0.0.1 dbname=sparkifydb user=student password=student")
    cur = conn.cursor()

    # Application of all the previous logic to song and log data.
    process_data(cur, conn, filepath='data/song_data', func=process_song_file)
    process_data(cur, conn, filepath='data/log_data', func=process_log_file)

    conn.close()


if __name__ == "__main__":
    main()