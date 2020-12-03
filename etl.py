import os
import glob
import psycopg2
import pandas as pd
from sql_queries import *


def process_song_file(cur, filepath):
    # open song file
    df = pd.read_json(filepath, typ='series')

    # insert song record
    song_data = song_data = df[['song_id', 'title', 'artist_id', 'year', 'duration']].values.tolist()
    cur.execute(song_table_insert, song_data)
    
    # insert artist recordo
    artist_data = artist_data = df[['artist_id', 'artist_name', 'artist_location', 'artist_latitude', 'artist_longitude']].values.tolist()
    cur.execute(artist_table_insert, artist_data)

def bulk_insert(cur, df, table, primary_key, tmp_csv='./tmp.csv'):
    # create temporary csv to use bulk insert to database
    df.to_csv(tmp_csv, header=False, index=False)
    f = open(tmp_csv, 'r')

    # create temp table in order to not violate unique constraint
    cur.execute('CREATE TEMP TABLE {}_tmp (LIKE {}) ON COMMIT DROP;'.format(table, table))
    cur.copy_from(f, '{}_tmp'.format(table), sep=',')
    cur.execute("INSERT INTO {} SELECT DISTINCT ON ({}) * FROM {}_tmp ON CONFLICT DO NOTHING;".format(table, primary_key, table))

    os.remove(tmp_csv)


def process_log_file(cur, filepath):
    # open log file
    df = pd.read_json(filepath, lines=True)

    # filter by NextSong action
    df = df[df['page']=='NextSong']
    
    df['ts_raw'] = df['ts']
    df['ts'] = pd.to_datetime(df['ts'], unit='ms')

    # convert timestamp column to datetime
    t = df['ts']
    
    # insert time data records
    time_data = [t, t.dt.hour, t.dt.day, t.dt.isocalendar().week, t.dt.month, t.dt.year, t.dt.weekday]
    column_labels = ('timestamp', 'hour', 'day', 'week of year', 'month', 'year', 'weekday')
    time_df = pd.DataFrame.from_dict({'timestamp': t, 'hour': t.dt.hour, 'day': t.dt.day, 'week of year': t.dt.isocalendar().week, 
                                      'month': t.dt.month, 'year': t.dt.year, 'weekday': t.dt.weekday})

    bulk_insert(cur=cur, df=time_df, table='time', primary_key='start_time', tmp_csv='./tmp.csv')

    # load user table
    user_df = df[['userId', 'firstName', 'lastName', 'gender', 'level']]

    # insert user records
    bulk_insert(cur=cur, df=user_df, table='users', primary_key='user_id', tmp_csv='./tmp.csv')

    ###

    # create temporary csv to use bulk insert to database

    df = df[['ts', 'userId', 'level', 'sessionId', 'location', 'userAgent', 'song', 'artist', 'length']]
    df.to_csv('tmp.csv', header=False, index=False, sep='\t')
    f = open('tmp.csv', 'r')

    # create temp table in order to not violate unique constraint
    table='songplays'
    cur.execute('CREATE TEMP TABLE tmp (ts TIMESTAMP, userId VARCHAR, level VARCHAR, sessionId VARCHAR, location VARCHAR, userAgent VARCHAR, song VARCHAR, artist VARCHAR, length NUMERIC) ON COMMIT DROP;')
    cur.copy_from(f, 'tmp', sep='\t')
    cur.execute("""
        INSERT INTO songplays (start_time, user_id, level, song_id, artist_id, session_id, location, user_agent) (SELECT tmp.ts, tmp.userId, tmp.level, songs.song_id, artists.artist_id, tmp.sessionId, tmp.location, tmp.userAgent FROM tmp 
        LEFT JOIN songs ON tmp.song = songs.title AND tmp.length = songs.duration
        LEFT JOIN artists ON tmp.artist = artists.name)
        ON CONFLICT DO NOTHING;""")

    os.remove('tmp.csv')

def process_data(cur, conn, filepath, func):
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
    conn = psycopg2.connect("host=127.0.0.1 dbname=sparkifydb user=student password=student")
    cur = conn.cursor()

    process_data(cur, conn, filepath='data/song_data', func=process_song_file)
    process_data(cur, conn, filepath='data/log_data', func=process_log_file)

    conn.close()


if __name__ == "__main__":
    main()