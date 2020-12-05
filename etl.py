import os
import glob
import psycopg2
import pandas as pd
from sql_queries import *


def process_song_file(cur, filepath):
    """
    Processes the individual song files in json format.

    - Read and process song data.

    - Read and process artists data.
    """

    # open song file
    df = pd.read_json(filepath, typ="series")

    # insert song record
    song_data = song_data = df[
        ["song_id", "title", "artist_id", "year", "duration"]
    ].values.tolist()
    cur.execute(song_table_insert, song_data)

    # insert artist recordo
    artist_data = artist_data = df[
        [
            "artist_id",
            "artist_name",
            "artist_location",
            "artist_latitude",
            "artist_longitude",
        ]
    ].values.tolist()
    cur.execute(artist_table_insert, artist_data)


def bulk_insert(cur, df, create_tmp_table, tmp_table, bulk_insert):
    """
    Bulk inserts dataframe to destination table.

    - Creates temporary csv

    - Opens csv and copys into the database table

    - Delets temporary csv
    """

    tmp_csv = "./tmp.csv"
    # create temporary csv to use bulk insert to database
    df.to_csv(tmp_csv, header=False, index=False, sep="\t")
    f = open(tmp_csv, "r")

    # create temp table in order to not violate unique constraint
    cur.execute(create_tmp_table)
    cur.copy_from(f, tmp_table, sep="\t")
    cur.execute(bulk_insert)

    # Remove temporary csv
    os.remove(tmp_csv)


def process_log_file(cur, filepath):
    """
    Processes log file and bulk inserts into users, time and songplays.

    - Reads json log file

    - Transforms and inserts time data in batches

    - Inserts user data in batches

    - Extracts and inserts songplay data in batches

    """

    # open log file
    df = pd.read_json(filepath, lines=True)

    # filter by NextSong action
    df = df[df["page"] == "NextSong"]

    df["ts_raw"] = df["ts"]
    df["ts"] = pd.to_datetime(df["ts"], unit="ms")

    # convert timestamp column to datetime
    t = df["ts"]

    # insert time data records
    time_data = [
        t,
        t.dt.hour,
        t.dt.day,
        t.dt.isocalendar().week,
        t.dt.month,
        t.dt.year,
        t.dt.weekday,
    ]
    column_labels = (
        "timestamp",
        "hour",
        "day",
        "week of year",
        "month",
        "year",
        "weekday",
    )
    time_df = pd.DataFrame.from_dict(dict(zip(column_labels, time_data)))
    bulk_insert(
        cur=cur,
        df=time_df,
        create_tmp_table=create_tmp_time_table,
        tmp_table="tmp_time",
        bulk_insert=time_table_bulk_insert,
    )

    # load user table
    user_df = df[["userId", "firstName", "lastName", "gender", "level"]]

    # insert user records
    bulk_insert(
        cur=cur,
        df=user_df,
        create_tmp_table=create_tmp_users_table,
        tmp_table="tmp_users",
        bulk_insert=users_table_bulk_insert,
    )

    # create temporary csv to use bulk insert to database

    songplay_df = df[
        [
            "ts",
            "userId",
            "level",
            "sessionId",
            "location",
            "userAgent",
            "song",
            "artist",
            "length",
        ]
    ]

    bulk_insert(
        cur=cur,
        df=songplay_df,
        create_tmp_table=create_tmp_songplays_table,
        tmp_table="tmp_songplays",
        bulk_insert=songplays_table_bulk_insert,
    )


def process_data(cur, conn, filepath, func):
    """
    Processes the song and log data and inserts to sparkifydb.
    """

    # get all files matching extension from directory
    all_files = []
    for root, dirs, files in os.walk(filepath):
        files = glob.glob(os.path.join(root, "*.json"))
        for f in files:
            all_files.append(os.path.abspath(f))

    # get total number of files found
    num_files = len(all_files)
    print("{} files found in {}".format(num_files, filepath))

    # iterate over files and process
    for i, datafile in enumerate(all_files, 1):
        func(cur, datafile)
        conn.commit()
        print("{}/{} files processed.".format(i, num_files))


def quality_check(cur, conn):
    """
    Runs some basic quality checks on data consistency.
    """

    # check for year that is null
    cur.execute(
        "SELECT COUNT(*) FROM songs WHERE year IS NULL OR year < 1900;"
        )
    print(
        "{} distinct songs where year is invalid. ".format(cur.fetchone()[0])
        )

    # check for empty artist
    cur.execute(
        "SELECT COUNT(*) FROM artists WHERE name IS NULL;"
        )
    print(
        "{} distinct artists where name is invalid.".format(cur.fetchone()[0])
    )

    # check for songplay that has incomplete information
    cur.execute(
        "SELECT COUNT(*) \
        FROM songplays \
        WHERE artist_id IS NULL or songplay_id IS NULL"
    )
    print(
        "{} distinct songplays with incomplete information. "
        .format(cur.fetchone()[0])
    )


def main():
    conn = psycopg2.connect(
        "host=127.0.0.1 dbname=sparkifydb user=student password=student"
    )
    cur = conn.cursor()

    process_data(cur, conn, filepath="data/song_data", func=process_song_file)
    process_data(cur, conn, filepath="data/log_data", func=process_log_file)

    quality_check(cur, conn)

    conn.close()


if __name__ == "__main__":
    main()
