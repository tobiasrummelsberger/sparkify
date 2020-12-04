# Sparkify
Udacity Data Engineering Nanodegree

This repository is part of the Udacity Data Engineering Nanodegree and focuses on data modeling of a relational database.

## Data

The data is taken from the Million Songs dataset and an event generator. The data is stored in the folder `data` in the subdirectories `data/log_data`
and `data/song_data`. The log data is further partitioned by year and month, the log data of each day is stored in one separate json.
The song data is partitioned by the first 3 letters of each songs track ID.

## Files

`sql_queries.py`: This file contains all the queries that are used in this repository for creation of tables and inserting of data.

`create_tables.py`: This script contains all the necessary steps to drop existing tables and create the tables.

`etl.py`: This script processes the song and log data and inserts it into the database. Finally, a data quality check is performed.

## Run

To run the script you need to put the data in the folder `data` in the root directory.

0.) Start the database

1.) Create the tables by running `create_tables.py`

2.) Insert the data by running `etl.py`

## Database Design

The database is designed in a dimensional way. The facts table consists of the songplay data and connects the dimension tables around that.
The dimension tables provide the contaxt surrounding the songplay events. It contains who listened when to what. The dimension tables hold
unique values. Insertion of duplicates which are detected based on the ids are ignored.

This database is designed for analytical purposes and is optimized for reading data.

## ETL Pipeline

For extraction of big amounts of data regularly it is important that the ETL pipeline runs in a short time and reliable. Using `COPY` to insert data into
the tables leads to a speed-up of the pipeline.

## Example queries

### Get all users for a certain song:

```
SELECT 
first_name,
last_name
FROM songplays
LEFT JOIN songs ON songplays.song_id = songs.song_id
LEFT JOIN users ON songplays.user_id = users.user_id
WHERE songs.title = '<title-name>'
```

### Get count of male users for each song the last 2 weeks:

```
SELECT 
songs.title,
COUNT(*)
FROM songplays
LEFT JOIN songs ON songplays.song_id = songs.song_id
LEFT JOIN users ON songplays.user_id = users.user_id
WHERE users.gender = 'm'
```

Further requests from business could be:
- Calculate the gender distribution per artist
- Calculate the distribution of songplays over the hours per day.
- ...